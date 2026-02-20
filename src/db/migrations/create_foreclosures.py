"""
Migration: Create foreclosures hub table + supporting objects in PostgreSQL.

Idempotent — safe to re-run.  All DDL uses IF NOT EXISTS / OR REPLACE.

Tables:
  - foreclosures          (one row per case+auction_date)
  - foreclosures_history  (aged past-auction rows for history endpoints)
  - foreclosure_events    (docket timeline, child of foreclosures)

Functions:
  - normalize_case_number_fn(text) → text   (IMMUTABLE, for JOINs)
  - normalize_foreclosure()                 (trigger fn)

View:
  - property_timeline     (UNION ALL across sales, case events, auctions, encumbrances)
"""

from __future__ import annotations

import argparse

from loguru import logger
from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn

# ---------------------------------------------------------------------------
# DDL statements (executed in order)
# ---------------------------------------------------------------------------

DDL: list[str] = [
    # ------------------------------------------------------------------
    # 1. Standalone normalize function (IMMUTABLE, usable in indexes/JOINs)
    # ------------------------------------------------------------------
    r"""
    CREATE OR REPLACE FUNCTION normalize_case_number_fn(raw TEXT)
    RETURNS TEXT AS $$
    BEGIN
        -- Pipeline format: 29YYYYTTNNNNNN  (14 chars, county prefix 29)
        --   positions:      12 3456 78 9..14
        -- Clerk format:     YY-TT-NNNNNN
        -- Handles 14-char (292025CA007149) and extended (292010CA000171A001HC)
        IF raw ~ '^29\d{4}[A-Z]{2}\d{6}' THEN
            RETURN SUBSTRING(raw FROM 5 FOR 2) || '-' ||
                   SUBSTRING(raw FROM 7 FOR 2) || '-' ||
                   SUBSTRING(raw FROM 9 FOR 6);
        -- Already in clerk format
        ELSIF raw ~ '^\d{2}-[A-Z]{2}-\d{5,6}' THEN
            RETURN raw;
        -- Tax deed format: YYYY-NNN (pass through as-is for now)
        ELSIF raw ~ '^\d{4}-\d+$' THEN
            RETURN raw;
        END IF;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """,

    # ------------------------------------------------------------------
    # 2. Main table
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS foreclosures (
        foreclosure_id          BIGSERIAL PRIMARY KEY,

        -- Identity
        listing_id              TEXT UNIQUE,
        case_number_raw         TEXT NOT NULL,
        case_number_norm        TEXT,
        auction_date            DATE NOT NULL,
        auction_type            TEXT NOT NULL DEFAULT 'foreclosure',
        auction_status          TEXT,

        -- Property
        folio                   TEXT,
        strap                   TEXT,
        property_address        TEXT,
        latitude                DOUBLE PRECISION,
        longitude               DOUBLE PRECISION,

        -- Auction economics
        winning_bid             NUMERIC(14,2),
        final_judgment_amount   NUMERIC(14,2),
        appraised_value         NUMERIC(14,2),
        sold_to                 TEXT,
        buyer_type              TEXT,

        -- Property enrichment (snapshot from hcpa_bulk_parcels)
        owner_name              TEXT,
        land_use                TEXT,
        year_built              INT,
        beds                    NUMERIC(6,2),
        baths                   NUMERIC(6,2),
        heated_area             NUMERIC(14,2),
        market_value            NUMERIC(14,2),
        assessed_value          NUMERIC(14,2),

        -- Clerk / case (from clerk_civil_cases)
        clerk_case_type         TEXT,
        clerk_case_status       TEXT,
        filing_date             DATE,
        judgment_date           DATE,
        is_foreclosure          BOOLEAN,

        -- Judgment extraction (Vision OCR — filled by pipeline)
        judgment_data           JSONB,
        pdf_path                TEXT,

        -- Resale analytics (computed from hcpa_allsales)
        first_valid_resale_date  DATE,
        first_valid_resale_price NUMERIC(14,2),
        hold_days               INT,
        resale_profit           NUMERIC(14,2),
        roi                     NUMERIC(10,4),

        -- Tax (from dor_nal_parcels)
        homestead_exempt        BOOLEAN,
        estimated_annual_tax    NUMERIC(14,2),

        -- Market (from property_market)
        zestimate               NUMERIC(14,2),
        list_price              NUMERIC(14,2),
        listing_status          TEXT,

        -- Risk
        has_ucc_liens           BOOLEAN,
        ucc_active_count        INT,
        encumbrance_count       INT,
        unsatisfied_encumbrance_count INT,

        -- Pipeline tracking
        step_pdf_downloaded     TIMESTAMPTZ,
        step_judgment_extracted TIMESTAMPTZ,
        step_ori_searched       TIMESTAMPTZ,
        step_survival_analyzed  TIMESTAMPTZ,

        -- Lifecycle
        created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
        archived_at             TIMESTAMPTZ,

        -- Dedup: one row per case + auction_date (postponements = separate rows)
        UNIQUE (case_number_raw, auction_date)
    );
    """,

    # ------------------------------------------------------------------
    # 3. History table (aged rows copied from foreclosures)
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS foreclosures_history (
        LIKE foreclosures INCLUDING DEFAULTS INCLUDING CONSTRAINTS
    );
    """,
    """
    ALTER TABLE foreclosures_history
    ADD COLUMN IF NOT EXISTS moved_to_history_at TIMESTAMPTZ NOT NULL DEFAULT now();
    """,

    # ------------------------------------------------------------------
    # 4. Child table (docket timeline)
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS foreclosure_events (
        id                  BIGSERIAL PRIMARY KEY,
        foreclosure_id      BIGINT NOT NULL
                            REFERENCES foreclosures(foreclosure_id) ON DELETE CASCADE,
        event_date          DATE,
        event_code          TEXT,
        event_description   TEXT,
        party_name          TEXT
    );
    """,

    # ------------------------------------------------------------------
    # 5. Normalize trigger function
    # ------------------------------------------------------------------
    r"""
    CREATE OR REPLACE FUNCTION normalize_foreclosure()
    RETURNS TRIGGER AS $$
    BEGIN
        -- Case number normalization
        NEW.case_number_norm := normalize_case_number_fn(NEW.case_number_raw);

        -- Cross-fill folio ↔ strap via hcpa_bulk_parcels
        IF NEW.folio IS NULL AND NEW.strap IS NOT NULL THEN
            SELECT bp.folio INTO NEW.folio
            FROM hcpa_bulk_parcels bp WHERE bp.strap = NEW.strap LIMIT 1;
        END IF;
        IF NEW.strap IS NULL AND NEW.folio IS NOT NULL THEN
            SELECT bp.strap INTO NEW.strap
            FROM hcpa_bulk_parcels bp WHERE bp.folio = NEW.folio LIMIT 1;
        END IF;

        -- Clean strings
        NEW.strap      := NULLIF(TRIM(NEW.strap), '');
        NEW.folio      := NULLIF(TRIM(NEW.folio), '');
        NEW.owner_name := UPPER(TRIM(NEW.owner_name));
        NEW.sold_to    := UPPER(TRIM(NEW.sold_to));

        -- Clean address: strip tabs and trailing "Foreclosure Information"
        IF NEW.property_address IS NOT NULL THEN
            NEW.property_address := REGEXP_REPLACE(
                REPLACE(NEW.property_address, E'\t', ' '),
                '\s*Foreclosure Information\s*$', '', 'i');
            NEW.property_address := TRIM(NEW.property_address);
        END IF;

        NEW.updated_at := now();

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """,

    # Drop + re-create trigger (no IF NOT EXISTS for triggers)
    "DROP TRIGGER IF EXISTS trg_normalize_foreclosure ON foreclosures;",
    """
    CREATE TRIGGER trg_normalize_foreclosure
    BEFORE INSERT OR UPDATE ON foreclosures
    FOR EACH ROW EXECUTE FUNCTION normalize_foreclosure();
    """,

    # ------------------------------------------------------------------
    # 6. Indexes (time-first)
    # ------------------------------------------------------------------
    "CREATE INDEX IF NOT EXISTS idx_fc_auction_date  ON foreclosures(auction_date);",
    "CREATE INDEX IF NOT EXISTS idx_fc_updated       ON foreclosures(updated_at);",
    "CREATE INDEX IF NOT EXISTS idx_fc_active        ON foreclosures(auction_date) WHERE archived_at IS NULL;",
    "CREATE INDEX IF NOT EXISTS idx_fc_strap         ON foreclosures(strap);",
    "CREATE INDEX IF NOT EXISTS idx_fc_folio         ON foreclosures(folio);",
    "CREATE INDEX IF NOT EXISTS idx_fc_case_norm     ON foreclosures(case_number_norm);",
    "CREATE INDEX IF NOT EXISTS idx_fc_case_raw      ON foreclosures(case_number_raw);",
    "CREATE INDEX IF NOT EXISTS idx_fch_auction_date ON foreclosures_history(auction_date);",
    "CREATE INDEX IF NOT EXISTS idx_fch_case_raw     ON foreclosures_history(case_number_raw);",
    "CREATE INDEX IF NOT EXISTS idx_fch_strap        ON foreclosures_history(strap);",
    "CREATE INDEX IF NOT EXISTS idx_fch_folio        ON foreclosures_history(folio);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_fch_case_date_unique ON foreclosures_history(case_number_raw, auction_date);",

    "CREATE INDEX IF NOT EXISTS idx_fe_foreclosure   ON foreclosure_events(foreclosure_id);",
    "CREATE INDEX IF NOT EXISTS idx_fe_date          ON foreclosure_events(event_date);",

    # ------------------------------------------------------------------
    # 7. Chain helpers (PG-only title chain analysis)
    # ------------------------------------------------------------------
    r"""
    CREATE OR REPLACE FUNCTION normalize_party_name(raw TEXT)
    RETURNS TEXT AS $$
        WITH src AS (
            SELECT lower(unaccent(COALESCE(raw, ''))) AS v
        ),
        stripped AS (
            SELECT regexp_replace(
                regexp_replace(v, '[^a-z0-9 ]+', ' ', 'g'),
                '\s+', ' ', 'g'
            ) AS v
            FROM src
        ),
        no_suffix AS (
            SELECT regexp_replace(
                ' ' || trim(v) || ' ',
                '\m(llc|inc|incorporated|corp|corporation|co|company|ltd|limited|lp|llp|pllc|trust|trustee|estate|holdings?)\M',
                ' ',
                'gi'
            ) AS v
            FROM stripped
        ),
        tokens AS (
            SELECT token
            FROM no_suffix, regexp_split_to_table(trim(v), '\s+') AS token
            WHERE token <> ''
              AND length(token) > 1
        )
        SELECT NULLIF(
            trim(COALESCE((SELECT string_agg(token, ' ' ORDER BY token) FROM tokens), '')),
            ''
        );
    $$ LANGUAGE sql STABLE;
    """,

    r"""
    CREATE OR REPLACE FUNCTION entity_match_score(a TEXT, b TEXT)
    RETURNS NUMERIC AS $$
        WITH names AS (
            SELECT normalize_party_name(a) AS na, normalize_party_name(b) AS nb
        )
        SELECT
            CASE
                WHEN na IS NULL OR nb IS NULL THEN NULL
                WHEN na = nb THEN 1.0::NUMERIC
                ELSE round(
                    (
                        greatest(
                        similarity(na, nb),
                        1 - (
                            levenshtein(na, nb)::NUMERIC
                            / greatest(length(na), length(nb), 1)
                        )
                        )
                    )::NUMERIC,
                    4
                )
            END
        FROM names;
    $$ LANGUAGE sql STABLE;
    """,

    r"""
    CREATE OR REPLACE FUNCTION is_same_entity(
        a TEXT,
        b TEXT,
        p_threshold NUMERIC DEFAULT 0.72
    ) RETURNS BOOLEAN AS $$
        WITH scored AS (
            SELECT entity_match_score(a, b) AS score
        )
        SELECT COALESCE(score >= p_threshold, FALSE) FROM scored;
    $$ LANGUAGE sql STABLE;
    """,

    r"""
    CREATE OR REPLACE FUNCTION first_valid_resale(
        p_folio TEXT,
        p_auction_date DATE,
        p_winning_bid NUMERIC,
        p_appraised_value NUMERIC
    ) RETURNS TABLE (
        sale_date DATE,
        sale_amount NUMERIC,
        hold_days INT,
        gross_profit NUMERIC,
        roi NUMERIC
    ) AS $$
        SELECT
            s.sale_date,
            s.sale_amount,
            (s.sale_date - p_auction_date)::INT AS hold_days,
            s.sale_amount - COALESCE(p_winning_bid, 0) AS gross_profit,
            CASE
                WHEN p_winning_bid > 0 THEN (s.sale_amount / p_winning_bid)
                ELSE NULL
            END AS roi
        FROM hcpa_allsales s
        WHERE s.folio = p_folio
          AND s.sale_date > p_auction_date
          AND COALESCE(s.sale_amount, 0) > 0
          AND (
              p_winning_bid IS NULL
              OR p_winning_bid <= 0
              OR ABS(s.sale_amount - p_winning_bid) >= 1
          )
          AND s.sale_amount >= 0.10 * COALESCE(
              NULLIF(p_appraised_value, 0),
              NULLIF(p_winning_bid, 0),
              s.sale_amount
          )
        ORDER BY s.sale_date
        LIMIT 1;
    $$ LANGUAGE sql STABLE;
    """,

    r"""
    CREATE OR REPLACE FUNCTION fn_title_chain(
        p_folio TEXT,
        p_as_of_date DATE DEFAULT CURRENT_DATE,
        p_match_threshold NUMERIC DEFAULT 0.72
    ) RETURNS TABLE (
        seq_no INT,
        sale_date DATE,
        sale_type TEXT,
        sale_amount NUMERIC,
        grantor TEXT,
        grantee TEXT,
        or_book TEXT,
        or_page TEXT,
        doc_num TEXT,
        prev_sale_date DATE,
        prev_grantee TEXT,
        days_since_prev INT,
        grantor_norm TEXT,
        grantee_norm TEXT,
        prev_grantee_norm TEXT,
        link_score NUMERIC,
        link_ok BOOLEAN,
        link_reason TEXT
    ) AS $$
        WITH ordered AS (
            SELECT
                row_number() OVER (
                    ORDER BY
                        s.sale_date,
                        COALESCE(s.or_book, ''),
                        COALESCE(s.or_page, ''),
                        COALESCE(s.doc_num, ''),
                        COALESCE(s.sale_amount, 0),
                        COALESCE(s.grantor, ''),
                        COALESCE(s.grantee, '')
                )::INT AS seq_no,
                s.sale_date,
                s.sale_type::TEXT AS sale_type,
                s.sale_amount,
                s.grantor::TEXT AS grantor,
                s.grantee::TEXT AS grantee,
                s.or_book::TEXT AS or_book,
                s.or_page::TEXT AS or_page,
                s.doc_num::TEXT AS doc_num,
                normalize_party_name(s.grantor) AS grantor_norm,
                normalize_party_name(s.grantee) AS grantee_norm
            FROM hcpa_allsales s
            WHERE s.folio = p_folio
              AND s.sale_date IS NOT NULL
              AND s.sale_date <= p_as_of_date
        ),
        annotated AS (
            SELECT
                o.*,
                lag(o.sale_date) OVER (ORDER BY o.seq_no) AS prev_sale_date,
                lag(o.grantee) OVER (ORDER BY o.seq_no) AS prev_grantee,
                lag(o.grantee_norm) OVER (ORDER BY o.seq_no) AS prev_grantee_norm
            FROM ordered o
        ),
        scored AS (
            SELECT
                a.*,
                CASE
                    WHEN a.prev_sale_date IS NULL THEN NULL
                    ELSE (a.sale_date - a.prev_sale_date)::INT
                END AS days_since_prev,
                CASE
                    WHEN a.seq_no = 1 THEN NULL
                    WHEN a.prev_grantee_norm IS NULL OR a.grantor_norm IS NULL THEN NULL
                    ELSE entity_match_score(a.prev_grantee, a.grantor)
                END AS link_score,
                CASE
                    WHEN a.seq_no = 1 THEN TRUE
                    WHEN a.prev_grantee_norm IS NULL OR a.grantor_norm IS NULL THEN FALSE
                    ELSE is_same_entity(a.prev_grantee, a.grantor, p_match_threshold)
                END AS link_ok
            FROM annotated a
        )
        SELECT
            s.seq_no,
            s.sale_date,
            s.sale_type,
            s.sale_amount,
            s.grantor,
            s.grantee,
            s.or_book,
            s.or_page,
            s.doc_num,
            s.prev_sale_date,
            s.prev_grantee,
            s.days_since_prev,
            s.grantor_norm,
            s.grantee_norm,
            s.prev_grantee_norm,
            s.link_score,
            s.link_ok,
            CASE
                WHEN s.seq_no = 1 THEN 'ROOT_BOUNDARY'
                WHEN s.prev_grantee_norm IS NULL OR s.grantor_norm IS NULL THEN 'MISSING_PARTY'
                WHEN s.prev_grantee_norm = s.grantor_norm THEN 'EXACT_MATCH'
                WHEN s.link_ok THEN 'FUZZY_MATCH'
                ELSE 'NAME_MISMATCH'
            END AS link_reason
        FROM scored s
        ORDER BY s.seq_no;
    $$ LANGUAGE sql STABLE;
    """,

    r"""
    CREATE OR REPLACE FUNCTION fn_title_chain_gaps(
        p_folio TEXT,
        p_as_of_date DATE DEFAULT CURRENT_DATE,
        p_match_threshold NUMERIC DEFAULT 0.72,
        p_temporal_gap_days INT DEFAULT 3650
    ) RETURNS TABLE (
        gap_type TEXT,
        seq_prev INT,
        seq_next INT,
        expected_from_party TEXT,
        observed_to_party TEXT,
        missing_from_date DATE,
        missing_to_date DATE,
        recommended_source TEXT,
        detail TEXT
    ) AS $$
        WITH chain AS (
            SELECT *
            FROM fn_title_chain(p_folio, p_as_of_date, p_match_threshold)
        ),
        root_boundary AS (
            SELECT
                'ROOT_BOUNDARY'::TEXT AS gap_type,
                NULL::INT AS seq_prev,
                c.seq_no AS seq_next,
                NULL::TEXT AS expected_from_party,
                c.grantee AS observed_to_party,
                NULL::DATE AS missing_from_date,
                c.sale_date AS missing_to_date,
                'HCPA'::TEXT AS recommended_source,
                'Earliest deed in local dataset; prior ownership may predate loaded records.'::TEXT AS detail
            FROM chain c
            WHERE c.seq_no = 1
        ),
        link_gaps AS (
            SELECT
                CASE
                    WHEN c.days_since_prev IS NOT NULL
                      AND c.days_since_prev > p_temporal_gap_days THEN 'TEMPORAL_GAP'
                    ELSE 'NAME_MISMATCH'
                END::TEXT AS gap_type,
                c.seq_no - 1 AS seq_prev,
                c.seq_no AS seq_next,
                c.prev_grantee AS expected_from_party,
                c.grantor AS observed_to_party,
                c.prev_sale_date AS missing_from_date,
                c.sale_date AS missing_to_date,
                'ORI'::TEXT AS recommended_source,
                CASE
                    WHEN c.days_since_prev IS NOT NULL
                      AND c.days_since_prev > p_temporal_gap_days
                        THEN 'Large time gap with weak party linkage; verify intervening deeds.'
                    ELSE 'Grantor does not match prior grantee; verify deed/order sequence.'
                END::TEXT AS detail
            FROM chain c
            WHERE c.seq_no > 1
              AND c.link_ok = FALSE
        ),
        latest_auction AS (
            SELECT
                x.auction_date,
                x.sold_to::TEXT AS sold_to,
                x.winning_bid,
                x.appraised_value,
                x.case_number_raw
            FROM (
                SELECT
                    f.auction_date,
                    f.sold_to,
                    f.winning_bid,
                    f.appraised_value,
                    f.case_number_raw
                FROM foreclosures f
                WHERE (f.folio = p_folio OR f.strap = p_folio)
                  AND f.auction_date <= p_as_of_date
                UNION ALL
                SELECT
                    fh.auction_date,
                    fh.sold_to,
                    fh.winning_bid,
                    fh.appraised_value,
                    fh.case_number_raw
                FROM foreclosures_history fh
                WHERE (fh.folio = p_folio OR fh.strap = p_folio)
                  AND fh.auction_date <= p_as_of_date
            ) x
            ORDER BY x.auction_date DESC
            LIMIT 1
        ),
        post_auction_sale AS (
            SELECT fr.sale_date, fr.sale_amount
            FROM latest_auction a
            JOIN LATERAL first_valid_resale(
                p_folio,
                a.auction_date,
                a.winning_bid,
                a.appraised_value
            ) fr ON TRUE
        ),
        missing_post_auction AS (
            SELECT
                'MISSING_POST_AUCTION_TRANSFER'::TEXT AS gap_type,
                NULL::INT AS seq_prev,
                NULL::INT AS seq_next,
                la.sold_to AS expected_from_party,
                NULL::TEXT AS observed_to_party,
                la.auction_date AS missing_from_date,
                NULL::DATE AS missing_to_date,
                'HCPA/CLERK'::TEXT AS recommended_source,
                ('No qualifying post-auction market transfer found after case '
                    || COALESCE(la.case_number_raw, '?'))::TEXT AS detail
            FROM latest_auction la
            WHERE NOT EXISTS (SELECT 1 FROM post_auction_sale)
        ),
        no_folio_match AS (
            SELECT
                'NO_FOLIO_MATCH'::TEXT AS gap_type,
                NULL::INT AS seq_prev,
                NULL::INT AS seq_next,
                NULL::TEXT AS expected_from_party,
                NULL::TEXT AS observed_to_party,
                NULL::DATE AS missing_from_date,
                NULL::DATE AS missing_to_date,
                'HCPA'::TEXT AS recommended_source,
                'No deed transfers found in hcpa_allsales for supplied folio.'::TEXT AS detail
            WHERE NOT EXISTS (SELECT 1 FROM chain)
        )
        SELECT *
        FROM (
            SELECT * FROM no_folio_match
            UNION ALL
            SELECT * FROM root_boundary
            UNION ALL
            SELECT * FROM link_gaps
            UNION ALL
            SELECT * FROM missing_post_auction
        ) g
        ORDER BY
            CASE g.gap_type
                WHEN 'NO_FOLIO_MATCH' THEN 0
                WHEN 'ROOT_BOUNDARY' THEN 1
                WHEN 'NAME_MISMATCH' THEN 2
                WHEN 'TEMPORAL_GAP' THEN 3
                WHEN 'MISSING_POST_AUCTION_TRANSFER' THEN 4
                ELSE 9
            END,
            g.seq_next NULLS LAST,
            g.missing_to_date NULLS LAST;
    $$ LANGUAGE sql STABLE;
    """,

    # ------------------------------------------------------------------
    # 8. PG functions for web app
    # ------------------------------------------------------------------

    # 8a. get_dashboard_auctions — single function for dashboard page
    r"""
    CREATE OR REPLACE FUNCTION get_dashboard_auctions(
        p_days_ahead INT DEFAULT 60,
        p_auction_type TEXT DEFAULT NULL,
        p_sort_by TEXT DEFAULT 'auction_date',
        p_sort_order TEXT DEFAULT 'ASC',
        p_limit INT DEFAULT 24,
        p_offset INT DEFAULT 0
    ) RETURNS TABLE (
        foreclosure_id BIGINT,
        case_number_raw TEXT, case_number_norm TEXT,
        auction_date DATE, auction_type TEXT, auction_status TEXT,
        folio TEXT, strap TEXT, property_address TEXT,
        latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
        winning_bid NUMERIC, final_judgment_amount NUMERIC,
        owner_name TEXT, land_use TEXT, year_built INT,
        beds NUMERIC, baths NUMERIC, heated_area NUMERIC,
        market_value NUMERIC, assessed_value NUMERIC,
        homestead_exempt BOOLEAN, estimated_annual_tax NUMERIC,
        zestimate NUMERIC, list_price NUMERIC,
        photo_url TEXT,
        permits_open INT, permits_total INT,
        enc_total INT, enc_survived INT, survived_debt NUMERIC,
        is_toxic_title BOOLEAN, net_equity NUMERIC,
        has_ucc_liens BOOLEAN, ucc_active_count INT,
        clerk_case_status TEXT, filing_date DATE, judgment_date DATE
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            f.foreclosure_id,
            f.case_number_raw::TEXT, f.case_number_norm::TEXT,
            f.auction_date, f.auction_type::TEXT, f.auction_status::TEXT,
            f.folio::TEXT, f.strap::TEXT, f.property_address::TEXT,
            f.latitude, f.longitude,
            f.winning_bid, f.final_judgment_amount,
            f.owner_name::TEXT, f.land_use::TEXT, f.year_built,
            f.beds, f.baths, f.heated_area,
            f.market_value, f.assessed_value,
            f.homestead_exempt, f.estimated_annual_tax,
            f.zestimate, f.list_price,
            -- Photo: first CDN url if available
            (pm.photo_cdn_urls->>0)::TEXT AS photo_url,
            -- Permits
            COALESCE(perm.open_count, 0)::INT,
            COALESCE(perm.total_count, 0)::INT,
            -- Encumbrances
            COALESCE(enc.total, 0)::INT,
            COALESCE(enc.survived, 0)::INT,
            COALESCE(enc.survived_debt, 0)::NUMERIC,
            -- Toxic title: survived_count > 2 OR survived_debt > judgment
            (COALESCE(enc.survived, 0) > 2
             OR COALESCE(enc.survived_debt, 0) > COALESCE(f.final_judgment_amount, 0)
            )::BOOLEAN AS is_toxic_title,
            -- Net equity
            (COALESCE(f.market_value, f.zestimate, f.assessed_value, 0)
             - COALESCE(f.final_judgment_amount, 0)
             - COALESCE(enc.survived_debt, 0)
            )::NUMERIC AS net_equity,
            f.has_ucc_liens, f.ucc_active_count,
            f.clerk_case_status::TEXT, f.filing_date, f.judgment_date
        FROM foreclosures f
        LEFT JOIN property_market pm ON f.strap = pm.strap
        LEFT JOIN LATERAL (
            SELECT COUNT(*) FILTER (WHERE cp.status NOT IN ('COMP','CO','CLSD','EXPIRED','VOID'))::INT AS open_count,
                   COUNT(*)::INT AS total_count
            FROM county_permits cp
            WHERE cp.folio_clean = f.strap
        ) perm ON TRUE
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE oe.survival_status = 'SURVIVED') AS survived,
                   COALESCE(SUM(oe.amount) FILTER (WHERE oe.survival_status = 'SURVIVED'), 0) AS survived_debt
            FROM ori_encumbrances oe
            WHERE oe.strap = f.strap
        ) enc ON TRUE
        WHERE f.archived_at IS NULL
          AND (p_auction_type IS NULL OR f.auction_type = p_auction_type)
        ORDER BY
            CASE WHEN p_sort_by = 'auction_date' AND p_sort_order = 'ASC' THEN f.auction_date END ASC,
            CASE WHEN p_sort_by = 'auction_date' AND p_sort_order = 'DESC' THEN f.auction_date END DESC,
            CASE WHEN p_sort_by = 'net_equity' AND p_sort_order = 'ASC' THEN
                COALESCE(f.market_value, f.zestimate, f.assessed_value, 0) - COALESCE(f.final_judgment_amount, 0) - COALESCE(enc.survived_debt, 0) END ASC,
            CASE WHEN p_sort_by = 'net_equity' AND p_sort_order = 'DESC' THEN
                COALESCE(f.market_value, f.zestimate, f.assessed_value, 0) - COALESCE(f.final_judgment_amount, 0) - COALESCE(enc.survived_debt, 0) END DESC,
            f.foreclosure_id
        LIMIT p_limit OFFSET p_offset;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # 8b. get_dashboard_stats — summary stats for dashboard header
    r"""
    CREATE OR REPLACE FUNCTION get_dashboard_stats(p_days_ahead INT DEFAULT 60)
    RETURNS JSON AS $$
    DECLARE
        result JSON;
    BEGIN
        SELECT json_build_object(
            'total_auctions', COUNT(*),
            'foreclosures', COUNT(*) FILTER (WHERE f.auction_type = 'foreclosure'),
            'tax_deeds', COUNT(*) FILTER (WHERE f.auction_type = 'tax_deed'),
            'this_week', COUNT(*) FILTER (WHERE f.auction_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 7),
            'with_judgment', COUNT(*) FILTER (WHERE f.judgment_data IS NOT NULL),
            'with_coords', COUNT(*) FILTER (WHERE f.latitude IS NOT NULL),
            'toxic_count', COUNT(*) FILTER (WHERE f.unsatisfied_encumbrance_count > 2),
            'total_survived_liens', COALESCE(SUM(f.unsatisfied_encumbrance_count), 0),
            'avg_judgment', ROUND(AVG(f.final_judgment_amount) FILTER (WHERE f.final_judgment_amount > 0)),
            'avg_market_value', ROUND(AVG(f.market_value) FILTER (WHERE f.market_value > 0))
        ) INTO result
        FROM foreclosures f
        WHERE f.archived_at IS NULL;

        RETURN result;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # 8c. get_property_encumbrances — all encumbrances for a property
    r"""
    CREATE OR REPLACE FUNCTION get_property_encumbrances(p_strap TEXT)
    RETURNS TABLE (
        id BIGINT, encumbrance_type TEXT, raw_document_type TEXT,
        party1 TEXT, party2 TEXT, amount NUMERIC,
        recording_date DATE, instrument_number TEXT, book TEXT, page TEXT,
        is_satisfied BOOLEAN, satisfaction_date DATE, satisfaction_instrument TEXT,
        survival_status TEXT, survival_reason TEXT,
        current_holder TEXT, assignment_count INT, mrta_expiration_date DATE,
        case_number TEXT
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT oe.id, oe.encumbrance_type::TEXT, oe.raw_document_type,
               oe.party1, oe.party2, oe.amount,
               oe.recording_date, oe.instrument_number::TEXT, oe.book::TEXT, oe.page::TEXT,
               oe.is_satisfied, oe.satisfaction_date, oe.satisfaction_instrument::TEXT,
               oe.survival_status::TEXT, oe.survival_reason,
               oe.current_holder, oe.assignment_count, oe.mrta_expiration_date,
               oe.case_number::TEXT
        FROM ori_encumbrances oe
        WHERE oe.strap = p_strap
        ORDER BY oe.recording_date DESC NULLS LAST, oe.id;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # 8d-1. strap_to_folio — HCPA strap → 10-digit PG folio
    r"""
    CREATE OR REPLACE FUNCTION strap_to_folio(p_strap TEXT)
    RETURNS TEXT AS $$
        SELECT folio FROM hcpa_bulk_parcels WHERE strap = p_strap LIMIT 1;
    $$ LANGUAGE sql STABLE;
    """,

    # 8d-2. folio_to_strap — 10-digit PG folio → HCPA strap
    r"""
    CREATE OR REPLACE FUNCTION folio_to_strap(p_folio TEXT)
    RETURNS TEXT AS $$
        SELECT strap FROM hcpa_bulk_parcels WHERE folio = p_folio LIMIT 1;
    $$ LANGUAGE sql STABLE;
    """,

    # 8d. get_property_permits — combined county + tampa permits
    # Accepts strap (HCPA format), resolves to 10-digit folio for county_permits join
    r"""
    CREATE OR REPLACE FUNCTION get_property_permits(p_strap TEXT)
    RETURNS TABLE (
        source TEXT, permit_number TEXT, permit_type TEXT,
        description TEXT, status TEXT, issue_date DATE,
        complete_date DATE, permit_value NUMERIC,
        address TEXT, detail_url TEXT
    ) AS $$
    DECLARE
        v_folio TEXT;
    BEGIN
        -- Resolve strap → 10-digit folio for county_permits
        v_folio := strap_to_folio(p_strap);

        RETURN QUERY
        -- County permits (joined by 10-digit folio)
        SELECT 'county'::TEXT, cp.permit_number::TEXT, cp.permit_type::TEXT,
               cp.description::TEXT, cp.status::TEXT,
               cp.issue_date, cp.complete_date, cp.permit_value,
               cp.address::TEXT, cp.aca_link::TEXT
        FROM county_permits cp
        WHERE cp.folio_clean = v_folio

        UNION ALL

        -- Tampa permits (matched by address from hcpa_bulk_parcels)
        SELECT 'tampa'::TEXT, ta.record_number::TEXT, ta.record_type::TEXT,
               ta.short_notes::TEXT, ta.status::TEXT,
               ta.record_date, NULL::DATE, ta.estimated_work_cost,
               ta.address_normalized::TEXT, ta.detail_url::TEXT
        FROM tampa_accela_records ta
        JOIN hcpa_bulk_parcels bp ON bp.strap = p_strap
        WHERE ta.address_normalized IS NOT NULL
          AND bp.property_address IS NOT NULL
          AND UPPER(SPLIT_PART(ta.address_normalized, ',', 1)) = bp.property_address

        ORDER BY issue_date DESC NULLS LAST;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # 8e. compute_net_equity — compute net equity for a property
    r"""
    CREATE OR REPLACE FUNCTION compute_net_equity(p_strap TEXT)
    RETURNS TABLE (
        market_value NUMERIC, final_judgment NUMERIC,
        survived_debt NUMERIC, net_equity NUMERIC,
        is_toxic BOOLEAN, survived_count INT, total_encumbrances INT
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT
            COALESCE(f.market_value, f.zestimate, f.assessed_value, 0) AS market_value,
            COALESCE(f.final_judgment_amount, 0) AS final_judgment,
            COALESCE(enc.survived_debt, 0) AS survived_debt,
            (COALESCE(f.market_value, f.zestimate, f.assessed_value, 0)
             - COALESCE(f.final_judgment_amount, 0)
             - COALESCE(enc.survived_debt, 0)) AS net_equity,
            (COALESCE(enc.survived_count, 0) > 2
             OR COALESCE(enc.survived_debt, 0) > COALESCE(f.final_judgment_amount, 0)) AS is_toxic,
            COALESCE(enc.survived_count, 0)::INT,
            COALESCE(enc.total_count, 0)::INT
        FROM foreclosures f
        LEFT JOIN LATERAL (
            SELECT COUNT(*) FILTER (WHERE oe.survival_status = 'SURVIVED') AS survived_count,
                   COALESCE(SUM(oe.amount) FILTER (WHERE oe.survival_status = 'SURVIVED'), 0) AS survived_debt,
                   COUNT(*) AS total_count
            FROM ori_encumbrances oe
            WHERE oe.strap = f.strap
        ) enc ON TRUE
        WHERE f.strap = p_strap
        ORDER BY f.auction_date DESC
        LIMIT 1;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # 8f. get_ucc_exposure — UCC filing exposure for an owner name
    r"""
    CREATE OR REPLACE FUNCTION get_ucc_exposure(p_owner_name TEXT)
    RETURNS TABLE (
        has_liens BOOLEAN, active_count BIGINT,
        doc_number TEXT, filing_date DATE, filing_type TEXT,
        secured_party TEXT, debtor_name TEXT, similarity_score REAL
    ) AS $$
    BEGIN
        RETURN QUERY
        WITH matches AS (
            SELECT p.doc_number, p.name AS debtor_name,
                   similarity(p.name, UPPER(p_owner_name)) AS sim_score
            FROM sunbiz_flr_parties p
            WHERE p.party_role = 'D'
              AND p.name % UPPER(p_owner_name)
              AND similarity(p.name, UPPER(p_owner_name)) > 0.4
        ),
        filings AS (
            SELECT m.doc_number, m.debtor_name, m.sim_score,
                   fl.filing_date, fl.filing_type,
                   sp.name AS secured_party
            FROM matches m
            JOIN sunbiz_flr_filings fl ON m.doc_number = fl.doc_number
            LEFT JOIN sunbiz_flr_parties sp ON m.doc_number = sp.doc_number AND sp.party_role = 'S'
            WHERE fl.filing_status = 'A'
              AND (fl.expiration_date IS NULL OR fl.expiration_date >= CURRENT_DATE)
        )
        SELECT
            (COUNT(*) > 0)::BOOLEAN AS has_liens,
            COUNT(DISTINCT f.doc_number) AS active_count,
            f.doc_number::TEXT, f.filing_date, f.filing_type::TEXT,
            f.secured_party::TEXT, f.debtor_name::TEXT, f.sim_score
        FROM filings f
        GROUP BY f.doc_number, f.filing_date, f.filing_type, f.secured_party, f.debtor_name, f.sim_score;
    END;
    $$ LANGUAGE plpgsql STABLE;
    """,

    # ------------------------------------------------------------------
    # 9. Property timeline view
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE VIEW property_timeline AS

    -- Sales from HCPA
    SELECT bp.strap,
           s.sale_date        AS event_date,
           'SALE'             AS event_type,
           s.sale_type        AS event_code,
           COALESCE(s.grantor, '') || ' → ' || COALESCE(s.grantee, '') AS description,
           s.sale_amount      AS amount
    FROM hcpa_allsales s
    JOIN hcpa_bulk_parcels bp USING (folio)

    UNION ALL

    -- Clerk docket events
    SELECT f.strap,
           fe.event_date,
           'CASE_EVENT',
           fe.event_code,
           fe.event_description || COALESCE(' [' || fe.party_name || ']', ''),
           NULL
    FROM foreclosure_events fe
    JOIN foreclosures f ON fe.foreclosure_id = f.foreclosure_id

    UNION ALL

    -- Auction events
    SELECT f.strap,
           f.auction_date,
           'AUCTION',
           f.auction_status,
           COALESCE(f.sold_to, 'No sale') || ' — '
               || COALESCE(f.case_number_norm, f.case_number_raw),
           f.winning_bid
    FROM foreclosures f

    UNION ALL

    -- ORI encumbrances
    SELECT oe.strap,
           oe.recording_date,
           'LIEN_' || UPPER(COALESCE(oe.encumbrance_type::TEXT, '')),
           oe.survival_status,
           COALESCE(oe.party1, '') || ' / ' || COALESCE(oe.party2, ''),
           oe.amount
    FROM ori_encumbrances oe;
    """,
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def migrate(dsn: str | None = None) -> None:
    """Run all DDL statements inside a single transaction."""
    engine = get_engine(resolve_pg_dsn(dsn))
    with engine.begin() as conn:
        for i, stmt in enumerate(DDL, 1):
            conn.execute(text(stmt))
            logger.debug(f"DDL {i}/{len(DDL)} OK")

    logger.info("foreclosures migration complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create foreclosures schema in PostgreSQL")
    parser.add_argument("--dsn", help="PostgreSQL DSN (default from env / sunbiz.db)")
    args = parser.parse_args()
    migrate(dsn=args.dsn)


if __name__ == "__main__":
    main()
