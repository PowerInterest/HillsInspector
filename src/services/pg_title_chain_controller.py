"""PG title-chain builder for foreclosure timelines.

Builds ownership/event outputs from PostgreSQL only.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from sunbiz.db import get_engine, resolve_pg_dsn


@dataclass(slots=True)
class ControllerConfig:
    dsn: str | None = None
    foreclosure_id: int | None = None
    case_number: str | None = None
    active_only: bool = False
    limit: int | None = None
    similarity_threshold: float = 0.68


DDL_STATEMENTS: list[str] = [
    # Similarity support (safe if already installed)
    "CREATE EXTENSION IF NOT EXISTS pg_trgm;",
    # Party normalization helper for link scoring
    """
    CREATE OR REPLACE FUNCTION normalize_party_name(raw TEXT)
    RETURNS TEXT AS $$
    DECLARE
        cleaned TEXT;
    BEGIN
        IF raw IS NULL THEN
            RETURN NULL;
        END IF;
        cleaned := UPPER(raw);
        cleaned := regexp_replace(cleaned, '[^A-Z0-9 ]+', ' ', 'g');
        cleaned := regexp_replace(cleaned, '\\s+', ' ', 'g');
        cleaned := btrim(cleaned);
        RETURN NULLIF(cleaned, '');
    END;
    $$ LANGUAGE plpgsql IMMUTABLE;
    """,
    # Unified timeline events tied to a foreclosure property
    """
    CREATE TABLE IF NOT EXISTS foreclosure_title_events (
        id              BIGSERIAL PRIMARY KEY,
        foreclosure_id  BIGINT NOT NULL
                        REFERENCES foreclosures(foreclosure_id) ON DELETE CASCADE,
        case_number_raw TEXT NOT NULL,
        case_number_norm TEXT,
        folio           TEXT,
        strap           TEXT,
        event_date      DATE NOT NULL,
        event_source    TEXT NOT NULL,
        event_subtype   TEXT,
        instrument_number TEXT,
        or_book         TEXT,
        or_page         TEXT,
        grantor         TEXT,
        grantee         TEXT,
        amount          NUMERIC(14,2),
        description     TEXT,
        sale_row_id     BIGINT,
        clerk_event_id  BIGINT,
        event_rank      INT,
        prior_event_id  BIGINT,
        link_status     TEXT,
        link_score      NUMERIC(6,4),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    # Ownership periods derived from SALE events
    """
    CREATE TABLE IF NOT EXISTS foreclosure_title_chain (
        chain_id         BIGSERIAL PRIMARY KEY,
        foreclosure_id   BIGINT NOT NULL
                         REFERENCES foreclosures(foreclosure_id) ON DELETE CASCADE,
        case_number_raw  TEXT NOT NULL,
        case_number_norm TEXT,
        folio            TEXT,
        strap            TEXT,
        sequence_no      INT NOT NULL,
        owner_name       TEXT,
        acquired_date    DATE,
        disposed_date    DATE,
        acquired_event_id BIGINT
                          REFERENCES foreclosure_title_events(id) ON DELETE CASCADE,
        next_event_id    BIGINT
                         REFERENCES foreclosure_title_events(id) ON DELETE SET NULL,
        acquired_sale_type TEXT,
        acquired_amount  NUMERIC(14,2),
        grantor          TEXT,
        grantee          TEXT,
        link_status      TEXT,
        link_score       NUMERIC(6,4),
        is_gap           BOOLEAN NOT NULL DEFAULT false,
        is_terminal      BOOLEAN NOT NULL DEFAULT false,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (foreclosure_id, sequence_no)
    );
    """,
    # Per-foreclosure chain completeness metrics
    """
    CREATE TABLE IF NOT EXISTS foreclosure_title_summary (
        foreclosure_id   BIGINT PRIMARY KEY
                         REFERENCES foreclosures(foreclosure_id) ON DELETE CASCADE,
        case_number_raw  TEXT NOT NULL,
        case_number_norm TEXT,
        folio            TEXT,
        strap            TEXT,
        auction_date     DATE,
        root_date        DATE,
        last_sale_date   DATE,
        root_owner       TEXT,
        pre_foreclosure_owner TEXT,
        sale_events_count INT NOT NULL DEFAULT 0,
        total_events_count INT NOT NULL DEFAULT 0,
        exact_links_count INT NOT NULL DEFAULT 0,
        fuzzy_links_count INT NOT NULL DEFAULT 0,
        missing_party_links_count INT NOT NULL DEFAULT 0,
        gap_count        INT NOT NULL DEFAULT 0,
        years_covered    NUMERIC(8,2),
        chain_status     TEXT NOT NULL,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_fte_foreclosure ON foreclosure_title_events(foreclosure_id);",
    "CREATE INDEX IF NOT EXISTS idx_fte_event_date ON foreclosure_title_events(event_date);",
    "CREATE INDEX IF NOT EXISTS idx_fte_source ON foreclosure_title_events(event_source);",
    "CREATE INDEX IF NOT EXISTS idx_fte_link_status ON foreclosure_title_events(link_status);",
    "CREATE INDEX IF NOT EXISTS idx_ftc_foreclosure ON foreclosure_title_chain(foreclosure_id);",
    "CREATE INDEX IF NOT EXISTS idx_ftc_gap ON foreclosure_title_chain(is_gap);",
    "CREATE INDEX IF NOT EXISTS idx_fts_status ON foreclosure_title_summary(chain_status);",
]


class TitleChainController:
    """Build foreclosure title chain from PG-only sources."""

    def __init__(self, config: ControllerConfig) -> None:
        self._config = config
        resolved = resolve_pg_dsn(config.dsn)
        self._engine = get_engine(resolved)

    def run(self) -> dict[str, Any]:
        t0 = time.monotonic()
        with self._engine.begin() as conn:
            self._ensure_schema(conn)
            scope_count = self._create_scope(conn)
            if scope_count == 0:
                return {
                    "scope_count": 0,
                    "events_inserted": 0,
                    "chain_rows": 0,
                    "summary_rows": 0,
                    "elapsed_seconds": round(time.monotonic() - t0, 2),
                }

            self._reset_outputs(conn)

            inserted_sales = conn.execute(text(self._insert_sales_events_sql())).rowcount
            inserted_case = conn.execute(text(self._insert_case_events_sql())).rowcount
            inserted_judgment = conn.execute(
                text(self._insert_judgment_events_sql())
            ).rowcount
            inserted_auction = conn.execute(text(self._insert_auction_events_sql())).rowcount
            inserted_history_auction = conn.execute(
                text(self._insert_history_auction_events_sql())
            ).rowcount
            inserted_tax = conn.execute(text(self._insert_tax_events_sql())).rowcount
            inserted_county_permits = conn.execute(
                text(self._insert_county_permit_events_sql())
            ).rowcount
            inserted_tampa_permits = conn.execute(
                text(self._insert_tampa_permit_events_sql())
            ).rowcount
            inserted_market = conn.execute(text(self._insert_market_events_sql())).rowcount

            conn.execute(text(self._rank_events_sql()))
            conn.execute(
                text(self._score_sales_links_sql()),
                {"threshold": self._config.similarity_threshold},
            )

            chain_rows = conn.execute(text(self._build_chain_sql())).rowcount
            summary_rows = conn.execute(text(self._build_summary_sql())).rowcount

            stats = conn.execute(text(self._summary_stats_sql())).mappings().one()

        elapsed = round(time.monotonic() - t0, 2)
        result = {
            "scope_count": scope_count,
            "events_inserted": (
                (inserted_sales or 0)
                + (inserted_case or 0)
                + (inserted_judgment or 0)
                + (inserted_auction or 0)
                + (inserted_history_auction or 0)
                + (inserted_tax or 0)
                + (inserted_county_permits or 0)
                + (inserted_tampa_permits or 0)
                + (inserted_market or 0)
            ),
            "sale_events_inserted": inserted_sales or 0,
            "case_events_inserted": inserted_case or 0,
            "judgment_events_inserted": inserted_judgment or 0,
            "auction_events_inserted": inserted_auction or 0,
            "historical_auction_events_inserted": inserted_history_auction or 0,
            "tax_events_inserted": inserted_tax or 0,
            "county_permit_events_inserted": inserted_county_permits or 0,
            "tampa_permit_events_inserted": inserted_tampa_permits or 0,
            "market_events_inserted": inserted_market or 0,
            "chain_rows": chain_rows or 0,
            "summary_rows": summary_rows or 0,
            "elapsed_seconds": elapsed,
        }
        result.update(dict(stats))
        return result

    def _ensure_schema(self, conn: Any) -> None:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))

    def _create_scope(self, conn: Any) -> int:
        where_clauses = ["1=1"]
        params: dict[str, Any] = {}

        if self._config.foreclosure_id is not None:
            where_clauses.append("f.foreclosure_id = :foreclosure_id")
            params["foreclosure_id"] = self._config.foreclosure_id
        if self._config.case_number:
            where_clauses.append("f.case_number_raw = :case_number")
            params["case_number"] = self._config.case_number
        if self._config.active_only:
            where_clauses.append("f.archived_at IS NULL")

        limit_clause = ""
        if self._config.limit and self._config.limit > 0:
            limit_clause = f"LIMIT {int(self._config.limit)}"

        conn.execute(text("DROP TABLE IF EXISTS controller_scope"))
        conn.execute(
            text(f"""
                CREATE TEMP TABLE controller_scope ON COMMIT DROP AS
                SELECT
                    f.foreclosure_id,
                    f.case_number_raw,
                    f.case_number_norm,
                    f.auction_date,
                    f.auction_status,
                    COALESCE(NULLIF(btrim(f.folio), ''), bp.folio) AS folio,
                    COALESCE(NULLIF(btrim(f.strap), ''), bp.strap) AS strap,
                    f.property_address,
                    f.winning_bid,
                    f.final_judgment_amount,
                    f.judgment_date,
                    f.sold_to
                FROM foreclosures f
                LEFT JOIN LATERAL (
                    SELECT bp2.folio, bp2.strap
                    FROM hcpa_bulk_parcels bp2
                    WHERE (f.strap IS NOT NULL AND bp2.strap = f.strap)
                       OR (f.folio IS NOT NULL AND bp2.folio = f.folio)
                    ORDER BY bp2.source_file_id DESC NULLS LAST
                    LIMIT 1
                ) bp ON TRUE
                WHERE {" AND ".join(where_clauses)}
                ORDER BY f.foreclosure_id
                {limit_clause}
            """),
            params,
        )

        return conn.execute(text("SELECT COUNT(*) FROM controller_scope")).scalar() or 0

    def _is_partial_run(self) -> bool:
        return any([
            self._config.foreclosure_id is not None,
            self._config.case_number is not None,
            self._config.active_only,
            self._config.limit is not None,
        ])

    def _reset_outputs(self, conn: Any) -> None:
        if self._is_partial_run():
            conn.execute(text("""
                DELETE FROM foreclosure_title_summary
                WHERE foreclosure_id IN (SELECT foreclosure_id FROM controller_scope)
            """))
            conn.execute(text("""
                DELETE FROM foreclosure_title_chain
                WHERE foreclosure_id IN (SELECT foreclosure_id FROM controller_scope)
            """))
            conn.execute(text("""
                DELETE FROM foreclosure_title_events
                WHERE foreclosure_id IN (SELECT foreclosure_id FROM controller_scope)
            """))
            return

        conn.execute(
            text(
                "TRUNCATE TABLE foreclosure_title_chain, foreclosure_title_summary, "
                "foreclosure_title_events RESTART IDENTITY"
            )
        )

    @staticmethod
    def _insert_sales_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                instrument_number, or_book, or_page,
                grantor, grantee, amount, description, sale_row_id
            )
            SELECT
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                s.sale_date,
                'SALE' AS event_source,
                s.sale_type,
                s.doc_num,
                s.or_book,
                s.or_page,
                COALESCE(s.grantor, ori.parties_from_text) AS grantor,
                COALESCE(s.grantee, ori.parties_to_text)  AS grantee,
                s.sale_amount,
                concat_ws(
                    ' ',
                    coalesce(s.sale_type, 'UNK'),
                    coalesce(s.grantor, ori.parties_from_text, ''),
                    '->',
                    coalesce(s.grantee, ori.parties_to_text, '')
                ) AS description,
                s.id
            FROM controller_scope sc
            JOIN hcpa_allsales s ON s.folio = sc.folio
            LEFT JOIN official_records_daily_instruments ori
              ON s.doc_num IS NOT NULL
             AND ori.instrument_number = s.doc_num
             AND (s.grantor IS NULL OR s.grantee IS NULL)
            WHERE sc.folio IS NOT NULL
              AND s.sale_date IS NOT NULL
              AND s.sale_date <= sc.auction_date
        """

    @staticmethod
    def _insert_case_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                description, clerk_event_id
            )
            SELECT
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(e.event_date, sc.auction_date) AS event_date,
                'CASE' AS event_source,
                e.event_code,
                concat_ws(
                    ' ',
                    coalesce(e.event_description, ''),
                    coalesce('[' || e.party_last_name || ']', '')
                ) AS description,
                e.id
            FROM controller_scope sc
            JOIN clerk_civil_events e ON e.case_number = sc.case_number_norm
            WHERE sc.case_number_norm IS NOT NULL
        """

    @staticmethod
    def _insert_judgment_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype, amount, description
            )
            SELECT
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(sc.judgment_date, sc.auction_date) AS event_date,
                'JUDGMENT' AS event_source,
                'FINAL_JUDGMENT' AS event_subtype,
                sc.final_judgment_amount,
                'Final judgment anchor'
            FROM controller_scope sc
            WHERE sc.judgment_date IS NOT NULL
               OR sc.final_judgment_amount IS NOT NULL
        """

    @staticmethod
    def _insert_auction_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype, amount, description
            )
            SELECT
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                sc.auction_date AS event_date,
                'AUCTION' AS event_source,
                sc.auction_status,
                sc.winning_bid,
                concat_ws(' ', 'Auction result:', coalesce(sc.sold_to, 'UNKNOWN BUYER'))
            FROM controller_scope sc
            WHERE sc.auction_date IS NOT NULL
        """

    @staticmethod
    def _insert_history_auction_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                instrument_number, amount, description
            )
            SELECT DISTINCT ON (sc.foreclosure_id, ha.foreclosure_id)
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                ha.auction_date AS event_date,
                'HILLS_AUCTION' AS event_source,
                ha.auction_status,
                ha.listing_id,
                ha.winning_bid,
                concat_ws(
                    ' ',
                    'HillsForeclosures listing:',
                    ha.listing_id,
                    'buyer:',
                    coalesce(ha.sold_to, 'UNKNOWN')
                ) AS description
            FROM controller_scope sc
            JOIN foreclosures_history ha
              ON (ha.case_number_raw = sc.case_number_raw)
              OR (sc.folio IS NOT NULL AND ha.folio = sc.folio)
              OR (sc.strap IS NOT NULL AND ha.strap = sc.strap)
            WHERE ha.auction_date IS NOT NULL
        """

    @staticmethod
    def _insert_tax_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                instrument_number, amount, description
            )
            SELECT DISTINCT ON (sc.foreclosure_id, d.id)
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(make_date(d.tax_year, 1, 1), sc.auction_date) AS event_date,
                'TAX' AS event_source,
                'TAX_YEAR' AS event_subtype,
                d.parcel_id AS instrument_number,
                d.estimated_annual_tax AS amount,
                concat_ws(
                    ' ',
                    'Tax year:',
                    d.tax_year::text,
                    'owner:',
                    coalesce(d.owner_name, ''),
                    'homestead:',
                    CASE WHEN d.homestead_exempt THEN 'YES' ELSE 'NO' END
                ) AS description
            FROM controller_scope sc
            JOIN dor_nal_parcels d
              ON (sc.folio IS NOT NULL AND d.folio = sc.folio)
              OR (sc.strap IS NOT NULL AND d.strap = sc.strap)
            WHERE d.tax_year IS NOT NULL
        """

    @staticmethod
    def _insert_county_permit_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                instrument_number, amount, description
            )
            SELECT DISTINCT ON (sc.foreclosure_id, cp.id)
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(cp.combined_date, cp.issue_date, cp.complete_date, sc.auction_date)
                    AS event_date,
                'COUNTY_PERMIT' AS event_source,
                concat_ws(' / ', cp.category, cp.permit_type, cp.type2) AS event_subtype,
                cp.permit_number AS instrument_number,
                cp.permit_value AS amount,
                concat_ws(
                    ' ',
                    'status:',
                    coalesce(cp.status, ''),
                    'address:',
                    coalesce(cp.address, '')
                ) AS description
            FROM controller_scope sc
            JOIN county_permits cp ON
                (
                    sc.folio IS NOT NULL
                    AND regexp_replace(
                        coalesce(cp.folio_clean, cp.folio_raw, ''),
                        '[^0-9]',
                        '',
                        'g'
                    ) = regexp_replace(sc.folio, '[^0-9]', '', 'g')
                )
                OR (
                    sc.property_address IS NOT NULL
                    AND cp.address IS NOT NULL
                    AND upper(trim(split_part(replace(cp.address, E'\\t', ' '), ',', 1)))
                        = upper(trim(
                            split_part(replace(sc.property_address, E'\\t', ' '), ',', 1)
                        ))
                )
            WHERE coalesce(cp.combined_date, cp.issue_date, cp.complete_date, sc.auction_date)
                  IS NOT NULL
        """

    @staticmethod
    def _insert_tampa_permit_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype,
                instrument_number, amount, description
            )
            SELECT DISTINCT ON (sc.foreclosure_id, tr.id)
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(tr.record_date, tr.updated_at::date, sc.auction_date) AS event_date,
                'TAMPA_PERMIT' AS event_source,
                concat_ws(' / ', tr.module, tr.record_type) AS event_subtype,
                tr.record_number AS instrument_number,
                tr.estimated_work_cost AS amount,
                concat_ws(
                    ' ',
                    'status:',
                    coalesce(tr.status, ''),
                    'notes:',
                    coalesce(tr.short_notes, '')
                ) AS description
            FROM controller_scope sc
            JOIN tampa_accela_records tr ON
                sc.property_address IS NOT NULL
                AND upper(trim(
                    split_part(
                        replace(coalesce(tr.address_normalized, tr.address_raw, ''), E'\\t', ' '),
                        ',',
                        1
                    )
                )) = upper(trim(
                    split_part(replace(sc.property_address, E'\\t', ' '), ',', 1)
                ))
            WHERE coalesce(tr.record_date, tr.updated_at::date, sc.auction_date) IS NOT NULL
        """

    @staticmethod
    def _insert_market_events_sql() -> str:
        return """
            INSERT INTO foreclosure_title_events (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                event_date, event_source, event_subtype, amount, description
            )
            SELECT DISTINCT ON (sc.foreclosure_id, pm.strap, pm.folio, pm.case_number)
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                coalesce(pm.updated_at::date, sc.auction_date) AS event_date,
                'MARKET' AS event_source,
                coalesce(pm.listing_status, pm.primary_source) AS event_subtype,
                coalesce(pm.list_price, pm.zestimate, pm.rent_zestimate) AS amount,
                concat_ws(
                    ' ',
                    'zestimate:',
                    coalesce(pm.zestimate::text, ''),
                    'list:',
                    coalesce(pm.list_price::text, ''),
                    coalesce(pm.detail_url, '')
                ) AS description
            FROM controller_scope sc
            JOIN property_market pm
              ON (sc.strap IS NOT NULL AND pm.strap = sc.strap)
              OR (sc.folio IS NOT NULL AND pm.folio = sc.folio)
              OR (sc.case_number_raw IS NOT NULL AND pm.case_number = sc.case_number_raw)
            WHERE coalesce(pm.updated_at::date, sc.auction_date) IS NOT NULL
        """

    @staticmethod
    def _rank_events_sql() -> str:
        return """
            WITH ranked AS (
                SELECT
                    e.id,
                    row_number() OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY
                            e.event_date,
                            CASE e.event_source
                                WHEN 'SALE' THEN 1
                                WHEN 'JUDGMENT' THEN 2
                                WHEN 'CASE' THEN 3
                                WHEN 'AUCTION' THEN 4
                                WHEN 'HILLS_AUCTION' THEN 5
                                WHEN 'TAX' THEN 6
                                WHEN 'COUNTY_PERMIT' THEN 7
                                WHEN 'TAMPA_PERMIT' THEN 8
                                WHEN 'MARKET' THEN 9
                                ELSE 9
                            END,
                            coalesce(e.sale_row_id, e.clerk_event_id, 0),
                            e.id
                    ) AS rn
                FROM foreclosure_title_events e
                WHERE e.foreclosure_id IN (
                    SELECT foreclosure_id FROM controller_scope
                )
            )
            UPDATE foreclosure_title_events e
            SET event_rank = ranked.rn
            FROM ranked
            WHERE e.id = ranked.id
        """

    @staticmethod
    def _score_sales_links_sql() -> str:
        return """
            WITH ordered_sales AS (
                SELECT
                    e.id,
                    e.foreclosure_id,
                    e.grantor,
                    e.grantee,
                    row_number() OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS seq,
                    lag(e.id) OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS prev_sale_event_id,
                    lag(e.grantee) OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS prev_grantee
                FROM foreclosure_title_events e
                WHERE e.event_source = 'SALE'
                  AND e.foreclosure_id IN (
                      SELECT foreclosure_id FROM controller_scope
                  )
            ),
            scored AS (
                SELECT
                    os.id,
                    os.prev_sale_event_id,
                    CASE
                        WHEN os.seq = 1 THEN 'ROOT'
                        WHEN normalize_party_name(os.grantor) IS NULL
                             OR normalize_party_name(os.prev_grantee) IS NULL
                             THEN 'MISSING_PARTY'
                        WHEN normalize_party_name(os.grantor)
                             = normalize_party_name(os.prev_grantee)
                             THEN 'LINKED_EXACT'
                        WHEN similarity(
                            normalize_party_name(os.grantor),
                            normalize_party_name(os.prev_grantee)
                        ) >= :threshold
                             THEN 'LINKED_FUZZY'
                        ELSE 'CHAINED_BY_FOLIO'
                    END AS link_status,
                    CASE
                        WHEN os.seq = 1 THEN 1.0
                        WHEN normalize_party_name(os.grantor) IS NULL
                             OR normalize_party_name(os.prev_grantee) IS NULL
                             THEN NULL
                        ELSE similarity(
                            normalize_party_name(os.grantor),
                            normalize_party_name(os.prev_grantee)
                        )
                    END AS link_score
                FROM ordered_sales os
            )
            UPDATE foreclosure_title_events e
            SET prior_event_id = s.prev_sale_event_id,
                link_status = s.link_status,
                link_score = s.link_score
            FROM scored s
            WHERE e.id = s.id
        """

    @staticmethod
    def _build_chain_sql() -> str:
        return """
            WITH sales AS (
                SELECT
                    e.id AS event_id,
                    e.foreclosure_id,
                    e.case_number_raw,
                    e.case_number_norm,
                    e.folio,
                    e.strap,
                    e.event_date,
                    e.event_subtype,
                    e.amount,
                    e.grantor,
                    e.grantee,
                    e.link_status,
                    e.link_score,
                    row_number() OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS seq,
                    lead(e.id) OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS next_event_id,
                    lead(e.event_date) OVER (
                        PARTITION BY e.foreclosure_id
                        ORDER BY e.event_date, e.id
                    ) AS next_event_date
                FROM foreclosure_title_events e
                WHERE e.event_source = 'SALE'
                  AND e.foreclosure_id IN (
                      SELECT foreclosure_id FROM controller_scope
                  )
            )
            INSERT INTO foreclosure_title_chain (
                foreclosure_id, case_number_raw, case_number_norm, folio, strap,
                sequence_no, owner_name, acquired_date, disposed_date,
                acquired_event_id, next_event_id, acquired_sale_type, acquired_amount,
                grantor, grantee, link_status, link_score, is_gap, is_terminal
            )
            SELECT
                s.foreclosure_id,
                s.case_number_raw,
                s.case_number_norm,
                s.folio,
                s.strap,
                s.seq,
                s.grantee,
                s.event_date AS acquired_date,
                CASE
                    WHEN s.next_event_date IS NOT NULL
                         AND s.next_event_date > s.event_date
                         THEN (s.next_event_date - INTERVAL '1 day')::DATE
                    WHEN s.next_event_date IS NOT NULL
                         AND s.next_event_date <= s.event_date
                         THEN s.event_date
                    ELSE sc.auction_date
                END AS disposed_date,
                s.event_id,
                s.next_event_id,
                s.event_subtype,
                s.amount,
                s.grantor,
                s.grantee,
                s.link_status,
                s.link_score,
                (s.link_status = 'GAP') AS is_gap,
                (s.next_event_id IS NULL) AS is_terminal
            FROM sales s
            JOIN controller_scope sc ON sc.foreclosure_id = s.foreclosure_id
        """

    @staticmethod
    def _build_summary_sql() -> str:
        return """
            WITH event_counts AS (
                SELECT
                    e.foreclosure_id,
                    COUNT(*) AS total_events_count
                FROM foreclosure_title_events e
                WHERE e.foreclosure_id IN (
                    SELECT foreclosure_id FROM controller_scope
                )
                GROUP BY e.foreclosure_id
            ),
            sale_stats AS (
                SELECT
                    e.foreclosure_id,
                    COUNT(*) AS sale_events_count,
                    MIN(e.event_date) AS root_date,
                    MAX(e.event_date) AS last_sale_date,
                    COUNT(*) FILTER (WHERE e.link_status = 'LINKED_EXACT')
                        AS exact_links_count,
                    COUNT(*) FILTER (WHERE e.link_status = 'LINKED_FUZZY')
                        AS fuzzy_links_count,
                    COUNT(*) FILTER (WHERE e.link_status = 'MISSING_PARTY')
                        AS missing_party_links_count,
                    COUNT(*) FILTER (WHERE e.link_status = 'GAP')
                        AS gap_count
                FROM foreclosure_title_events e
                WHERE e.event_source = 'SALE'
                  AND e.foreclosure_id IN (
                      SELECT foreclosure_id FROM controller_scope
                  )
                GROUP BY e.foreclosure_id
            ),
            root_owner AS (
                SELECT DISTINCT ON (e.foreclosure_id)
                    e.foreclosure_id,
                    e.grantee AS root_owner
                FROM foreclosure_title_events e
                WHERE e.event_source = 'SALE'
                  AND e.foreclosure_id IN (
                      SELECT foreclosure_id FROM controller_scope
                  )
                ORDER BY e.foreclosure_id, e.event_date, e.id
            ),
            last_owner AS (
                SELECT DISTINCT ON (e.foreclosure_id)
                    e.foreclosure_id,
                    e.grantee AS pre_foreclosure_owner
                FROM foreclosure_title_events e
                WHERE e.event_source = 'SALE'
                  AND e.foreclosure_id IN (
                      SELECT foreclosure_id FROM controller_scope
                  )
                ORDER BY e.foreclosure_id, e.event_date DESC, e.id DESC
            )
            INSERT INTO foreclosure_title_summary (
                foreclosure_id, case_number_raw, case_number_norm,
                folio, strap, auction_date, root_date, last_sale_date,
                root_owner, pre_foreclosure_owner, sale_events_count,
                total_events_count, exact_links_count, fuzzy_links_count,
                missing_party_links_count, gap_count, years_covered,
                chain_status, created_at, updated_at
            )
            SELECT
                sc.foreclosure_id,
                sc.case_number_raw,
                sc.case_number_norm,
                sc.folio,
                sc.strap,
                sc.auction_date,
                ss.root_date,
                ss.last_sale_date,
                ro.root_owner,
                lo.pre_foreclosure_owner,
                coalesce(ss.sale_events_count, 0) AS sale_events_count,
                coalesce(ec.total_events_count, 0) AS total_events_count,
                coalesce(ss.exact_links_count, 0) AS exact_links_count,
                coalesce(ss.fuzzy_links_count, 0) AS fuzzy_links_count,
                coalesce(ss.missing_party_links_count, 0) AS missing_party_links_count,
                coalesce(ss.gap_count, 0) AS gap_count,
                CASE
                    WHEN ss.root_date IS NULL THEN NULL
                    ELSE ROUND(
                        (sc.auction_date - ss.root_date)::NUMERIC / 365.25,
                        2
                    )
                END AS years_covered,
                CASE
                    WHEN sc.folio IS NULL THEN 'MISSING_FOLIO'
                    WHEN coalesce(ss.sale_events_count, 0) = 0 THEN 'NO_SALES'
                    WHEN coalesce(ss.gap_count, 0) = 0 THEN 'COMPLETE'
                    ELSE 'BROKEN'
                END AS chain_status,
                now(),
                now()
            FROM controller_scope sc
            LEFT JOIN event_counts ec ON ec.foreclosure_id = sc.foreclosure_id
            LEFT JOIN sale_stats ss ON ss.foreclosure_id = sc.foreclosure_id
            LEFT JOIN root_owner ro ON ro.foreclosure_id = sc.foreclosure_id
            LEFT JOIN last_owner lo ON lo.foreclosure_id = sc.foreclosure_id
        """

    @staticmethod
    def _summary_stats_sql() -> str:
        return """
            SELECT
                COUNT(*) AS total_foreclosures,
                COUNT(*) FILTER (WHERE chain_status = 'COMPLETE') AS complete_chains,
                COUNT(*) FILTER (WHERE chain_status = 'BROKEN') AS broken_chains,
                COUNT(*) FILTER (WHERE chain_status = 'NO_SALES') AS no_sales_chains,
                COUNT(*) FILTER (WHERE chain_status = 'MISSING_FOLIO')
                    AS missing_folio_chains,
                COALESCE(SUM(gap_count), 0) AS total_gaps,
                ROUND(
                    AVG(years_covered) FILTER (WHERE years_covered IS NOT NULL),
                    2
                ) AS avg_years_covered
            FROM foreclosure_title_summary
            WHERE foreclosure_id IN (SELECT foreclosure_id FROM controller_scope)
        """
