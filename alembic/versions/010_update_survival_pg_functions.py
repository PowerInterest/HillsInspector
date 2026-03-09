"""Update survival-aware PG helper functions for per-foreclosure reads.

Revision ID: 010_update_survival_pg_functions
Revises: 009_add_fc_enc_survival
Create Date: 2026-03-09
"""

from alembic import op

revision = "010_update_survival_pg_functions"
down_revision = "009_add_fc_enc_survival"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
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
                (pm.photo_cdn_urls->>0)::TEXT AS photo_url,
                COALESCE(perm.open_count, 0)::INT,
                COALESCE(perm.total_count, 0)::INT,
                COALESCE(enc.total, 0)::INT,
                COALESCE(enc.survived, 0)::INT,
                COALESCE(enc.survived_debt, 0)::NUMERIC,
                (COALESCE(enc.survived, 0) > 2
                 OR COALESCE(enc.survived_debt, 0) > COALESCE(f.final_judgment_amount, 0)
                )::BOOLEAN AS is_toxic_title,
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
                       COUNT(*) FILTER (
                           WHERE COALESCE(fes.survival_status, oe.survival_status) = 'SURVIVED'
                       ) AS survived,
                       COALESCE(
                           SUM(oe.amount) FILTER (
                               WHERE COALESCE(fes.survival_status, oe.survival_status) = 'SURVIVED'
                           ),
                           0
                       ) AS survived_debt
                FROM ori_encumbrances oe
                LEFT JOIN foreclosure_encumbrance_survival fes
                  ON fes.encumbrance_id = oe.id
                 AND fes.foreclosure_id = f.foreclosure_id
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
        """
    )

    op.execute("DROP FUNCTION IF EXISTS get_property_encumbrances(TEXT);")
    op.execute("DROP FUNCTION IF EXISTS get_property_encumbrances(TEXT, BIGINT);")
    op.execute(
        """
        CREATE FUNCTION get_property_encumbrances(
            p_strap TEXT,
            p_foreclosure_id BIGINT DEFAULT NULL
        )
        RETURNS TABLE (
            id BIGINT, encumbrance_type TEXT, raw_document_type TEXT,
            party1 TEXT, party2 TEXT, amount NUMERIC,
            recording_date DATE, instrument_number TEXT, book TEXT, page TEXT,
            is_satisfied BOOLEAN, satisfaction_date DATE, satisfaction_instrument TEXT,
            survival_status TEXT, survival_reason TEXT,
            current_holder TEXT, assignment_count INT, mrta_expiration_date DATE,
            case_number TEXT
        ) AS $$
        DECLARE
            v_foreclosure_id BIGINT;
        BEGIN
            v_foreclosure_id := p_foreclosure_id;
            IF v_foreclosure_id IS NULL THEN
                SELECT f.foreclosure_id
                  INTO v_foreclosure_id
                FROM foreclosures f
                WHERE f.archived_at IS NULL
                  AND f.strap = p_strap
                ORDER BY f.auction_date DESC NULLS LAST, f.foreclosure_id DESC
                LIMIT 1;
            END IF;

            RETURN QUERY
            SELECT oe.id, oe.encumbrance_type::TEXT, oe.raw_document_type,
                   oe.party1, oe.party2, oe.amount,
                   oe.recording_date, oe.instrument_number::TEXT, oe.book::TEXT, oe.page::TEXT,
                   oe.is_satisfied, oe.satisfaction_date, oe.satisfaction_instrument::TEXT,
                   COALESCE(fes.survival_status, oe.survival_status)::TEXT,
                   COALESCE(fes.survival_reason, oe.survival_reason),
                   oe.current_holder, oe.assignment_count, oe.mrta_expiration_date,
                   oe.case_number::TEXT
            FROM ori_encumbrances oe
            LEFT JOIN foreclosure_encumbrance_survival fes
              ON fes.encumbrance_id = oe.id
             AND fes.foreclosure_id = v_foreclosure_id
            WHERE oe.strap = p_strap
              AND oe.encumbrance_type != 'noc'
            ORDER BY oe.recording_date DESC NULLS LAST, oe.id;
        END;
        $$ LANGUAGE plpgsql STABLE;
        """
    )

    op.execute("DROP FUNCTION IF EXISTS compute_net_equity(TEXT);")
    op.execute("DROP FUNCTION IF EXISTS compute_net_equity(TEXT, BIGINT);")
    op.execute(
        """
        CREATE FUNCTION compute_net_equity(
            p_strap TEXT,
            p_foreclosure_id BIGINT DEFAULT NULL
        )
        RETURNS TABLE (
            market_value NUMERIC, final_judgment NUMERIC,
            per_diem_accrual NUMERIC,
            survived_debt NUMERIC, net_equity NUMERIC,
            is_toxic BOOLEAN, survived_count INT, total_encumbrances INT
        ) AS $$
        DECLARE
            v_foreclosure_id BIGINT;
        BEGIN
            v_foreclosure_id := p_foreclosure_id;
            IF v_foreclosure_id IS NULL THEN
                SELECT f2.foreclosure_id
                  INTO v_foreclosure_id
                FROM foreclosures f2
                WHERE f2.archived_at IS NULL
                  AND f2.strap = p_strap
                ORDER BY f2.auction_date DESC NULLS LAST, f2.foreclosure_id DESC
                LIMIT 1;
            END IF;

            RETURN QUERY
            SELECT
                COALESCE(f.market_value, f.zestimate, f.assessed_value, 0) AS market_value,
                COALESCE(f.final_judgment_amount, 0) AS final_judgment,
                COALESCE(
                    CASE
                        WHEN (f.judgment_data->>'per_diem_rate') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                         AND (f.judgment_data->>'interest_through_date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                        THEN (f.judgment_data->>'per_diem_rate')::NUMERIC
                             * GREATEST(
                                 0,
                                 COALESCE(f.auction_date, CURRENT_DATE)
                                 - (f.judgment_data->>'interest_through_date')::DATE
                             )
                        ELSE 0
                    END,
                    0
                ) AS per_diem_accrual,
                COALESCE(enc.survived_debt, 0) AS survived_debt,
                (COALESCE(f.market_value, f.zestimate, f.assessed_value, 0)
                 - COALESCE(f.final_judgment_amount, 0)
                 - COALESCE(
                    CASE
                        WHEN (f.judgment_data->>'per_diem_rate') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                         AND (f.judgment_data->>'interest_through_date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                        THEN (f.judgment_data->>'per_diem_rate')::NUMERIC
                             * GREATEST(
                                 0,
                                 COALESCE(f.auction_date, CURRENT_DATE)
                                 - (f.judgment_data->>'interest_through_date')::DATE
                             )
                        ELSE 0
                    END,
                    0)
                 - COALESCE(enc.survived_debt, 0)) AS net_equity,
                (COALESCE(enc.survived_count, 0) > 2
                 OR COALESCE(enc.survived_debt, 0) > COALESCE(f.final_judgment_amount, 0)) AS is_toxic,
                COALESCE(enc.survived_count, 0)::INT,
                COALESCE(enc.total_count, 0)::INT
            FROM foreclosures f
            LEFT JOIN LATERAL (
                SELECT COUNT(*) FILTER (
                           WHERE COALESCE(fes.survival_status, oe.survival_status) IN ('SURVIVED', 'UNCERTAIN')
                       ) AS survived_count,
                       COALESCE(
                           SUM(oe.amount) FILTER (
                               WHERE COALESCE(fes.survival_status, oe.survival_status) IN ('SURVIVED', 'UNCERTAIN')
                           ),
                           0
                       ) AS survived_debt,
                       COUNT(*) AS total_count
                FROM ori_encumbrances oe
                LEFT JOIN foreclosure_encumbrance_survival fes
                  ON fes.encumbrance_id = oe.id
                 AND fes.foreclosure_id = f.foreclosure_id
                WHERE oe.strap = f.strap
            ) enc ON TRUE
            WHERE f.foreclosure_id = COALESCE(v_foreclosure_id, f.foreclosure_id)
              AND f.strap = p_strap
            ORDER BY f.auction_date DESC
            LIMIT 1;
        END;
        $$ LANGUAGE plpgsql STABLE;
        """
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
