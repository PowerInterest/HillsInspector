"""Convert foreclosures_history from physical table to view.

This migration eliminates the data-duplication pattern where rows were copied
between ``foreclosures`` and ``foreclosures_history``.  Instead of a separate
table, ``foreclosures_history`` becomes a lightweight view over the
``foreclosures`` table, filtering on ``archived_at IS NOT NULL``.

Steps:
  1. Guard: skip if already a view (idempotent).
  2. Sync any orphan history rows back into ``foreclosures`` so no data is lost.
  3. Drop the physical table (CASCADE removes its indexes/constraints).
  4. Create the replacement view.

Dependent PG functions (e.g. ``fn_title_chain_gaps``) reference the name
``foreclosures_history`` inside their SQL body and resolve it at execution
time, so they do NOT need to be dropped and recreated.

Revision ID: 001_viewify
Revises: (initial)
Create Date: 2026-02-27
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "001_viewify"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # Guard: if foreclosures_history is already a view, nothing to do.
    # ------------------------------------------------------------------
    result = conn.execute(
        sa.text(
            """
            SELECT table_type FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = 'foreclosures_history'
            """
        )
    )
    row = result.fetchone()
    if row is None:
        # Table doesn't exist at all — just create the view.
        pass
    elif row[0] == "VIEW":
        # Already a view — nothing to do.
        return
    else:
        # It's a BASE TABLE — sync orphans back into foreclosures, then drop.

        # ------------------------------------------------------------------
        # Sync orphan history rows back into foreclosures.
        #
        # Any row in foreclosures_history that is missing from foreclosures
        # (by the natural key case_number_raw + auction_date) gets inserted.
        # If the row already exists, we COALESCE-merge enrichment columns
        # to preserve data that may only exist in the history copy.
        # ------------------------------------------------------------------
        conn.execute(
            sa.text(
                """
                INSERT INTO foreclosures (
                    case_number_raw, auction_date, auction_type,
                    listing_id, case_number_norm, auction_status,
                    folio, strap, property_address, latitude, longitude,
                    winning_bid, final_judgment_amount, appraised_value,
                    sold_to, buyer_type,
                    owner_name, land_use, year_built, beds, baths,
                    heated_area, market_value, assessed_value,
                    clerk_case_type, clerk_case_status, filing_date, judgment_date,
                    is_foreclosure,
                    judgment_data, pdf_path,
                    first_valid_resale_date, first_valid_resale_price,
                    hold_days, resale_profit, roi,
                    homestead_exempt, estimated_annual_tax,
                    zestimate, list_price, listing_status,
                    has_ucc_liens, ucc_active_count,
                    encumbrance_count, unsatisfied_encumbrance_count,
                    step_pdf_downloaded, step_judgment_extracted,
                    step_ori_searched, step_survival_analyzed,
                    created_at, updated_at, archived_at
                )
                SELECT
                    fh.case_number_raw, fh.auction_date, fh.auction_type,
                    fh.listing_id, fh.case_number_norm, fh.auction_status,
                    fh.folio, fh.strap, fh.property_address, fh.latitude, fh.longitude,
                    fh.winning_bid, fh.final_judgment_amount, fh.appraised_value,
                    fh.sold_to, fh.buyer_type,
                    fh.owner_name, fh.land_use, fh.year_built, fh.beds, fh.baths,
                    fh.heated_area, fh.market_value, fh.assessed_value,
                    fh.clerk_case_type, fh.clerk_case_status, fh.filing_date, fh.judgment_date,
                    fh.is_foreclosure,
                    fh.judgment_data, fh.pdf_path,
                    fh.first_valid_resale_date, fh.first_valid_resale_price,
                    fh.hold_days, fh.resale_profit, fh.roi,
                    fh.homestead_exempt, fh.estimated_annual_tax,
                    fh.zestimate, fh.list_price, fh.listing_status,
                    fh.has_ucc_liens, fh.ucc_active_count,
                    fh.encumbrance_count, fh.unsatisfied_encumbrance_count,
                    fh.step_pdf_downloaded, fh.step_judgment_extracted,
                    fh.step_ori_searched, fh.step_survival_analyzed,
                    fh.created_at, fh.updated_at,
                    COALESCE(fh.archived_at, fh.moved_to_history_at)
                FROM foreclosures_history fh
                ON CONFLICT (case_number_raw, auction_date) DO UPDATE SET
                    judgment_data       = COALESCE(foreclosures.judgment_data,       EXCLUDED.judgment_data),
                    strap               = COALESCE(foreclosures.strap,               EXCLUDED.strap),
                    folio               = COALESCE(foreclosures.folio,               EXCLUDED.folio),
                    property_address    = COALESCE(foreclosures.property_address,    EXCLUDED.property_address),
                    latitude            = COALESCE(foreclosures.latitude,            EXCLUDED.latitude),
                    longitude           = COALESCE(foreclosures.longitude,           EXCLUDED.longitude),
                    winning_bid         = COALESCE(foreclosures.winning_bid,         EXCLUDED.winning_bid),
                    final_judgment_amount = COALESCE(foreclosures.final_judgment_amount, EXCLUDED.final_judgment_amount),
                    appraised_value     = COALESCE(foreclosures.appraised_value,     EXCLUDED.appraised_value),
                    sold_to             = COALESCE(foreclosures.sold_to,             EXCLUDED.sold_to),
                    buyer_type          = COALESCE(foreclosures.buyer_type,          EXCLUDED.buyer_type),
                    owner_name          = COALESCE(foreclosures.owner_name,          EXCLUDED.owner_name),
                    land_use            = COALESCE(foreclosures.land_use,            EXCLUDED.land_use),
                    year_built          = COALESCE(foreclosures.year_built,          EXCLUDED.year_built),
                    beds                = COALESCE(foreclosures.beds,                EXCLUDED.beds),
                    baths               = COALESCE(foreclosures.baths,               EXCLUDED.baths),
                    heated_area         = COALESCE(foreclosures.heated_area,         EXCLUDED.heated_area),
                    market_value        = COALESCE(foreclosures.market_value,        EXCLUDED.market_value),
                    assessed_value      = COALESCE(foreclosures.assessed_value,      EXCLUDED.assessed_value),
                    clerk_case_type     = COALESCE(foreclosures.clerk_case_type,     EXCLUDED.clerk_case_type),
                    clerk_case_status   = COALESCE(foreclosures.clerk_case_status,   EXCLUDED.clerk_case_status),
                    filing_date         = COALESCE(foreclosures.filing_date,         EXCLUDED.filing_date),
                    judgment_date       = COALESCE(foreclosures.judgment_date,       EXCLUDED.judgment_date),
                    is_foreclosure      = COALESCE(foreclosures.is_foreclosure,      EXCLUDED.is_foreclosure),
                    pdf_path            = COALESCE(foreclosures.pdf_path,            EXCLUDED.pdf_path),
                    first_valid_resale_date  = COALESCE(foreclosures.first_valid_resale_date,  EXCLUDED.first_valid_resale_date),
                    first_valid_resale_price = COALESCE(foreclosures.first_valid_resale_price, EXCLUDED.first_valid_resale_price),
                    hold_days           = COALESCE(foreclosures.hold_days,           EXCLUDED.hold_days),
                    resale_profit       = COALESCE(foreclosures.resale_profit,       EXCLUDED.resale_profit),
                    roi                 = COALESCE(foreclosures.roi,                 EXCLUDED.roi),
                    homestead_exempt    = COALESCE(foreclosures.homestead_exempt,    EXCLUDED.homestead_exempt),
                    estimated_annual_tax = COALESCE(foreclosures.estimated_annual_tax, EXCLUDED.estimated_annual_tax),
                    zestimate           = COALESCE(foreclosures.zestimate,           EXCLUDED.zestimate),
                    list_price          = COALESCE(foreclosures.list_price,          EXCLUDED.list_price),
                    listing_status      = COALESCE(foreclosures.listing_status,      EXCLUDED.listing_status),
                    has_ucc_liens       = COALESCE(foreclosures.has_ucc_liens,       EXCLUDED.has_ucc_liens),
                    ucc_active_count    = COALESCE(foreclosures.ucc_active_count,    EXCLUDED.ucc_active_count),
                    encumbrance_count   = COALESCE(foreclosures.encumbrance_count,   EXCLUDED.encumbrance_count),
                    unsatisfied_encumbrance_count = COALESCE(foreclosures.unsatisfied_encumbrance_count, EXCLUDED.unsatisfied_encumbrance_count),
                    step_pdf_downloaded     = COALESCE(foreclosures.step_pdf_downloaded,     EXCLUDED.step_pdf_downloaded),
                    step_judgment_extracted  = COALESCE(foreclosures.step_judgment_extracted,  EXCLUDED.step_judgment_extracted),
                    step_ori_searched       = COALESCE(foreclosures.step_ori_searched,       EXCLUDED.step_ori_searched),
                    step_survival_analyzed  = COALESCE(foreclosures.step_survival_analyzed,  EXCLUDED.step_survival_analyzed),
                    archived_at         = COALESCE(foreclosures.archived_at,         EXCLUDED.archived_at)
                """
            )
        )

        # ------------------------------------------------------------------
        # Drop the physical table (CASCADE drops its indexes too).
        # ------------------------------------------------------------------
        conn.execute(sa.text("DROP TABLE foreclosures_history CASCADE"))

    # ------------------------------------------------------------------
    # Create the replacement view.
    # ------------------------------------------------------------------
    conn.execute(
        sa.text(
            """
            CREATE OR REPLACE VIEW foreclosures_history AS
            SELECT *, archived_at AS moved_to_history_at
            FROM foreclosures
            WHERE archived_at IS NOT NULL
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()

    # ------------------------------------------------------------------
    # Drop the view.
    # ------------------------------------------------------------------
    conn.execute(sa.text("DROP VIEW IF EXISTS foreclosures_history"))

    # ------------------------------------------------------------------
    # Recreate the physical table with the original schema.
    # ------------------------------------------------------------------
    conn.execute(
        sa.text(
            """
            CREATE TABLE foreclosures_history (
                LIKE foreclosures INCLUDING DEFAULTS INCLUDING CONSTRAINTS
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            ALTER TABLE foreclosures_history
            ADD COLUMN IF NOT EXISTS moved_to_history_at TIMESTAMPTZ NOT NULL DEFAULT now()
            """
        )
    )

    # Recreate indexes that existed on the original table.
    conn.execute(sa.text("CREATE INDEX idx_fch_auction_date ON foreclosures_history(auction_date)"))
    conn.execute(sa.text("CREATE INDEX idx_fch_case_raw ON foreclosures_history(case_number_raw)"))
    conn.execute(sa.text("CREATE INDEX idx_fch_strap ON foreclosures_history(strap)"))
    conn.execute(sa.text("CREATE INDEX idx_fch_folio ON foreclosures_history(folio)"))
    conn.execute(
        sa.text(
            "CREATE UNIQUE INDEX idx_fch_case_date_unique ON foreclosures_history(case_number_raw, auction_date)"
        )
    )

    # ------------------------------------------------------------------
    # Populate from archived rows in foreclosures.
    # ------------------------------------------------------------------
    conn.execute(
        sa.text(
            """
            INSERT INTO foreclosures_history
            SELECT f.*, now() AS moved_to_history_at
            FROM foreclosures f
            WHERE f.archived_at IS NOT NULL
            """
        )
    )
