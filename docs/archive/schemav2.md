DO NOT USE YET

# Schema Review (Inconsistencies and Redundancies)

This document lists potential schema inconsistencies and redundancies observed in
`docs/schema.md`. It is a review artifact, not a replacement schema.

## Likely Redundancies

- Property core fields are repeated across `parcels`, `bulk_parcels`, and
  `auctions` (e.g., `owner_name`, `property_address`, `city`, `zip_code`,
  `land_use`, `year_built`, `beds`, `baths`, `heated_area`, `lot_size`,
  `assessed_value`, `market_value`, `last_sale_date`, `last_sale_price`). If
  these represent different sources, the schema does not clearly distinguish
  the authoritative source.
- Valuation fields are duplicated across multiple tables: `parcels.market_value`,
  `bulk_parcels.market_value`, `analysis_results.market_value`, plus
  `home_harvest.estimated_value` and `market_data.zestimate`.
- Address fields overlap across `auctions.property_address`,
  `parcels.property_address`, `bulk_parcels.property_address`, and
  `home_harvest.formatted_address` (plus `street`/`unit`).
- `bulk_parcels` stores both `folio` and `strap`, and `sales_history` stores both
  `folio` and `strap`. If `strap` is the canonical join key (as noted in the
  relationships), keeping both may be redundant without a clear mapping rule.
- `home_harvest` contains both `unit` and `units` (JSON). If `units` is a
  multi-unit representation, clarify how it coexists with `unit`.
- `home_harvest` includes both `status` and `mls_status`, which may be
  duplicative if they mirror each other.
- `documents` includes `ori_uuid`, `ori_id`, and `instrument_number`, which may
  represent the same identifier family.
- `liens` is explicitly a legacy table but overlaps heavily with
  `encumbrances`, creating parallel sources for lien data.

## Inconsistencies

- Monetary fields are usually `FLOAT`/`DOUBLE`, but `auctions.plaintiff_max_bid`
  is `VARCHAR`, which is inconsistent with `opening_bid`, `auction_bid`, and
  other money fields.
- Date types vary for similar concepts: `sales_history.sale_date` is `VARCHAR`
  while similar fields elsewhere are `DATE` or `TIMESTAMP` (e.g.,
  `parcels.last_sale_date`, `documents.recording_date`).
- `market_data.price_history` and `market_data.raw_json` are `VARCHAR`, while
  similar data in `home_harvest` uses `JSON` (`tax_history`, `nearby_schools`,
  `photos`, etc.). This inconsistency complicates downstream parsing.
- `sales_history` shows `UNI` on `folio`, `book`, and `page`. As documented, this
  would prevent multiple sales per folio and multiple rows sharing the same book
  or page, which conflicts with the concept of a history table.
- Instrument fields use inconsistent naming across tables:
  `documents.instrument_number`, `encumbrances.instrument`,
  `sales_history.instrument`, and `legal_variations.source_instrument`.
- Party naming is inconsistent across legal-related tables:
  `documents.party1`/`party2`, `encumbrances.creditor`/`debtor`,
  `liens.grantor`/`grantee`. Mapping rules are not documented.

## Handling Multi-Source Differences (Thoughts)

The steps show that many fields have multiple sources: auctions (Step 1/1.5),
bulk parcels (Step 3), HCPA GIS or fallback (Step 4/12), HomeHarvest MLS
(Step 3.5), market scrapers (Steps 10/11), final judgments (Step 2), and tax
scrapes (Step 13). A practical approach is to keep raw, source-specific values
and add a consistent, resolved layer for downstream use.

### Consistency Baselines

- Currency: standardize all money values to a single numeric type and scale
  (e.g., `DECIMAL(14,2)` or `DOUBLE` with a documented rounding rule). Store the
  original string in `*_raw` when parsing is lossy.
- Dates: standardize to `DATE` or `TIMESTAMP` (documented per field). Preserve
  raw scraped strings in `*_raw` columns when precision is unclear.

### Provenance and Resolution Strategies

- Add `*_source`, `*_as_of`, and `*_confidence` columns for any field that can
  be overwritten by multiple steps (owner, address, values, bed/bath, etc.).
- Treat some sources as authoritative per field:
  - Final judgment for judgment amounts and foreclosure parties/dates.
  - Bulk parcels for assessed/just values and legal description baseline.
  - HCPA GIS for sales history (book/page/instrument).
  - HomeHarvest for MLS photos/listing context.
  - Market scrapers for Zestimate/rent estimates and active listing signals.
- When two authoritative sources disagree, prefer deterministic rules
  (source rank + newest capture date) and keep both raw values for audit.

### A "Facts" Table Pattern (Clever, Minimal Changes)

Instead of duplicating columns across tables, add a generic observations table
to record multiple values with provenance, and build a view for "resolved"
fields:

- `property_facts` (suggested):
  - `folio`
  - `attribute` (e.g., `market_value`, `beds`, `property_address`)
  - `value_text`, `value_num`, `value_date`
  - `source` (e.g., `bulk_parcels`, `hcpa_gis`, `home_harvest`, `auction`)
  - `captured_at`, `confidence`, `raw_value`

Then define a `property_resolved` view that picks the best value by:
1) source priority, 2) most recent `captured_at`, 3) confidence score.

This keeps multiple sources without losing history and allows you to surface
both "best guess" and "all observations" to the web UI or analysis pipeline.

### Minimal Schema Sketch

```sql
-- Store all observations with provenance.
CREATE TABLE property_facts (
    fact_id BIGINT PRIMARY KEY,
    folio VARCHAR NOT NULL,
    attribute VARCHAR NOT NULL,
    value_text VARCHAR,
    value_num DOUBLE,
    value_date DATE,
    value_ts TIMESTAMP,
    value_bool BOOLEAN,
    unit VARCHAR,
    source VARCHAR NOT NULL,
    source_ref VARCHAR,
    captured_at TIMESTAMP,
    confidence DOUBLE,
    raw_value VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: rank sources per attribute.
CREATE TABLE property_source_priority (
    attribute VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    priority INTEGER NOT NULL
);

-- Resolve to a single best value per attribute.
CREATE VIEW property_resolved AS
SELECT
    folio,
    attribute,
    value_text,
    value_num,
    value_date,
    value_ts,
    value_bool,
    unit,
    source,
    source_ref,
    captured_at,
    confidence
FROM (
    SELECT
        f.*,
        p.priority,
        ROW_NUMBER() OVER (
            PARTITION BY f.folio, f.attribute
            ORDER BY
                p.priority ASC NULLS LAST,
                f.captured_at DESC NULLS LAST,
                f.confidence DESC NULLS LAST,
                f.fact_id DESC
        ) AS rn
    FROM property_facts f
    LEFT JOIN property_source_priority p
        ON f.attribute = p.attribute AND f.source = p.source
) ranked
WHERE rn = 1;
```

Notes:
- Only one of `value_text`, `value_num`, `value_date`, `value_ts`, or
  `value_bool` should be set per row.
- `source_ref` can store the origin row id (e.g., `parcels.folio`,
  `home_harvest.id`, `market_data.id`) to make audits easy.
