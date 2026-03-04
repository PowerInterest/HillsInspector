# Query Modules (`src/db/`)

All complex SQL queries live in `src/db/` as a centralized query layer. Each module provides a singleton class with read-only methods that both the web app and pipeline can import. This prevents query duplication across routers, services, and scripts.

**Pattern:** Each module exposes a `get_*_queries()` factory that returns a lazily-initialized singleton. All singletons use graceful degradation — if PostgreSQL is unreachable at init time, every method returns empty results instead of raising.

---

## `encumbrance_queries.py`

ORI encumbrance queries using the `OriEncumbrance` ORM model. SQLAlchemy Core `select()` — no raw SQL.

| Method | Description |
|---|---|
| `get_encumbrances(strap, folio, limit)` | General encumbrances excluding NOCs, with creditor resolution |
| `get_tax_liens(strap, folio, limit)` | IRS, tax collector, and corporate tax liens (LNCORPTX, TL) |
| `get_nocs(strap, folio, limit)` | Notices of Commencement |
| `summarize(encumbrances)` | Pure-Python risk summary: survived/uncertain counts, total amount |

**Tables:** `ori_encumbrances`
**Factory:** `get_encumbrance_queries()`

---

## `foreclosure_queries.py`

Foreclosure case resolution and permit queries using `Foreclosure`, `ForeclosureTitleEvent`, `HcpaBulkParcel`, `HcpaAllSale` ORM models. SQLAlchemy Core `select()` — no raw SQL.

| Method | Description |
|---|---|
| `get_case_numbers(identifier)` | Distinct case numbers matching strap/folio/case_number (active + archived) |
| `resolve_folio(identifier, case_number)` | 4-tier folio resolution: case match → identifier match → bulk parcels → allsales |
| `get_permits(foreclosure_id)` | Permit records from title events (COUNTY_PERMIT / TAMPA_PERMIT sources) |

**Tables:** `foreclosures`, `foreclosures_history` (view), `foreclosure_title_events`, `hcpa_bulk_parcels`, `hcpa_allsales`
**Factory:** `get_foreclosure_queries()`

---

## `tax_queries.py`

DOR NAL tax data queries using the `DorNalParcel` ORM model. SQLAlchemy Core `select()` — no raw SQL.

| Method | Description |
|---|---|
| `get_current_year(strap, folio, identifier)` | Most recent tax year: valuations, exemptions, millage rates |
| `get_history(strap, folio, identifier)` | All tax years with `DISTINCT ON (tax_year)` dedup, sorted descending |

**Tables:** `dor_nal_parcels`
**Factory:** `get_tax_queries()`

---

## `sales_queries.py`

Sales chain and parcel resolution queries. Raw SQL via `text()`.

| Method | Description |
|---|---|
| `resolve_strap_to_folio(strap)` | Convert pipeline strap to 10-digit PG folio |
| `get_sales_chain(pg_folio)` | Full sales history from `hcpa_allsales`, newest-first |
| `get_sale_instruments(pg_folio)` | Instrument numbers from sales for ORI search seeding |
| `resolve_property_by_name(name)` | Fuzzy defendant name resolution via PG function |
| `get_bulk_parcel(strap)` | Single parcel lookup from `hcpa_bulk_parcels` |

**Tables:** `hcpa_allsales`, `hcpa_bulk_parcels`
**Factory:** `get_sales_queries()`

---

## `sunbiz_queries.py`

Sunbiz UCC/FLR filing queries and entity lookups. Raw SQL via `text()`.

| Method | Description |
|---|---|
| `search_filings_by_debtor(name)` | Fuzzy UCC filing search by debtor name (pg_trgm) |
| `search_filings_by_secured_party(name)` | UCC filing search by secured party |
| `get_filing_details(filing_number)` | Full filing detail (filing + parties + events) |
| `get_active_liens_for_debtor(name)` | Active UCC/federal liens for a debtor |
| `check_owner_ucc_exposure(owner_name)` | Quick yes/no exposure check for auction screening |
| `get_ucc_summary_for_auction(owner, defendant)` | Aggregated UCC summary for property detail page |
| `search_entities_by_name(name)` | LLC/Corp/Partnership entity search |
| `get_entity_profile(dataset_type, doc_number)` | Full entity profile with officers and events |
| `get_entity_table_stats()` / `get_table_stats()` | Row counts for monitoring |

**Tables:** `sunbiz_flr_filings`, `sunbiz_flr_parties`, `sunbiz_flr_events`, `sunbiz_entity_filings`, `sunbiz_entity_parties`, `sunbiz_entity_events`
**Factory:** `get_sunbiz_queries()`

---

## `type_normalizer.py`

Not a query module — provides canonical type normalization functions for all DB-bound encumbrance/document type strings. Used by both the pipeline ingestion layer and query modules.
