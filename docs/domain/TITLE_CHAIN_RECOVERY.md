# Title Chain Recovery Architecture

## The Problem: Data Gaps in Bulk County Records
Hillsborough County's bulk property transfer data (`hcpa_allsales`) provides a fast, comprehensive baseline for building the chain of title. However, this raw data is imperfect:
1. **Missing Transfers (Temporal Gaps):** Properties sometimes change hands without an `hcpa_allsales` record appearing, causing temporal or ownership name gaps in the chain.
2. **Missing Parties (Data Omissions):** Some deeds exist in the data feed (with an instrument number and date) but are mysteriously missing the Grantor and Grantee names.

If we allow these gaps to persist, the pipeline cannot accurately determine who owned a property when a lien was issued, breaking our Lien Survival Analysis algorithm.

## The Solution: Unified Title Recovery Service 
We resolve these issues using **`PgTitleBreakService`** (`src/services/pg_title_break_service.py`), which acts as the definitive handler for bulk data fixups. 

### Why a Separate Service?
Previously, this logic was scattered (e.g., inside the main pipeline controller). Centralizing it allows us to precisely target data failures without cluttering the main ingestion logic.

### 2 Main Fixups Handled
The service automatically runs 2 distinct gap-fillers:

1. **`ORI_DEED_SEARCH`:** 
   - Uses `fn_title_chain_gaps` to detect completely missing transfers (temporal gaps or name mismatches).
   - Automatically searches the Clerk's Official Records (PAV) using the expected party names to find the missing Warranty/Quit Claim Deeds.
2. **`ORI_DEED_BACKFILL`:** 
   - Identifies existing deeds in the `hcpa_allsales` feed that are missing their grantor/grantee names.
   - Fetches the exact deed from the Clerk API to backfill the missing parties.

## Safe Execution: The Overlay Model
**CRITICAL RULE:** We NEVER modify the raw bulk data tables (`hcpa_allsales`, `official_records_daily_instruments`) directly. 

Instead, all recovered information (both found deeds and backfilled names) is safely stored in the **`foreclosure_title_events`** table as an "overlay". 

The dynamic SQL view **`fn_title_chain`** was updated with dynamic CTEs (`UNION ALL` and `LEFT JOIN LATERAL`) to seamlessly stitch these recovery events together with the raw `hcpa_allsales` data on-the-fly.

By treating `foreclosure_title_events` as the priority layer, we guarantee a gapless, accurate chain of title without ever risking the corruption or accidental deletion of the original historical source data during nightly bulk refreshes.

## Materialized Rebuild Contract
The controller's materialized title-chain step must honor the same overlay model as
`fn_title_chain`:

- `TitleChainController` preserves `ORI_DEED_SEARCH` and `ORI_DEED_BACKFILL`
  rows when resetting scoped outputs.
- Rebuilt `SALE` events coalesce missing parties from those overlay rows before
  falling back to `official_records_daily_instruments`.
- `ORI_DEED_SEARCH` rows that represent deeds missing from `hcpa_allsales` are
  injected as synthetic `SALE` events during the rebuild.
- After `title_breaks` writes any repairs, the pipeline immediately reruns the
  title-chain materialization step so `foreclosure_title_chain` and
  `foreclosure_title_summary` reflect the repaired chain in the same controller
  run.
