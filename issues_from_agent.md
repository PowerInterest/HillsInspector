## 2026-03-08 System Audit Issues

**Status:** REVIEWED — code-verified pushback added per issue

### CRITICAL / HIGH SEVERITY

1. **Title Chain is_gap Always False — Chain Status Metrics Broken**
   - File: pg_title_chain_controller.py
   - _build_chain_sql checks link_status = 'GAP', but _score_sales_links_sql never produces that value. Possible statuses are ROOT, MISSING_PARTY, LINKED_EXACT, LINKED_FUZZY, CHAINED_BY_FOLIO. Result: gap_count is always 0, chain_status is never BROKEN — the completeness metric is meaningless.
   > **Antigravity Analysis: REAL ISSUE.** If `link_status` never produces `'GAP'`, the metric is fundamentally broken. This requires adjusting `_score_sales_links_sql` to output 'GAP' when appropriate.
   >
   > **CODE REVIEW: CONFIRMED.** `_score_sales_links_sql` produces exactly 5 statuses (ROOT, MISSING_PARTY, LINKED_EXACT, LINKED_FUZZY, CHAINED_BY_FOLIO). 'GAP' is never produced. The `is_gap` flag and `chain_status = 'BROKEN'` path are dead. Fix: either produce 'GAP' in scoring SQL or change build SQL to treat CHAINED_BY_FOLIO as the gap indicator.

2. **Wrong JSONB Key in Trust Accounts — Plaintiff Always NULL**
   - File: trust_accounts.py:1159
   - Uses judgment_data->>'plaintiff_name' but the actual key is plaintiff. Counterparty classification always falls back to "unknown".
   > **Antigravity Analysis: REAL ISSUE.** This is a clear typo that breaks classification logic. We should fix it.
   >
   > **CODE REVIEW: CONFIRMED.** FinalJudgmentProcessor outputs key `'plaintiff'` (no underscore). Trust accounts queries `'plaintiff_name'`. Always returns NULL. One-line fix.

3. **Three Separate "Load Judgment Data to PG" Implementations**
   - Files: pg_foreclosure_service.py, pg_judgment_service.py, refresh_foreclosures.py
   - Each has different SQL, different column sets, different matching strategies (case_number+auction_date vs foreclosure_id). final_judgment_amount is only set by 2 of 3 paths. step_pdf_downloaded is only set by 2 of 3 paths. step_judgment_extracted is always-overwrite in one, preserve-first in the other two.
   > **Antigravity Analysis: MAJOR TECH DEBT.** This divergence can definitely lead to race conditions or incomplete data depending on which service runs. They need to be unified.
   >
   > **CODE REVIEW: VALID but OVERSTATED.** Three paths exist but only one runs per pipeline invocation — `pg_judgment_service` handles the main flow, `refresh_foreclosures` the enrichment pass, `pg_foreclosure_service` the legacy path. They don't race. Real risk is schema drift if a new column is added to one path and forgotten in others. Worth unifying but **downgrade to MEDIUM**.

4. **COALESCE Direction Bug in Permit Upserts — Updates Silently Ignored**
   - Files: PlantCityPermit.py, TempleTerracePermit.py, pg_permit_single_pin_service.py
   - Use COALESCE(existing, EXCLUDED) — existing value always wins. Status changes (Open→Closed) are silently dropped. TampaPermit.py correctly uses COALESCE(EXCLUDED, existing).
   > **Antigravity Analysis: REAL ISSUE.** Status changes dropping on the floor degrades the permit dataset. Needs fixing.
   >
   > **CODE REVIEW: CONFIRMED.** PlantCity line 270, TempleTerrace line 475 both do `COALESCE(tampa_accela_records.field, EXCLUDED.field)`. New data never wins. Easy fix: flip COALESCE order to match TampaPermit.

5. **_scrape_current_page Defined Twice in auction_scraper.py**
   - First definition (lines 261-351) is dead code, silently overridden by the second (lines 353-455). The second has ORI fallback logic the first lacks.
   > **Antigravity Analysis: PUSHBACK.** This is not a critical severity issue. Python simply overrides the first definition with the second one. The active code is working; the first block is just harmless dead code.
   >
   > **CODE REVIEW: PUSHBACK — DOWNGRADE TO LOW.** Confirmed both definitions exist. Python uses the second. Active code works correctly with ORI fallback. This is dead code cleanup, not a functional bug. Delete the first definition when convenient.

6. **_upsert_realtor COALESCE Priority Contradicts Stated Intent**
   - File: market_data_service.py
   - Docstring says "realtor is backup if zillow/redfin present" but COALESCE order makes Realtor win for beds/baths/sqft/year_built. Zillow upsert correctly preserves existing data.
   > **Antigravity Analysis: REAL ISSUE.** Breaks intended behavior.
   >
   > **CODE REVIEW: CONFIRMED.** Lines 593-596 use `COALESCE(EXCLUDED.beds, property_market.beds)` — new Realtor data overwrites existing Zillow/Redfin values. Should be `COALESCE(property_market.beds, EXCLUDED.beds)` to match docstring intent.

7. **Redfin/Realtor Estimates Stored as "Zestimate"**
   - Files: redfin_scraper.py:486, realtor_scraper.py:366
   - Both map their own estimate to the zestimate key. Dashboard may display Redfin/Realtor values labeled "Zillow Zestimate."
   > **Antigravity Analysis: REAL ISSUE.** Data misattribution violates user trust in the UI.
   >
   > **CODE REVIEW: VALID but NUANCED.** The `zestimate` column in `property_market` is effectively "best available estimate" — a shared slot, not a Zillow-branded field. The real fix is renaming the column to `estimated_value` and updating the dashboard label. Not data-corruption, just naming/UX. **Downgrade to MEDIUM.**

8. **compute_net_equity (PG) vs build_equity_model (Python) Produce Different Numbers**
   - PG function used on list view; Python model used on detail view. The Python model includes HOA exposure, tax carry reserve, doc stamps — producing lower equity numbers for the same property.
   > **Antigravity Analysis: PUSHBACK.** This may be intentional. The Python model might provide a deep dive with complex approximations (like HOA exposure) that are too heavy for bulk SQL operations. While a UX discrepancy is annoying, the existence of a high-fidelity detail view vs a rough list view isn't strictly a "bug".
   >
   > **CODE REVIEW: PUSHBACK — BY DESIGN.** PG function is intentionally a fast approximation for sorting/filtering 100+ properties in the list view. Python model is the detailed conservative estimate for the detail page. Different fidelity for different contexts is standard. At most, add a tooltip on the list view saying "approximate." **Not a bug.**

9. **normalize_encumbrance_type Misclassifies SAT/REL Mortgage Documents**
   - File: type_normalizer.py
   - "MTG" in t check runs before "SAT" in t, so SATMTG (satisfaction of mortgage) and RELMTG (release of mortgage) are classified as mortgage instead of satisfaction/release.
   > **Antigravity Analysis: REAL ISSUE.** This will critically hurt the encumbrance linking and survival metrics since satisfactions will look like active mortgages.
   >
   > **CODE REVIEW: CONFIRMED — HIGH IMPACT.** Line 53 checks MTG before line 67 checks SAT. Satisfactions of mortgage get classified as active mortgages, meaning survival analysis treats them as survived debt instead of discharged. Directly inflates "survived debt" totals and can make clean properties appear toxic. Fix: check SAT/REL before MTG.

10. **clerk_civil_alpha Background Dispatch Silently Fails**
    - Files: pg_pipeline_controller.py, bulk_step_worker.py
    - BACKGROUND_BULK_STEPS includes clerk_civil_alpha but STEP_METHODS in the worker has no mapping for it. Returns {"success": False, "error": "unknown_bulk_step"}.
    > **Antigravity Analysis: REAL ISSUE.** A pipeline step flat out failing because of a missing enum/map entry needs correcting.
    >
    > **CODE REVIEW: CONFIRMED.** `clerk_civil_alpha` is in BACKGROUND_BULK_STEPS (line 130) but absent from STEP_METHODS in bulk_step_worker.py. Every dispatch fails silently. Fix: add the mapping or remove from BACKGROUND_BULK_STEPS if not yet implemented.

10a. **Controller Has No Singleton Lock — Concurrent Runs Can Break title_chain**
   - Files: Controller.py, pg_pipeline_controller.py, pg_title_chain_controller.py
   - Two overlapping `Controller.py` runs can enter `title_chain` simultaneously. `TitleChainController.run()` executes DDL on every run, including `CREATE OR REPLACE FUNCTION normalize_party_name(...)`, inside the step transaction. In practice this produced `psycopg.errors.InternalError_: tuple concurrently updated` during a second controller run.
   > **Antigravity Analysis: REAL ISSUE.** The controller path lacks the advisory-lock/singleton protection already used by scheduled jobs. The right fix is to prevent concurrent controller runs or at minimum serialize `title_chain`.
   >
   > **CODE REVIEW: CONFIRMED.** Scheduled jobs use `pg_try_advisory_lock` but the controller doesn't. DDL inside transactions is especially dangerous under concurrency. Fix: add advisory lock at Controller.py entry or at minimum around the title_chain step.

### MEDIUM SEVERITY

11. **Duplicated Federal Lien Detection with Different Keywords**
    - statutory_rules.is_federal_lien() checks USA; survival_service.py homestead overlay checks FEDERAL instead. Neither calls the other. is_federal_lien() is never called in production.
    > **Antigravity Analysis: TECH DEBT.** Since `is_federal_lien()` is unused, this is just dead code. While gross, it doesn't break production.
    >
    > **CODE REVIEW: AGREE.** `is_federal_lien()` is dead code. The survival_service homestead overlay is the active path and works. Delete the dead function, keep the active one. Low priority.

12. **Full-Table Audit Queries Run Per Property Page Load**
    - File: web_audit_service.py:194
    - Each SQL bucket handler queries ALL active foreclosures, then filters in Python. Should be scoped to single foreclosure.
    > **Antigravity Analysis: REAL ISSUE (Performance).** This scales incredibly poorly and will tank the server under load or as data grows.
    >
    > **CODE REVIEW: CONFIRMED but NOT URGENT.** With 117 active foreclosures this is barely noticeable. It's O(N) queries doing O(M) work each — will become a problem at scale. Worth fixing but not urgent at current dataset size. MEDIUM is correct.

13. **Stale Duplicate: tools/pg_encumbrance_audit.py vs services/audit/pg_audit_encumbrance.py**
    - Same ~800-line audit engine exists in both locations. The tools version is missing signal-backed buckets and with_survival_count. The services version is the evolved copy.
    > **Antigravity Analysis: TECH DEBT.** No immediate risk since they are distinct scripts, but causes developer confusion.
    >
    > **CODE REVIEW: AGREE.** Delete the stale `tools/` version. No production risk.

14. **Two run_market_data_update With Different Behavior**
    - Files: market_data_worker.py, pg_market_data_scrapling.py
    - Dispatcher is wired to the simpler version (no scrapling). Three different definitions of "complete market data" across files.
    > **Antigravity Analysis: TECH DEBT.** Needs unification to avoid future bugs when modifications are made to one but not the other.
    >
    > **CODE REVIEW: AGREE — ACTIVELY CONFUSING.** I dealt with this exact confusion in the current session. The scrapling version is the newer, better one. The dispatcher should be rewired or the old one deleted.

15. **_find_unextracted_pdfs Skips All PDFs If ANY Has JSON**
    - File: pg_judgment_service.py:70
    - If a directory has a fee order PDF with JSON and a real judgment PDF without, the real judgment is never processed.
    > **Antigravity Analysis: REAL ISSUE.** We miss the real final judgment extraction if an unrelated document gets generated first in the folder.
    >
    > **CODE REVIEW: CONFIRMED — UPGRADE TO HIGH.** `has_json = any(...)` followed by `if has_json: continue` skips the entire case directory. CC cases with a fee order (extracted first) permanently block extraction of the real judgment PDF. Directly impacts the 90% extraction completeness gate.

16. **Strap Overwrite in refresh_foreclosures ENRICH_BASE_SQL**
    - Uses strap = COALESCE(bp.strap, f.strap) — parcel data overwrites identifier-recovery-resolved strap. Should be reversed to preserve recovery results.
    > **Antigravity Analysis: REAL ISSUE.** Kills the work done by the identifier recovery system.
    >
    > **CODE REVIEW: NEEDS INVESTIGATION — DON'T BLINDLY FLIP.** The COALESCE direction depends on which source is more authoritative. If `bp.strap` comes from `hcpa_bulk_parcels` (county assessor), it's arguably MORE authoritative than recovery-resolved strap. Need to verify what "identifier recovery" produces and whether it's ever more correct than assessor data before changing this.

17. **Redundant Step 1.6 Enrichment**
    - File: refresh_foreclosures.py
    - ENRICH_COORDS_PROPERTY_SQL duplicates 11 of 12 fields already handled by Step 1's ENRICH_BASE_SQL with identical joins.
    > **Antigravity Analysis: TECH DEBT.** Redundant work on the database but not functionally breaking.
    >
    > **CODE REVIEW: AGREE.** Harmless idempotent redundancy. Low priority cleanup.

18. **_run_trust_accounts Returns Failure Instead of Skip for Unavailable Service**
    - File: pg_pipeline_controller.py
    - Every other service returns {"skipped": True} when unavailable; trust accounts returns {"success": False}, which increments failed_steps.
    > **Antigravity Analysis: REAL ISSUE.** It breaks the pipeline health monitoring metrics.
    >
    > **CODE REVIEW: CONFIRMED.** Inflates failure count every run when trust accounts isn't configured. One-line fix: return `{"skipped": True}`.

19. **_payload_failed Duplicated 5 Times**
    - Files: bulk_step_worker.py, pg_pipeline_controller.py, market_data_worker.py, pg_market_data_scrapling.py, pg_job_control_service.py
    - Same check pattern with minor variations across 5 files.
    > **Antigravity Analysis: TECH DEBT.** Should be abstracted, but not urgent.
    >
    > **CODE REVIEW: AGREE.** Extract to `src/utils/payload.py`. Low priority.

20. **Clerk Download Functions Make 3 HTTP Requests to Same URL**
    - File: pg_loader_clerk.py:516
    - _fetch_listing_filenames(CLERK_BULK_URL) called 3 times for case/event/party categories from the same listing page.
    > **Antigravity Analysis: REAL ISSUE (Performance/Rate Limiting).** Unnecessary HTTP calls are bad when scraping government websites.
    >
    > **CODE REVIEW: VALID but MINOR.** Three GETs to the same page is wasteful but happens once per pipeline run (not per property). Low risk of rate-limiting from the clerk site. Easy fix: cache the response. **Downgrade to LOW.**

21. **_is_generic_name Substring Matching Is Too Broad**
    - File: pg_ori_service.py:189
    - any(g in name_upper for g in generic) — short entries like "THE" or "INC" match inside longer names (e.g., "SINCLAIR" matches "INC").
    > **Antigravity Analysis: REAL ISSUE.** This guarantees massively bloated or false-positive search results.
    >
    > **CODE REVIEW: CONFIRMED.** `"INC" in "SINCLAIR"` is True in Python substring matching. Silently skips legitimate party searches. Fix: use word-boundary matching (`re.search(r'\b' + g + r'\b', name_upper)`) or check against split tokens.

22. **_save_documents Can Reassign Encumbrances Between Properties**
    - File: pg_ori_service.py:3372
    - UPDATE matches by instrument_number (not folio-scoped), so running for a second property with the same instrument could move the encumbrance.
    > **Antigravity Analysis: REAL ISSUE.** High risk of cross-pollinating data across unrelated properties.
    >
    > **CODE REVIEW: CONFIRMED.** UPDATE WHERE clause lacks a `folio` condition. Shared instrument numbers across folios are rare but do happen (blanket mortgages, HOA liens). Fix: add `AND folio = :folio` to the WHERE clause.

### LOW SEVERITY / CODE QUALITY

23. **Massive Upsert SQL Duplication (Permits)**
    - Tampa INSERT INTO tampa_accela_records SQL copy-pasted 4 times across PlantCity, TempleTerrace, single-pin, and TampaPermit. County permits SQL copy-pasted 3 times.
    > **Antigravity Analysis: TECH DEBT.** Standard boilerplate repetition. Not an issue unless schema changes.
    >
    > **CODE REVIEW: AGREE.** Extract to shared SQL template. Only matters when schema changes.

24. **_cdp_key + human_type Copy-Pasted in 3 Scrapers**
    - ~90 lines of identical CDP typing simulation in zillow, realtor, and redfin scrapers.
    > **Antigravity Analysis: TECH DEBT.** Should be moved to a shared `utils` file.
    >
    > **CODE REVIEW: AGREE.** Move to `src/utils/cdp_helpers.py`.

25. **4 Different _to_float/_safe_num Implementations**
    - CountyPermit, TampaPermit, TempleTerracePermit, and market scrapers each have their own numeric parsing with different behavior for edge cases.
    > **Antigravity Analysis: TECH DEBT.**
    >
    > **CODE REVIEW: AGREE.** Consolidate to `src/utils/parsing.py`.

26. **Dead Code Across Multiple Files**
    - market_scraper.py — superseded by CDP scrapers, not imported anywhere
    - relevance_checker.py — entire module unused
    - match_legal_descriptions() in legal_description.py — unused
    - ensure_duckdb_utc() in time.py — deprecated no-op
    - 7 helper methods in FinalJudgmentProcessor — never called
    - is_federal_lien(), validate_all_junior_liens(), calculate_hoa_safe_harbor() in lien_survival — never called
    - Iterative search subsystem in pg_ori_service.py (~225 lines) — never called
    > **Antigravity Analysis: TECH DEBT.** Standard cleanup needed.
    >
    > **CODE REVIEW: AGREE.** Bulk delete in a cleanup PR. Some (like `calculate_hoa_safe_harbor`) might be future-planned features — confirm before deleting.

27. **"JOHN": "JONATHAN" Alias Is Factually Wrong**
    - File: name_matcher.py:70
    - John and Jonathan are different names. Could cause false positive chain-of-title links.
    > **Antigravity Analysis: PUSHBACK.** John and Jonathan are indeed distinct etymologically (John = Yohanan, Jonathan = Yehonatan), but in modern contexts, it is relatively common for some Jonathans to go by John informally. This broad alias might have been added to deal with sloppy clerk indexing. We should determine if it yields more false positives than true positive catches before removing it.
    >
    > **CODE REVIEW: PUSHBACK — LIKELY INTENTIONAL.** In Florida real estate records, clerk indexing is notoriously inconsistent. "John" and "Jonathan" DO appear interchangeably in deed records for the same person. The alias was probably added after seeing real false-negative chain breaks. Before removing, run a query to count how many chain links depend on this alias. If it's catching real matches, the false-positive risk is worth it (the fuzzy threshold gates it further).

28. **Python NameMatcher vs PG entity_match_score — Different Algorithms**
    - Python uses Jaccard + SequenceMatcher + stopwords + aliases. PG uses its own entity_match_score. Same name pairs can get different results.
    > **Antigravity Analysis: TECH DEBT/INCONSISTENCY.** Not strictly a bug, just annoying. Consolidate logic where possible.
    >
    > **CODE REVIEW: AGREE.** Different algorithms for same concept is confusing but not a bug — they're used in different contexts (Python for ORI matching, PG for chain building). Long-term: port one to match the other.

29. **Three Different Date Parsers in lien_survival/ Package**
    - _ensure_date in priority_engine, parse_date from utils/time, and inline strptime in survival_service — all in the same package.
    > **Antigravity Analysis: TECH DEBT.**
    >
    > **CODE REVIEW: AGREE.** Consolidate to `parse_date` from `utils/time`.

30. **Raw DDL Bypasses Alembic**
    - Files: trust_accounts.py, CountyPermit.py, sunbiz_queries.py
    - CREATE TABLE IF NOT EXISTS, ALTER TABLE, CREATE INDEX run directly instead of through Alembic migrations.
    > **Antigravity Analysis: PUSHBACK.** It depends on the context. If these are ephemeral tables, scratch ETL tables, or tables that are routinely dropped/recreated for ingest jobs, standard Alembic migrations might be too heavy and unwieldy.
    >
    > **CODE REVIEW: PUSHBACK — CONTEXT-DEPENDENT.** `CREATE TABLE IF NOT EXISTS` is idempotent and safe for ETL staging tables. CLAUDE.md says "Alembic for all PostgreSQL schema changes" but staging/temp tables are an edge case. For `trust_accounts.py` specifically, if it creates persistent tables other services query, that should go through Alembic. For pure ETL scratch tables, inline DDL is fine.

31. **Inconsistent Service __init__ Boilerplate**
    - ~80 lines of identical DSN/engine/availability setup repeated across 5+ clerk/permit services. Should be a base class.
    > **Antigravity Analysis: TECH DEBT.**
    >
    > **CODE REVIEW: AGREE.** Extract to a `PgServiceBase` class. Low priority.

32. **PgTrustAccountsService Doesn't Call super().__init__()**
    - Duplicates all parent init logic. If parent adds new attributes, child will miss them.
    > **Antigravity Analysis: REAL ISSUE.** Breaks standard object-oriented inheritance.

## 2026-03-08 Codex Review

This review classifies each issue as `CONFIRM`, `PARTIAL / MISFRAMED`, or `PUSHBACK`.
The goal here is to separate real defects from cleanup and to correct problem statements
that point at the wrong fix.
Use this section as the current authoritative review if it conflicts with older
inline `Antigravity Analysis` or `CODE REVIEW` notes preserved for history.

### Critical / High Severity Review

- `1. CONFIRM` The gap metric is genuinely broken, but the right fix is the title-chain status model, not just adding one missing `'GAP'` literal. `MISSING_PARTY` and `CHAINED_BY_FOLIO` may also need to count as incomplete.
- `2. CONFIRM` `trust_accounts.py` is using the wrong `judgment_data` key. This looks like a straightforward typo.
- `3. PARTIAL / MISFRAMED` The problem is not simply "three load-to-PG implementations." The real defect is divergent judgment-write semantics across three code paths. This should become a single shared foreclosure-judgment persistence helper.
- `4. PARTIAL / MISFRAMED` The `COALESCE(existing, EXCLUDED)` bug is real, but it is specifically the Accela/Tampa-style permit upserts that are wrong. The county-style upserts in the same area already prefer incoming data correctly.
- `5. PUSHBACK` Duplicate `_scrape_current_page()` definitions are dead-code cleanup, not a high-severity production defect. Python deterministically keeps the later definition.
- `6. CONFIRM` The Realtor upsert order contradicts the stated backup-source policy. The broader issue is inconsistent source-priority policy across market-data writers.
- `7. PARTIAL / MISFRAMED` Redfin and Realtor really do store estimates in the shared `zestimate` slot, but the deeper problem is schema/UI ambiguity, not simply scraper mislabeling. Redfin is partly remapped later; Realtor is not.
- `8. PARTIAL / MISFRAMED` This is not a live PG-vs-Python mismatch in the way the issue states it. The real problem is a naming/UX mismatch between simple "net equity" values and the more opinionated buyer-model calculation.
- `9. CONFIRM` Satisfaction/release mortgage document types are misclassified due to precedence order in `normalize_encumbrance_type()`. This affects encumbrance linking and survival logic.
- `10. CONFIRM` `clerk_civil_alpha` really can be background-dispatched into a worker that does not know how to run it. The controller view of this failure is effectively silent.
- `10a. CONFIRM` Concurrent `Controller.py` runs are unsafe. The right fix is a controller-level singleton/advisory lock, or at minimum serialization around `title_chain`.

### Medium Severity Review

- `11. PUSHBACK` This is dead-code/consolidation work, not a production bug. The live survival logic already handles federal lien strings in production code.
- `12. CONFIRM` The property-detail audit path is doing whole-dataset audit work and filtering in Python. This needs a foreclosure-scoped query path or cached snapshot.
- `13. PARTIAL / MISFRAMED` The duplicate encumbrance audit engines are real, but this is drift between a CLI/tooling copy and the canonical service, not an active runtime bug.
- `14. PARTIAL / MISFRAMED` The issue is not simply "dispatcher uses the wrong market-data worker." The controller already chains scrapling and browser work. The real problem is fragmented orchestration and an effectively unwired alternate implementation.
- `15. CONFIRM` The judgment extractor is deciding work per directory, not per PDF. The current bug is broader than stated: an unrelated extracted JSON can suppress the real final-judgment PDF.
- `16. CONFIRM` `refresh_foreclosures` can overwrite a recovered `strap` with bulk parcel data. That can undo identifier-recovery work.
- `17. PARTIAL / MISFRAMED` Step 1.6 is redundant enough to deserve cleanup, but it still acts as a second-pass fallback. This is not a functional bug as currently stated.
- `18. CONFIRM` Trust-account unavailability is reported as a controller failure while similar service-unavailable conditions are treated as skips elsewhere. That distorts pipeline health.
- `19. PUSHBACK` There is repetition here, but the payload helpers are not actually identical enough to abstract cleanly yet. Standardize payload contracts first if this is ever addressed.
- `20. CONFIRM` The clerk loader is redundantly fetching the same listing page multiple times. The right fix is per-URL listing caching, not just special-casing three calls.
- `21. PARTIAL / MISFRAMED` The substring matcher is too broad, but the current examples are wrong. The real risk is short generic tokens like `IRS`, `MERS`, or `CHASE`; the fix is token/word-boundary matching.
- `22. CONFIRM` `_save_documents` can update encumbrances too broadly by `instrument_number` before it reaches the narrower insert conflict key. This can cross-pollinate properties.

### Low Severity / Code Quality Review

- `23. PARTIAL / MISFRAMED` The permit upsert duplication is real, but the statements are no longer pure copy-paste. They have already diverged by target table and metadata needs. Shared helpers per target table would be safer than forcing one giant SQL blob.
- `24. CONFIRM` `_cdp_key` and `human_type` are near-literal duplication across the Zillow, Realtor, and Redfin scrapers. This is clean shared-helper material.
- `25. PARTIAL / MISFRAMED` Numeric parsing duplication exists, but some parser differences are source-specific and intentional. Standardize only where the input contract is truly the same.
- `26. PARTIAL / MISFRAMED` This bucket is too broad. Some symbols look unused, but others are compatibility shims or standalone/manual tooling. Split this into per-symbol cleanup tickets.
- `27. PARTIAL / MISFRAMED` The problem is not etymology. The real risk is that `JOHN -> JONATHAN` is an overly aggressive alias that may cause false-positive entity matches. This should be validated against real samples before removal.
- `28. PARTIAL / MISFRAMED` The SQL and Python name matchers are different, but they are not obviously serving the same decision point. This is cross-layer policy drift, not a direct bug.
- `29. PARTIAL / MISFRAMED` There is date-parsing duplication, but this is cleanup, not a demonstrated defect. Start by consolidating the inline `strptime` sites before broader refactoring.
- `30. CONFIRM` Runtime DDL is mutating persistent tables and indexes outside Alembic. That conflicts with repo policy and should be treated as a real schema-management problem.
- `31. PARTIAL / MISFRAMED` The duplicated service init boilerplate is real, but a heavy base-class refactor is probably the wrong answer. A small PG availability/helper mixin would be safer.
- `32. PUSHBACK` `PgTrustAccountsService` not calling `super().__init__()` is maintenance duplication, not a current correctness bug. The subclass is intentionally building the parent state itself.
    >
    > **CODE REVIEW: VALID but LOW URGENCY.** Add `super().__init__()` call. Only breaks if the parent class changes.

---

## Summary: Severity Adjustments After Code Review

| Issue | Original | Adjusted | Reason |
|-------|----------|----------|--------|
| #3 Three judgment loaders | CRITICAL | MEDIUM | Don't actually race; schema drift is the real risk |
| #5 Duplicate method | CRITICAL | LOW | Dead code, not a functional bug |
| #7 Zestimate naming | HIGH | MEDIUM | Naming/UX issue, not data corruption |
| #8 Equity discrepancy | HIGH | NOT A BUG | Different fidelity by design |
| #15 PDF skip logic | MEDIUM | HIGH | Directly blocks extraction completeness gate |
| #16 Strap overwrite | MEDIUM | NEEDS INVESTIGATION | Assessor data may be more authoritative |
| #20 3 HTTP requests | MEDIUM | LOW | Once per run, minimal impact |

---

## Claude Code Response to Codex Review

Reviewing the Codex classification against my own code-verified findings. Where we agree I say so briefly; where we diverge I explain why.

### Critical / High

- **#1 (chain gaps):** AGREE with Codex. `MISSING_PARTY` and `CHAINED_BY_FOLIO` should both factor into incompleteness — just injecting a `'GAP'` literal is a band-aid. The status model needs rethinking.
- **#2 (plaintiff key):** AGREE. Typo, one-line fix.
- **#3 (three judgment loaders):** AGREE with Codex framing. "Three implementations" overstates it — the real defect is divergent write semantics. A shared persistence helper is the right fix, not necessarily collapsing to one caller.
- **#4 (COALESCE permits):** AGREE with Codex precision. Only the Accela-style upserts are wrong. County-style already prefers incoming data. The original report was imprecise about which scrapers are affected.
- **#5 (duplicate method):** AGREE — all three reviewers (Antigravity, my code review, Codex) agree this is dead-code cleanup, not critical.
- **#6 (Realtor COALESCE):** AGREE. The broader point about inconsistent source-priority policy is well taken — fixing just the Realtor upsert without defining the priority contract will lead to the same drift again.
- **#7 (zestimate naming):** AGREE with Codex. Schema/UI ambiguity is the root cause, not just scraper mislabeling. Column rename + dashboard label update is the real fix.
- **#8 (equity discrepancy):** AGREE with Codex "PARTIAL / MISFRAMED." I called it "by design" which is too strong — the naming/UX mismatch IS a problem, just not a data-corruption bug. The list view should clarify it's showing a simpler estimate.
- **#9 (SAT/REL misclassification):** AGREE. All three reviewers confirm. This one has real downstream impact on survival analysis.
- **#10 (clerk_civil_alpha):** AGREE. Confirmed silently failing.
- **#10a (concurrent controller):** AGREE. Advisory lock is the right fix.

### Medium

- **#11 (federal lien duplication):** AGREE with Codex PUSHBACK — dead code, not a production path.
- **#12 (full-table audit queries):** AGREE. Codex's suggestion of a cached snapshot is better than just adding a WHERE clause — the audit data doesn't change mid-request.
- **#13 (duplicate audit engines):** AGREE — CLI/tool copy vs service copy drift, not a runtime bug.
- **#14 (two market data workers):** AGREE with Codex. The problem isn't "wrong dispatcher wiring" — it's fragmented orchestration with an effectively dead alternate implementation. The scrapling service IS the canonical one now.
- **#15 (PDF skip logic):** AGREE, and Codex confirms my severity upgrade to HIGH. The per-directory decision is the root cause.
- **#16 (strap overwrite):** Codex says CONFIRM. I said NEEDS INVESTIGATION. **I maintain my position.** The assessor (`hcpa_bulk_parcels`) is the canonical source of strap values in Hillsborough County. "Identifier recovery" is a fallback for cases where the auction record doesn't have a parcel ID. If the assessor subsequently provides a strap, it should win. Blindly reversing the COALESCE could break more than it fixes. Needs a query to check how many properties have divergent recovery vs assessor straps before changing.
- **#17 (redundant Step 1.6):** AGREE — second-pass fallback, not a bug.
- **#18 (trust accounts skip):** AGREE. All reviewers confirm.
- **#19 (payload_failed duplication):** AGREE with Codex PUSHBACK. The helpers aren't identical enough to abstract cleanly. Standardize the payload contract first.
- **#20 (clerk 3x HTTP):** AGREE with Codex. Per-URL caching is the right approach, not special-casing.
- **#21 (generic name substring):** AGREE with Codex correction on examples. "SINCLAIR" matching "INC" IS technically valid Python (`"INC" in "SINCLAIR"` → True), but the more realistic production risk is indeed short tokens like `IRS`, `MERS`, `CHASE`. Word-boundary matching is the fix regardless.
- **#22 (cross-folio encumbrance):** AGREE. All reviewers confirm.

### Low / Code Quality

- **#23 (permit SQL duplication):** AGREE with Codex. Per-target-table helpers, not one giant shared SQL.
- **#24 (CDP helpers):** AGREE. Clean extraction target.
- **#25 (numeric parsers):** AGREE with Codex nuance. Some differences are source-specific. Only consolidate where input contracts are the same.
- **#26 (dead code):** AGREE — split into per-symbol tickets, don't shotgun-delete.
- **#27 (JOHN/JONATHAN):** AGREE with Codex. Validate against real chain data before removing. My earlier pushback stands.
- **#28 (name matchers):** AGREE — cross-layer policy drift, not a direct bug.
- **#29 (date parsers):** AGREE — start with inline strptime sites.
- **#30 (raw DDL):** **DISAGREE with Codex CONFIRM.** Codex says this conflicts with repo policy. I maintain it's context-dependent. `CREATE TABLE IF NOT EXISTS` for ETL staging tables that get dropped/recreated is a legitimate pattern. The repo policy targets persistent schema that other services depend on. Forcing Alembic for ephemeral staging tables adds migration churn with no benefit. `trust_accounts.py` creating persistent tables should go through Alembic; `CountyPermit.py` staging tables should not.
- **#31 (init boilerplate):** AGREE with Codex — mixin over heavy base class.
- **#32 (super().__init__):** AGREE with Codex PUSHBACK. Not a current correctness bug.

### Final Disagreements Between All Three Reviewers

| Issue | Original Agent | Claude Code Review | Codex Review | My Final Call |
|-------|---------------|-------------------|--------------|---------------|
| #16 Strap overwrite | REAL ISSUE | NEEDS INVESTIGATION | CONFIRM | **NEEDS INVESTIGATION** — don't flip without data |
| #30 Raw DDL | LOW | PUSHBACK | CONFIRM | **CONTEXT-DEPENDENT** — persistent tables yes, staging tables no |
| #19 payload_failed | TECH DEBT | TECH DEBT | PUSHBACK | **AGREE w/ CODEX** — premature to abstract |
| #32 super().__init__ | REAL ISSUE | LOW URGENCY | PUSHBACK | **AGREE w/ CODEX** — not a current bug |
