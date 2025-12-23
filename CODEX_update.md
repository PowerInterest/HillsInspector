# CODEX Update (Chain of Title + Liens/Encumbrances + Web Wiring)

Date: 2025-12-18

This note documents the changes made in this workspace around:
- Chain of title construction + display
- Lien/encumbrance survival analysis + display
- DuckDB schema migration hardening
- Web UI wiring so the “foreclosing lien” and chain quality are visible and consistent

## Summary of What Changed

### 1) Name matching (token normalization + tightening)
- File: `src/utils/name_matcher.py`
- Changes:
  - Stop tokenizing away to noisy single-letter tokens (initials are dropped).
  - Stopword list uses `"AS"` (not `"A"`); `"A"` is no longer a stopword.
  - Tightened subset/superset matching to reduce false positives on single surnames.
- Why:
  - Single-letter tokens inflate false matches in chain linkage and joined-defendant checks.
  - “A” is too common and can represent real meaning (e.g., “A TRUST”), while “AS” is the typical legal connector we want to ignore.
  - Subset/superset logic is powerful but dangerous unless constrained (e.g., `"SMITH"` matching everything).

### 2) DuckDB migrations are now resilient (no more “missing column” runtime errors)
- File: `src/db/operations.py`
- Changes:
  - Added `_apply_schema_migrations()` and call it automatically from `PropertyDB.connect()`.
  - Migrations are intentionally *lightweight and idempotent* (only `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).
  - Ensures `encumbrances.is_joined` and `encumbrances.is_inferred` exist before any inserts/updates rely on them.
- Why:
  - We hit the exact failure mode already: code expects a new column, but the existing DB file hasn’t been migrated.
  - Relying on “someone calls `create_chain_tables()`” is brittle; DB schema needs to self-heal on connect.

### 3) Chain-of-title “BROKEN” reduction: prevent false breaks caused by irrelevant deeds
- File: `src/services/title_chain_service.py`
- Changes:
  - Added `_select_best_deed_path()` and use it to build a single most-plausible deed path, walking backward from the most recent deed.
  - Removed hard `BROKEN` statuses in this service path; true gaps are now flagged as `INCOMPLETE` with gap metadata (so you still see the defect, but don’t blow up the chain).
- Why:
  - ORI legal-description searches can return unrelated deeds from the same subdivision or broad legal snippet.
  - Treating every returned deed as part of one strict chronological chain produces false “broken chain” flags.
  - The correct model is “find the best supported chain for *this parcel*”, not “all deeds returned are one chain”.

### 4) Lien survival analyzer: correctness fixes + safer “joined/omitted” handling + foreclosing-lien inference
- File: `src/services/lien_survival_analyzer.py`
- Changes:
  - `is_joined` is now treated as **unknown** when we have no defendants list; we do not infer “omitted” from missing data.
  - Mortgage foreclosure priority now uses the **foreclosing lien’s recording date** (when known) rather than using **lis pendens** as the priority cutoff.
  - Added a fallback to infer the foreclosing lien when judgment refs are missing:
    - selects the best candidate mortgage (pre-lis-pendens) based on name similarity + amount proximity + date proximity
    - marks that foreclosing selection as `is_inferred=True`
- Why:
  - Using lis pendens as “senior vs junior” for all liens is not generally correct (junior liens can be recorded before lis pendens and still be wiped).
  - “Omitted junior lienor survives” is legally important but should only be asserted when we truly have the joined-defendants list; otherwise it creates false survivals.
  - Users asked: “Which lien is foreclosing?” — if judgment parsing misses book/page/instrument, we need a clearly-labeled inference path rather than showing nothing.

### 5) Pipeline writes richer survival metadata to DB
- File: `src/orchestrator.py`
- Changes:
  - Passes `defendants=None` when we have no extracted list (so the analyzer can treat “joined” as unknown instead of false).
  - When analyzer returns `is_inferred=True` for the foreclosing lien, pipeline persists it via `update_encumbrance_survival(..., is_inferred=True)`.
- Why:
  - Avoid false “omitted” logic when defendants are not extracted.
  - Ensure the UI can label a foreclosing lien as “(Inferred)” rather than silently guessing.

### 6) Preserve lien survival flags across chain rebuilds
- File: `src/db/operations.py`
- Changes:
  - `save_chain_of_title()` previously did `DELETE FROM encumbrances WHERE folio = ?` and re-inserted from the new analysis.
  - This can wipe computed `survival_status`, `is_joined`, `is_inferred` if chain is rebuilt (re-ingest).
  - Now:
    - it snapshots prior survival fields keyed by `instrument` and/or `book/page`
    - re-applies them if the incoming encumbrance doesn’t already have survival fields
    - sets `auctions.needs_lien_survival = TRUE` for the folio (so the pipeline can recompute if needed)
- Why:
  - Chain rebuilds are common; survival analysis may be expensive/async; we shouldn’t destroy computed state unless we mean to.
  - Marking `needs_lien_survival` ensures stale survival status can be recalculated deterministically.

### 7) Web server wiring fixes (chain ordering, link_status display, and lien-source de-duplication)
- Files:
  - `app/web/database.py`
  - `app/web/routers/properties.py`
  - `app/web/templates/partials/chain_of_title.html`
  - `app/web/templates/partials/lien_table.html`
  - `app/web/templates/title_report.html`
- Changes:
  - Chain query now orders ascending by `acquisition_date` so it reads chronologically and aligns with service linkage.
  - Removed duplicate, router-local chain break detection (`_names_match`) and instead display `link_status` and `confidence_score` from the DB/service.
  - Liens tab + title report now prefer `encumbrances` as the primary “analyzed” lien set, and only fall back to legacy `liens` table when encumbrances are absent.
  - Lien table shows “FORECLOSING (INFERRED)” and the foreclosing-lien banner adds an “(Inferred)” label.
- Why:
  - We should not compute chain correctness twice with different heuristics (service vs router); it creates contradictions in the UI.
  - Showing both `liens` and `encumbrances` at once is confusing; `encumbrances` is the pipeline-integrated set.
  - Foreclosing lien must be visible and explainable (exact vs inferred).

## What Still Concerns Me (Liens/Encumbrances)

1) **Two sources of truth still exist (`liens` vs `encumbrances`)**
- Even though the UI now prefers `encumbrances`, the `liens` table remains and can drift.
- Long-term, we likely want:
  - either fully deprecate `liens` and unify on `encumbrances`, or
  - explicitly define that `liens` is “raw extracted liens” and `encumbrances` is “normalized + survival-classified”, and ensure there’s a reliable ETL between them.

2) **Joined-defendant accuracy is only as good as Final Judgment extraction**
- If defendants are missing or OCR/extraction is imperfect:
  - `is_joined` becomes unknown (correctly), but then “omitted survives” can’t be asserted.
- This means we need to treat “joined” as a *confidence* signal, not absolute truth, unless we can validate it from court docket joins or a more reliable structured extract.

3) **Foreclosing-lien inference can be wrong**
- The new inference is intentionally labeled `is_inferred=true`, but it can still mis-pick:
  - servicer/trustee vs original lender name mismatch
  - multiple mortgages before lis pendens
  - missing/incorrect amounts
- We should consider adding:
  - a confidence score for inferred foreclosing match
  - alternative candidates list shown in UI when confidence is low

4) **Mortgage foreclosure wiping logic needs more nuance for certain lien types**
- Some liens don’t behave like typical private junior liens (e.g., municipal special assessments, certain code enforcement scenarios, federal liens with redemption, etc.).
- The analyzer currently makes pragmatic rules, but the edge cases can produce wrong “survived vs extinguished”.

5) **Encumbrance satisfaction/assignment tracking depends on references in text**
- In `TitleChainService`, satisfaction/assignment linking is based on instrument/book-page references extracted from legal text and some name matching.
- If the reference parsing misses or the recorded doc uses unusual formatting, we can fail to mark a mortgage as satisfied or update the creditor chain properly.

6) **Chain rebuilds can still clobber survival status if identifiers don’t match**
- The preservation logic uses instrument and/or book/page as keys.
- If a newly ingested doc changes those fields (format differences, missing book/page, instrument normalization), the match may fail and survival status may be lost until re-analysis runs.

## Suggested Next Steps

- Decide whether `liens` is legacy and should be retired, or formally define its role vs `encumbrances`.
- Add a “foreclosing match confidence” and (optionally) show top 2–3 candidate mortgages when inferred.
- Improve defendant/joinder extraction reliability (and/or cross-check against docket party lists).
- Add a small “re-run lien survival” control in the UI for a folio to make recomputation explicit when documents are updated.
