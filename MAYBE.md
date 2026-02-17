# --update Pipeline Review Findings

Date: 2026-02-17
Scope: `--update` pipeline with focus on chain-of-title completion, lis pendens capture, and encumbrance survival analysis.

## Critical

1. ORI exact-reference results are not persisted due to schema mismatch between scraper output and saver input.
- `src/services/step4v2/discovery.py:580` expects `Instrument` or `instrument_number`.
- `src/services/step4v2/discovery.py:597` expects `RecordDate`.
- `src/services/step4v2/discovery.py:598` expects `DocType` or `document_type`.
- `src/scrapers/ori_api_scraper.py:333` returns instrument-search rows as lowercase keys: `instrument`, `record_date`, `doc_type`, etc.
- `src/scrapers/ori_api_scraper.py:553` returns plat/book-page rows with the same lowercase key style.
- Effect: searches succeed but documents are dropped before insert.
- Evidence from live DB:
  - instrument searches with results but zero new docs: `1993/1993`
  - plat searches with results but zero new docs: `63/63`

## High

2. Chain completeness can be marked true without a deed chain.
- `src/services/step4v2/discovery.py:976` checks MRTA span first.
- `src/services/step4v2/discovery.py:978` returns complete when span >= 30 years.
- `src/services/step4v2/discovery.py:1184` span uses all docs, not deed transfers.
- Effect: long-running non-deed documents can terminate discovery as “complete” with no chain periods.

3. ORI step is marked complete even when discovery output is unusable.
- `src/orchestrator.py:1793` runs discovery.
- `src/orchestrator.py:1811` marks `step_ori_ingested` complete unconditionally.
- `src/orchestrator.py:1080` also marks `needs_ori_ingestion` complete regardless of discovery quality.
- `DiscoveryResult.is_complete` and `stopped_reason` are not used for gating.
- Evidence from live DB:
  - cases with `step_ori_ingested` set but `0` `chain_of_title` rows: `33`

4. Gap-bounded name searches are silently ignored by uniqueness constraints.
- `src/services/step4v2/discovery.py:1386` and `src/services/step4v2/discovery.py:1411` insert with `INSERT OR IGNORE`.
- `src/db/operations.py:1507` unique key is `(folio, search_type, search_term, search_operator)`, excluding date bounds.
- Effect: bounded searches collide with existing unbounded name searches and do not get queued.

## Medium

5. Survival is marked complete even when no survival updates are written.
- `src/orchestrator.py:1946` warns on zero `survival_updates`.
- `src/orchestrator.py:1983` and `src/orchestrator.py:1987` still mark survival complete.
- Evidence from live DB:
  - cases with `step_survival_analyzed` but no encumbrance survival status updates for that case folio: `11`

6. Encumbrances are never linked to chain periods (`chain_period_id` stays null).
- `src/services/step4v2/chain_builder.py:121` builds encumbrances before period IDs exist.
- `src/services/step4v2/chain_builder.py:322` skips periods where `id is None`.
- `src/services/step4v2/chain_builder.py:143` only assigns period IDs later during save.
- Evidence from live DB:
  - encumbrances with `chain_period_id IS NULL`: `9388/9388` (100%)

7. Legal-search short-circuit can cancel useful permutations too early.
- `src/services/step4v2/discovery.py:493` uses returned count (`valid_doc_count`) rather than newly inserted docs.
- `src/services/step4v2/discovery.py:495` cancels pending legal searches even if returned docs are duplicates/noisy.

## Current Impact Snapshot (live SQLite)

DB: `/home/user/hills_data/property_master_sqlite.db`

- Foreclosures with extracted judgment: `123`
- Chain-of-title folios: `78` (`63.4%`) — below 80% target
- Encumbrance folios: `99` (`80.5%`)
- Survival-status folios: `98` (`79.7%`) — below 80% target

## Fix Review (2026-02-17)

Review target: claimed fixes for issues `#1`-`#7` in this file.

### Agreed

1. `#1` is fixed in code: Step4v2 now accepts lowercase ORI keys (`instrument`, `record_date`, `doc_type`, `legal`, `book_num`, `page_num`) in save/vector/reference paths.
2. `#2` is fixed in code: MRTA fallback years now filters to deed document types in `_calculate_chain_years()`.
3. `#4` is fixed in code: queue uniqueness now includes date bounds in schema and upsert conflict target.
4. `#5` is fixed in code: survival completion is now gated on `survival_updates` or `encumbrance_count == 0`.
5. `#6` is fixed in code: periods are saved before encumbrance build so `chain_period_id` can be assigned.
6. `#7` is fixed in code: legal short-circuit now uses `new_count` (newly inserted docs), not returned result count.

### Not Fully Fixed

1. `#3` is only partially fixed.
- `src/orchestrator.py:1081` still marks `needs_ori_ingestion` complete unconditionally after `_run_ori_ingestion(...)`, even if Step4v2 produced no usable chain output.
- `src/orchestrator.py:1812` still treats `stopped_reason == "exhausted"` as complete, even when `len(chain_result.periods) == 0`, which can still mark ORI complete with unusable chain output.
- `src/orchestrator.py:1042` and `src/orchestrator.py:1047` still mark ORI complete in the no-legal/no-party fallback path (manual review path), which prevents automatic retries despite missing chain data.
