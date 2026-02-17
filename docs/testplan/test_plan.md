# HillsInspector Testing Plan (Revised)

Date: 2026-02-17
Scope: `--update` pipeline reliability, schema contracts, and database state correctness.

## 0) Plain-Language Glossary

- `P0` (Priority 0): the most important tests.  
  If these fail, we should not merge code.
- `Gating`: required checks that must pass before code is merged.
- `CI`: automated checks that run on pull requests (tests/lint/type checks).
- `Fixture`: test setup data (rows/files) created before a test runs.
- `Mock`: fake replacement for a dependency (network call, browser, DB error) to test behavior safely.

## 1) What DI / SF Mean

- `DI` = **Data Integrity** tests
  Verifies DB state and data correctness after a step runs.
- `SF` = **Silent Failure** tests
  Verifies failures are surfaced (logs/errors), not swallowed.

## 2) Pushback On Original Plan

The original plan direction is strong, but implementation needed tighter sequencing:

1. It was too broad for one implementation pass.
2. Some checks depended on brittle exact log strings.
3. DB behavior under WAL/checkpoint should use file-backed temp DBs, not only `:memory:`.
4. Browser/network behavior should be mocked in unit tests.
5. Repo pytest baseline/config needs stabilization first.
6. Some scenarios were conceptual and needed direct code-path targets.

## 3) Core Principles

1. **Safety first:** never test against production DBs.
2. **Behavior over text:** assert state transitions first, logs second.
3. **Deterministic tests:** no real external network in CI unit tests.
4. **Small blast radius:** per-test fresh DB fixture and isolated seed data.
5. **Phased delivery:** build infra first, then highest-risk tests.
6. **Cache-first external testing:** use your stored API-call cache as replay fixtures before considering live calls.

## 4) Implementation Strategy (Phased)

## Phase 0: Test Infrastructure (Required Before New Cases)

Deliverables:

1. Stable pytest invocation for this repo.
2. `tests/conftest.py` fixtures:
   - file-backed temporary SQLite DB fixture (supports WAL testing).
   - schema initialization helper.
   - Pydantic factories or SQL helpers for seeding (`auctions`, `status`, `documents`).
3. Logging assertion pattern (`caplog`) guidance.
4. Shared mock utilities for external integrations.
5. API replay fixture layer using your full cached API responses (offline, deterministic).

Exit criteria:

1. `uv run pytest -q -o addopts=''` can run selected suite reliably.
2. At least one smoke test passes with fresh DB fixture.
3. At least one external-service test passes using cached replay data only.

## Phase 1: P0 Contract Tests (Merge-Gating)

Priority targets (highest risk regressions):

1. ORI lowercase-key contract persists documents.
2. ORI search queue uniqueness includes date bounds.
3. Chain builder assigns `encumbrances.chain_period_id` when ownership periods exist.
4. ORI completion gating does not mark complete on unusable output.
5. Survival completion gating does not mark complete when encumbrances exist but no survival updates.
6. Inbox scanner propagates `auction_type` into `status`.

Exit criteria:

1. All P0 tests pass locally.
2. P0 suite added to CI required checks.

## Phase 2: Silent Failure (SF) Coverage

Targets:

1. Migration commit failure path logs/raises appropriately (no silent ignore).
2. WAL checkpoint lock path logs warning and returns controlled failure.
3. Pagination timeout path logs and exits gracefully.

Exit criteria:

1. Each SF test verifies both control flow and observability.

## Phase 3: Data Integrity (DI) Expansion

Targets:

1. Legacy date normalization (`MM/DD/YYYY` -> ISO).
2. Survival status update correctness (`NULL` -> classified value).
3. Unknown/extra-column input policy behavior.

Exit criteria:

1. DI tests cover key mutation points and expected final DB state.

## Phase 4: CI Policy

1. Required on PR:
   - Phase 1 (P0) suite
   - lint/type checks already used by repo
2. Nightly:
   - wider DB migration/integration scenarios
   - heavier mocked resilience tests
3. Flaky policy:
   - quarantine with issue link and owner
   - no silent retries in CI masking failures

## 5) API Cache Policy (New)

You already have a full cache of API calls. We will treat that as a primary testing asset.

1. Use cached API payloads as fixtures for ORI/HCPA/related integrations.
2. Default all integration tests to replay mode (no live network).
3. Add a small set of optional "live smoke" checks only if explicitly enabled.
4. Version fixture files by endpoint and scenario so regressions are traceable.
5. Prefer cache-based tests in CI for speed and reliability.

## 6) Test Case Catalog (Re-scoped)

## DI (Data Integrity)

| Case ID | Scenario | Target Code Path | Assertion Focus |
|---|---|---|---|
| DI-01 | ORI doc contract with lowercase keys | `src/services/step4v2/discovery.py` `_save_document` | New row inserted in `documents` with expected fields |
| DI-02 | Queue uniqueness with date bounds | `src/services/step4v2/search_queue.py`, `src/db/operations.py` | Different bounded searches coexist |
| DI-03 | Chain period linking | `src/services/step4v2/chain_builder.py` | `encumbrances.chain_period_id` not null when period is matchable |
| DI-04 | Survival status persistence | `src/orchestrator.py` + DB op update path | `survival_status` written when analysis returns updates |
| DI-05 | Auction type propagation | `src/ingest/inbox_scanner.py` | `status.auction_type` matches source `Property.auction_type` |

## SF (Silent Failure)

| Case ID | Scenario | Target Code Path | Assertion Focus |
|---|---|---|---|
| SF-01 | Migration commit failure | `src/db/operations.py` `_apply_schema_migrations` | Failure is logged; path not silently swallowed |
| SF-02 | WAL checkpoint lock failure | `src/db/operations.py` `checkpoint` | Warning emitted; controlled behavior |
| SF-03 | Scraper pagination timeout | `src/scrapers/auction_scraper.py` pagination loop | Timeout logged; loop exits predictably |
| SF-04 | Dedup operation failure | migration/dedup delete paths | Error log emitted with context |

## LR (Logic & Reasoning)

| Case ID | Scenario | Target Code Path | Assertion Focus |
|---|---|---|---|
| LR-01 | Zero-results discovery path | Step4v2 discovery flow | Explicit warning/info exists for zero docs |
| LR-02 | Zero-row update mutation | DB update helpers | Rowcount-aware logging/diagnostic behavior |

## 7) Implementation Order (Immediate)

1. Implement Phase 0 infra.
2. Implement first 3 P0 tests (highest-priority tests):
   - DI-01 (lowercase ORI contract)
   - DI-02 (queue date-bound uniqueness)
   - DI-03 (chain-period linking)
3. Run and stabilize.
4. Add remaining Phase 1 tests.

## 8) Definition of Done

Plan is considered successfully implemented when:

1. Phase 0 and Phase 1 are complete and green in CI.
2. No unit test depends on live network/browser services.
3. Tests are deterministic and isolate DB state per test.
4. Regressions in chain/encumbrance/survival completion are caught pre-merge.
5. External-service behavior is covered by replay tests using cached API responses.

---

## 9) Archived Findings Snapshot (Source Preservation)

This section preserves test-relevant findings from files that may be deleted:

- `MAYBE.md`
- `MAYBE2.md`
- `MAYBE3.md`
- `DB_ISSUES.md`
- `LOGGING_FIXES.md`

If those files are removed, this section becomes the canonical test backlog source.

### 9.1 MAYBE-Derived Regression Backlog

| ID | Scenario | Category | Test Intent |
|---|---|---|---|
| MG-01 | ORI lowercase contract (`instrument`, `doc_type`, `record_date`, etc.) | DI | Ensure rows persist from lowercase-key payloads |
| MG-02 | MRTA years computed from non-deed docs | DI | Ensure chain-year completeness is deed-based only |
| MG-03 | ORI completion marked with unusable output | DI/LR | Ensure completion gates on usable result policy |
| MG-04 | Queue uniqueness ignored date bounds | DI | Ensure bounded/unbounded searches can coexist |
| MG-05 | Survival marked complete with zero updates | DI/LR | Ensure completion withheld when encumbrances exist but no updates |
| MG-06 | `encumbrances.chain_period_id` always NULL | DI | Ensure period-linking assigns IDs when date matches |
| MG-07 | Legal short-circuit uses returned count, not inserted count | LR | Ensure cancellation uses inserted/new rows |
| MG-08 | `status.completed_at` migration gap | DI/SF | Ensure migration adds/maintains `completed_at` availability |
| MG-09 | `foreclosing_refs` not passed to survival | DI | Ensure judgment handoff includes exact foreclosing refs |
| MG-10 | `status.auction_type` hardcoded as foreclosure | DI | Ensure auction_type source value is preserved in status |
| MG-11 | Raw OCR text dropped before persistence | DI | Ensure raw text is retained/saved for auditability |
| MG-12 | Cleaned amounts computed but ignored | DI | Ensure normalized numeric amounts are what gets persisted |
| MG-13 | `parties_one/two` JSON text treated as list without parsing | DI | Ensure read-path deserializes JSON text safely |
| MG-14 | Self-transfer metadata missing | DI | Ensure self-transfer signal is set and respected by chain logic |
| MG-15 | `mark_status_retriable_error` still consumes retry budget | LR | Ensure retry/quarantine behavior is explicit and tested |
| MG-16 | Recovery timeout bursts handling | SF | Ensure cooldown/abort paths preserve thin results without crash |
| MG-17 | Party fallback completion behavior on zero docs | DI/LR | Ensure policy for zero-doc fallback is tested and explicit |
| MG-18 | Completeness thresholds from run outputs | DI | Ensure chain/encumbrance/survival thresholds are measurable in validation suite |

### 9.2 DB_ISSUES-Derived Integrity Backlog

| ID | Scenario | Category | Test Intent |
|---|---|---|---|
| DB-01 | Missing parcel linkage cases | DI | Ensure missing parcel IDs are detectable and handled deterministically |
| DB-02 | Judgment legal description backfill | DI | Ensure extracted judgment data can backfill parcel legal fields |
| DB-03 | `encumbrances.survival_status` NULL coverage gap | DI | Ensure survival writes non-null statuses when analysis runs |
| DB-04 | Non-ISO `sales_history.sale_date` | DI | Ensure normalization migrates to ISO ordering-safe format |
| DB-05 | Status-step vs needs-flag mismatches | DI | Ensure status and needs flags stay synchronized |
| DB-06 | `property_sources` migration mapping (`property_id/source_type/source_url`) | DI | Ensure old schema migrates to new column names correctly |
| DB-07 | `parcels` required columns (`raw_legal2/3/4`, `strap`) | DI | Ensure schema/backfill remains present across migrations |
| DB-08 | `has_valid_parcel_id` consistency | DI | Ensure flag is false when parcel ID is missing |
| DB-09 | `sales_history` dedup uniqueness behavior | DI | Ensure duplicate groups do not survive dedup migration |
| DB-10 | Empty/coverage tables (`market_data`, `scraper_outputs`, `property_sources`) | DI | Add health assertions (not necessarily merge-gating) for pipeline coverage |

### 9.3 LOGGING_FIXES-Derived Silent-Failure Backlog

| ID | Scenario | Category | Test Intent |
|---|---|---|---|
| LG-01 | Migration commit failure visibility | SF | Ensure commit failure logs and is not silent |
| LG-02 | Migration UPDATE block failures visibility | SF | Ensure failed migration updates log table/operation labels |
| LG-03 | Document dedup DELETE failures visibility | SF | Ensure dedup failures log with impact context |
| LG-04 | Auction pagination failure path | SF | Ensure pagination stop is logged with page/date context |
| LG-05 | WAL checkpoint failure visibility | SF | Ensure checkpoint exceptions are logged with DB context |
| LG-06 | Broad fallback in `get_folio_from_strap` | SF | Ensure query failures are logged, not silently converted to `None` |
| LG-07 | Preserve-prior-survival query failure | SF | Ensure survival-preservation failures are logged before destructive writes |
| LG-08 | Gap-search rate-limit and exception paths | SF | Ensure both rate-limit and generic gap-search failures log context |
| LG-09 | Sales-history extraction failures using `print` | SF | Ensure production paths use logger (no silent console-only errors) |
| LG-10 | `_parse_ori_date` silent `None` returns | SF | Ensure date-parse failures log input/type diagnostics |
| LG-11 | Corrupt judgment JSON parse in orchestrator/survival | SF | Ensure parse failures log case/folio context |
| LG-12 | Vision health-check silent failures | SF | Ensure health-check failures are logged with endpoint context |
| LG-13 | Web DB `safe_connection` commit suppression | SF | Ensure write commit failures are observable |
| LG-14 | `folio_has_*` helper broad exception returns | SF | Ensure helper query failures log instead of silently returning false |
| LG-15 | Geocoder/address-source mismatch diagnostics | SF/LR | Ensure invalid source address paths log source metadata |

### 9.4 Assertion Style Rules (From Logging/DB Audits)

1. Prefer state assertions first (row content, status flags, counts).
2. Log assertions should use stable substrings/fields, not full exact messages.
3. For every negative-path test, assert both:
   - control-flow behavior (no crash / expected return / expected raise), and
   - observability (at least one contextual log line).

---

# 10) Industry Standards & Improvements Review

## Industry Standard Check
Research into Python/SQLite testing best practices confirms:
1.  **Isolation**: The "Phase 0" strategy of using fresh DBs per test is the gold standard.
2.  **In-Memory vs. File**: While industry often defaults to in-memory (`:memory:`) for speed, your choice to use **file-backed temp DBs** is **correct and superior** for this specific project.
    *   *Reason*: You explicitly need to test WAL checkpoints (SF-02) and database locking behavior. In-memory DBs do not emulate file-system locking or WAL files accurately enough for these specific "Silent Failure" tests.
3.  **Mocking**: Your plan to mock external services (browser/network) matches standard practice for unit/CI tests.

## Recommended Improvements (Added to Plan)

### A. The "Catch-All" Integrity Check
We should add an automatic "teardown" check to the DB fixture.
*   **Action**: After *every* test, run `PRAGMA integrity_check`.
*   **Why**: This catches corruption that specific assertions might miss (e.g., broken indexes, malformed pages). If a test passes but breaks the DB structure, this will fail the test.

### B. Concurrency Coverage
*   **Gap**: The plan mentions "Queue uniqueness" but is light on concurrent write testing.
*   **Action for Phase 2**: Add a test that attempts concurrent writes to the file-backed DB to verify `busy_timeout` handling. This is a common source of "Silent Failures" (SQLite locked errors) in Python applications.

### C. Seed Factories
*   **Refinement**: Instead of raw SQL inserts for seeding, consider using a lightweight "Factory" pattern (or just helper functions returning Pydantic models). This makes tests more readable (`create_auction(case="123")`) compared to verbose SQL strings.
