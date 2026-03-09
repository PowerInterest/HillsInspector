# Repository Guidelines

## Pipeline Success Criteria (READ THIS FIRST)

A successful `Controller.py` pipeline run is measured by **data completeness and classification quality**, not by steps completing without errors. The pipeline's purpose is to produce actionable foreclosure analysis. If the output data is missing, or if the system is classifying title/encumbrance outcomes inconsistently, the run has failed regardless of whether the code ran without exceptions.

Some properties are legitimately broken or unresolved. A low rate of fully complete title chains is not, by itself, proof of pipeline failure. The pipeline fails when it does not build summaries, does not classify gaps consistently, or misclassifies encumbrance outcomes.

**After any `Controller.py` run, you MUST validate these hard gates:**

| Metric | Target | Validation Query |
|--------|--------|-----------------|
| Final Judgment PDFs | 90%+ of active foreclosures | Count `data/Foreclosure/*/documents/*.pdf` and compare to `SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL` |
| Extracted judgment data | 90%+ of active PDF-backed foreclosures | `SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL AND judgment_data IS NOT NULL` |
| Title summaries built | **80%+ of active foreclosures with judgments** | `SELECT COUNT(DISTINCT f.foreclosure_id) FROM foreclosures f JOIN foreclosure_title_summary ts ON ts.foreclosure_id = f.foreclosure_id WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL` |
| Title gap consistency | **0 contradictions** | `SELECT COUNT(*) FROM ((SELECT 1 FROM foreclosure_title_chain tc JOIN foreclosures f ON f.foreclosure_id = tc.foreclosure_id WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND tc.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO') AND COALESCE(tc.is_gap, FALSE) = FALSE) UNION ALL (SELECT 1 FROM foreclosure_title_summary ts JOIN foreclosures f ON f.foreclosure_id = ts.foreclosure_id WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND COALESCE(ts.gap_count, 0) > 0 AND ts.chain_status <> 'BROKEN')) AS q` |
| Encumbrances identified | **80%+ of active foreclosures with judgments** | `SELECT COUNT(DISTINCT f.foreclosure_id) FROM foreclosures f JOIN ori_encumbrances oe ON oe.strap = f.strap WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND oe.encumbrance_type != 'noc'` |
| Lien survival analysis | **80%+ of active foreclosures with judgments** | `SELECT COUNT(DISTINCT f.foreclosure_id) FROM foreclosures f JOIN ori_encumbrances oe ON oe.strap = f.strap LEFT JOIN foreclosure_encumbrance_survival fes ON fes.foreclosure_id = f.foreclosure_id AND fes.encumbrance_id = oe.id WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND oe.encumbrance_type != 'noc' AND COALESCE(fes.survival_status, oe.survival_status) IS NOT NULL` |
| Foreclosing lien uniqueness | **Exactly 1 `FORECLOSING` row per analyzed foreclosure with encumbrances** | `SELECT COUNT(*) FROM (SELECT f.foreclosure_id, COUNT(*) FILTER (WHERE COALESCE(fes.survival_status, oe.survival_status) = 'FORECLOSING') AS foreclosing_count FROM foreclosures f JOIN ori_encumbrances oe ON oe.strap = f.strap AND oe.encumbrance_type != 'noc' LEFT JOIN foreclosure_encumbrance_survival fes ON fes.foreclosure_id = f.foreclosure_id AND fes.encumbrance_id = oe.id WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL GROUP BY f.foreclosure_id) q WHERE foreclosing_count <> 1` |
| Procedural doc sanity | **0 misclassified LP / assignment / same-case judgment rows** | See `docs/domain/PIPELINE_QUALITY_THRESHOLDS.md` for the exact validation SQL pack |

**If any hard gate is not met, the run is a FAILURE.** Do not report success. Instead:
1. Diagnose why the data is missing (query the `status` table, check logs, read the relevant step code)
2. Fix the root cause
3. Re-run the affected steps
4. Keep iterating until thresholds are met

You must also report the detailed diagnostics in [docs/domain/PIPELINE_QUALITY_THRESHOLDS.md](docs/domain/PIPELINE_QUALITY_THRESHOLDS.md), especially:
- fully complete title chains with `chain_status = 'COMPLETE'` and `gap_count = 0`
- broken / missing-folio / no-sales title outcomes
- lis pendens, assignment, and same-case judgment misclassification counts
- satisfied-mortgage linkage completeness

The chain of title and encumbrance data are the core deliverable. Without them, the pipeline produces no investment-grade analysis. Judgment PDFs and enrichment data are intermediate steps toward that goal.

## Project Structure & Module Organization
- Entry point: `Controller.py`; orchestration in `src/services/pg_pipeline_controller.py`.
- Scrapers in `src/scrapers/`; ingest helpers in `src/ingest/`; transforms/enrichment in `src/services/`; shared utilities in `src/utils/`.
- Database schema and scripts in `src/db/`; FastAPI + Jinja app in `app/web/` with API helpers in `app/services/` and DB wiring in `app/web/database.py`.
- **Operational + Analytical Database**: PostgreSQL (`hills_sunbiz`) for pipeline state, enrichment, and analytics.
- **Data Flow**:
    - Scrapers write raw data (Parquet) and assets (PDFs) to case-specific folders: `data/Foreclosure/{case_number}/`.
    - Controller/services ingest and enrich directly into PostgreSQL.
- Logs in `logs/`; docs in `docs/`; maintenance scripts in `scripts/`.
- Tests belong in `tests/` mirroring module paths; fixtures in `tests/fixtures/`.

## Build, Test, and Development Commands
- `uv sync` installs locked dependencies; avoid pip/poetry.
- Quick sanity run: `uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5`.
- `uv run Controller.py` runs the full PG-first pipeline; `uv run python -m app.web.main` launches the dashboard.
- **When developing/testing**: Use controller skip flags and per-step limits (for example `--skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-title-breaks --skip-market-data` for Phase B only). Only run the full pipeline when explicitly requested by the user.
- `uv run ruff check .` (add `--fix` when safe) for linting python; `uv run ty check` for typing; `uv run pytest` for unit tests.
- **Jinja Linting**: We use `djlint`. Run `uv run djlint app/web/templates/ --lint --profile=jinja` to check Jinja/HTML templates for syntax and formatting errors.
- One-time scraper setup: `uv run playwright install chromium`.

## Coding Style & Naming Conventions
- Python 3.12, 4-space indents, prefer double quotes; avoid relative imports (see `pyproject.toml`).
- Modules/files snake_case; classes PascalCase; functions/vars snake_case; constants UPPER_SNAKE.
- **Data Persistence**:
    - **Raw Data**: Write to Parquet files in `data/Foreclosure/{case_number}/`.
    - **Transactional + Analysis**: Use PostgreSQL for active pipeline updates and cross-source linking.
    - **Dataframes**: Use Polars for data manipulation.
- Ruff formatter target width ~88 chars; keep comments minimal and purposeful.

## Multi-LLM Workflow Rule
- Do NOT rely on internal, hidden markdown artifacts (such as `walkthrough.md`, `task.md`, or scratchpads) for architectural documentation.
- If you design a new system, fix a complex bug, or discover important project context, you MUST write that documentation directly into the repository `docs/` folder and link it in `README.md`.
- Other LLMs are reading this codebase, so all critical knowledge must be surfaced in the repository itself.
- Python source files that implement architecture or complex behavior MUST include a clear, detailed module-level docstring at the top of the file explaining architectural purpose and how the file fits into the broader system.

## Testing Guidelines
- Use pytest; name files `test_*.py` mirroring module paths.
- Prefer schema/column presence checks and aggregate assertions for pipelines; include fixture HTML/PDF snippets for scrapers in `tests/fixtures/`.
- Run `uv run pytest` plus a quick `Controller.py` sanity run before PRs.
- After every change, run `uv run ruff check .` and `uv run ty check`.

## Commit & Pull Request Guidelines
- Commits: short, present-tense verbs (e.g., `Add lien summary parser`); keep scope focused.
- PRs: include goal, scope, commands run (`uv run ruff check .`, `uv run ty check`, tests), and data-impact notes (new parquet, DB migrations). Attach screenshots for UI tweaks and link issues/tickets.
- Avoid committing large raw files from `data/properties/`; prefer minimal samples in `docs/` or `tests/fixtures/`.

## Database Schema Version Control
- Use **Alembic** for all PostgreSQL schema changes (column additions, table creates, index changes).
- Never apply raw `ALTER TABLE` / `CREATE TABLE` manually — generate a migration with `alembic revision --autogenerate -m "description"` and apply with `alembic upgrade head`.
- Migration scripts live in `alembic/versions/`. Review autogenerated migrations before committing.
- Migration policy is **forward-only**. Do not use `alembic downgrade`; new migrations should keep `downgrade()` disabled with a clear `NotImplementedError`.

## Security & Configuration
- Keep secrets in `.env`; never log credentials; respect site rate limits; run scrapers in headed mode if blocked.
- Dashboard data should include `latitude` and `longitude` so map endpoints remain populated.
