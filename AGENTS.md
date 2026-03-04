# Repository Guidelines

## Pipeline Success Criteria (READ THIS FIRST)

A successful `Controller.py` pipeline run is measured by **data completeness**, not by steps completing without errors. The pipeline's purpose is to produce actionable foreclosure analysis. If the output data is missing, the run has failed regardless of whether the code ran without exceptions.

**After any `Controller.py` run, you MUST validate these thresholds:**

| Metric | Target | Validation Query |
|--------|--------|-----------------|
| Final Judgment PDFs | 90%+ of foreclosures | Count `data/Foreclosure/*/documents/*.pdf` vs total auctions |
| Extracted judgment data | 90%+ of PDFs | `SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL` |
| Chain of title | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT foreclosure_id) FROM foreclosure_title_chain` (PostgreSQL) |
| Encumbrances identified | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT f.foreclosure_id) FROM foreclosures f JOIN ori_encumbrances oe ON oe.strap = f.strap WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL` (PostgreSQL) |
| Lien survival analysis | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT f.foreclosure_id) FROM foreclosures f JOIN ori_encumbrances oe ON oe.strap = f.strap WHERE f.archived_at IS NULL AND f.judgment_data IS NOT NULL AND oe.survival_status IS NOT NULL` (PostgreSQL) |

**If any threshold is not met, the run is a FAILURE.** Do not report success. Instead:
1. Diagnose why the data is missing (query the `status` table, check logs, read the relevant step code)
2. Fix the root cause
3. Re-run the affected steps
4. Keep iterating until thresholds are met

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
- `uv run ruff check .` (add `--fix` when safe) for linting; `uv run ty check` for typing; `uv run pytest` for unit tests.
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

## Security & Configuration
- Keep secrets in `.env`; never log credentials; respect site rate limits; run scrapers in headed mode if blocked.
- Dashboard data should include `latitude` and `longitude` so map endpoints remain populated.
