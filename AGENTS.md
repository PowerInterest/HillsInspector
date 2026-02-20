# Repository Guidelines

## Pipeline Success Criteria (READ THIS FIRST)

A successful `--update` run is measured by **data completeness**, not by steps completing without errors. The pipeline's purpose is to produce actionable foreclosure analysis. If the output data is missing, the run has failed regardless of whether the code ran without exceptions.

**After any `--update` run, you MUST validate these thresholds:**

| Metric | Target | Validation Query |
|--------|--------|-----------------|
| Final Judgment PDFs | 90%+ of foreclosures | Count `data/Foreclosure/*/documents/*.pdf` vs total auctions |
| Extracted judgment data | 90%+ of PDFs | `SELECT COUNT(*) FROM auctions WHERE extracted_judgment_data IS NOT NULL` |
| Chain of title | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM chain_of_title` (SQLite) |
| Encumbrances identified | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM encumbrances` (SQLite) |
| Lien survival analysis | **80%+ of foreclosures with judgments** | `SELECT COUNT(DISTINCT folio) FROM encumbrances WHERE survival_status IS NOT NULL` (SQLite) |

**If any threshold is not met, the run is a FAILURE.** Do not report success. Instead:
1. Diagnose why the data is missing (query the `status` table, check logs, read the relevant step code)
2. Fix the root cause
3. Re-run the affected steps
4. Keep iterating until thresholds are met

The chain of title and encumbrance data are the core deliverable. Without them, the pipeline produces no investment-grade analysis. Judgment PDFs and enrichment data are intermediate steps toward that goal.

## Project Structure & Module Organization
- Entry point: `main.py` (flags: `--update`, `--web`, `--new`); orchestration in `src/orchestrator.py`.
- Scrapers in `src/scrapers/`; ingest helpers in `src/ingest/`; transforms/enrichment in `src/services/`; shared utilities in `src/utils/`.
- Database schema and scripts in `src/db/`; FastAPI + Jinja app in `app/web/` with API helpers in `app/services/` and DB wiring in `app/web/database.py`.
- **Operational Database**: `data/property_master_sqlite.db` (SQLite) for transactional data, status tracking, and active pipeline enrichment.
- **Analytical Database**: PostgreSQL for high-volume historical data (Clerk, Sales, Sunbiz) and complex analytics.
- **Web Snapshot**: `data/property_master_web.db` (SQLite) refreshable snapshot for read-only web access.
- **Data Flow (Inbox Strategy)**:
    - Scrapers write raw data (Parquet) and assets (PDFs) to case-specific folders: `data/Foreclosure/{case_number}/`.
    - `InboxScanner` (`src/ingest/inbox_scanner.py`) picks up `auction.parquet` files, ingests them into SQLite, and moves them to a `consumed/` subfolder.
    - This decouples scraping (io-bound, parallel) from database writes (locking, serial), preventing `database is locked` errors.
- Logs in `logs/`; docs in `docs/`; maintenance scripts in `scripts/`.
- Tests belong in `tests/` mirroring module paths; fixtures in `tests/fixtures/`.

## Build, Test, and Development Commands
- `uv sync` installs locked dependencies; avoid pip/poetry.
- Quick sanity run: `uv run main.py --update --start-date YYYY-MM-DD --end-date YYYY-MM-DD --auction-limit 5`.
- `uv run main.py --update` does the full scrape/analysis; `uv run main.py --web` launches the dashboard; `uv run main.py --new` resets the DB (archives old DB first).
- **When developing/testing**: Use `--start-step <step #>` to limit `--update` to the step you're working on (e.g., `--start-step 5` for ORI ingestion). Only run a full update when explicitly requested by the user.
- `uv run ruff check .` (add `--fix` when safe) for linting; `uv run ty check` for typing; `uv run pytest` for unit tests.
- One-time scraper setup: `uv run playwright install chromium`.

## Coding Style & Naming Conventions
- Python 3.12, 4-space indents, prefer double quotes; avoid relative imports (see `pyproject.toml`).
- Modules/files snake_case; classes PascalCase; functions/vars snake_case; constants UPPER_SNAKE.
- **Data Persistence**:
    - **Raw Data**: Write to Parquet files in `data/Foreclosure/{case_number}/`.
    - **Transactional**: Use SQLite (`src/db/operations.py`) for property status and active pipeline updates.
    - **Analysis**: Use PostgreSQL for large-scale historical datasets and cross-source linking.
    - **Dataframes**: Use Polars for data manipulation.
- Ruff formatter target width ~88 chars; keep comments minimal and purposeful.

## Testing Guidelines
- Use pytest; name files `test_*.py` mirroring module paths.
- Prefer schema/column presence checks and aggregate assertions for pipelines; include fixture HTML/PDF snippets for scrapers in `tests/fixtures/`.
- Run `uv run pytest` plus a quick `--update` sanity run before PRs.
- After every change, run `uv run ruff check .` and `uv run ty check`.

## Commit & Pull Request Guidelines
- Commits: short, present-tense verbs (e.g., `Add lien summary parser`); keep scope focused.
- PRs: include goal, scope, commands run (`uv run ruff check .`, `uv run ty check`, tests), and data-impact notes (new parquet, DB migrations). Attach screenshots for UI tweaks and link issues/tickets.
- Avoid committing large raw files from `data/properties/`; prefer minimal samples in `docs/` or `tests/fixtures/`.

## Security & Configuration
- Keep secrets in `.env`; never log credentials; respect site rate limits; run scrapers in headed mode if blocked.
- Dashboard data should include `latitude` and `longitude` so map endpoints remain populated.
