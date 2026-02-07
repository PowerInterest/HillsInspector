# Repository Guidelines

## Project Structure & Module Organization
- Entry point: `main.py` (flags: `--update`, `--web`, `--new`); orchestration in `src/orchestrator.py`.
- Scrapers in `src/scrapers/`; ingest helpers in `src/ingest/`; transforms/enrichment in `src/services/`; shared utilities in `src/utils/`.
- DuckDB schema and scripts in `src/db/`; FastAPI + Jinja app in `app/web/` with API helpers in `app/services/` and DB wiring in `app/web/database.py`.
- **Primary Database**: `data/property_master_sqlite.db` (SQLite) for transactional data, status tracking, and simple queries.
- **Analytics Database**: `data/property_master_v2.db` (DuckDB) for complex title chain analysis and heavy-lifting queries.
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
    - **Transactional**: Use SQLite (`src/db/operations.py`) for property status and updates.
    - **Analysis**: Use DuckDB (`src/db/v2/`) for complex aggregations and title chains.
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
