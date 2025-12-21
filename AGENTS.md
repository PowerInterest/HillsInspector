# Repository Guidelines

## Project Structure & Module Organization
- Entry point: `main.py` (flags: `--test`, `--update`, `--web`, `--new`); orchestration in `src/pipeline.py`.
- Scrapers in `src/scrapers/`; ingest helpers in `src/ingest/`; transforms/enrichment in `src/services/`; shared utilities in `src/utils/`.
- DuckDB schema and scripts in `src/db/`; FastAPI + Jinja app in `app/web/` with API helpers in `app/services/` and DB wiring in `app/web/database.py`.
- Raw artifacts live under `data/properties/` (per-folio parquet/json/pdfs/photos); logs in `logs/`; docs in `docs/`; maintenance scripts in `scripts/`.
- Tests belong in `tests/` mirroring module paths; fixtures in `tests/fixtures/`.

## Build, Test, and Development Commands
- `uv sync` installs locked dependencies; avoid pip/poetry.
- `uv run main.py --test` runs a quick end-to-end sanity on the next 5 auctions.
- `uv run main.py --update` does the full scrape/analysis; `uv run main.py --web` launches the dashboard; `uv run main.py --new` resets the DB (archives old DB first).
- `uv run ruff check .` (add `--fix` when safe) for linting; `uv run ty check` for typing; `uv run pytest` for unit tests.
- One-time scraper setup: `uv run playwright install chromium`.

## Coding Style & Naming Conventions
- Python 3.12, 4-space indents, prefer double quotes; avoid relative imports (see `pyproject.toml`).
- Modules/files snake_case; classes PascalCase; functions/vars snake_case; constants UPPER_SNAKE.
- Use Polars for dataframes and DuckDB for storage; avoid pandas/sqlite; favor bulk operations over row-by-row inserts.
- Ruff formatter target width ~88 chars; keep comments minimal and purposeful.

## Testing Guidelines
- Use pytest; name files `test_*.py` mirroring module paths.
- Prefer schema/column presence checks and aggregate assertions for pipelines; include fixture HTML/PDF snippets for scrapers in `tests/fixtures/`.
- Run `uv run pytest` plus `uv run main.py --test` before PRs.
- After every change, run `uv run ruff check .` and `uv run ty check`.

## Commit & Pull Request Guidelines
- Commits: short, present-tense verbs (e.g., `Add lien summary parser`); keep scope focused.
- PRs: include goal, scope, commands run (`uv run ruff check .`, `uv run ty check`, tests), and data-impact notes (new parquet, DB migrations). Attach screenshots for UI tweaks and link issues/tickets.
- Avoid committing large raw files from `data/properties/`; prefer minimal samples in `docs/` or `tests/fixtures/`.

## Security & Configuration
- Keep secrets in `.env`; never log credentials; respect site rate limits; run scrapers in headed mode if blocked.
- Dashboard data should include `latitude` and `longitude` so map endpoints remain populated.
