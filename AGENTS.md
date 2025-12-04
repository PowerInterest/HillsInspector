# Repository Guidelines

## Project Structure & Module Organization
- `main.py` is the entry point for running the pipeline (`--test`, `--update`, `--web`, `--new`); deeper orchestration lives in `src/pipeline.py`.
- `src/scrapers/` pull auction/records data; `src/ingest/` handles pre-scrape imports; `src/services/` transforms/enriches data; `src/db/` holds DuckDB schema/scripts; `src/utils/` contains shared helpers.
- `app/web/` houses the FastAPI + Jinja UI (zero-JS except optional HTMX); `app/services/` wires API helpers; `app/database.py` manages connections.
- `data/properties/` stores raw artifacts (parquet/json/pdfs/photos, per-folio); `logs/` holds loguru outputs; `docs/` keeps scraper notes; `scripts/` contains maintenance utilities.

## Build, Test, and Development Commands
- `uv sync` installs locked dependencies; do not use pip/poetry.
- `uv run main.py --test` runs a quick end-to-end sanity on the next 5 auctions.
- `uv run main.py --update` performs the full scrape/analysis; `uv run main.py --web` launches the dashboard; `uv run main.py --new` resets the database (archives the previous DB first).
- `uv run ruff check .` and `uv run ty check` handle linting and typing; add `--fix` to ruff when safe.
- `uv run playwright install chromium` is needed once to fetch the browser for scrapers.

## Coding Style & Naming Conventions
- Python 3.12, 4-space indents, Ruff formatter with an 88-char target (longer lines allowed when clearer).
- Prefer double quotes; avoid relative imports (see `pyproject.toml` Ruff rules).
- Modules/files: snake_case; classes: PascalCase; functions/vars: snake_case; constants: UPPER_SNAKE.
- Use Polars for dataframes and DuckDB for storage; never pandas/sqlite; favor bulk operations over row-by-row inserts.

## Testing Guidelines
- Add or extend `pytest` suites under `tests/` with files named `test_*.py`; mirror module paths where possible.
- For data pipelines, assert schema/column presence and sample aggregates rather than brittle snapshots.
- For scrapers, include fixture HTML/PDF snippets in `tests/fixtures/` and cover parsing logic.
- Run `uv run pytest` plus `uv run main.py --test` before opening a PR.

## Commit & Pull Request Guidelines
- Commits: short, present-tense verbs (e.g., `Add lien summary parser`); keep scope focused; emojis optional.
- PRs: include goal, scope, commands run (`uv run ruff check .`, `uv run ty check`, tests), and data-impact notes (new parquet, DB migrations). Attach screenshots for UI tweaks and link issues/tickets.
- Avoid committing large raw files from `data/properties/`; prefer minimal samples in `docs/` or `tests/fixtures/`.

## Security & Configuration
- Keep secrets in `.env`; never log credentials. Respect site rate limits; run scrapers in headed mode if blocked.
- Database is DuckDB; expect Parquet inputs. Ensure dashboard data includes `latitude` and `longitude` so map endpoints stay populated.
