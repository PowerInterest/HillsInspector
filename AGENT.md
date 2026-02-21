# Agent Guidelines

Canonical agent instructions live in `AGENTS.md`.

Pipeline startup documentation is `Controller.py`-first:
- Full run: `uv run Controller.py`
- Sanity run: `uv run Controller.py --auction-limit 5 --judgment-limit 5 --ori-limit 5 --survival-limit 5 --limit 5`
- Web app: `uv run python -m app.web.main`
