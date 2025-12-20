.PHONY: install setup check format lint

install:
	uv sync
	uv run playwright install

install-deps:
	sudo apt-get install -y libnspr4 libnss3 libasound2t64

setup: install-deps install

check:
	uv run pytest

format:
	uv run ruff format .

lint:
	uv run ruff check .
