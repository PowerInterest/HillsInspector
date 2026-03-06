# Scrapling and PostgreSQL Photo Tests

These tests are intentionally isolated from the default suite and require explicit opt-in.

- `test_property_photo_backfill.py`  
  - Reads `property_market` rows in PostgreSQL with `photo_cdn_urls` but no/insufficient local photos.
  - Downloads a sample of those CDN images to a persistent `data/realtor_photo_backfill/foreclosure/<run_id>/Foreclosure/<case_number>/photos/` tree.
  - Asserts a minimum number of image files are written.

## Run the test

```bash
PG_PHOTO_BACKFILL_TEST=1 \
PG_PHOTO_TEST_LIMIT=10 \
PG_PHOTO_PER_PROPERTY=10 \
PG_PHOTO_MIN_SAVED=1 \
UV_CACHE_DIR=/tmp/uv \
uv run pytest tests/scrapling/test_property_photo_backfill.py -m integration --no-cov
```

This test uses `SUNBIZ_PG_DSN` if set. If not set, it uses the project default DSN.

If no eligible records are found, the test skips with a clear message.

Downloaded photos are written under:

```text
data/realtor_photo_backfill/foreclosure/<run_id>/Foreclosure/<case_number>/photos/
```

The test initializes Loguru for each run and writes a file log to:

```text
logs/scrapling/realtor_photo_backfill_<UTC_TIMESTAMP>.log
```

All request/parse/write failures are logged explicitly rather than silently skipped.
