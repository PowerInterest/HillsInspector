# Sunbiz Bulk SFTP Mirror

This directory contains a local-mirror tool for Florida Sunbiz bulk downloads,
plus a PostgreSQL loader so pipeline logic can prefer local datasets over live scraping.

## What It Does

- Connects to Sunbiz SFTP (`sftp.floridados.gov:22`)
- Discovers and lists daily / quarterly directories
- Downloads files into a local mirror (`data/sunbiz/`)
- Tracks downloaded files with `manifest.json` to skip unchanged files

## Defaults

- Host: `sftp.floridados.gov`
- User: `Public`
- Password: `PubAccess1845!`
- Daily dir: `/public/doc` (auto-discovery fallback)
- Quarterly dir: `/public/doc/quarterly` (auto-discovery fallback)

Override any of these with environment variables:

- `SUNBIZ_SFTP_HOST`
- `SUNBIZ_SFTP_PORT`
- `SUNBIZ_SFTP_USER`
- `SUNBIZ_SFTP_PASSWORD`
- `SUNBIZ_SFTP_DAILY_DIR`
- `SUNBIZ_SFTP_QUARTERLY_DIR`
- `SUNBIZ_DATA_DIR`
- `SUNBIZ_MANIFEST`

## Commands

List available files:

```bash
uv run python sunbiz/sync.py list --mode all --max-files 50
```

Initial bootstrap (quarterly snapshots):

```bash
uv run python sunbiz/sync.py sync --mode quarterly
```

Daily refresh (recent files only):

```bash
uv run python sunbiz/sync.py sync --mode daily --modified-since 2026-01-01
```

Dry run:

```bash
uv run python sunbiz/sync.py sync --mode daily --dry-run --max-files 20
```

## Notes

- By default, files are stored under `data/sunbiz/<remote path>`.
- Manifest file is written to `data/sunbiz/manifest.json`.
- Use `--force` to re-download files even if unchanged in manifest.

## PostgreSQL + SQLAlchemy Loader

Install dependencies:

```bash
uv add sqlalchemy psycopg[binary]
```

Configure database DSN:

```bash
export SUNBIZ_PG_DSN="postgresql+psycopg://hills:hills_dev@localhost:5432/hills_sunbiz"
```

Initialize schema:

```bash
uv run python sunbiz/pg_loader.py init-db
```

Load Sunbiz raw lines:

```bash
uv run python sunbiz/pg_loader.py load-sunbiz-raw --root data/sunbiz/public/doc
```

Load structured FLR tables:

```bash
uv run python sunbiz/pg_loader.py load-sunbiz-flr --root data/sunbiz/public/doc
```

Load HCPA bulk parcel + optional LatLon data:

```bash
uv run python sunbiz/pg_loader.py load-hcpa \
  --parcel-file data/parquet/bulk_parcels_latest.parquet \
  --latlon-file data/bulk_data/LatLon_Table_YYYY_MM_DD.zip
```

Download the latest HCPA weekly datasets:

```bash
uv run python sunbiz/pg_loader.py sync-hcpa \
  --output-dir data/bulk_data/hcpa \
  --datasets hcparcel allsales subdivisions special_districts
```

Load the full weekly HCPA suite into Postgres:

```bash
uv run python sunbiz/pg_loader.py load-hcpa-suite \
  --downloads-dir data/bulk_data/hcpa
```

Smoke test with row limits:

```bash
uv run python sunbiz/pg_loader.py load-hcpa-suite \
  --downloads-dir /tmp/hcpa_docs \
  --parcel-file /tmp/hcpa_docs/HCparcel_4_public_02_13_2026.zip \
  --allsales-file /tmp/hcpa_docs/allsales_02_13_2026.zip \
  --subdivisions-file /tmp/hcpa_docs/subdivisions_02_13_2026.zip \
  --special-districts-file /tmp/hcpa_docs/special_districts_02_13_2026.zip \
  --limit-rows 1000
```

Tables loaded by `load-hcpa-suite`:

- `hcpa_bulk_parcels`
- `hcpa_parcel_dor_names`
- `hcpa_parcel_sub_names`
- `hcpa_allsales`
- `hcpa_subdivisions`
- `hcpa_special_district_tifs`
- `hcpa_special_district_cdds`
- `hcpa_special_district_sd`
- `hcpa_special_district_sd2`
- `hcpa_special_district_lds`
