# Plant City + Temple Terrace Permit Expansion

## Scope

This guide documents the municipal permit expansion implemented for:
- City of Plant City
- City of Temple Terrace

The implementation is integrated into the existing `controller.py` pipeline via
the `single_pin_permits` step, using jurisdiction-aware routing.

## Architecture

### Pipeline integration point

Routing is executed inside:
- `src/services/pg_permit_single_pin_service.py`

For each targeted PIN:
1. Existing county/tampa single-pin enrichment still runs.
2. Site address city is inferred from HCPA parcel context.
3. If city resolves to Plant City or Temple Terrace, a municipality-specific
   service runs automatically.

### New municipal services

- `src/services/PlantCityPermit.py`
- `src/services/TempleTerracePermit.py`

Both services normalize records into `tampa_accela_records` so downstream
queries/event logic continue to work without schema changes.

## Reverse-engineered endpoints

### Plant City (Maintstar)

Portal:
- `https://h8.maintstar.co/plantcity/portal/`

Search endpoint:
- `GET https://h8.maintstar.co/plantcity/api/Public/Record/Search`
- Query params: `query`, `skip`, `take`
- JSON shape includes top-level keys: `data`, `total`, `showMoreMode`
- Record fields include: `id`, `number`, `msType`, `type`, `dateVal`,
  `address`, `status`

### Temple Terrace (Click2Gov)

Portal:
- `https://temp-egov.aspgov.com/Click2GovBP/`

Search flow:
- Initial page/token bootstrap: `GET /Click2GovBP/selectpermit.html?initialSearchView=true`
- Results query: `POST /Click2GovBP/selectpermit.html`
- Payload pattern:
  - `searchResultsView=true`
  - `searchType=1` (address search)
  - `parcel.streetNumber`, `parcel.streetDirection`, `parcel.streetName`,
    `parcel.streetSuffix`
  - `streetSearchType=contains`
  - `target1=Continue`
  - `OWASP_CSRFTOKEN=<token>`
- Detail pages are parsed from:
  - `selectpermit.html?...&permit.appYearAndNumber=<id>&validatePermitView=true`

## Data model strategy

No new permit table was introduced. Municipal rows are written to
`tampa_accela_records` with:
- namespaced `record_number` (`PLANTCITY:*`, `TEMPLETERRACE:*`) to prevent
  cross-jurisdiction ID collisions
- `source_query_text` prefixes (`plant_city:*`, `temple_terrace:*`)
- full raw payload in `source_payload`

## Guardrails

- Retries with backoff are applied for municipal HTTP requests.
- Municipal fetch errors are logged and surfaced in single-pin step stats as
  `municipal_errors`.
- Candidate scoring in `pg_pipeline_controller` now adds city-sensitive address
  matching to reduce cross-city false positives when checking permit gaps.
