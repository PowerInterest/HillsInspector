# Municipal Utility Lien Closure Plan (Hillsborough)

Last updated: March 6, 2026

## Objective

Close the municipal utility lien gap for foreclosure underwriting by moving from
ad-hoc checks to a tracked, provider-aware workflow with measurable coverage.

Primary target metric:

- `>= 80%` of active foreclosures with judgment data have `municipal_lien_status`
  set to a non-unknown value.

Secondary target metrics:

- `>= 95%` of candidate properties are routed to the correct provider workflow.
- `>= 95%` of provider requests have SLA timers and queue status.
- `0` silent failures in request submission, response ingestion, or expiry logic.

## Provider Reality (What Is Automatable)

## Hillsborough County Water Resources

- Official process is written-request driven and instrument-number aware.
- County says responses are typically within 72 business hours (3 business days)
  for verified recorded liens.
- Submission surface is a Formstack form, not a published API.

Automation posture:

- Automate prefill + queue + tracking.
- Keep submission and response retrieval as human-in-loop or controlled browser
  automation.

## City of Tampa (Conduits)

- Official process is portal-based (Conduits), paid per search, with reported
  turnaround of up to 7 business days and 45-day payoff-letter validity.
- No public API is documented.
- Flow is authenticated and protected by onboarding/login/anti-bot patterns.

Automation posture:

- Treat as operator workflow with strict queueing and SLA tracking.
- Do not depend on reverse-engineered private endpoints.

## TECO (Tampa Electric)

- TECO electric arrears are generally account/customer-level risk, not default
  parcel-level municipal lien risk.
- No public address-level lien API discovered.

Automation posture:

- Default classification: `not_applicable` for municipal-lien queue.
- Allow manual override path for special contractual edge cases.

## End-State Architecture

1. `Recorded Utility Lien Detector` (automated)
- Uses existing ORI/official-records ingestion to detect likely utility-lien
  recordings by party + doc cues + instrument context.
- Produces provider candidates and evidence rows.

2. `Provider Request Queue` (tracked workflow)
- One queue row per foreclosure/provider request cycle.
- Owns status transitions, SLA deadlines, and escalation flags.

3. `Response Ingest + Decision Layer`
- Normalizes provider responses/payoff letters into structured findings.
- Computes latest `municipal_lien_status` per foreclosure/provider.

4. `Operator Inbox`
- Review page for pending/overdue requests and expired payoff letters.
- Filters by provider, status, age bucket, and SLA breach.

5. `Pipeline Hook`
- New pipeline step after ORI enrichment and before final refresh:
  `municipal_lien_queue_sync`.

## Proposed Data Model

Use Alembic migrations for all schema additions.

## Table: `municipal_lien_requests`

- `id` bigint pk
- `foreclosure_id` bigint not null fk -> `foreclosures.foreclosure_id`
- `provider` text not null (`hillsborough_water_resources`, `tampa_conduits`, `teco`)
- `status` text not null
  - `ready_for_request`
  - `submitted`
  - `awaiting_provider`
  - `response_received`
  - `closed_no_lien`
  - `closed_lien_found`
  - `expired`
  - `cancelled`
- `request_channel` text not null (`manual`, `browser_automation`, `api`)
- `request_payload_json` jsonb null
- `submitted_at` timestamptz null
- `due_at` timestamptz null
- `response_received_at` timestamptz null
- `payoff_valid_until` date null
- `response_document_path` text null
- `response_json` jsonb null
- `notes` text null
- `created_at` timestamptz not null default now()
- `updated_at` timestamptz not null default now()

Recommended indexes:

- `(provider, status, due_at)`
- `(foreclosure_id, provider, created_at desc)`
- partial index for open queue statuses

## Table: `municipal_lien_findings`

- `id` bigint pk
- `foreclosure_id` bigint not null fk -> `foreclosures.foreclosure_id`
- `provider` text not null
- `status` text not null
  - `unknown`
  - `no_lien_found`
  - `lien_recorded`
  - `payoff_pending`
  - `payoff_received`
  - `not_applicable`
- `source` text not null (`ori_detector`, `provider_response`, `manual_override`)
- `instrument_number` text null
- `amount` numeric(14,2) null
- `as_of_date` date null
- `confidence` text null (`high`, `medium`, `low`)
- `reason` text null
- `raw_json` jsonb null
- `created_at` timestamptz not null default now()

Recommended uniqueness:

- unique `(foreclosure_id, provider, source, coalesce(instrument_number, ''))`

## SLA Rules

1. Hillsborough Water Resources
- If instrument number provided:
  - `due_at = submitted_at + 3 business days`
- If no instrument number:
  - `due_at = submitted_at + 30 calendar days`
  - mark as low-priority workflow

2. Tampa Conduits
- `due_at = submitted_at + 7 business days`
- If payoff received:
  - `payoff_valid_until = response_date + 45 days`

3. TECO
- Default: no queue row unless explicit manual escalation.
- Write finding `status = not_applicable`, `source = policy`.

## Integration Plan by File

1. `src/services/pg_ori_service.py`
- Add utility-lien detector helper:
  - provider party dictionary
  - instrument-level match scoring
  - output candidate rows for request queue

2. `src/services/pg_pipeline_controller.py`
- Add step `municipal_lien_queue_sync` after ORI and before final refresh.
- Step responsibilities:
  - upsert detector findings
  - create/open queue requests where needed
  - compute due dates

3. `src/services/` new module
- `pg_municipal_lien_service.py`
  - queue transitions
  - SLA computation
  - response ingestion
  - foreclosure-level status aggregation

4. `app/web/routers/review.py` + template
- Add `/review/municipal-liens` operator inbox.
- Filters: provider, status, overdue, no-instrument, payoff-expiring.

5. `app/web/routers/properties.py` + property template
- Add per-property municipal lien summary card:
  - provider statuses
  - open requests
  - next due date
  - payoff expiry warning

## Rollout Phases

1. Phase 0: Detector only (fastest value)
- Implement recorded-lien detection and findings table.
- Populate `unknown` vs `lien_recorded` vs `not_applicable`.
- No provider submission automation yet.

2. Phase 1: Queue + SLA
- Add request queue table and status transitions.
- Add operator inbox and overdue alerts.

3. Phase 2: Submission helpers
- Hillsborough prefill and controlled submission support.
- Tampa Conduits operator assist workflow.

4. Phase 3: Full closure metrics
- Add dashboard metrics and gate checks in pipeline reporting.

## QA and Regression Gates

1. Unit tests
- detector party matching
- SLA date calculation (business-day edge cases)
- status transition guards
- expiry and overdue flags

2. Integration tests
- queue creation from ORI candidates
- response ingestion updates findings + closure status
- property/review pages render expected counts

3. Operational checks
- no silent exception paths in queue step
- every failed transition logs `provider`, `foreclosure_id`, `request_id`
- every retry increments attempt metadata

## Compliance and Safety Rules

1. Do not bypass anti-bot controls or reverse engineer private endpoints.
2. Respect provider terms and request limits.
3. Preserve traceability:
- persist submission timestamps
- retain response artifacts and provenance
4. Keep human override path for disputes and edge cases.

## Source References

- Hillsborough Water Resources lien inquiry page:
  https://hcfl.gov/residents/property-owners-and-renters/water-and-sewer/water-resources-lien-inquiries
- Hillsborough lien inquiry form:
  https://hcflgov.formstack.com/forms/lien_inquiry
- HCPA property search:
  https://www.hcpafl.org/CamaDisplay.aspx?OutputMode=Input&SearchType=RealEstate&Page=FindByFolioId
- Hills Clerk ORI search:
  https://pubrec6.hillsclerk.com/ORIPublicAccess/customSearch.html
- Hills Clerk public data files:
  https://www.hillsclerk.com/en/records-and-reports/public-data-files
- City of Tampa lien search + Conduits:
  https://www.tampa.gov/neighborhood-enhancement/lien-search
- Conduits getting started:
  https://conduits.netassets.net/conduits/fl/tampa/getting_started.html
- TECO tariff section:
  https://www.tampaelectric.com/4a22ba/siteassets/files/tariff/tariffsection5.pdf
- Florida statutes:
  https://www.leg.state.fl.us/statutes/index.cfm?App_mode=Display_Statute&URL=0100-0199/0125/Sections/0125.485.html
  https://www.leg.state.fl.us/statutes/index.cfm?App_mode=Display_Statute&URL=0100-0199/0180/Sections/0180.135.html
  https://www.leg.state.fl.us/Statutes/index.cfm?App_mode=Display_Statute&URL=0100-0199%2F0153%2FSections%2F0153.67.html
