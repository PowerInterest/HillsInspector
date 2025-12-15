```
Async, performant pipeline plan (skip rework, bounded concurrency)

Orchestrator design
- Use asyncio with bounded semaphores per source (auction site, OnBase, ORI, geocode).
- Define stages as idempotent tasks with persisted checkpoints (DB flags/columns) so reruns skip completed work.

Stages & parallelism
- Discovery: scrape auctions (foreclosure/tax deed) for N days ahead; concurrent per date (small cap). Persist cases.
- Downloads: Final Judgment PDFs from OnBase instrument links; concurrent with rate limiting.
- Extraction: FinalJudgmentProcessor over downloaded PDFs; skip if judgment_extracted_at set.
- Geocode: batch addresses missing lat/lon; cache results; cap outbound geocode calls.
- Enrichment: HCPA/ORI fetches per parcel/case; concurrent with per-source caps; store liens/docs.
- Analysis: chain-of-title + lien survival using extracted data; run in workers over pending cases.
- Market/Permits: optional staged jobs with their own caps/schedules.

Performance & resilience
- Skip done work: gate every stage by DB flags (downloaded_at, judgment_extracted_at, geocoded_at, ori_fetched_at, analyzed_at).
- Bounded concurrency per domain to avoid bans.
- Batching: group geocodes; bulk DB writes for new liens/docs/geocodes.
- Retry/timeout: short timeouts, minimal retries; log failures and requeue separately.
- Idempotency: UPSERT writes; file existence checks before download; hash PDFs to skip re-OCR; ORI fetches keyed by instrument.

Data model / checkpoints
- Add columns: downloaded_at, judgment_extracted_at, geocoded_at, ori_fetched_at, analysis_at.
- Store instrument_number from auction Case# link; lat/long on parcels; ORI docs/liens with instrument metadata.

Execution flow (async tasks)
1) Discover auctions → upsert auctions.
2) Download PDFs concurrently (bounded).
3) Extract judgments concurrently → update auctions.
4) Geocode missing parcels → update lat/lon.
5) Fetch ORI docs/liens per case/parcel → store → mark ori_fetched_at.
6) Analyze (chain-of-title + lien survival) for cases with judgment + liens → persist results.
7) Optional: market/permits stage on schedule.
8) Summarize stats; expose via API/UI.

Tech stack
- asyncio orchestration with semaphores.
- DuckDB bulk inserts/updates.
- Logging with context; metrics per stage.

Next steps to implement
- Add checkpoint columns/flags.
- Refactor pipeline into staged async tasks with bounded semaphores.
- Implement full ORI fetch/storage (not just instrument metadata).
- Add geocode job using cache + rate-limit.
- Add “skip if done” guards in each stage.
```
