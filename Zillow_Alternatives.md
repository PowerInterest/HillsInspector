# Zillow Alternatives Test Plan

## Goal
Evaluate whether `HomeHarvest`, `Auction.com`, `RealtyTrac`, `HillsForeclosures`, and Redfin county foreclosures can replace Zillow for:
- market data coverage
- property photo availability
- operational reliability (headless scraping without frequent blocking)

This is a short bakeoff using **10 properties from our database**.

## Scope
- In scope:
  - URL access reliability (`200` vs blocked/challenge)
  - data extraction completeness for key market fields
  - image/photo availability
  - extraction accuracy against known property identity (address/folio consistency)
- Out of scope:
  - full pipeline refactor
  - long-run production hardening

## Test Data (10 Properties from DB)
Use 10 records from `auctions` with non-empty address and folio/parcel identifiers.

Suggested query (SQLite):

```sql
SELECT
  case_number,
  COALESCE(parcel_id, folio) AS folio,
  property_address,
  auction_date,
  final_judgment_amount
FROM auctions
WHERE property_address IS NOT NULL
  AND TRIM(property_address) <> ''
  AND COALESCE(parcel_id, folio) IS NOT NULL
  AND TRIM(COALESCE(parcel_id, folio)) <> ''
ORDER BY auction_date DESC, case_number
LIMIT 10;
```

If you want stronger representativeness, use:
- 5 recent foreclosure cases (latest dates)
- 5 older foreclosure cases (earlier dates)

## Candidate Sources
1. `HomeHarvest` (current baseline integration)
2. `Auction.com` (`https://www.auction.com/details/{address-like-slug}`)
3. `RealtyTrac` (`https://www.realtytrac.com/search/?q={encoded address}`)
4. `HillsForeclosures` (`https://www.hillsforeclosures.com/`)
5. `Redfin County Foreclosures` (`https://www.redfin.com/county/464/FL/Hillsborough-County/foreclosures`)

## Source-Specific Run Mode
- `HomeHarvest`: headless automation/API-style access.
- `Auction.com`: headless browser scraping.
- `RealtyTrac`: headless browser scraping.
- `HillsForeclosures`: headless browser scraping.
- `Redfin County Foreclosures`: **real surfaced Chrome only** (non-headless) with debugger tools enabled.

For Redfin runs, capture debugger artifacts:
- network request log (status codes, redirects, challenge endpoints)
- console errors/warnings
- final URL/title and page state (`listing page` vs `robot/rate-limit wall`)
- screenshot on every attempt (success or failure)

## Required Output Fields
For each property+source attempt, capture:
- request URL
- HTTP/result status (`success`, `blocked`, `timeout`, `not_found`)
- canonical address returned (if any)
- list price (if available)
- beds/baths/sqft (if available)
- status (active/sold/off-market if available)
- photo count (or at least one photo URL flag)
- scrape duration (seconds)
- failure reason (if failed)

## Success Criteria
A source is considered viable if all are met on the 10-property batch:
- Access success rate: `>= 80%` (8/10) without manual CAPTCHA solving
- Core field completeness:
  - address present: `>= 90%`
  - at least one of `list_price | est_value`: `>= 70%`
  - at least two of `beds/baths/sqft`: `>= 60%`
- Photo availability: `>= 70%` with at least one photo/link
- Accuracy: `>= 90%` of successful rows match expected property identity

If two sources pass, prefer the one with:
1. higher access success rate
2. better photo coverage
3. lower median scrape time

## Execution Steps
1. Pull the 10-property test set from `auctions` and save as a static test manifest (`case_number`, `folio`, `address`).
2. For each source, run scraping on the same 10 properties using source-specific mode:
   - headless for `HomeHarvest`, `Auction.com`, `RealtyTrac`, `HillsForeclosures`
   - surfaced real Chrome + debugger tools for `Redfin County Foreclosures`
3. Store per-attempt results in a single comparison table/file.
4. Re-run the same batch once more after at least 1 hour to detect short-term anti-bot drift.
5. Score each source against success criteria.
6. Select winner + fallback strategy.

## Logging/Artifacts
For each source run, save:
- raw response metadata (status code, final URL, timing)
- parsed payload (normalized JSON/CSV row per property)
- screenshot only for failures (challenge pages, rate limits, missing data)
- for Redfin specifically: debugger network + console logs for every attempt

Suggested artifact folders:
- `logs/market_bakeoff/{source}/`
- `data/temp/market_bakeoff/{source}/`

## Decision Matrix
Use a simple weighted score:
- reliability (40%)
- data completeness (30%)
- photo coverage (20%)
- speed (10%)

Winner = highest weighted score among sources that meet minimum success criteria.

## Rollout Plan (After Test)
1. Set winning source as primary market/photo enrichment.
2. Keep second-best source as fallback on transient failures.
3. Mark Zillow step as disabled (or optional) so it cannot block pipeline completion.
4. Re-run enrichment on recent auctions and measure improvement in market/photo coverage.

## Risks
- Site markup/API changes can drop extraction overnight.
- Anti-bot policies may tighten after repeated runs.
- RealtyTrac may have paywall or partial data for some addresses.

Mitigations:
- strict timeout + retry cap
- per-source circuit breaker
- fallback ordering (`HomeHarvest -> Auction.com -> HillsForeclosures -> RealtyTrac`)
- treat `Redfin County Foreclosures` as experimental until it passes stability checks in surfaced-browser mode

---

## Online Foreclosure Listing Sites

Source article:
- https://www.theownteam.com/blog/top-10-websites-for-accurate-foreclosure-listings/

1. Zillow — https://www.zillow.com/
2. Realtor.com — https://www.realtor.com/
3. Foreclosure.com — https://www.foreclosure.com/
4. RealtyTrac — https://www.realtytrac.com/
5. Bank of America Real Estate Center — https://www.bankofamerica.com/mortgage/
6. HUD Homes — https://www.hudhomestore.gov/
7. HomePath by Fannie Mae — https://www.homepath.fanniemae.com/
8. Auction.com — https://www.auction.com/
9. RealtyStore — https://www.realtystore.com/home-search/foreclosures.htm
10. Trulia — https://www.trulia.com/
