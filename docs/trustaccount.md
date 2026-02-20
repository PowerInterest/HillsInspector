# Trust & Real Auction Balances Analysis

Date of analysis: 2026-02-18
Data source: `https://publicrec.hillsclerk.com/Civil/`
Files analyzed (weekday snapshots): 2026-02-11, 2026-02-12, 2026-02-13, 2026-02-16, 2026-02-17
Update cadence: new files publish once per business day near midnight (observed timestamps:
- Real Auction Balances: about 10:55 PM
- Registry/Trust Balances: about 11:57 PM)

## Scope
This document analyzes two feeds:

1. `Civil/real_auction_balances/Realauction_Mortgage_Foreclosure_Balances_as_of_*.pdf`
2. `Civil/registry_trust_balances/Registry_and_TrustAccounts_Balances_as_of_*.pdf`

Goal: determine what investable/operational signal exists for foreclosure work, and check what our current foreclosure DB says for case `24-CA-000543`.

## What These Feeds Actually Provide

### 1) Real Auction Balances (`real_auction_balances`)
Per case, this feed provides:
- Case number
- Case style
- Recipient
- Hold (column exists in spec; not visibly populated in sampled pages)
- In escrow since date
- Amount as-of date
- Amount to disburse

This is effectively a daily escrow/disbursement ledger for foreclosure-related balances.

### 2) Registry/Trust Balances (`registry_trust_balances`)
Per case and account type, this feed provides:
- Case number
- Party name
- Increases
- Decreases
- Net credit balance

Includes multiple account types/divisions, including foreclosure-relevant `2408 MORTGAGE FORECLOSURE DEPOSIT`.

## Quantitative Findings (2026-02-11 to 2026-02-17)

### Real Auction Balances
Daily case counts:
- 2026-02-11: 125
- 2026-02-12: 126
- 2026-02-13: 129
- 2026-02-16: 128
- 2026-02-17: 128

Daily composition highlights:
- `Multiple Recipients`: 76-81 cases/day
- Cases with negative disbursement lines: 14/day (stable)
- Cases with offset pair behavior (positive + matching negative pattern): 11/day
- Cases with absolute primary disbursement >= $100,000: 26-31/day

Day-over-day movement:
- 2026-02-11 -> 2026-02-12: +1 entered
- 2026-02-12 -> 2026-02-13: +4 entered, -1 dropped
- 2026-02-13 -> 2026-02-16: -1 dropped
- 2026-02-16 -> 2026-02-17: set stable, but 2 cases changed primary amount

Largest mover (Mon -> Tue):
- `24-CA-000543`: $44,354.05 -> $15.00 (delta `-44,339.05`)

Other mover (Mon -> Tue):
- `25-CA-006970`: $232,200.00 -> $232,172.00 (delta `-28.00`)

Age/size profile on 2026-02-17:
- Escrow age range: 4 to 4,819 days
- Median escrow age: 285 days
- Age buckets:
  - `<30d`: 18
  - `30-179d`: 37
  - `180-364d`: 17
  - `1-2y`: 34
  - `3-4y`: 4
  - `5y+`: 18
- Primary disbursement size buckets:
  - `<$1k`: 17
  - `$1k-$9k`: 31
  - `$10k-$49k`: 30
  - `$50k-$99k`: 21
  - `$100k+`: 29

### Registry/Trust Balances
Daily case counts:
- 2026-02-11: 445
- 2026-02-12: 442
- 2026-02-13: 443
- 2026-02-16: 434
- 2026-02-17: 431

By case code (approx daily):
- `CA`: ~40-42
- `CC`: ~237-248
- `CP`: 41

Day-over-day movement:
- 2026-02-11 -> 2026-02-12: 3 dropped, 3 changed
- 2026-02-12 -> 2026-02-13: 2 entered, 1 dropped, 0 changed
- 2026-02-13 -> 2026-02-16: 9 dropped, 3 changed
- 2026-02-16 -> 2026-02-17: 1 entered, 4 dropped, 1 changed

Largest mover (Mon -> Tue):
- `26-CC-003432`: $1,200.00 -> $2,700.00 (delta `+1,500.00`)

Foreclosure-specific division signal (`2408 MORTGAGE FORECLOSURE DEPOSIT`):
- 2026-02-11: 3 cases, total net $119,529.30
- 2026-02-12: 3 cases, total net $119,529.30
- 2026-02-13: 4 cases, total net $125,256.71
- 2026-02-16: 4 cases, total net $125,256.71
- 2026-02-17: 3 cases, total net $45,260.31

Primary driver of the 2026-02-17 drop:
- `25-CA-001739` ($79,996.40) disappeared from division 2408.

### Cross-feed overlap
Same-case overlap between both feeds is low but real:
- 1 persistent overlapping case each day: `19-CA-006095`

This indicates the two feeds are complementary (not duplicates), and triangulation is possible for select cases.

## Alignment to Winning Auctions (`/history`)

### Why this requires inference
- `http://localhost:8080/history` and `data/history_web.db` currently mask case numbers (`292******** (document link)`), so direct case-number joins are not possible.
- We aligned by exact amount and date behavior:
  - New amount first appears in `real_auction_balances` on day `D`
  - Same amount exists as `winning_bid` in history on prior business day `D-1`
  - Match count for that amount on `D-1` is unique (count = 1)

### Observed inflow-to-auction matches
Window analyzed: 2026-02-11 through 2026-02-17 (all currently published PDFs).

All 5 new inflow entries matched a unique prior-business-day winning bid:

| Case | First Seen in Real Balance | First Amount | Matching Winning Bid Date (`/history`) | Match Count |
|---|---|---:|---|---:|
| `24-CA-008259` | 2026-02-12 | 185,300.00 | 2026-02-11 | 1 |
| `24-CA-005004` | 2026-02-13 | 388,200.00 | 2026-02-12 | 1 |
| `24-CA-006820` | 2026-02-13 | 416,300.00 | 2026-02-12 | 1 |
| `25-CA-000811` | 2026-02-13 | 171,000.00 | 2026-02-12 | 1 |
| `25-CA-009377` | 2026-02-13 | 133,600.00 | 2026-02-12 | 1 |

Result:
- Exact unique alignment rate in this sample: `5/5`.
- Practical interpretation: first posted real-auction escrow amount behaves like a next-day mirror of winning bid.

### Money moving out (observed outflow/disbursement behavior)
Detected major outflow-style events:
- `24-CA-000543`: 44,354.05 -> 15.00 on 2026-02-17 (delta `-44,339.05`)
- `23-CA-002828`: 9,054.85 -> 15.00 on 2026-02-16 (delta `-9,039.85`)
- Case disappearance events:
  - `24-CA-006336` dropped after 2026-02-12 (prior amount 6,843.17)
  - `16-CA-004539` dropped after 2026-02-13 (prior amount 725,200.00)

In registry/trust foreclosure division `2408`, net also collapsed on 2026-02-17:
- 2026-02-16 total: 125,256.71
- 2026-02-17 total: 45,260.31
- Key contributor: `25-CA-001739` (79,996.40) no longer present.

## Pre-Auction Signal Hypothesis Test

Hypothesis:
- If money moves into these accounts days before auction, that might indicate expected max/high bid.

Test using matched inflow cases above:
- For each matched case, `In Escrow Since` in the real-auction PDF is one day after the inferred auction date.
- Days before auction (`auction_date - in_escrow_since`) for all 5 matches = `-1`.

Meaning:
- In this observed sample, inflows are post-auction settlement postings, not pre-auction bidder staging.
- So these feeds are very useful for near-real-time confirmation and post-sale tracking, but not currently proven as pre-auction bid-cap predictors.

When this could become predictive:
- If future data shows repeated cases with `In Escrow Since` before auction date, or trust-account jumps tied to upcoming docketed sale dates.
- That requires longer historical retention than currently published in these folders (currently only 5 business days accessible).

## Case Study: `24-CA-000543`

Observed in real auction balances:
- 2026-02-11 to 2026-02-16: balance at $44,354.05, marked with `Multiple Recipients`
- Embedded splits shown as:
  - $35,694.05 (CLERK)
  - $8,660.00 (CLERK)
- In Escrow Since: `10/23/2025`

On 2026-02-17:
- Case still present
- Balance reduced to `$15.00`
- `Multiple Recipients` no longer shown in extracted block for that page segment

Interpretation:
- This strongly suggests a major disbursement/cleanup event occurred between 2026-02-16 and 2026-02-17.
- It does **not** prove the property sold on 2026-02-17; rather, it indicates escrow funds were mostly disbursed by that date.
- Since escrow start is shown as 2025-10-23, the sale/fund receipt event likely predates this February movement.

## What Our Foreclosure Database Says About `24-CA-000543`

Checked:
- `data/property_master_web.db`
  - `auctions`: no match for `24-CA-000543`, `292024CA000543A001HC`, or `%000543%`
  - `documents`: no match
  - `liens`: no match
  - `status`: no match
- `data/history_web.db`
  - `auctions`: no match

Current DB coverage snapshot:
- `auctions` table currently has 36 rows
- Auction date range: `2026-02-05` to `2026-02-16`

Conclusion for this specific case:
- Our current foreclosure DB has **no record** of `24-CA-000543`.
- So today our pipeline cannot answer sale status or downstream lien/title posture for this case from internal tables.

## Why These Feeds Matter for the Project

These feeds provide high-value post-sale/post-judgment money-flow signals that are missing from core ORI/title outputs:

1. Detect disbursement events quickly (large day-over-day balance collapse).
2. Rank operational opportunities (large balances, long-aged escrow, multi-recipient complexity).
3. Identify unresolved/high-friction payouts (stale balances, negative/offset patterns).
4. Cross-check foreclosure cash movement against case lifecycle and court account activity.

They do **not** replace chain-of-title or encumbrance analysis because they lack folio/instrument/book-page links, but they are strong additive intelligence.

Operational implication:
- Treat these feeds as nightly snapshots, not intraday monitors.
- Best practice is to refresh analysis after midnight publication, then use that view for next-day auction intelligence.

## Recommended Integration

1. Ingest both feeds daily into normalized tables (`real_auction_balance_snapshots`, `registry_trust_snapshots`).
2. Compute daily deltas per case (`entered`, `dropped`, `amount_delta`, `recipient_pattern_change`).
3. Alert on high-signal rules:
   - `abs(delta) >= $25,000`
   - case dropped from 2408 division
   - aged escrow `> 365` days with large net balance
4. Add UCN normalization bridge:
   - short case (`24-CA-000543`) <-> full UCN (`292024CA000543A001HC`) where possible.
5. Surface these signals in the review/dashboard layer as "Escrow/Registry Events".

## Local Artifacts Generated

- `data/tmp/civil_real_auction_parsed_summary.json`
- `data/tmp/civil_registry_parsed_summary.json`
- `data/tmp/civil_balance_analysis_summary.json`
- Raw sampled files in `data/tmp/` prefixed with `civil_...`
