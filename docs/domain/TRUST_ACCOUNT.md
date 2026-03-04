# Trust Accounts & Escrow Analysis

## Domain Overview

In Hillsborough County, the Clerk of Court publishes daily PDF ledgers containing the current balances of trust accounts and real auction escrows. In the context of foreclosure auctions, tracking these ledgers provides critical **follow-the-money intelligence**. 

By observing the flow of funds into and out of the court's registry, we can infer bidder staging, post-sale disbursements, and competitive interest days before an auction occurs.

## Data Sources (Endpoints)

The data is sourced directly from the Hillsborough Clerk's public index, which updates once per business day near midnight:

1. **Real Auction Balances:** `https://publicrec.hillsclerk.com/Civil/real_auction_balances/`
   - Provides a daily escrow ledger specific to foreclosures.
   - Captures case number, recipient names, hold status, original escrow date, and disbursement amounts.

2. **Registry & Trust Balances:** `https://publicrec.hillsclerk.com/Civil/registry_trust_balances/`
   - Provides cross-division accounting.
   - We specifically target Division **`2408 MORTGAGE FORECLOSURE DEPOSIT`**.
   - Captures case number, party names, daily increases, and daily decreases.

## Domain Intelligence & Derived Signals

Tracking the daily differences (deltas) in these accounts allows the system to generate predictive and operational signals:

- **Pre-Auction Staging:** When a large deposit enters the `2408` division days before a docketed auction, it strongly indicates active third-party bidder staging and high intent to contest the auction.
- **High Competition:** If a single case number shows "Multiple Recipients" depositing funds, several independent bidders have wired money to compete for the same property.
- **Third-Party Interest:** If the name of the party wiring the escrow funds does *not* match the foreclosing Plaintiff bank, a third-party investor is actively involved.
- **Post-Auction Validation (Disbursement):** A sudden, massive balance collapse (e.g., $250,000 dropping to $15) indicates the court has successfully disbursed the winning bid funds, confirming the finalization of a sale.
- **Toxic Bid Alert:** If the predicted maximum bid (inferred from a 5% statutory escrow deposit) is vastly outweighed by surviving code enforcement or municipal liens, the bidder made a highly toxic/unprofitable deposit.

## PostgreSQL Storage

The pipeline downloads, parses the PDFs, calculates the day-over-day movement, and stores the results relationally.

### 1. `TrustAccount`
Stores the daily snapshot per case.
- **Core Fields:** `source`, `report_date`, `case_number`, `amount`
- **Movement Tracking:** `previous_amount`, `delta_amount`, `movement_type` (entered, changed, stable, dropped)
- **Intelligence Flags:** `in_escrow_since`, `multiple_recipients`, `plaintiff_name`, `counterparty_type` (e.g., bank vs. third_party)
- **Correlations:** `match_upcoming_auction`, `winning_bid_match_count`, `is_pre_auction_signal`

### 2. `TrustAccountSummary`
Stores daily aggregate rollups for dashboard reporting.
- **Core Fields:** `report_date`, `scope`, `counterparty_type`, `case_count`, `total_amount`, `avg_amount`, `max_amount`
