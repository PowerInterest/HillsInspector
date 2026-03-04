# Auction Intelligence Dashboard

**Goal:** Build a new `/auction-intel` dashboard tab that shows the *next auction date's* properties merged with TrustAccount escrow data to predict bidder behavior, competition intensity, and risk.

## Background

The `TrustAccount` table (populated by `trust_accounts.py`) already parses daily PDFs from `publicrec.hillsclerk.com/Civil/real_auction_balances/` and `registry_trust_balances/`. It tracks per-case escrow balances, movement deltas, counterparty classification, and winning bid correlations.

**Live data snapshot (2026-02-27):**
- 120 real-auction escrow rows, 76 with `multiple_recipients=1`
- 432 registry/trust balance rows
- All `counterparty_type` currently `unknown` — the intelligence logic must infer bidder intent from plaintiff name comparison and escrow patterns

## User Requirements

- The dashboard shows the **next auction date** (weekdays only, skip dates with zero auctions)
- After 2:00 PM EST on auction day, auto-advance to the next business day
- Display each property as a card with intelligence flags (colored pill tags)
- Sort by "Predicted Bidding Intensity" (highest escrow + multiple recipients first)

---

## Proposed Changes

### Backend

---

#### [NEW] [auction_intel.py](../../app/web/routers/auction_intel.py)

New FastAPI router at `/auction-intel`. Contains:

- `GET /auction-intel` — Main page. Computes "next auction date" logic:
  1. If current time < 2:00 PM EST and today is a weekday, target = today
  2. If current time >= 2:00 PM EST or today is weekend, target = next weekday
  3. Walk forward through weekdays until we find a date with `COUNT(*) > 0` in `foreclosures WHERE auction_date = :target AND archived_at IS NULL`
  4. Pass `target_date` and auction intel data to template

- `GET /auction-intel/{target_date}` — Override to view any specific date

#### [MODIFY] [pg_web.py](../../app/web/pg_web.py)

Add a new function `get_auction_intel_for_date(target_date)` that executes a single SQL query joining:

```sql
SELECT
    f.foreclosure_id AS id,
    f.case_number_raw AS case_number,
    COALESCE(f.strap, f.folio) AS folio,
    f.auction_date,
    COALESCE(f.property_address, bp.property_address) AS property_address,
    f.assessed_value,
    f.final_judgment_amount,
    f.plaintiff,
    f.defendant,
    COALESCE(f.owner_name, bp.owner_name) AS owner_name,
    COALESCE(f.market_value, bp.market_value) AS hcpa_market_value,
    COALESCE(f.beds, bp.beds) AS beds,
    COALESCE(f.baths, bp.baths) AS baths,
    COALESCE(f.heated_area, bp.heated_area) AS heated_area,
    COALESCE(f.year_built, bp.year_built) AS year_built,
    -- Escrow intel
    ta.amount AS escrow_amount,
    ta.in_escrow_since,
    ta.multiple_recipients,
    ta.plaintiff_name AS escrow_plaintiff,
    ta.counterparty_type,
    ta.movement_type,
    ta.previous_amount AS escrow_previous_amount,
    ta.delta_amount AS escrow_delta,
    ta.winning_bid_match_count,
    ta.is_pre_auction_signal,
    -- Lien survival
    COALESCE(enc.liens_surviving, 0) AS liens_surviving,
    COALESCE(enc.est_surviving_debt, 0) AS est_surviving_debt,
    COALESCE(enc.liens_total, 0) AS liens_total,
    -- Photo
    f.photo_url
FROM foreclosures f
LEFT JOIN LATERAL (...hcpa_bulk_parcels...) bp ON TRUE
LEFT JOIN LATERAL (
    SELECT *
    FROM "TrustAccount"
    WHERE source = 'real'
      AND case_number = SUBSTRING(f.case_number_raw FROM 3)
      AND movement_type != 'dropped'
    ORDER BY report_date DESC
    LIMIT 1
) ta ON TRUE
LEFT JOIN LATERAL (...encumbrance_lateral_join...) enc ON TRUE
WHERE f.auction_date = :target_date
  AND f.archived_at IS NULL
ORDER BY
    ta.multiple_recipients DESC NULLS LAST,
    ta.amount DESC NULLS LAST,
    f.case_number_raw
```

Then add Python post-processing to compute the intelligence flags:

```python
def _compute_intel_flags(auction: dict) -> dict:
    flags = []
    escrow = auction.get("escrow_amount")
    judgment = auction.get("final_judgment_amount") or 0
    assessed = auction.get("hcpa_market_value") or 0
    surviving_debt = auction.get("est_surviving_debt") or 0

    # 1. Predicted Max Bid (5% deposit rule => multiply by 20)
    predicted_max_bid = (escrow * 20) if escrow else None

    # 2. HIGH COMPETITION — multiple parties wired funds
    if auction.get("multiple_recipients"):
        flags.append({"tag": "HIGH COMPETITION", "color": "red"})

    # 3. TOXIC BID — surviving liens exceed 25% of predicted max bid
    if predicted_max_bid and surviving_debt > (predicted_max_bid * 0.25):
        flags.append({"tag": "TOXIC BID", "color": "orange"})

    # 4. OVERPAY ALERT — predicted max bid > 140% of assessed value
    if predicted_max_bid and assessed > 0:
        overpay_ratio = predicted_max_bid / assessed
        if overpay_ratio > 1.4:
            flags.append({
                "tag": f"ANOMALOUS VALUATION ({overpay_ratio:.0%})",
                "color": "purple",
            })

    # 5. 3RD PARTY INTEREST — escrow plaintiff != auction plaintiff
    auction_plaintiff = (auction.get("plaintiff") or "").upper()
    escrow_plaintiff = (auction.get("escrow_plaintiff") or "").upper()
    if escrow and escrow_plaintiff and auction_plaintiff:
        # If the escrow plaintiff doesn't match the foreclosing bank,
        # a third party is depositing money
        if escrow_plaintiff not in auction_plaintiff \
           and auction_plaintiff not in escrow_plaintiff:
            flags.append({"tag": "3RD PARTY INTEREST", "color": "green"})

    # 6. ESCROW SURGE — delta > 20% increase from prior report
    prev = auction.get("escrow_previous_amount")
    delta = auction.get("escrow_delta")
    if prev and delta and prev > 0 and (delta / prev) > 0.20:
        flags.append({"tag": "ESCROW SURGE", "color": "blue"})

    auction["predicted_max_bid"] = predicted_max_bid
    auction["intel_flags"] = flags
    return auction
```

#### [MODIFY] [main.py](../../app/web/main.py)

- Import and register the new router: `from app.web.routers import auction_intel`
- `app.include_router(auction_intel.router)`

#### [MODIFY] [base.html](../../app/web/templates/base.html)

Add nav link between "Auctions" and "Review Queue":
```html
<a href="/auction-intel" class="nav-link{% if request.url.path.startswith('/auction-intel') %} is-active{% endif %}">Intel</a>
```

### Frontend

---

#### [NEW] [auction_intel.html](../../app/web/templates/auction_intel.html)

Template extending `base.html`. Displays:

1. **Header bar**: Target auction date, property count, total escrow capital
2. **Date navigation**: Prev/Next arrows to browse auction dates
3. **Property cards** (sorted by bidding intensity):
   - Address, case number, photo thumbnail
   - Final Judgment Amount vs Predicted Max Bid (side-by-side, color-coded)
   - Intelligence flag pills (colored tags)
   - Escrow details: amount, in-escrow-since date, movement delta
   - Surviving liens count + estimated debt
   - Property basics: beds/baths/sqft/year built

#### [MODIFY] [styles.css](../../app/web/static/styles.css)

Add styles for the intelligence flag pills and the intel card layout.

---

## Verification Plan

### Manual Verification
1. Start the web server with `uv run python -m app.web.main`
2. Navigate to `/auction-intel` and confirm it auto-detects the next auction date
3. Verify escrow data joins correctly to the foreclosure cards
4. Verify the intelligence flags display with correct colors
5. Test date navigation (prev/next arrows)
6. Confirm the 2:00 PM EST auto-advance rule works correctly
