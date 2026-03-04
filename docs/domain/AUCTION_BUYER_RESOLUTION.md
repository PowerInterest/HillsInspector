# Auction Buyer Resolution (hcpa_allsales)

The auction website only shows "3rd Party Bidder" — never the real buyer's name. We resolve the real buyer from `hcpa_allsales` (2.4M property transfer records in PostgreSQL) by looking at the **first deed recorded after the auction date** for the same folio.

The key insight is that **different deed types put the auction winner on different sides of the transfer**:

| Deed Type | Code | Winner is | Why |
|-----------|------|-----------|-----|
| Certificate of Title | **CT** | **grantee** | Clerk issues certificate directly **to** the auction winner; grantor is the old foreclosed homeowner |
| Certificate of Deed | **CD** | **grantee** | Same as CT — Clerk-issued certificate **to** the winner |
| Warranty Deed | **WD** | **grantor** | Auction winner already owns the property, now **selling** it |
| Quit Claim Deed | **QC** | **grantor** | Same — winner is **selling/transferring** out |
| Transfer | **TR** | **grantor** | Same — winner is **transferring** |
| Fee / Final Deed | **FD** | **grantor** | Same — winner is **selling** |
| Deed (generic) | **DD** | **grantor** | Same — winner is **selling** |

**How it works in practice:**
- After a foreclosure sale, the Clerk issues a CT or CD to the auction winner (avg 74 days after auction). The `grantee` on that deed IS the buyer.
- If no CT/CD appears in `hcpa_allsales` (HCPA doesn't always record these), we fall back to the first WD/QC/etc., where the `grantor` is the person who bought at auction and is now reselling.
- This logic runs automatically via `_classify_buyer()` in `src/services/pg_auction_results_service.py` as part of the scheduled jobs. It natively computes the buyer in Python rather than relying on a PostgreSQL trigger.

**Coverage:** ~80% of auctions get a real buyer name. The remaining ~20% have no post-auction deed in `hcpa_allsales` (property not yet resold, or folio data gap).
