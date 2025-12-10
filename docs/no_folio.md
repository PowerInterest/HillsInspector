# Handling Properties Without Valid Folio/Parcel IDs

## Problem

Some foreclosure auctions don't have valid parcel IDs (folio numbers). This commonly occurs with:

1. **Mobile Home Foreclosures** - The borrower owns the mobile home but not the land (which belongs to a mobile home park). Example: Case `292025CC016626A001HC` - a 1984 KING mobile home on Lot 104 in Camelot Mobile Home Park.

2. **Personal Property Liens** - Liens on vehicles, boats, or other personal property.

3. **Scraping Errors** - The auction site sometimes shows "Property Appraiser" or other garbage instead of the actual parcel ID.

When the folio is invalid, we can't:
- Look up property details from HCPA bulk data
- Search ORI by legal description (since we don't have one)
- Build a chain of title in the traditional way

## Solution: Party-Based ORI Search

Instead of searching ORI by legal description, we search by **plaintiff** (foreclosing party) and **defendant** (borrower) names. These are extracted from the PAV (Public Access Viewer) page during auction scraping.

### Data Flow

```
1. Auction Scraper extracts Party 1 (plaintiff) and Party 2 (defendant)
   from the KeywordSearch API response when downloading the Final Judgment

2. Party data is stored in the `auctions` table (plaintiff, defendant columns)

3. During Step 5 (ORI Ingestion), if is_valid_folio() returns False:
   - Check if we have plaintiff/defendant data
   - If yes, call ingest_property_by_party() instead of skipping
   - Search ORI by defendant name (borrower - likely has title docs)
   - Search ORI by plaintiff name (may find lis pendens, mortgages)
   - Build chain of title from found documents
```

### Implementation Details

#### 1. Party Extraction (auction_scraper.py)

The `_download_final_judgment` method was modified to:
- Return a dict instead of just the PDF path
- Extract Party 1 and Party 2 from the KeywordSearch API response
- Handle various field name formats (Party1, Party 1, party1, PARTY1)

```python
async def _download_final_judgment(...) -> Dict[str, Any]:
    """Returns: Dict with keys: pdf_path, plaintiff, defendant (any may be None)"""
    result = {"pdf_path": None, "plaintiff": None, "defendant": None}

    # In the response handler:
    party_info["plaintiff"] = first_record.get("Party1") or first_record.get("Party 1")
    party_info["defendant"] = first_record.get("Party2") or first_record.get("Party 2")
```

#### 2. Folio Validation (pipeline.py)

```python
INVALID_FOLIO_VALUES = {
    'property appraiser', 'n/a', 'none', '', 'unknown', 'pending',
    'see document', 'multiple', 'various', 'tbd', 'na'
}

def is_valid_folio(folio: str) -> bool:
    """
    Returns False for:
    - Empty/None values
    - Known invalid values like "Property Appraiser"
    - Values that are too short (< 6 chars)
    - Values that are all letters (likely labels, not IDs)
    """
```

#### 3. Party-Based Ingestion (ingestion_service.py)

New method `ingest_property_by_party()`:

```python
def ingest_property_by_party(
    self,
    prop: Property,
    plaintiff: Optional[str] = None,
    defendant: Optional[str] = None
):
    """
    Ingest ORI documents by searching party names instead of legal description.
    Used as fallback when folio is invalid (e.g., mobile home foreclosures).
    """
    # Search by defendant first (borrower - more likely to have title docs)
    if defendant:
        search_name = self._normalize_party_name_for_search(defendant)
        docs = self.ori_scraper.search_by_party(search_name)

    # Also search by plaintiff (may find lis pendens, mortgages)
    if plaintiff:
        search_name = self._normalize_party_name_for_search(plaintiff)
        docs.extend(self.ori_scraper.search_by_party(search_name))

    # Group, dedupe, download PDFs, build chain...
```

#### 4. Party Name Normalization

The `_normalize_party_name_for_search()` method cleans up party names:

| Input | Output |
|-------|--------|
| `SMITH, JOHN DOE ET AL` | `SMITH JOHN DOE*` |
| `JONES MARY ANN A/K/A JONES MARY` | `JONES MARY ANN*` |
| `RODRIGUEZ, MARIA F/K/A GARCIA MARIA` | `RODRIGUEZ MARIA*` |
| `WELLS FARGO BANK, N.A.` | `WELLS FARGO BANK N.A.*` |

Transformations:
- Converts "LASTNAME, FIRSTNAME" to "LASTNAME FIRSTNAME"
- Truncates at A/K/A, F/K/A, D/B/A patterns (keeps primary name only)
- Removes suffixes: ET AL, ET UX, AS TRUSTEE, INDIVIDUALLY, AND ALL
- Removes content in parentheses
- Adds wildcard (*) for partial matching

#### 5. Pipeline Integration (Step 5)

```python
# In run_full_pipeline(), Step 5:
if not is_valid_folio(folio):
    plaintiff = row.get("plaintiff")
    defendant = row.get("defendant")

    if plaintiff or defendant:
        logger.info(f"Invalid folio '{folio}', trying party-based ORI search")
        prop = Property(case_number=case_number, parcel_id=folio, ...)
        ingestion_service.ingest_property_by_party(prop, plaintiff, defendant)
        party_search_count += 1
    else:
        logger.warning(f"Invalid folio '{folio}', no party data for fallback")
        invalid_folio_count += 1
    continue
```

## Database Schema

The `auctions` table already had columns for plaintiff/defendant:

```sql
CREATE TABLE auctions (
    ...
    plaintiff VARCHAR,      -- Party 1 from PAV page
    defendant VARCHAR,      -- Party 2 from PAV page
    ...
);
```

## Limitations

1. **Party search returns ALL documents** for that party, not just those related to the foreclosed property. This can include unrelated transactions.

2. **Name variations** - The same person may be recorded under different names (maiden name, misspellings, etc.). The wildcard helps but isn't perfect.

3. **No legal description filtering** - Unlike folio-based searches, we can't filter results by legal description since mobile homes don't have traditional legal descriptions.

4. **Dependent on PAV data** - If the auction scraper doesn't capture plaintiff/defendant, we have no fallback.

## Future Improvements

1. **VIN-based search** - For mobile homes, search ORI by the Vehicle Identification Number (VIN) if extracted from the Final Judgment.

2. **Address-based filtering** - Filter party search results by matching the property address in the legal description.

3. **Backfill script** - Create a script to backfill plaintiff/defendant for existing auctions that were scraped before this feature was added.

## Related Files

- `src/scrapers/auction_scraper.py` - Party extraction during scraping
- `src/services/ingestion_service.py` - `ingest_property_by_party()` and `_normalize_party_name_for_search()`
- `src/pipeline.py` - `is_valid_folio()` and Step 5 fallback logic
- `src/models/property.py` - `plaintiff` and `defendant` fields on Property model
- `src/db/operations.py` - `upsert_auction()` saves plaintiff/defendant to DB
