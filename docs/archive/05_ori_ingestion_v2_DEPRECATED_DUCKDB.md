# Step 5: ORI Ingestion & Chain of Title (V2)

**Version:** 2.0
**Status:** DISABLED (2026-02-09) - V1 SQLite path is active
**Last Updated:** 2026-02-09

> **IMPORTANT**: As of 2026-02-09, the V2 DuckDB-based ORI ingestion is **disabled**.
> `USE_STEP4_V2 = False` in `config/step4v2.py`. All ORI ingestion now uses the V1 path
> via `IngestionService` with SQLite (`data/property_master_sqlite.db`).
>
> **Why disabled:** The V2 path had systemic issues - it queried V1 SQLite tables
> (`parcels`, `bulk_parcels`, `sales_history`) from a DuckDB connection, the V2 DuckDB
> schema was out of sync with the step4v2 code (column name mismatches like `doc_type`
> vs `document_type`), and maintaining two databases added unnecessary complexity.
> The project is migrating to SQLite only.
>
> The V1 path (`_run_ori_ingestion_v1` in orchestrator -> `IngestionService`) searches ORI
> by legal description via browser (CQID 321) with API POST fallback, builds chain of title,
> and stores everything in SQLite.
>
> This document is preserved as a design reference for the iterative discovery algorithm,
> which may be reimplemented against SQLite in the future.

## Overview

Step 4v2 is a complete redesign of the ORI (Official Records Index) ingestion pipeline. It replaces the simple legal description search with an iterative, multi-source discovery algorithm that:

1. Maintains multiple legal description variants with priority ordering
2. Tracks party names with date ranges for bounded searches
3. Links identity variations (name changes, trusts) to the same person
4. Uses a search queue to systematically exhaust all discovery vectors
5. Builds complete 30-year chains for MRTA compliance

## Problem Statement

### Current Issues (v1)

| Issue | Impact |
|-------|--------|
| 18% of auctions have NO folio | Can't start chain search |
| 30 auctions have folio but NO chain | ORI search not finding documents |
| API limited to 25 results | Missing documents for properties with many records |
| Legal description variations | "LOT 44" vs "L 44" vs "LT 44" not matching |
| Shallow chain depth | Average 1.7 periods (need 30 years for MRTA) |
| No cross-party from API | Requires separate browser lookup |

### v2 Solutions

| Solution | Benefit |
|----------|---------|
| Use `/oripublicaccess/` interface | 6,000 result limit (vs 25) |
| Multiple legal description variants | Try all known formats |
| Book/Page search from HCPA Sales History | 100% accurate lookups |
| Case # search for no-folio properties | Bootstrap from Final Judgment |
| Date-bounded name searches | Filter large result sets |
| Iterative discovery | Each document reveals new search vectors |
| Linked identity tracking | Handle name changes, trusts |

---

## ORI Endpoints Reference

### Available Interfaces

| Interface | URL | Limit | Cross-Party | Best For |
|-----------|-----|-------|-------------|----------|
| **oripublicaccess** | `/oripublicaccess/` | 6,000 | Yes (inline) | Primary searches |
| **API Search** | `/Public/ORIUtilities/DocumentSearch/api/Search` | 25 | Yes (`PartiesOne[]`, `PartiesTwo[]`) | Quick targeted lookups |
| **CQID 319** | `/PAVDirectSearch/?CQID=319` | Unlimited | No (one row/party) | Book/Page lookup |
| **CQID 320** | `/PAVDirectSearch/?CQID=320` | Unlimited | No | Instrument lookup |
| **CQID 321** | `/PAVDirectSearch/?CQID=321` | Unlimited | No | Legal description |
| **CQID 326** | `/PAVDirectSearch/?CQID=326` | Unlimited | No | Party name |

### Legal Search Operators (oripublicaccess)

| Operator | Behavior | Use Case |
|----------|----------|----------|
| **Equals** | Exact match only | When you have exact legal description |
| **Begins** | Prefix match | `LOT 44 BLOCK 2 SYMPHONY*` |
| **Contains** | Substring anywhere | Subdivision name discovery |

### Rate Limiting Strategy

```
Primary:   /oripublicaccess/     (6,000 limit, may rate limit)
    │
    ▼ [rate limited or timeout?]
    │
Fallback:  CQID endpoints        (unlimited, slower)
    │
    ▼ [still limited?]
    │
Last:      /api/Search           (25 limit, fast)
```

---

## Database Schema

### Enhanced `legal_variations` Table

Stores multiple legal description formats per property with search tracking.

```sql
CREATE TABLE IF NOT EXISTS legal_variations (
    id INTEGER PRIMARY KEY,
    folio VARCHAR NOT NULL,
    variation_text VARCHAR NOT NULL,

    -- Source tracking
    source_instrument VARCHAR,      -- Instrument # where this was found
    source_type VARCHAR NOT NULL,   -- 'final_judgment', 'hcpa', 'ori_document', 'bulk_import'
    is_canonical BOOLEAN DEFAULT FALSE,

    -- Priority (lower = search first)
    priority INTEGER DEFAULT 99,

    -- Search tracking
    search_attempted BOOLEAN DEFAULT FALSE,
    search_operator VARCHAR,        -- 'EQUALS', 'BEGINS', 'CONTAINS'
    search_result_count INTEGER,
    last_searched_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(folio, variation_text)
);

-- Priority values:
-- 1 = Final Judgment (extracted via VisionService)
-- 2 = HCPA GIS (scraped from property appraiser)
-- 3 = ORI Document (found in chain deed/mortgage)
-- 4 = Bulk Import (raw_legal1-4 from parquet)
-- 5 = Inferred/Normalized (generated variations)
```

### New `property_parties` Table

Tracks all parties discovered for a property with date ranges.

```sql
CREATE TABLE IF NOT EXISTS property_parties (
    id INTEGER PRIMARY KEY,
    folio VARCHAR NOT NULL,

    -- Party identification
    party_name VARCHAR NOT NULL,           -- Exactly as recorded
    party_name_normalized VARCHAR,         -- Cleaned, sorted for matching
    party_role VARCHAR,                    -- 'owner', 'grantor', 'grantee', 'mortgagor', 'mortgagee', 'defendant', 'plaintiff'
    linked_identity_id INTEGER,            -- FK to linked_identities

    -- Date range when this party was active on this property
    active_from DATE,                      -- Acquisition/recording date
    active_to DATE,                        -- Disposition/satisfaction date (NULL = current)

    -- Source tracking
    source_instrument VARCHAR,
    source_document_type VARCHAR,
    recording_date DATE,

    -- Search tracking
    search_attempted BOOLEAN DEFAULT FALSE,
    search_result_count INTEGER,
    last_searched_at TIMESTAMP,

    -- Flags
    is_generic BOOLEAN DEFAULT FALSE,      -- TRUE for "BANK OF AMERICA", etc.

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(folio, party_name, source_instrument)
);

CREATE INDEX idx_property_parties_folio ON property_parties(folio);
CREATE INDEX idx_property_parties_linked ON property_parties(linked_identity_id);
CREATE INDEX idx_property_parties_dates ON property_parties(folio, active_from, active_to);
```

### New `linked_identities` Table

Links name variations to the same person/entity.

```sql
CREATE TABLE IF NOT EXISTS linked_identities (
    id INTEGER PRIMARY KEY,

    -- Primary identification
    canonical_name VARCHAR NOT NULL,       -- Best/primary name
    entity_type VARCHAR,                   -- 'individual', 'trust', 'llc', 'corporation', 'bank'

    -- Linking metadata
    link_type VARCHAR,                     -- 'exact', 'name_change', 'trust_transfer', 'entity_dba', 'spelling_variation'
    confidence FLOAT DEFAULT 1.0,

    -- For Sunbiz integration (Step 6)
    sunbiz_doc_number VARCHAR,
    sunbiz_status VARCHAR,

    notes VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_linked_identities_canonical ON linked_identities(canonical_name);
```

### New `ori_search_queue` Table

Manages the search queue for iterative discovery.

```sql
CREATE TABLE IF NOT EXISTS ori_search_queue (
    id INTEGER PRIMARY KEY,
    folio VARCHAR NOT NULL,

    -- Search parameters
    search_type VARCHAR NOT NULL,          -- 'legal', 'name', 'book_page', 'instrument', 'case'
    search_term VARCHAR NOT NULL,
    search_operator VARCHAR,               -- 'EQUALS', 'BEGINS', 'CONTAINS' (for legal)

    -- Priority (lower = search first)
    priority INTEGER DEFAULT 50,

    -- Status tracking
    status VARCHAR DEFAULT 'pending',      -- 'pending', 'in_progress', 'completed', 'failed', 'rate_limited', 'exhausted'
    attempt_count INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,

    -- Date bounds (for name searches)
    date_from DATE,
    date_to DATE,

    -- Cross-reference (what triggered this search)
    triggered_by_instrument VARCHAR,
    triggered_by_search_id INTEGER,

    -- Results
    result_count INTEGER,
    new_documents_found INTEGER,
    error_message VARCHAR,

    -- Timestamps
    queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    next_retry_at TIMESTAMP,

    UNIQUE(folio, search_type, search_term, COALESCE(search_operator, ''))
);

CREATE INDEX idx_search_queue_status ON ori_search_queue(status, priority);
CREATE INDEX idx_search_queue_folio ON ori_search_queue(folio);
CREATE INDEX idx_search_queue_retry ON ori_search_queue(next_retry_at) WHERE status = 'rate_limited';
```

### Search Queue Priority Values

| Priority | Search Type | Description |
|----------|-------------|-------------|
| 10 | `book_page` | HCPA Sales History (100% accurate) |
| 15 | `instrument` | Direct instrument lookup |
| 20 | `case` | Foreclosure case number |
| 30 | `legal` + BEGINS | Structured legal description |
| 40 | `legal` + CONTAINS | Subdivision discovery |
| 50 | `name` (owner) | Current owner search |
| 60 | `name` (chain party) | Historical owner search |
| 90 | `name` (generic) | Bank/lender names (last resort) |

---

## Algorithm: Iterative Discovery

### Initialization

```python
def initialize_search_queue(folio: str, auction: dict, hcpa_data: dict, final_judgment: dict):
    """
    Bootstrap the search queue with all known data sources.
    Called at the start of Step 4v2 for each property.
    """

    # 1. Legal descriptions (priority order)
    legal_sources = [
        (final_judgment.get('legal_description'), 'final_judgment', 1),
        (hcpa_data.get('legal_description'), 'hcpa', 2),
        (bulk_parcels.get('raw_legal1'), 'bulk_import', 4),
        (bulk_parcels.get('raw_legal2'), 'bulk_import', 4),
        (bulk_parcels.get('raw_legal3'), 'bulk_import', 4),
        (bulk_parcels.get('raw_legal4'), 'bulk_import', 4),
    ]

    for legal_text, source, priority in legal_sources:
        if legal_text:
            save_legal_variation(folio, legal_text, source, priority)
            queue_legal_search(folio, legal_text, operator='BEGINS', priority=30)

    # 2. Book/Page from HCPA Sales History (highest priority)
    for sale in hcpa_data.get('sales_history', []):
        if sale.get('book') and sale.get('page'):
            queue_book_page_search(
                folio,
                sale['book'],
                sale['page'],
                priority=10
            )

    # 3. Case number search (for foreclosure docs)
    case_number = auction.get('case_number')
    if case_number:
        queue_case_search(folio, case_number, priority=20)

    # 4. Party names with date bounds
    defendant = auction.get('defendant') or final_judgment.get('defendant')
    if defendant:
        save_party(folio, defendant, role='defendant', active_to=auction['auction_date'])
        queue_name_search(
            folio,
            defendant,
            date_to=auction['auction_date'],
            priority=50
        )

    owner = hcpa_data.get('owner_name')
    if owner:
        save_party(folio, owner, role='owner', active_from=hcpa_data.get('last_sale_date'))
        queue_name_search(
            folio,
            owner,
            date_from=hcpa_data.get('last_sale_date'),
            priority=50
        )
```

### Main Discovery Loop

```python
def run_discovery(folio: str, max_iterations: int = 50):
    """
    Main iterative discovery loop.
    Continues until chain is complete or all searches exhausted.
    """

    iteration = 0

    while iteration < max_iterations:
        iteration += 1

        # Check stopping conditions
        if is_chain_complete(folio):
            logger.info(f"Chain complete for {folio} after {iteration} iterations")
            break

        # Get next pending search (by priority)
        search = get_next_pending_search(folio)
        if not search:
            logger.info(f"All searches exhausted for {folio}")
            break

        # Execute search
        try:
            mark_search_in_progress(search.id)
            documents = execute_search(search)

            # Process results
            new_docs = 0
            for doc in documents:
                if save_document_if_new(folio, doc):
                    new_docs += 1
                    process_document_for_new_vectors(folio, doc, search.id)

            mark_search_completed(search.id, len(documents), new_docs)

        except RateLimitError:
            mark_search_rate_limited(search.id, retry_after=300)

        except Exception as e:
            mark_search_failed(search.id, str(e))

    # Build chain from all discovered documents
    build_chain_of_title(folio)

    return get_chain_stats(folio)
```

### Processing Documents for New Search Vectors

```python
def process_document_for_new_vectors(folio: str, doc: dict, triggered_by: int):
    """
    Extract new search vectors from a discovered document.
    """

    # 1. Extract legal description variant
    legal = doc.get('legal_description')
    if legal:
        existing = get_legal_variation(folio, legal)
        if not existing:
            save_legal_variation(folio, legal, source='ori_document',
                               source_instrument=doc['instrument'], priority=3)
            queue_legal_search(folio, legal, operator='BEGINS',
                             priority=35, triggered_by=triggered_by)

    # 2. Extract party names with date context
    recording_date = doc.get('recording_date')

    for party in doc.get('PartiesOne', []) or [doc.get('party1')]:
        if party and not is_generic_name(party):
            # Grantor owned property BEFORE this recording
            existing = get_party(folio, party)
            if not existing:
                save_party(folio, party, role='grantor',
                          active_to=recording_date,
                          source_instrument=doc['instrument'])

                # Queue search bounded to before this date
                queue_name_search(folio, party,
                                date_to=recording_date,
                                priority=60,
                                triggered_by=triggered_by)

    for party in doc.get('PartiesTwo', []) or [doc.get('party2')]:
        if party and not is_generic_name(party):
            # Grantee owned property AFTER this recording
            existing = get_party(folio, party)
            if not existing:
                save_party(folio, party, role='grantee',
                          active_from=recording_date,
                          source_instrument=doc['instrument'])

                # Queue search bounded to after this date
                queue_name_search(folio, party,
                                date_from=recording_date,
                                priority=60,
                                triggered_by=triggered_by)

    # 3. Extract referenced instruments (e.g., "mortgage recorded in Book 1234 Page 567")
    # This requires OCR/VisionService extraction
    if doc.get('extracted_data'):
        for ref in doc['extracted_data'].get('referenced_instruments', []):
            if ref.get('book') and ref.get('page'):
                queue_book_page_search(folio, ref['book'], ref['page'],
                                      priority=15, triggered_by=triggered_by)
            if ref.get('instrument'):
                queue_instrument_search(folio, ref['instrument'],
                                       priority=15, triggered_by=triggered_by)

    # 4. Check for linked identity (name change, trust)
    check_and_link_identities(folio, doc)
```

---

## Linked Identity Matching

### Detection Rules

```python
def check_and_link_identities(folio: str, doc: dict):
    """
    Detect when grantor and grantee might be the same person.
    """

    party1 = doc.get('party1') or ', '.join(doc.get('PartiesOne', []))
    party2 = doc.get('party2') or ', '.join(doc.get('PartiesTwo', []))

    if not party1 or not party2:
        return

    # Normalize names
    p1_norm = normalize_name(party1)
    p2_norm = normalize_name(party2)

    link_type = None
    confidence = 0.0

    # Rule 1: Exact match after normalization
    if p1_norm == p2_norm:
        link_type = 'exact'
        confidence = 1.0

    # Rule 2: Trust transfer (SMITH JOHN -> SMITH JOHN TRUSTEE)
    elif 'TRUSTEE' in party2.upper() or 'TRUST' in party2.upper():
        base_name = extract_base_name_from_trust(party2)
        if names_match(p1_norm, base_name, threshold=0.85):
            link_type = 'trust_transfer'
            confidence = 0.9

    # Rule 3: Name variation (SMITH JOHN H -> SMITH JOHN)
    elif fuzzy_match(p1_norm, p2_norm) > 0.85:
        link_type = 'spelling_variation'
        confidence = fuzzy_match(p1_norm, p2_norm)

    # Rule 4: Marriage name change (same first name, different last)
    # Only try this as last resort
    elif same_first_name(party1, party2):
        link_type = 'name_change'
        confidence = 0.5  # Low confidence, needs verification

    if link_type:
        # Create or find linked identity
        identity = get_or_create_linked_identity(party1, party2, link_type, confidence)

        # Update both party records
        update_party_linked_identity(folio, party1, identity.id)
        update_party_linked_identity(folio, party2, identity.id)

        # Mark document as self-transfer if applicable
        if doc.get('document_type') in DEED_TYPES:
            update_document_self_transfer(doc['instrument'], link_type)
```

### Name Normalization

```python
def normalize_name(name: str) -> str:
    """
    Normalize a name for comparison.

    Steps:
    1. Uppercase
    2. Remove suffixes (JR, SR, II, III, IV, ESQ, MD, PHD)
    3. Remove titles (MR, MRS, MS, DR, MISS)
    4. Remove punctuation
    5. Sort parts alphabetically (handles LAST FIRST vs FIRST LAST)
    """
    if not name:
        return ""

    name = name.upper()

    # Remove suffixes
    for suffix in [' JR', ' SR', ' II', ' III', ' IV', ' ESQ', ' MD', ' PHD']:
        name = name.removesuffix(suffix)

    # Remove titles
    for title in ['MR ', 'MRS ', 'MS ', 'DR ', 'MISS ']:
        name = name.removeprefix(title)

    # Remove punctuation
    name = re.sub(r'[.,;:]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()

    # Sort parts alphabetically
    parts = sorted(name.split())
    return ' '.join(parts)
```

### Generic Name Detection

```python
# Names that should not trigger name searches (too many results)
GENERIC_NAMES = {
    # Banks
    'WELLS FARGO', 'BANK OF AMERICA', 'CHASE', 'JPMORGAN', 'CITIBANK',
    'US BANK', 'PNC BANK', 'TRUIST', 'REGIONS BANK', 'FIFTH THIRD',

    # Mortgage companies
    'MERS', 'MORTGAGE ELECTRONIC', 'FANNIE MAE', 'FREDDIE MAC',
    'QUICKEN LOANS', 'ROCKET MORTGAGE', 'FREEDOM MORTGAGE',

    # Title companies
    'FIDELITY NATIONAL', 'FIRST AMERICAN', 'OLD REPUBLIC',
    'STEWART TITLE', 'CHICAGO TITLE',

    # Government
    'HILLSBOROUGH COUNTY', 'STATE OF FLORIDA', 'UNITED STATES',
}

def is_generic_name(name: str) -> bool:
    """Check if a party name is too generic for searching."""
    name_upper = name.upper()
    return any(generic in name_upper for generic in GENERIC_NAMES)
```

---

## Chain Completeness Check

### MRTA Requirements

The Marketable Record Title Act (MRTA) requires a 30-year chain of title.

```python
def is_chain_complete(folio: str) -> bool:
    """
    Check if we have a complete chain of title.

    Complete means:
    1. At least 30 years of ownership history, OR
    2. Chain goes back to a root of title (plat, government patent), OR
    3. All search vectors exhausted
    """

    chain = get_chain_of_title(folio)

    if not chain:
        return False

    # Check 1: Total years covered
    years_covered = calculate_chain_years(chain)
    if years_covered >= 30:
        return True

    # Check 2: Root of title found
    earliest = chain[-1]  # Oldest period
    if earliest.get('acquisition_doc_type') in ['PLAT', 'PATENT', 'GOVERNMENT DEED']:
        return True

    # Check 3: All searches exhausted (handled by caller)
    return False

def calculate_chain_years(chain: list) -> float:
    """Calculate total years covered by chain."""
    if not chain:
        return 0.0

    # Chain is ordered newest to oldest
    newest_date = chain[0].get('disposition_date') or date.today()
    oldest_date = chain[-1].get('acquisition_date')

    if not oldest_date:
        return 0.0

    return (newest_date - oldest_date).days / 365.25
```

---

## Stopping Conditions

The discovery loop stops when ANY of these conditions are met:

| Condition | Description |
|-----------|-------------|
| **Chain Complete** | 30+ years covered OR root of title found |
| **Searches Exhausted** | No more pending searches in queue |
| **Max Iterations** | Safety limit (default 50) reached |
| **Max Documents** | Property has 500+ documents (likely commercial) |
| **Rate Limited** | All searches rate-limited, none can retry yet |

---

## Error Handling

### Rate Limiting

```python
def handle_rate_limit(search_id: int):
    """Handle rate limiting from ORI."""

    # Mark search for retry
    update_search_queue(
        search_id,
        status='rate_limited',
        next_retry_at=datetime.now() + timedelta(seconds=300),
        attempt_count=increment
    )

    # If too many rate limits, switch to fallback endpoint
    rate_limit_count = count_rate_limited_searches(last_minutes=5)
    if rate_limit_count > 5:
        switch_to_fallback_endpoint()
```

### Missing Data Handling

```python
def handle_no_folio(auction: dict) -> str:
    """
    Handle auctions with no folio/parcel_id.

    Strategy:
    1. Search by case number to find Lis Pendens
    2. Extract legal description from Lis Pendens
    3. Use that to find property
    """

    case_number = auction.get('case_number')
    if not case_number:
        return None

    # Search ORI by case number
    docs = search_by_case_number(case_number)

    # Look for Lis Pendens (filed at start of foreclosure)
    for doc in docs:
        if 'LIS PENDENS' in doc.get('doc_type', '').upper():
            legal = doc.get('legal_description')
            if legal:
                # Use legal description to find folio in HCPA
                folio = lookup_folio_by_legal(legal)
                if folio:
                    return folio

    # Fallback: use defendant name + address to find folio
    return None
```

---

## Integration with Other Steps

### Input from Previous Steps

| Step | Data Used |
|------|-----------|
| Step 1 (Auction Scrape) | `case_number`, `defendant`, `auction_date` |
| Step 2 (Final Judgment) | `legal_description`, `defendant`, `plaintiff`, `mortgage_info` |
| Step 3 (HCPA GIS) | `legal_description`, `owner_name`, `sales_history[]` |
| Bulk Import | `raw_legal1`, `raw_legal2`, `raw_legal3`, `raw_legal4` |

### Output to Next Steps

| Step | Data Provided |
|------|---------------|
| Step 5 (Lien Survival) | `documents[]`, `chain_of_title[]`, `encumbrances[]` |
| Step 6 (Sunbiz) | `linked_identities[]` where `entity_type` in ('llc', 'corporation', 'trust') |

---

## Metrics and Monitoring

### Success Metrics

```sql
-- Chain completeness by folio
SELECT
    folio,
    COUNT(DISTINCT id) as chain_periods,
    SUM(years_covered) as total_years,
    CASE
        WHEN SUM(years_covered) >= 30 THEN 'COMPLETE'
        WHEN mrta_status = 'ROOT_OF_TITLE' THEN 'COMPLETE'
        ELSE 'INCOMPLETE'
    END as status
FROM chain_of_title
GROUP BY folio;

-- Search efficiency
SELECT
    search_type,
    COUNT(*) as total_searches,
    AVG(result_count) as avg_results,
    AVG(new_documents_found) as avg_new_docs,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
FROM ori_search_queue
GROUP BY search_type;

-- Document discovery sources
SELECT
    source_type,
    COUNT(*) as documents_found
FROM documents d
JOIN ori_search_queue q ON d.triggered_by_search_id = q.id
GROUP BY source_type;
```

---

## Configuration

```python
# config/step4v2.py

STEP4V2_CONFIG = {
    # Discovery limits
    'max_iterations_per_folio': 50,
    'max_documents_per_folio': 500,
    'max_searches_per_folio': 200,

    # Rate limiting
    'requests_per_minute': 30,
    'rate_limit_backoff_seconds': 300,
    'max_consecutive_rate_limits': 5,

    # Search priorities
    'priority_book_page': 10,
    'priority_instrument': 15,
    'priority_case': 20,
    'priority_legal_begins': 30,
    'priority_legal_contains': 40,
    'priority_name_owner': 50,
    'priority_name_chain': 60,
    'priority_name_generic': 90,

    # Chain requirements
    'mrta_years_required': 30,

    # Matching thresholds
    'name_fuzzy_threshold': 0.85,
    'name_change_confidence': 0.5,
    'trust_transfer_confidence': 0.9,

    # Generic names to skip
    'generic_name_file': 'config/generic_names.txt',
}
```

---

## User Decisions

These decisions were made during implementation planning:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Database Location** | New file (`property_master_v2.db`) | Keep old database intact for comparison and fallback |
| **Data Migration** | Auctions + parcels only | Step 4v2 rebuilds chain/documents/encumbrances from scratch |
| **Old Step 4 Code** | Keep as fallback (v1) | Rename existing code, use v2 by default with v1 available |
| **Max File Size** | 800 lines per file | Keep code readable and maintainable |

### Migrated Tables

| Table | Rows | Notes |
|-------|------|-------|
| `auctions` | 106 | Core auction data |
| `parcels` | 92 | HCPA enriched data |
| `bulk_parcels` | 529,076 | HCPA bulk import |
| `sales_history` | 312 | HCPA GIS sales |
| `permits` | 165 | Building permits |
| `market_data` | 130 | Zillow/Realtor data |
| `home_harvest` | 81 | MLS data |
| `scraper_outputs` | 2,539 | Debug data |

### Tables Rebuilt by Step 4v2

| Table | Notes |
|-------|-------|
| `documents` | ORI document metadata |
| `chain_of_title` | Ownership timeline |
| `encumbrances` | Liens/mortgages |
| `legal_variations` | Legal description variants |
| `property_parties` | Party names with dates (NEW) |
| `linked_identities` | Name linking (NEW) |
| `ori_search_queue` | Search tracking (NEW) |

---

## Migration Plan

### Phase 1: Database Migration ✅ COMPLETE

Created `property_master_v2.db` with:
- All existing tables from v1 schema
- New Step4v2 tables (`property_parties`, `linked_identities`, `ori_search_queue`)
- Enhanced `legal_variations` with priority and search tracking
- Migrated auction/parcel data, left chain tables empty for rebuild

**Migration Script:** `src/db/migrations/create_v2_database.py`

```bash
uv run python -m src.db.migrations.create_v2_database
```

### Phase 2: Code Implementation (CURRENT)

Each service file must be ≤800 lines for readability.

1. **`src/services/step4v2/search_queue.py`** - Search queue management
2. **`src/services/step4v2/name_matcher.py`** - Name normalization and matching
3. **`src/services/step4v2/discovery.py`** - Main iterative discovery loop
4. **`src/services/step4v2/chain_builder.py`** - Chain of title construction

### Phase 3: Integration

1. Update `TitleChainService` to use v2 when configured
2. Add v2 endpoints to `ori_api_scraper.py`
3. Update orchestrator to call Step 4v2

### Phase 4: Testing ✅ COMPLETE

Tested on multiple properties with successful results:

**Test 1 - Folio 19311163D000011000160U:**
- 63 documents found in 7 iterations
- 55.5 years chain coverage (MRTA complete)
- 27 ownership periods, 36 encumbrances
- Name linking: "RODRIGUEZ JUAN B" <-> "RODRIGUEZ MARIA M RODRIGUEZ"

**Test 2 - Folio 203131B48000004000220U:**
- 49 documents found in 10 iterations
- 61.7 years chain coverage (MRTA complete)
- 22 ownership periods, 28 encumbrances
- Name linking: "BERKSTRESSER LEONARD MICHAEL" <-> "BERKSTRESSER FAMILY TRUST"

---

## Known Issues & Fixes

### Issue 1: DuckDB `last_insert_rowid()` Not Supported

**Error:** `Catalog Error: Scalar Function with name last_insert_rowid does not exist!`

**Cause:** SQLite's `last_insert_rowid()` function doesn't exist in DuckDB.

**Fix:** Use DuckDB's `RETURNING` clause instead:
```python
# Before (SQLite-style):
conn.execute("INSERT INTO linked_identities (...) VALUES (...)")
result = conn.execute("SELECT last_insert_rowid()").fetchone()

# After (DuckDB-style):
result = conn.execute("""
    INSERT INTO linked_identities (...) VALUES (...)
    RETURNING id
""").fetchone()
```

**File:** `src/services/step4v2/name_matcher.py:359-370`

---

### Issue 2: ORI API Timestamps in Seconds, Not Milliseconds

**Error:** All recording dates showing as `1970-01-21` (Unix epoch)

**Cause:** The ORI API returns `RecordDate` as Unix timestamps in **seconds** (e.g., `1765796303`), but the code was dividing by 1000 assuming milliseconds.

**Evidence:**
- `1765796303` seconds = 2025-12-15 (correct)
- `1765796303 / 1000` = 1970-01-21 (incorrect)

**Fix:** Auto-detect timestamp format:
```python
def _parse_ori_date(self, date_val):
    if isinstance(date_val, (int, float)):
        # Timestamps > 4 billion are likely in milliseconds
        # (dates after year 2096 in seconds)
        ts = date_val / 1000 if date_val > 4_000_000_000 else date_val
        return datetime.fromtimestamp(ts, tz=UTC).date()
```

**File:** `src/services/step4v2/discovery.py:285-302`

---

### Issue 3: Legal Description Searches Too Specific

**Error:** Legal searches returning 0 results despite property having documents

**Cause:** Raw legal descriptions like `"Lot 16, Block 11, KINGS LAKE PHASE 3, according to the plat..."` are too specific for ORI's BEGINS search.

**Evidence:**
- `"KINGS LAKE PHASE 3"` → 68 results ✓
- `"Lot 16, Block 11, KINGS LAKE PHASE 3, according to..."` → 0 results ✗

**Fix:** Use `parse_legal_description()` and `generate_search_permutations()` to create multiple search terms:
```python
# Generated permutations:
# 1. "L 16 B 11 KINGS LAKE PHASE*"
# 2. "L 16 B 11 KINGS LAKE*"
# 3. "L 16 B 11 KINGS*"
# 4. etc.
```

**File:** `src/services/step4v2/search_queue.py:184-234`

---

### Issue 4: Instrument Number Type Mismatch

**Error:** `Could not convert string '129097217491' to INT32 when casting from source column instrument_number`

**Cause:** ORI API returns `Instrument` as an integer, but the `instrument_number` column is VARCHAR. When passing an integer to the query, DuckDB tried to cast the column instead.

**Fix:** Convert instrument to string before querying:
```python
instrument = doc.get("Instrument") or doc.get("instrument_number")
if not instrument:
    return False
# Ensure instrument is a string for VARCHAR column comparison
instrument = str(instrument)
```

**File:** `src/services/step4v2/discovery.py:220-225`

---

### Issue 5: DuckDB UNIQUE Constraint with COALESCE

**Error:** `COALESCE(search_operator, '')` not supported in UNIQUE constraint

**Cause:** DuckDB doesn't support function calls in UNIQUE constraint definitions.

**Fix:** Use `DEFAULT ''` on the column instead:
```sql
-- Before:
search_operator VARCHAR,
UNIQUE(folio, search_type, search_term, COALESCE(search_operator, ''))

-- After:
search_operator VARCHAR DEFAULT '',
UNIQUE(folio, search_type, search_term, search_operator)
```

**File:** `src/db/migrations/create_v2_database.py`

---

### Issue 6: Document Lot/Block Filtering Too Permissive (FIXED)

**Error:** Properties with >300 chain periods due to cross-lot contamination

**Cause:** The `_document_matches_lot_block` function in `discovery.py` was too permissive. When a document had a legal description that could be parsed but didn't contain a specific lot number, the function returned `True` (allowed the document).

**Example:**
- Searching for: TOUCHSTONE PHASE 2 LOT 4 BLOCK 8
- Document legal: "TOUCHSTONE PHASE 2" (no lot specified)
- Old behavior: Document was **allowed** (wrong!)
- New behavior: Document is **rejected**

**Evidence (before fix):**
- Folio 192935B6Y000008000040U (TOUCHSTONE LOT 4): 1,542 chain periods
- Folio 2228285BZ000020000030P (LINCOLN PARK SOUTH LOT 3): 925 chain periods

**Rule:** If a document's legal description mentions the same subdivision but we cannot parse a specific lot from it, the document is **rejected** rather than allowed. This prevents documents for other lots in the same subdivision from contaminating the chain of title.

**Fix Applied (2025-12-25):**
```python
# discovery.py:106-116
if not doc_lots:
    # Document has no lot info - check if it mentions our subdivision
    # If it does, this is likely a subdivision-wide document (HOA, plat, etc.)
    # that applies to ALL lots, not specifically to our lot - REJECT it
    if expected_subdivision and expected_subdivision.upper() in (doc_legal or "").upper():
        # Document mentions our subdivision but has no specific lot
        # This could be for ANY lot in the subdivision - reject it
        return False
    # Document doesn't mention our subdivision at all
    # Could be a general lien/mortgage - allow with caution
    return True
```

**File:** `src/services/step4v2/discovery.py:65-131`

**Status:** ✅ FIXED - Implemented 2025-12-25

**Note:** Properties with existing contaminated data need to be reset and reprocessed using `scripts/reset_problematic_properties.py`.

---

## Files Created/Modified

| File | Status | Lines | Description |
|------|--------|-------|-------------|
| `src/db/migrations/create_v2_database.py` | ✅ Created | ~400 | Schema migration |
| `src/services/step4v2/__init__.py` | ✅ Created | 24 | Package exports |
| `src/services/step4v2/search_queue.py` | ✅ Created | 620 | Search queue management |
| `src/services/step4v2/name_matcher.py` | ✅ Created | 475 | Name matching logic |
| `src/services/step4v2/discovery.py` | ✅ Created | 525 | Main discovery loop |
| `src/services/step4v2/chain_builder.py` | ✅ Created | 595 | Chain construction |
| `src/scrapers/ori_api_scraper.py` | ✅ Modified | - | Added Book/Page search (CQID 319) |
| `src/orchestrator.py` | ✅ Modified | - | Added v2 dispatch logic |
| `config/step4v2.py` | ✅ Created | ~100 | Configuration |
| `config/generic_names.txt` | ✅ Created | ~80 | Generic name list |
