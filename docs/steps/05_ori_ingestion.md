# Step 5: ORI Ingestion & Chain of Title

## Overview
This is the core of the title analysis. It searches the Hillsborough County Official Records Index (ORI) for all documents affecting the property's title (Deeds, Mortgages, Liens, etc.), downloads them, and builds a Chain of Title to determine ownership and encumbrances.

## Source
- **URL**: [Hillsborough Clerk ORI](https://publicaccess.hillsclerk.com/PAVDirectSearch/index.html)
- **Method**: Hybrid
    - **API**: `.../DocumentSearch/api/Search` (Primary, fast)
    - **Browser**: Playwright (Fallback for hard-to-find records)

## Search Strategies

### 1. Legal Description Search (CQID 321)
**Primary Method.** We construct search terms from the property's legal description (from HCPA).
- **Strategy**: Use prefix wildcards (e.g., `L 198 TUSCANY*`).
- **Logic**: `src/utils/legal_description.py` parses the full legal description to extract Lot, Block, and Subdivision name.
- **Challenge**: Clerk data entry varies (e.g., "SUB" vs "SUBD", "AT TAMPA PALMS" vs omitted). We store multiple legal variations to catch all documents.

### 2. Book/Page Search (CQID 319)
**Secondary Method.** Used when we have a specific reference from HCPA Sales History.
- **Precision**: 100% accurate for specific documents.
- **Limitation**: Only finds the document referenced, not the full history.

### 3. Party Name Search (CQID 326)
**Gap-Filling Method.** Used to find missing links in the chain or resolve "Party 2" (Grantee).
- **Use Case**: Finding a deed where the legal description was typoed but the owner name is correct.
- **Use Case**: Resolving missing Grantees (see below).

### 4. Party-Based Fallback (No Folio)
**Edge Case Method.** Used when a property has no valid Folio (e.g., Mobile Homes, Personal Property).
- We search ORI using the **Plaintiff** and **Defendant** names extracted from the Auction/Final Judgment.
- This returns *all* documents for those parties, which we then filter.

## Document Processing

1.  **Download**: Documents are downloaded as PDFs to `data/properties/{folio}/documents/`.
2.  **Analysis**: `VisionService` (Qwen-VL) analyzes the PDF to extract:
    - **Parties**: Grantor, Grantee, Borrower, Lender.
    - **Dates**: Recording Date, Execution Date.
    - **Amounts**: Mortgage principal, Lien amount.
    - **Legal**: Verbatim legal description (for verification).

## Chain of Title Logic

The `TitleChainService` (`src/services/title_chain_service.py`) builds an ownership timeline using sophisticated algorithms.

### Core Algorithm: Anchor & Fill

1. **Sort Transfer Docs**: Orders deeds by recording date (oldest to newest)
2. **Select Best Path**: Walk backwards from most recent deed to avoid unrelated deeds
3. **Link Owners**: Match Grantee of Deed A → Grantor of Deed B using NameMatcher
4. **Identify Gaps**: Flag where Grantor B doesn't match Grantee A
5. **Fill Gaps**: Infer ownership from support docs (mortgages, NOCs, liens)
6. **Tail Inference**: Detect new owners from mortgages after the last recorded deed

### Transfer Document Detection

```python
def _is_transfer_doc(self, doc_type: str) -> bool:
    """Documents that anchor the chain."""
    # Core anchors
    if "CERTIFICATE OF TITLE" in dt: return True
    if "TAX DEED" in dt: return True
    if "DEED" in dt: return True  # Warranty, Quit Claim, etc.

    # Non-standard transfers
    if "CONTRACT FOR DEED" in dt: return True
    if "AGREEMENT FOR DEED" in dt: return True
    if "PERSONAL REPRESENTATIVE" in dt and "DEED" in dt: return True

    # Probate transfers
    if "PROBATE" in dt or "SUMMARY ADMINISTRATION" in dt: return True

    # Exclusions (mortgage-like, not transfers)
    if "DEED OF TRUST" in dt: return False  # Security instrument
```

### Best Path Selection

ORI searches can return unrelated deeds for the same subdivision. This method keeps only the best-linked path:

```python
def _select_best_deed_path(self, deed_entries):
    """Walk backwards from most recent deed, selecting best-matching predecessors."""
    entries = sorted(deed_entries, key=lambda d: self._parse_date(d.get("date")))
    anchor = entries[-1]  # Start with most recent
    path_rev = [anchor]
    current = anchor

    while True:
        current_grantor = current.get("grantor")
        best = None
        best_score = 0.0

        for cand in entries:
            if cand in used: continue
            if cand_date >= current_date: continue

            cand_grantee = cand.get("grantee")
            match_type, score = NameMatcher.match(cand_grantee, current_grantor)

            if match_type != "NONE" and score >= 0.8:
                if score > best_score:
                    best = cand
                    best_score = score

        if not best: break
        path_rev.append(best)
        current = best

    return list(reversed(path_rev))
```

### Gap Inference from Support Documents

When the chain has gaps, we infer ownership from non-transfer documents:

```python
def _infer_owner_in_interval(self, start, end, support_docs, desired_owner):
    """Find ownership signals from mortgages, NOCs, liens in date range."""

    for doc in support_docs:
        doc_date = self._parse_date(doc.get("recording_date"))
        if doc_date <= start or doc_date >= end:
            continue

        owner = self._owner_candidate_from_support_doc(doc)
        if owner:
            candidates.append({
                "owner": owner,
                "date": doc_date,
                "source_type": doc.get("doc_type"),
                "confidence": 0.6
            })

    # Prefer candidate that matches the deed grantor
    for c in candidates:
        if NameMatcher.are_linked(c["owner"], desired_owner):
            return c

    # Otherwise, return most frequent owner candidate
    return most_frequent_candidate
```

### Owner Extraction from Support Docs

```python
def _owner_candidate_from_support_doc(self, doc):
    """Extract owner name based on document type."""
    doc_type = doc.get("doc_type").upper()
    party1 = doc.get("party1")
    party2 = doc.get("party2")

    # Mortgages: owner/borrower is party1
    if "MORTGAGE" in doc_type: return party1

    # NOC: signed by owner (party1)
    if "NOTICE OF COMMENCEMENT" in doc_type: return party1

    # Lis Pendens: defendant is party2
    if "LIS PENDENS" in doc_type: return party2

    # HOA/Liens: debtor is party2
    if "HOA" in doc_type or "LIEN" in doc_type: return party2
```

### Tail Inference

Handles cases where the last deed is old but a new owner appears in later mortgages:

```python
# Example: Last deed is 1978, but mortgage in 2001 shows new owner
if chain:
    last_owner = chain[-1].get("grantee")

    # Look for signals from last_date to NOW
    tail_implied = self._infer_owner_in_interval(
        start=last_date,
        end=datetime.max,
        support_docs=support_docs,
        desired_owner=""  # Any new owner
    )

    if tail_implied:
        new_owner = tail_implied.get("owner")
        # Only add if it's actually a NEW owner
        if not NameMatcher.are_linked(last_owner, new_owner):
            chain.append({
                "grantor": last_owner,
                "grantee": new_owner,
                "doc_type": "IMPLIED",
                "link_status": "IMPLIED",
                "notes": ["Implied from support doc (Tail)"]
            })
```

### MRTA Check (Marketable Record Title Act)

Florida's 30-year rule for marketable title:

```python
mrta_status = "INSUFFICIENT"
if chain:
    oldest_date = chain[0].get("date")
    years_covered = (datetime.now() - oldest_date).days / 365.25

    if years_covered >= 30:
        mrta_status = "SATISFIED"
    else:
        mrta_status = f"PARTIAL ({int(years_covered)} years)"
```

## Name Matching Logic

The `NameMatcher` (`src/utils/name_matcher.py`) handles robust name comparison for chain linking.

### Match Types

| Type | Score | Description | Example |
|------|-------|-------------|---------|
| EXACT | 1.0 | Identical token sets | "John Smith" = "John Smith" |
| SUPERSET | 0.95 | Name2 adds parties to Name1 | "John Smith" → "John Smith and Jane Doe" |
| SUBSET | 0.95 | Name2 removes parties from Name1 | "John Smith and Jane Doe" → "John Smith" |
| ALIAS | 0.90 | Matches via nickname table | "Bob Johnson" = "Robert Johnson" |
| FUZZY_JACCARD | 0.65+ | Token overlap similarity | "John A Smith" ≈ "John Smith" |
| FUZZY_STRING | 0.88+ | Levenshtein for typos | "Steven Jobs" ≈ "Stephen Jobs" |

### Token Normalization

Names are normalized before comparison:

```python
STOPWORDS = {
    "THE", "AND", "OR", "OF", "&",
    "LLC", "INC", "CORP", "PA", "LTD",  # Entity suffixes
    "TRUST", "TRUSTEE", "REVOCABLE", "LIVING", "FAMILY", "ESTATE",
    "HUSBAND", "WIFE", "SINGLE", "MARRIED",  # Marital status
    "FKA", "NKA", "AKA", "DBA"  # Name change prefixes
}

def normalize(name: str) -> Set[str]:
    clean = name.upper()
    clean = re.sub(r"[^\w\s]", " ", clean)  # Remove punctuation
    tokens = set(clean.split())
    # Remove stopwords and single-char tokens (initials)
    return {t for t in tokens if t not in STOPWORDS and len(t) > 1}
```

### Nickname/Alias Resolution

Common nicknames are mapped to canonical names:

```python
ALIASES = {
    "BOB": "ROBERT", "ROB": "ROBERT", "BOBBY": "ROBERT",
    "BILL": "WILLIAM", "WILL": "WILLIAM", "WILLIE": "WILLIAM",
    "JIM": "JAMES", "JIMMY": "JAMES",
    "MIKE": "MICHAEL",
    "TOM": "THOMAS",
    "DAVE": "DAVID",
    "DAN": "DANIEL", "DANNY": "DANIEL",
    "CHRIS": "CHRISTOPHER",
    "JOE": "JOSEPH",
    "STEVE": "STEVEN", "STEPHEN": "STEVEN",
    "DICK": "RICHARD", "RICK": "RICHARD"
}
```

### Match Algorithm

```python
def match(name1: str, name2: str) -> Tuple[str, float]:
    set1 = normalize(name1)
    set2 = normalize(name2)

    # 1. Exact Match
    if set1 == set2:
        return "EXACT", 1.0

    # 2. Superset/Subset (require 2+ token overlap)
    intersection = set1.intersection(set2)
    if len(intersection) >= 2:
        if set1.issubset(set2): return "SUPERSET", 0.95
        if set2.issubset(set1): return "SUBSET", 0.95

    # 3. Alias Check (after mapping nicknames)
    set1_mapped = {ALIASES.get(t, t) for t in set1}
    set2_mapped = {ALIASES.get(t, t) for t in set2}
    if set1_mapped == set2_mapped:
        return "ALIAS", 0.90

    # 4. Fuzzy Jaccard
    jaccard = len(intersection) / len(set1.union(set2))
    if jaccard >= 0.65:
        return "FUZZY_JACCARD", jaccard

    # 5. Levenshtein String Similarity
    ratio = SequenceMatcher(None, name1.upper(), name2.upper()).ratio()
    if ratio > 0.88:
        return "FUZZY_STRING", ratio

    return "NONE", 0.0
```

### Chain Linking Threshold

```python
def are_linked(name1: str, name2: str, threshold: float = 0.8) -> bool:
    """Check if two names should be considered linked in chain."""
    match_type, score = match(name1, name2)
    valid_types = {"EXACT", "SUPERSET", "SUBSET", "ALIAS", "FUZZY_JACCARD", "FUZZY_STRING"}
    return match_type in valid_types and score >= threshold
```

## Special Handling

### Missing Party 2 (Grantee)
ORI sometimes indexes only Party 1 (Grantor).
- **Detection**: A deed exists but has no "Party 2".
- **Resolution**:
    1.  Search ORI by Grantor Name (CQID 326) to find the "Party 2" entry.
    2.  Use vLLM to read the PDF and extract the Grantee name.

### Self-Transfers
Deeds where Grantor == Grantee (e.g., transfer to Trust, name change).
- **Handling**: These are noted but do not start a new ownership period in the chain.

## Database Updates

-   **`documents`**: Stores metadata for every found document.
-   **`chain_of_title`**: Stores the derived ownership timeline.
-   **`encumbrances`**: Stores all active and satisfied liens/mortgages found.

## Orchestrator Integration (Parallel Pipeline)

### Skip Logic
The orchestrator skips ORI ingestion when:
```python
# Skip if folio already has chain AND was analyzed for this case
last_case = db.get_last_analyzed_case(parcel_id)
if db.folio_has_chain_of_title(parcel_id) and last_case == case_number:
    # Skip - already analyzed for this foreclosure
    db.mark_step_complete(case_number, "needs_ori_ingestion")
```

This prevents re-analyzing the same property for the same foreclosure case while allowing re-analysis when a new case is filed.

### Legal Description Fallback Chain
The orchestrator implements a full fallback chain for legal descriptions:

```python
# Priority order for legal description sources:
# 1. HCPA parcels.legal_description (most authoritative)
# 2. Extracted from Final Judgment PDF
# 3. Constructed from bulk data (raw_legal1...raw_legal4)

legal_desc = None

# Try HCPA first
parcel = db.get_property(parcel_id)
if parcel:
    legal_desc = parcel.get("legal_description")

# Fallback to judgment extraction
if not legal_desc:
    auction = db.get_auction_by_case(case_number)
    if auction and auction.get("extracted_judgment_data"):
        judgment = json.loads(auction["extracted_judgment_data"])
        legal_desc = judgment.get("legal_description")

# Fallback to bulk raw_legal
if not legal_desc:
    bulk = db.get_bulk_parcel(parcel_id)
    if bulk:
        legal_desc = combine_legal_fields(bulk)
```

### Lot/Block Filtering
ORI searches are filtered by Lot/Block from the legal description:
```python
filter_info = parse_legal_description(legal_desc)
# Returns: {"lot": "12", "block": "A", "subdivision": "TAMPA PALMS"}

# Documents are filtered to only include those matching lot/block
if filter_info.get("lot") and doc_legal:
    if filter_info["lot"] not in doc_legal:
        continue  # Skip irrelevant document
```

### Relevance Filtering
Additional filtering removes irrelevant documents:
- Documents with legal descriptions that don't contain expected lot/block
- Documents for different units in multi-unit buildings
- Plat maps and other non-title documents

### Owner Party Fallback
When legal description search yields no results:
```python
# Fallback to owner name search
if not documents and owner_name:
    documents = ori_scraper.search_by_party(owner_name)
```

### Gap Filling
After building the initial chain, gaps are automatically filled:
```python
# For each gap in the chain (missing links between owners)
for gap in chain_gaps:
    previous_owner = gap.previous_owner
    next_owner = gap.next_owner

    # Search for deeds between these parties
    missing_deeds = ori_scraper.search_by_party(
        grantor=previous_owner,
        grantee=next_owner
    )

    if missing_deeds:
        # Insert into chain
        chain_service.add_documents(missing_deeds)
```

### Case Tracking
After successful analysis, the case is tracked to prevent duplicate work:
```python
db.mark_as_analyzed(parcel_id)
db.set_last_analyzed_case(parcel_id, case_number)
```

## Invalid Folio Handling

For properties without valid folios (mobile homes, personal property):

```python
INVALID_FOLIO_VALUES = {
    'property appraiser', 'n/a', 'none', '', 'unknown', 'pending',
    'see document', 'multiple', 'various', 'tbd', 'na'
}

def is_valid_folio(folio: str) -> bool:
    if not folio:
        return False
    folio_clean = folio.strip().lower()
    if folio_clean in INVALID_FOLIO_VALUES:
        return False
    if len(folio_clean) < 6:
        return False
    return any(c.isdigit() for c in folio_clean)
```

When folio is invalid, the orchestrator uses party-based search:
```python
if not is_valid_folio(parcel_id):
    plaintiff = auction.get('plaintiff')
    defendant = auction.get('defendant')

    if plaintiff or defendant:
        # Run party-based ingestion
        ingestion_service.ingest_property_by_party(prop, plaintiff, defendant)
```
