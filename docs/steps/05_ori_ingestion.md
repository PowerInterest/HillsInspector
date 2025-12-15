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

The `TitleChainService` builds an ownership timeline:
1.  **Sort Deeds**: Orders deeds by recording date.
2.  **Link Owners**: Matches Grantee of Deed A -> Grantor of Deed B.
3.  **Identify Gaps**: If the chain is broken (Grantor B != Grantee A), it flags a gap.
4.  **Gap Filling**: The system automatically searches ORI by party name to find the missing deed.

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
