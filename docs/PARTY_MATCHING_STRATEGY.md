# Party Matching Strategy

## The Challenge of Bulk Name Matching
When attempting to link bulk county encumbrances (Mortgages, Judgments, Liens) to specific properties, relying solely on exact string matching for property owner names is prone to failure due to typos, middle initials, and trailing suffixes (e.g., "JR", "LLC").

Conversely, naive substring matching (e.g., matching any document where the word "Smith" appears) leads to catastrophic data pollution, attaching thousands of false-positive encumbrances to a single property simply because the owner shares a common surname.

## The Solution: Memory-Optimized Fuzzy Matching
To resolve this, the pipeline employs a robust, memory-optimized fuzzy matching approach using **RapidFuzz** (`rapidfuzz.fuzz.token_set_ratio`).

### 1. Integration with the Title Chain
A key architectural decision in the `PgOriService` is that we do not merely match incoming encumbrances against the *current* property owner.

Before searching for encumbrances, the service fetches the complete historical **Ownership Chain** (`fn_title_chain`). It extracts every `grantee` and `grantor` name from the property's history. 

This ensures that an encumbrance filed 10 years ago against a previous owner is correctly identified and attached to the property's timeline, directly enabling accurate Lien Survival Analysis.

### 2. The RapidFuzz Token Set Ratio
When the Clerk API returns unstructured JSON documents:
1. We aggregate all parties on the document into a single string.
2. We iterate through our compiled list of historical title chain owners.
3. We calculate the fuzzy similarity using `fuzz.token_set_ratio > 80`.

This specific algorithm is chosen because it ignores word order and duplicate words, making it perfectly suited for matching "John A. Doe" against "Doe John". By enforcing a strict 80% similarity threshold, we eliminate the "common name" pollution bug while remaining highly resilient to minor typos and varied name formatting.

### 3. Pipeline Execution
Because this matching occurs in memory during the Python ingestion phase:
- It requires zero complex database joins or heavy `pg_trgm` similarity queries on raw data.
- The pipeline effortlessly discards junk API responses and only persists highly-confident, true-positive encumbrances into the `ori_encumbrances` table.
