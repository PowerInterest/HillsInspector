## Professional Title Search Strategies (Pro-Tips)

See also [Encumbrance Audit Buckets](ENCUMBRANCE_AUDIT_BUCKETS.md) for the
active-foreclosure taxonomy that turns these search patterns into explicit audit
and recovery cohorts.

To find all encumbrances and resolve title gaps, the following techniques are used by industry professionals:

1. **Solving Name Breaks (Personal)**
   - **Phonetic/Soundex Search**: Always toggle "Phonetic" search in the Hillsborough Clerk portal to capture misspellings (e.g., "Smyth" vs "Smith").
   - **Trustee Cross-Reference**: Liens are often filed against the **Trustee Name** personally rather than the **Trust Name**. Search both (e.g., "John Doe, Trustee" and "The Doe Family Trust").
   - **Middle Name Verification**: Cross-reference with Voter Registration or Sunbiz to find full middle names, improving name-match precision in the Clerk's Official Records.

2. **Solving Name Breaks (Corporate) - Sunbiz Strategy**
   - **Articles of Merger/Amendment**: Use [Sunbiz.org](https://www.sunbiz.org) to track entity evolution. If a deed is held by "Entity A" but "Entity B" is foreclosing, search Sunbiz for a "Certificate of Merger" or "Name Change Amendment."
   - **Fictitious Names**: Search DBAs to link a trade name back to the legal entity responsible for the lien/mortgage.

3. **Identifying Breaks in Title**
   - **Scrivener's Errors**: Check for "Corrective Deeds" that fix typos in the legal description or grantee name.
   - **Wild Deeds**: Look for deeds recorded outside the direct chain. Use the **Parcel ID / Strap** search in the Clerk's office (not just name search) to find "Wild Deeds" that aren't properly indexed to the owner.
   - **Probate Gaps**: If an owner died, search the **Clerk's Probate Progress Docket**. A title break often exists because the "Order of Summary Administration" or "Certificate of Ancillary Administration" wasn't recorded in the Official Records.

4. **Insurance & Private Resources**
   - **Title Insurance Lookup**: No public database exists. Look for an "Owner's Affidavit" or "Notice of Title Insurance" in the Official Records to identify the last underwriter.
   - **CLUE Reports**: (Comprehensive Loss Underwriting Exchange) contains claim histories that can hint at unrecorded issues.
   - **Title Plants**: Platforms like **DataTrace** or **PropertyInfo** provide "geographic indexing" (searching by land plot rather than name), which is the most reliable way to find all liens.

## Cross-Agency Intelligence Routing (Interaction Strategies)
Different types of encumbrances act as strong signals that require cross-referencing with other Hillsborough County systems to build a complete property profile. The discovery of one instrument type should dynamically route the crawler to secondary data sources:

1. **Notice of Commencement (NOC)**
   - **Signal**: Ongoing or recently completed construction/renovation. 
   - **Action**: Trigger a structured search on the Hillsborough County Permit portal for the specific property. Look for open permits, expired permits without final inspection, or recent Certificate of Occupancy.
   - **Risk**: An open permit transfers liability to the auction buyer and prevents clear title. A recently expired NOC where the contractor wasn't fully paid often precedes a Mechanic's Lien.

2. **Code Enforcement Lien (Chapter 162)**
   - **Signal**: Severe, long-term property neglect (often a "toxic asset" physically).
   - **Action**: Query the Hillsborough County Special Magistrate / Code Enforcement records. Extract the specific violations (e.g., structural damage, overgrown lot, unpermitted additions).
   - **Interaction**: This lien holds super-priority. If a bidder is placing a high escrow deposit but a massive code enforcement lien exists, flag this as heavily toxic.

3. **Municipal Utility Lien (Chapter 159)**
   - **Signal**: Unpaid water/sewer/gas, strongly indicating long-term **property vacancy**.
   - **Action**: These liens survive "until paid" and don't require recording to be valid against a new owner. Trigger an unrecorded municipal debt search or flag the property as highly likely to be vacant and distressed. 

4. **Lis Pendens (LP) & Final Judgment**
   - **Signal**: Active or completed foreclosure litigation.
   - **Action**: Query the Hillsborough County Clerk's Civil Docket for the case number. Extract the Final Judgment amount, the specific auction date, and monitor the docket for a Certificate of Sale or Certificate of Title.

5. **Tax Certificates & Tax Deeds**
   - **Signal**: Severe tax delinquency.
   - **Action**: Route to `hillsborough.realtaxdeed.com` to check for scheduled tax deed sales or surplus "excess proceeds". 
   - **Interaction**: A tax deed sale will extinguish almost all private liens (including mortgages). If the property is in the traditional foreclosure pipeline *and* the tax deed pipeline simultaneously, the tax deed timeline usually wins and wipes out the mortgage plaintiff.

6. **Federal IRS Tax Lien**
   - **Signal**: The property owner owes federal taxes.
   - **Action**: Calculate the 120-day redemption window post-auction. The winning bidder cannot get clear, marketable title until 120 days have passed, as the IRS retains the statutory right to buy the property back for the winning bid price plus 6% interest.


## Comprehensive Title Search Resource Guide

| System | Resource | Access Type | Data Provided |
|--------|----------|-------------|---------------|
| **Federal** | PACER | Paid Portal | Bankruptcies, Federal Judgments. |
| **Federal** | EPA SEMS | Public | Environmental (Superfund) Liens. |
| **State** | Sunbiz | Public | Mergers, Name Changes, DBAs. |
| **State** | Florida UCC | Paid Portal | Fixture Liens (HVAC, Solar). |
| **Local** | Clerk OR | Public | Deeds, Mortgages, Recorded Liens. |
| **Local** | Clerk Civil | Public | Foreclosure Docket/Case History. |
| **Local** | HCPA | Public | Sale Chain & Address Linkage. |
| **Private** | PropLogix | Paid Svc | Unrecorded Municipal Utility/Code Liens. |
| **Private** | DataTrace | Paid Svc | Geographic Search (ParcelID-based indexing). |


## Recommended Production Strategy
- Default: `chain_adjacent`.
- Conditional fallback: only run legal/party fallback when either:
  - LP/Judgment anchor is missing, or
  - document count is below a minimum threshold after chain-adjacent pass.
- Keep clerk-party expansion disabled by default; use only as targeted fallback.

## Phase 3 Category-Aware Gate (2026-03-03)

The original Phase 3 gate was a simple doc-count threshold
(`_MIN_DOCS_FOR_NO_FALLBACK = 5`). This caused 60% of properties to skip
legal-description and party-name fallback searches entirely, even when critical
encumbrance categories were absent.

### Problem

The `chain_adjacent` strategy finds documents that are physically proximate to
deed recordings (same-day instrument offsets) or linked by case number. This works
well for mortgages recorded at purchase and for the foreclosure case's own LP/JUD.
It systematically misses:

- **Superpriority liens** (code enforcement Ch. 162, utility Ch. 159, tax certs)
  recorded independently of any deed transaction.
- **Standalone refinance mortgages / HELOCs** recorded without a companion deed.
- **Municipal special assessments** (CDD, PACE) billed on the tax roll but
  sometimes also recorded in ORI.

These are exactly the encumbrances that matter most to an auction bidder per
`LIEN_SURVIVAL.md` — they survive foreclosure and transfer to the buyer.

### Solution

Replace the doc-count gate with a **category-aware check** that examines what
classes of encumbrance are represented after Phases 0-2, not just how many
documents were found. Phase 3 runs when any of these conditions is true:

1. **Doc count < `_MIN_DOCS_FOR_NO_FALLBACK`** (existing threshold, kept as
   floor).
2. **Zero mortgages** — every foreclosure involves a mortgage.
3. **No lien found** — superpriority liens (code enforcement Ch.162, utility
   Ch.159, tax certificates) are never adjacent to deeds and only discoverable
   via legal-description or party-name search. Since these are the liens that
   survive foreclosure and transfer to the buyer, missing them is worse than
   wasting a few API calls.
4. **CC case type** — CC Enforce Lien and CC Real Property cases almost always
   involve a standalone lien that won't be in the foreclosure case file.

This is implemented as `_needs_targeted_fallback` in `_discover_property()`. The
practical effect is that Phase 3 runs for nearly all foreclosure properties,
since most properties lack a lien row after the deed-chain-adjacent pass. The
cost is bounded by the existing `_MAX_DOCUMENTS=500` cap and PAV rate limiting.
The tradeoff is correct: missing a superpriority lien is far more expensive
(for the auction bidder) than one extra legal-description search.

### Phase 1B+ Lifecycle Chain Following

In addition to the Phase 3 gate change, a new Phase 1B+ was added after the deed
chain walk. This searches adjacent instruments around **all** discovered
encumbrance instruments (mortgage, lien, LP, satisfaction, release, assignment) —
not just deeds. This catches 2nd/3rd mortgages, assignments, and satisfactions
recorded near the original mortgage.

### Benchmark Gap

The current benchmark metrics do not measure lien discovery rate. Future benchmark
runs should add:
- `lien_found_rate` — fraction of sampled properties with at least one lien.
- `superpriority_lien_found_rate` — fraction with code enforcement, utility, or
  tax liens when such liens exist in the reference set.
- `satisfaction_link_rate` — fraction of discovered SAT/REL docs successfully
  linked to their parent encumbrance.


## Source: ENCUMBRANCE_ALGORITHM_TESTPLAN.md

# Encumbrance Algorithm Test Plan

## Objective
Determine the most complete and efficient encumbrance discovery algorithm for
Hillsborough foreclosure properties using current PG data plus ORI API calls.

The algorithm must satisfy these business rules:
- If foreclosure exists, find lis pendens (`LP`) and final judgment (`JUD/FNLJ`).
- If mortgage exists, find downstream lifecycle docs (satisfaction/release/assignment).
- If notice of commencement (`NOC`) exists, link it to permit evidence in PG.

## Scope
- In scope:
  - ORI discovery strategy comparison.
  - PG-only enrichment joins (sales chain, clerk parties, permits).
  - Per-property rule coverage and API-cost measurement.
- Out of scope:
  - Schema migrations.
  - Web endpoint wiring.
  - PDF OCR extraction.

## Candidate Strategies
1. `baseline_case_legal_party`
- Seed by case number.
- Add legal-term search and party search (plaintiff/defendant/owner).
- Chase references for discovered lien/mortgage instruments.

2. `chain_adjacent`
- Seed by case number.
- Use `hcpa_allsales` deed chain (PG) and query deed instrument + offsets.
- Chase references for discovered lien/mortgage instruments.

3. `chain_adjacent_clerk`
- Strategy 2 plus defendant-name search from `clerk_civil_parties`
  (date-bounded).

4. `chain_adjacent_clerk_legal_fallback`
- Strategy 3 plus targeted legal fallback only when LP or mortgage coverage is
  insufficient.

## Test Corpus
- Source: `foreclosures` where `archived_at IS NULL` and strap is valid.
- Stratification buckets by sales-chain depth (`hcpa_allsales` rows):
  - Low complexity (0-2 transfers)
  - Medium complexity (3-6 transfers)
  - High complexity (7+ transfers)
- Default benchmark size: 12 properties (balanced by bucket).

## Gold Truth / Reference Set
For each property, build a per-case reference universe:
- Union of discovered instruments across all tested strategies.
- Union with existing `ori_encumbrances` rows for the same strap.

This creates a practical completeness baseline without requiring manual labeling
for every case.

## Metrics
- Completeness:
  - `instrument_recall = strategy_instruments / reference_instruments`
  - `mortgage_lifecycle_recall = matched_mortgage_release_links / reference_links`
- Rule coverage:
  - `lp_found_rate`
  - `judgment_found_rate`
  - `mortgage_release_rule_rate`
  - `noc_permit_link_rate`
- Efficiency:
  - `avg_api_calls_per_case`
  - `avg_runtime_seconds_per_case`
  - `truncated_response_rate`
  - `error_rate`

## Pass/Selection Criteria
- Hard minimums:
  - LP found in 95%+ of sampled foreclosure cases.
  - Mortgage lifecycle rule satisfied in 80%+ of sampled cases with mortgages.
  - NOC→permit linkage in 80%+ of sampled cases that contain NOC docs.
- Ranking:
  1. Highest instrument recall.
  2. Highest mortgage lifecycle rule rate.
  3. Lowest API calls per case.
  4. Lowest runtime per case.

## Execution Plan
1. Run benchmark script:
```bash
uv run python scripts/benchmark_encumbrance_algorithms.py --sample-size 12
```
2. Review generated JSON + markdown summary in `logs/`.
3. Promote winner to default ORI strategy design.
4. Keep fallback-only paths for edge cases with truncation/noisy party matches.

## Expected Decision Pattern
- Preferred default is expected to be `chain_adjacent` family.
- `baseline_case_legal_party` likely has lower precision and higher truncation.
- `chain_adjacent_clerk_legal_fallback` should win on completeness if added API
  cost remains within acceptable bounds.
