# Encumbrance Linking: Satisfactions, Modifications & Seed Filtering

This document describes the algorithms that link lifecycle ORI documents
(satisfactions, releases, modifications, subordinations) back to their parent
encumbrances, and the doc-type filtering applied to the Phase 0 seed query.

**Primary file**: `src/services/pg_ori_service.py`
**Related**: [Encumbrance Audit Buckets](ENCUMBRANCE_AUDIT_BUCKETS.md),
[Lien Survival](LIEN_SURVIVAL.md), [DOC_TYPES](DOC_TYPES.md)

---

## 1. Phase 0 Seed Doc-Type Filter

### Problem

The `_seed_from_official_records()` method queries the
`official_records_daily_instruments` table (~103K rows) for documents matching
a property's case number, legal description, or party names. Results are capped
at 400 rows (`_MAX_OFFICIAL_RECORDS_CANDIDATES`). Without a `doc_type` filter,
noise types (deeds, government docs, powers of attorney, notary certificates,
bonds, affidavits, certified copies) crowd out high-value encumbrance documents
before the LIMIT is applied.

### Solution

A `doc_type IN (...)` clause restricts the seed query to encumbrance-relevant
document types only. The allowed types are drawn from the full
[DOC_TYPES.md](DOC_TYPES.md) taxonomy:

| Category | Included Codes |
|----------|----------------|
| Mortgages | MTG, MTGNT, MTGNIT, MTGREV, DOT, HELOC, AGD |
| Judgments | JUD, CCJ, FJ, DRJUD, CTF |
| Liens | LN, LNCORPTX, FIN, MEDLN, HOA, MECH, CEL, SA, SPECASMT, ML |
| Lis Pendens | LP, RELLP |
| Satisfactions | SAT, SATCORPTX, SATMTG, RELMTG |
| Releases | REL, PR, TER, PRREL |
| Assignments | ASG, ASGT, ASGN, ASGNMTG, ASINT |
| Court Papers | ORD, DRCP |
| Other Relevant | NOC, MOD, SUB, NCL, EAS |

**Excluded**: Deeds (D, WD, QCD, etc.), GOV, POA, NOT, BND, AFF, CP, AGR, PL,
RES, COHOME, PRO. Deeds are discovered separately via the ownership chain
(Phase 1B) and are not encumbrances.

### Impact

Reduces the effective candidate pool from ~103K to ~56K rows, so the 400-row
LIMIT captures more relevant docs per property.

---

## 2. Satisfaction Linking (`_link_satisfactions`)

Links SAT/REL documents to their parent mortgage, lien, or judgment. This
populates the `is_satisfied`, `satisfaction_date`, `satisfaction_instrument`,
`satisfaction_method` columns on the parent and `satisfies_encumbrance_id` on
the SAT/REL row.

### Matching Strategies (Priority Order)

#### Strategy 1: Instrument Reference
The SAT/REL's `legal_description` is scanned for patterns like `CLK #NNNNNNN`,
`INST #NNNNNNN`, or `O.R. NNNNNNN`. If the extracted instrument number matches
a known encumbrance, the link is established.

**Regex patterns** (defined as `_INST_REF_PATTERNS` module-level):
```
CLK\s*#?\s*(\d{7,10})
INST(?:RUMENT)?\s*(?:#|NO\.?)?\s*(\d{7,10})
O\.?R\.?\s+(\d{7,10})
```

#### Strategy 2: Book/Page Reference
The `legal_description` is scanned for `OR BK NNN PG NNN` patterns. If the
extracted book/page pair matches a known encumbrance, the link is made.

**Regex** (`_BKPG_REF_PATTERN`):
```
(?:OR|O\.?R\.?)\s*(?:BK|BOOK)\s*(\d+)\s*(?:PG|PAGE)\s*(\d+)
```

#### Strategy 3: Case Number Match
If the SAT/REL and an encumbrance share the same `case_number`, and there is
exactly one such encumbrance (unambiguous), the link is made.

#### Strategy 4: Party + Date Heuristic
When strategies 1-3 fail, this fuzzy fallback uses party name similarity and
recording date ordering:

1. Extract `party1` from SAT/REL and from each candidate encumbrance.
2. **Date guard**: The SAT/REL must be recorded *after* the encumbrance
   (`sat_date > enc_date`).
3. **Fuzzy match**: `rapidfuzz.fuzz.token_set_ratio(sat_party, enc_party) >= 85`.
   The 85% threshold is intentionally higher than the general 80% match
   threshold since party name is the sole discriminator.
4. **Ambiguity guard**: Only links when exactly 1 candidate matches. Multiple
   matches are skipped to avoid false positives.

**PG enum**: The `satisfaction_link_method` enum already includes
`'party_date_heuristic'` as a valid value.

### Data Flow

```
SAT/REL row
   ├─ satisfies_encumbrance_id → parent encumbrance ID
   └─ satisfaction_method → which strategy matched

Parent encumbrance row
   ├─ is_satisfied → true
   ├─ satisfaction_date → SAT recording date
   ├─ satisfaction_instrument → SAT instrument number
   └─ satisfaction_method → which strategy matched
```

---

## 3. Modification Linking (`_link_modifications`)

Links MOD/SUB/NCL/CTF lifecycle documents to their parent encumbrance via the
`modifies_encumbrance_id` foreign key column on `ori_encumbrances`.

### Schema

Added via Alembic migration `007_add_mod_link`:
```sql
ALTER TABLE ori_encumbrances
ADD COLUMN modifies_encumbrance_id BIGINT REFERENCES ori_encumbrances(id);
```

### Target Documents

Documents with `encumbrance_type = 'other'` and `raw_document_type` in:
- **MOD** — Loan modification
- **SUB** — Subordination agreement
- **NCL** — Notice of claim of lien (follow-up)
- **CTF** — Certificate (court-issued lifecycle doc)

### Matching Strategies

Uses the same three reference-chasing strategies as satisfaction linking
(instrument reference, book/page reference, case number match). The party
heuristic (Strategy 4) is intentionally **not** used for modifications because
MOD/SUB docs frequently involve different parties (e.g., a subordination
agreement between a 2nd mortgage holder and the 1st mortgage holder).

### Safety

- **Column guard**: The method checks `information_schema.columns` for
  `modifies_encumbrance_id` before querying. If the migration hasn't run,
  it returns 0 silently.
- **Idempotent**: Only processes rows where `modifies_encumbrance_id IS NULL`.
- **No false positives**: Case number match requires exactly 1 candidate.

### Wiring

Called in the per-property ORI flow (`search_one_property`) immediately after
`_link_satisfactions()`, gated on `strap and saved > 0`.

---

## 4. Relationship Diagram

```
ori_encumbrances (parent: mortgage/lien/judgment)
   │
   ├── satisfies_encumbrance_id ← SAT/REL row (via _link_satisfactions)
   │     └── satisfaction_method: instrument_reference | book_page_reference
   │                              | case_number_match | party_date_heuristic
   │
   └── modifies_encumbrance_id ← MOD/SUB/NCL/CTF row (via _link_modifications)
         └── matched via: instrument_reference | book_page_reference
                          | case_number_match
```

---

## 5. Debugging & Audit Queries

```sql
-- Satisfaction link rate by method
SELECT satisfaction_method, COUNT(*)
FROM ori_encumbrances
WHERE satisfaction_method IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- Unlinked satisfactions (candidates for Strategy 4)
SELECT id, instrument_number, party1, recording_date
FROM ori_encumbrances
WHERE encumbrance_type IN ('satisfaction', 'release')
  AND satisfies_encumbrance_id IS NULL;

-- Linked modifications
SELECT m.id, m.raw_document_type, m.instrument_number,
       p.instrument_number AS parent_instrument, p.encumbrance_type
FROM ori_encumbrances m
JOIN ori_encumbrances p ON m.modifies_encumbrance_id = p.id
ORDER BY m.recording_date;

-- Unlinked modifications
SELECT id, raw_document_type, instrument_number, legal_description
FROM ori_encumbrances
WHERE encumbrance_type = 'other'
  AND raw_document_type IN ('MOD', 'SUB', 'NCL', 'CTF')
  AND modifies_encumbrance_id IS NULL;
```
