# Design 2: Judgment Strap Resolution Improvements

## Goal

Resolve the 9 foreclosures that have judgment data but no strap/folio. The existing identifier recovery service fails on these due to specific, fixable gaps.

## Root Cause Analysis

Each unresolved case maps to one of 5 failure patterns:

### Pattern 1: Address matches HCPA exactly, but legal cross-check fails (3 cases)

**Cases:** 100038, 100047, 100058

The `_resolve_one` address lookup path requires address + legal description cross-validation at threshold 0.80. But abbreviated HCPA legals like `CUSCADEN A W` produce a 0.0 match score against full judgment legals because `legal_descriptions_match()` finds "no comparable components (lot, block, subdivision, unit)."

When the address matches **exactly** (not fuzzy), requiring legal confirmation is overly conservative. An exact address match against HCPA is already high-confidence — there's no ambiguity to resolve.

**Fix:** Add an `_exact_address_match` path that bypasses legal cross-check when:
- The normalized address matches exactly one HCPA parcel
- The match is on the full address string (not fuzzy/token-based)

This is safe because exact HCPA address matches are unambiguous (HCPA is the authoritative parcel database).

### Pattern 2: Address normalization mismatches (3 cases)

**Cases:** 15319, 100057, 100059

| Judgment address | HCPA address | Issue |
|---|---|---|
| `3127 W SLIGH AVENUE` | `3127 W SLIGH AVE 203B` | Unit lost at comma, AVENUE not abbreviated |
| `821 LUENT SANDS COURT` | `821 LUCENT SANDS CT` | OCR typo + COURT not abbreviated |
| `2303 BRIANA DRIVE` | `2303 BRIANA DR` | DRIVE not abbreviated |

**Fix:** Add street suffix normalization to `_address_head()`:
- Map `AVENUE→AVE`, `DRIVE→DR`, `COURT→CT`, `STREET→ST`, `BOULEVARD→BLVD`, `LANE→LN`, `CIRCLE→CIR`, `PLACE→PL`, `WAY→WAY`, `TERRACE→TER`, `TRAIL→TRL`, `PARKWAY→PKWY`, `HIGHWAY→HWY`
- The `_ADDRESS_TERMINATORS` set already has these pairs (lines 131-168) but they're only used for *stripping*, not normalizing.

For the unit number issue (15319): when address head doesn't match, also try appending the unit from the full address. The judgment says `3127 W. Sligh Avenue, #203B` — after comma split we lose `203B`. If the head doesn't match, re-extract unit from the remainder and try `{head} {unit}`.

For OCR typos (100057): this is a single-character substitution (`LUENT` → `LUCENT`). The existing fuzzy token search (`_lookup_by_address` lines 1188-1214) uses house number + street tokens. This *should* match on `821` + `SANDS` but the `LUENT` token won't match `LUCENT`. Adding Levenshtein distance=1 tolerance on street name tokens would fix this, or a simpler approach: also try the HCPA search with just house number + the last street token (e.g., `821` + `CT`).

### Pattern 3: LLM extracted wrong address (1 case)

**Case:** 100040 — extracted `951 Yamato Road, Boca Raton, FL 33431` (lender's office) instead of the property address.

**Fix:** This is handled by Design 1 (repair prompt). The repair prompt will detect the non-Hillsborough zip code and re-extract. The identifier recovery service doesn't need changes for this — it correctly rejects a Boca Raton address.

However, the recovery service should fall through to legal description matching when the address is out-of-county. The judgment legal says `Bloomingdale Section W, Lot 14, Block 6` which should match via `_lookup_by_legal_description()`. Let me check why it doesn't.

The HCPA legal for this parcel is just `BLOOMINGDALE SECTION W` — no lot/block. So `_lookup_by_legal_description` needs the subdivision term `BLOOMINGDALE` plus lot/block from the judgment's structured fields (`jd_lot`, `jd_block`). If those aren't populated in `judgment_data`, the legal search only uses subdivision terms, which returns 260+ candidates in Bloomingdale Section W — exceeding `_MAX_CANDIDATES_LEGAL=300` or producing ambiguity.

**Fix:** Ensure the judgment extractor populates `lot`, `block`, `subdivision` fields. Then the recovery service's legal search can construct precise queries like `BLOOMINGDALE + LOT 14 + BLOCK 6`. This may already work if the judgment extraction populates those fields — need to verify.

### Pattern 4: Non-standard parcel ID format (1 case)

**Case:** 100046 — parcel `A-13-28-18-3C7-000004-00012.4`

The `_hcpa_strap_from_segmented_parcel` function (line 1592-1598) tries to convert segmented parcels to HCPA strap format, but it rearranges segments as `{seg3}{seg2}{seg1}{seg4}{seg5}{seg6}{decimal}U`. For this input:
- seg1=A, seg2=13, seg3=28, seg4=18, seg5=3C7, seg6=000004, etc.

The regex may not match this format. The actual strap is `1828133C7000004000124A` which decodes as:
- `18` (Township) + `28` (Range??) + `13` (Section) + `3C7` (subdivision code) + `000004` (block) + `00012` (lot) + `4` (sub-lot) + `A`

The parcel ID `A-13-28-18-3C7-000004-00012.4` maps to this as:
- A = suffix, 13 = section, 28 = township, 18 = range, 3C7 = code, 000004 = block, 00012 = lot, .4 = sub-lot

**Fix:** Improve `_hcpa_strap_from_segmented_parcel` to handle this format. The conversion is: `{range}{township}{section}{code}{block}{lot}{sub}{suffix}` → `1828133C7000004000124A`. The key insight is that the `A` prefix/suffix in the parcel is the strap suffix (not a segment position indicator).

Alternatively, the legal description match should resolve this: `W.E. HAMNER'S FOREST ACRES, Lot 12, Block 4`. The judgment has these structured fields. If the recovery service's `_lookup_by_legal_description` correctly sends `HAMNER + FOREST + ACRES + LOT 12 + BLOCK 4`, it would find the strap `1828133C7000004000120A` and the sub-lot `000124A` would need further disambiguation. But the defendant name `PASCO BAKER` = HCPA owner `PASCO AND REBECCA BAKER` at strap `1828133C7000004000124A` — the owner cross-party check should resolve this.

**Fix:** Ensure `_resolve_by_ori_owner_cross_party` is reached and that the defendant name fuzzy match works against HCPA owner names.

### Pattern 5: No address, no parcel (1 case)

**Case:** 100056 — no address, no parcel ID in judgment. Only the legal description and defendant names.

The legal is `Lots 7-14, Block 1, Zambito Subdivision, PB 30 P 53`. The defendant is `FRIENDS OF DOLPHINS, LLC` = HCPA owner at `2110 W HILLSBOROUGH AVE`.

**Fix:** This should be resolvable by the existing `_resolve_by_ori_owner_cross_party` path, which searches ORI case docs for party names and cross-references against HCPA. If the case docs aren't found or the owner match threshold is too strict, relaxing `_OWNER_MATCH_THRESHOLD` from 0.50 to a lower value or adding a direct HCPA owner name search could help.

Alternatively, add a new resolution step: **defendant-to-HCPA-owner match**. Search the defendant names directly against `hcpa_bulk_parcels.owner_name`. If exactly one HCPA parcel matches a defendant name, and the legal description confirms the subdivision, that's a resolution. This is a high-confidence match because the defendant in a foreclosure is typically the property owner.

## Summary of Changes

### 1. Exact address match (bypass legal cross-check)

In `_resolve_one()`, after the legal description and address+legal paths, add a new fallback:

```python
# Exact address-only match (no legal cross-check needed)
for address_source in ("jd_property_address", "property_address"):
    address = _address_head(row.get(address_source))
    if not address:
        continue
    exact_matches = self._lookup_by_exact_address(conn, address=address)
    if len(exact_matches) == 1:
        return _ResolutionDecision(
            candidate=exact_matches[0],
            method="resolved_exact_address",
            ambiguous=False,
            reason=f"{address_source}_exact",
        )
```

### 2. Street suffix normalization in `_address_head()`

Add a normalization step that maps full street suffixes to USPS abbreviations before returning.

### 3. Unit number recovery

When the comma-split head doesn't contain a unit but the original address does (e.g., `#203B`), try appending the unit to the head for a second lookup attempt.

### 4. Defendant-to-owner fallback

New resolution step after all existing paths fail:

```python
# Defendant name → HCPA owner match
defendants = _extract_defendant_names(row)
for name in defendants:
    owner_matches = self._lookup_by_owner_name(conn, name)
    if len(owner_matches) == 1:
        return _ResolutionDecision(...)
    if len(owner_matches) > 1 and judgment_legal:
        picked = self._pick_single_legal_match(judgment_legal, owner_matches, 0.60)
        if picked.candidate:
            return _ResolutionDecision(...)
```

## Files to Modify

- `src/services/pg_foreclosure_identifier_recovery_service.py`:
  - `_address_head()`: Add street suffix normalization
  - `_resolve_one()`: Add exact address match path + defendant-to-owner fallback
  - New: `_lookup_by_exact_address()`, `_lookup_by_owner_name()`, `_extract_defendant_names()`
- `tests/`: Add tests for new resolution paths

## Success Criteria

All 9 currently unresolved cases should resolve:

| fid | Expected strap | Resolution method |
|---|---|---|
| 15319 | `18283418D000203B00000U` | Street suffix normalization + unit recovery |
| 100038 | `182811104000055000300U` | Exact address match (bypass legal cross-check) |
| 100040 | `2030132PY000006000140U` | Design 1 repair prompt → legal description match |
| 100046 | `1828133C7000004000124A` | Legal description + defendant-to-owner |
| 100047 | `2032072W5BB0000000190U` | Street suffix normalization (strip city/state from no-comma address) |
| 100056 | `1829023HX000001000070A` | Defendant-to-owner match |
| 100057 | `203010B6M000000000230U` | Street suffix normalization + fuzzy token search |
| 100058 | `192918516000000000110A` | Exact address match (bypass legal cross-check) |
| 100059 | `2029342JR000001000130U` | Street suffix normalization |

No regressions on already-resolved foreclosures (the new paths are all fallbacks after existing paths fail).
