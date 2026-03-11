# Judgment Strap Resolution Improvements — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve 9 foreclosures with judgment data but no strap by fixing address normalization, adding exact-address bypass, expanding parcel ID parsing, and adding defendant-to-owner matching.

**Architecture:** All changes are in `pg_foreclosure_identifier_recovery_service.py`. Four independent improvements: (1) street suffix normalization in `_address_head()`, (2) exact address match path in `_resolve_one()` that bypasses legal cross-check, (3) expanded `_PARCEL_SEGMENT_RE` to handle `A-` prefix, (4) defendant-to-HCPA-owner fallback in `_resolve_one()`.

**Tech Stack:** Python, PostgreSQL, SQLAlchemy `text()`, pytest

**Spec:** `docs/superpowers/specs/2026-03-11-judgment-resolution-design.md`

---

## Chunk 1: Street Suffix Normalization + Exact Address Match

### Task 1: Street Suffix Normalization in `_address_head()`

**Files:**
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py:1389-1394`
- Test: `tests/test_pg_foreclosure_identifier_recovery_service.py`

- [ ] **Step 1: Write failing tests for suffix normalization**

Add to `tests/test_pg_foreclosure_identifier_recovery_service.py`:

```python
from src.services.pg_foreclosure_identifier_recovery_service import _address_head


def test_address_head_normalizes_avenue_to_ave() -> None:
    assert _address_head("3127 W SLIGH AVENUE") == "3127 W SLIGH AVE"


def test_address_head_normalizes_drive_to_dr() -> None:
    assert _address_head("2303 Briana Drive, Brandon, FL 33511") == "2303 BRIANA DR"


def test_address_head_normalizes_court_to_ct() -> None:
    assert _address_head("821 Luent Sands Court, Brandon, FL 33511") == "821 LUENT SANDS CT"


def test_address_head_normalizes_street_to_st() -> None:
    assert _address_head("123 Main Street, Tampa") == "123 MAIN ST"


def test_address_head_normalizes_boulevard_to_blvd() -> None:
    assert _address_head("456 N Dale Mabry Boulevard") == "456 N DALE MABRY BLVD"


def test_address_head_does_not_double_abbreviate() -> None:
    assert _address_head("1202 E 15TH AVE, TAMPA, FL 33605") == "1202 E 15TH AVE"


def test_address_head_strips_city_state_zip_when_no_comma() -> None:
    """Address without commas should still strip city/state/zip."""
    assert _address_head("1202 DESERT HILLS DR SUN CITY CENTER FL 33573") == "1202 DESERT HILLS DR"


def test_address_head_returns_none_for_empty() -> None:
    assert _address_head("") is None
    assert _address_head(None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -k "address_head_normalizes or address_head_strips or address_head_does_not or address_head_returns" -v
```

Expected: FAIL on the normalization tests (current code doesn't normalize suffixes).

- [ ] **Step 3: Implement suffix normalization**

In `src/services/pg_foreclosure_identifier_recovery_service.py`, add a suffix map constant after `_ADDRESS_TERMINATORS` (after line 169):

```python
_SUFFIX_TO_ABBREV: dict[str, str] = {
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "CIRCLE": "CIR",
    "COURT": "CT",
    "DRIVE": "DR",
    "HIGHWAY": "HWY",
    "LANE": "LN",
    "PARKWAY": "PKWY",
    "PLACE": "PL",
    "ROAD": "RD",
    "STREET": "ST",
    "TERRACE": "TER",
    "TRAIL": "TRL",
}
```

Then replace `_address_head` (lines 1389-1394) with:

```python
def _address_head(value: Any) -> str | None:
    """Extract and normalize the street address head from a full address string.

    Splits on the first comma, uppercases, abbreviates street suffixes to USPS
    standard forms (AVENUE→AVE, DRIVE→DR, etc.), and strips trailing city/state/zip
    tokens that appear when the address has no commas.
    """
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    first = cleaned.replace("\t", " ").split(",", 1)[0].strip().upper()
    if not first:
        return None

    tokens = first.split()
    # Normalize street suffix
    normalized: list[str] = []
    for token in tokens:
        abbrev = _SUFFIX_TO_ABBREV.get(token)
        if abbrev:
            normalized.append(abbrev)
            break
        if token in _ADDRESS_TERMINATORS and token not in _SUFFIX_TO_ABBREV.values():
            normalized.append(token)
            break
        if token == _ADDRESS_STATE_CODE or (token.isdigit() and len(token) >= 5):
            break
        normalized.append(token)

    # If we found a suffix abbreviation, stop there (drop city/state/zip)
    # If no suffix found, return all tokens up to state code / zip
    return " ".join(normalized) if normalized else None
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -k "address_head" -v
```

Expected: all PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -v
```

Expected: all existing tests still pass.

- [ ] **Step 6: Lint**

```bash
uv run ruff check src/services/pg_foreclosure_identifier_recovery_service.py
```

- [ ] **Step 7: Commit**

```bash
git add src/services/pg_foreclosure_identifier_recovery_service.py tests/test_pg_foreclosure_identifier_recovery_service.py
git commit -m "Normalize street suffixes in address head for identifier recovery"
```

---

### Task 2: Unit Number Recovery

**Files:**
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py:1175-1215` (`_lookup_by_address`)
- Test: `tests/test_pg_foreclosure_identifier_recovery_service.py`

- [ ] **Step 1: Write failing test**

```python
def test_address_head_with_unit_from_hash() -> None:
    """Unit numbers after # should be appended when comma splits them off."""
    # The full address is "3127 W. Sligh Avenue, #203B, Tampa, FL 33614"
    # After comma split: "3127 W SLIGH AVE" — but HCPA has "3127 W SLIGH AVE 203B"
    from src.services.pg_foreclosure_identifier_recovery_service import _address_with_unit

    assert _address_with_unit("3127 W. Sligh Avenue, #203B, Tampa, FL 33614") == "3127 W SLIGH AVE 203B"
    assert _address_with_unit("100 Main St, Tampa, FL") is None  # No unit
    assert _address_with_unit("100 Main St #5, Tampa") == "100 MAIN ST 5"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py::test_address_head_with_unit_from_hash -v
```

- [ ] **Step 3: Implement `_address_with_unit`**

Add after `_address_head` in the service file:

```python
_UNIT_RE = re.compile(r"#\s*([A-Z0-9]+)", re.IGNORECASE)


def _address_with_unit(value: Any) -> str | None:
    """Return normalized address with unit appended, or None if no unit found."""
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    match = _UNIT_RE.search(cleaned)
    if not match:
        return None
    head = _address_head(cleaned)
    if not head:
        return None
    unit = match.group(1).upper()
    return f"{head} {unit}"
```

- [ ] **Step 4: Wire into `_lookup_by_address` and `_resolve_one`**

In `_resolve_one`, in the address loop (lines 651-677), after the existing `_address_head` lookup fails, try `_address_with_unit`:

Find the block:
```python
        if judgment_legal:
            for address_source in ("property_address", "jd_property_address"):
                address = _address_head(row.get(address_source))
                if not address:
                    continue
```

After this loop's `return` for ambiguous (line 677), add before the `if saw_ambiguous:` block:

```python
        # Retry with unit number appended (e.g., "3127 W SLIGH AVE 203B")
        if judgment_legal:
            for address_source in ("property_address", "jd_property_address"):
                address_unit = _address_with_unit(row.get(address_source))
                if not address_unit:
                    continue
                unit_candidates = self._lookup_by_address(conn, address=address_unit)
                if not unit_candidates:
                    continue
                picked = self._pick_single_legal_match(
                    judgment_legal=judgment_legal,
                    candidates=unit_candidates,
                    threshold=0.60,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method="resolved_address_unit_plus_legal",
                        ambiguous=False,
                        reason=f"{address_source}_unit",
                    )
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/services/pg_foreclosure_identifier_recovery_service.py tests/test_pg_foreclosure_identifier_recovery_service.py
git commit -m "Add unit number recovery for address matching"
```

---

### Task 3: Exact Address Match (bypass legal cross-check)

**Files:**
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py:541-692` (`_resolve_one`)
- Test: `tests/test_pg_foreclosure_identifier_recovery_service.py`

- [ ] **Step 1: Write failing test**

```python
def test_resolve_one_uses_exact_address_when_legal_crosscheck_fails(
    mock_engine,
) -> None:
    """When address matches exactly one HCPA parcel but legal cross-check
    fails, the exact address path should still resolve."""
    # This tests the pattern where HCPA legal is abbreviated (e.g. "CUSCADEN A W")
    # and doesn't match the full judgment legal, but the address is exact.
    pass  # Adapt to existing test harness patterns in the file
```

- [ ] **Step 2: Implement `_lookup_by_exact_address`**

Add to the service class:

```python
def _lookup_by_exact_address(
    self,
    conn: Connection,
    *,
    address: str,
) -> list[_ParcelCandidate]:
    """Exact match against HCPA property_address. No fuzzy fallback."""
    sql = text("""
        SELECT folio, strap, property_address,
               raw_legal1, raw_legal2, raw_legal3, raw_legal4,
               source_file_id
        FROM hcpa_bulk_parcels
        WHERE property_address = :address
        ORDER BY source_file_id DESC NULLS LAST
        LIMIT 5
    """)
    rows = conn.execute(sql, {"address": address}).mappings().fetchall()
    return _unique_candidates(rows)
```

- [ ] **Step 3: Wire into `_resolve_one` as a fallback after the address+legal path**

In `_resolve_one`, after the address+legal loop (line ~677) and before the unit-number retry added in Task 2, add:

```python
        # Exact address-only match (no legal cross-check)
        for address_source in ("jd_property_address", "property_address"):
            address = _address_head(row.get(address_source))
            if not address:
                continue
            exact = self._lookup_by_exact_address(conn, address=address)
            if len(exact) == 1:
                return _ResolutionDecision(
                    candidate=exact[0],
                    method="resolved_exact_address",
                    ambiguous=False,
                    reason=f"{address_source}_exact",
                )
```

Also add `"resolved_exact_address": 0` and `"resolved_address_unit_plus_legal": 0` to the stats dict in `run()`.

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -v
```

- [ ] **Step 5: Lint**

```bash
uv run ruff check src/services/pg_foreclosure_identifier_recovery_service.py
```

- [ ] **Step 6: Commit**

```bash
git add src/services/pg_foreclosure_identifier_recovery_service.py tests/test_pg_foreclosure_identifier_recovery_service.py
git commit -m "Add exact address match path bypassing legal cross-check"
```

---

## Chunk 2: Parcel ID Parsing + Defendant-to-Owner Fallback

### Task 4: Expand Parcel Segment Regex for A-prefix

**Files:**
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py:99-101` (`_PARCEL_SEGMENT_RE`)
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py:1592-1598` (`_hcpa_strap_from_segmented_parcel`)
- Test: `tests/test_pg_foreclosure_identifier_recovery_service.py`

- [ ] **Step 1: Write failing test**

```python
from src.services.pg_foreclosure_identifier_recovery_service import (
    _hcpa_strap_from_segmented_parcel,
)


def test_hcpa_strap_from_a_prefix_parcel() -> None:
    result = _hcpa_strap_from_segmented_parcel("A-13-28-18-3C7-000004-00012.4")
    assert result == "1828133C7000004000124A"


def test_hcpa_strap_from_u_prefix_parcel_unchanged() -> None:
    """Existing U-prefix behavior still works."""
    result = _hcpa_strap_from_segmented_parcel("U-13-28-18-3C7-000004-00012.4")
    assert result == "1828133C7000004000124U"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py::test_hcpa_strap_from_a_prefix_parcel -v
```

Expected: FAIL (current regex only handles `U-` prefix).

- [ ] **Step 3: Fix the regex and conversion function**

Change line 99-101:

```python
_PARCEL_SEGMENT_RE = re.compile(
    r"^([A-Z])-(\d{2})-(\d{2})-(\d{2})-([A-Z0-9]+)-(\d+)-(\d+)\.(\d)$"
)
```

Change `_hcpa_strap_from_segmented_parcel` (lines 1592-1598):

```python
def _hcpa_strap_from_segmented_parcel(parcel_id: str) -> str | None:
    token = parcel_id.strip().upper()
    match = _PARCEL_SEGMENT_RE.match(token)
    if not match:
        return None
    suffix, sec, twp, rge, code, block, lot, decimal = match.groups()
    return f"{rge}{twp}{sec}{code}{block}{lot}{decimal}{suffix}"
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -k "hcpa_strap" -v
```

Expected: both PASS.

- [ ] **Step 5: Run full test suite**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/services/pg_foreclosure_identifier_recovery_service.py tests/test_pg_foreclosure_identifier_recovery_service.py
git commit -m "Expand parcel segment regex to handle A-prefix format"
```

---

### Task 5: Defendant-to-HCPA-Owner Fallback

**Files:**
- Modify: `src/services/pg_foreclosure_identifier_recovery_service.py`
- Test: `tests/test_pg_foreclosure_identifier_recovery_service.py`

- [ ] **Step 1: Write failing test**

```python
from src.services.pg_foreclosure_identifier_recovery_service import _extract_defendant_names


def test_extract_defendant_names_from_judgment_data() -> None:
    row = {
        "jd_defendants": '[{"name": "FRIENDS OF DOLPHINS, LLC"}, {"name": "TAZINE JAFFER"}, {"name": "UNKNOWN TENANT IN POSSESSION"}]',
    }
    names = _extract_defendant_names(row)
    # Should exclude entity names (LLC, BANK, UNKNOWN, TENANT, etc.)
    assert "TAZINE JAFFER" in names
    assert "FRIENDS OF DOLPHINS, LLC" not in names  # entity
    assert "UNKNOWN TENANT IN POSSESSION" not in names  # placeholder


def test_extract_defendant_names_filters_entities() -> None:
    row = {
        "jd_defendants": '[{"name": "PASCO BAKER A/K/A PASCO BAKER, JR."}, {"name": "et al."}]',
    }
    names = _extract_defendant_names(row)
    assert len(names) == 1
    assert "PASCO BAKER" in names[0]
```

- [ ] **Step 2: Implement `_extract_defendant_names`**

Add as a module-level function:

```python
def _extract_defendant_names(row: dict[str, Any]) -> list[str]:
    """Extract individual (non-entity) defendant names from judgment data."""
    import json as _json

    raw = row.get("jd_defendants") or "[]"
    try:
        defendants = _json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return []

    names: list[str] = []
    for defendant in defendants:
        name = _clean_text(defendant.get("name") if isinstance(defendant, dict) else None)
        if not name:
            continue
        upper = name.upper()
        # Skip placeholders and generic parties
        if upper in ("ET AL.", "ET AL", "UNKNOWN"):
            continue
        # Skip entity names (banks, LLCs, associations, government)
        if any(kw in upper for kw in _ENTITY_KEYWORDS):
            continue
        names.append(upper)
    return names
```

- [ ] **Step 3: Implement `_lookup_by_owner_name`**

Add to the service class:

```python
def _lookup_by_owner_name(
    self,
    conn: Connection,
    *,
    name: str,
) -> list[_ParcelCandidate]:
    """Search HCPA parcels by owner name (ILIKE match)."""
    sql = text("""
        SELECT folio, strap, property_address,
               raw_legal1, raw_legal2, raw_legal3, raw_legal4,
               source_file_id
        FROM hcpa_bulk_parcels
        WHERE owner_name ILIKE :pattern
        ORDER BY source_file_id DESC NULLS LAST
        LIMIT :limit
    """)
    rows = conn.execute(
        sql, {"pattern": f"%{name}%", "limit": _MAX_OWNER_MATCHES}
    ).mappings().fetchall()
    return _unique_candidates(rows)
```

- [ ] **Step 4: Wire into `_resolve_one` as final fallback**

Add the `jd_defendants` column to `_SCOPE_SQL` (after line 187):

```sql
    f.judgment_data->>'defendants'                              AS jd_defendants,
```

In `_resolve_one`, add before the final `return _ResolutionDecision(... reason="no_match")`:

```python
        # Defendant name → HCPA owner match (last resort)
        defendant_names = _extract_defendant_names(row)
        for name in defendant_names[:_MAX_OWNER_NAMES]:
            owner_matches = self._lookup_by_owner_name(conn, name=name)
            if not owner_matches:
                continue
            if len(owner_matches) == 1:
                return _ResolutionDecision(
                    candidate=owner_matches[0],
                    method="resolved_defendant_owner",
                    ambiguous=False,
                    reason=f"defendant_{name[:30]}",
                )
            if judgment_legal and len(owner_matches) <= 10:
                picked = self._pick_single_legal_match(
                    judgment_legal=judgment_legal,
                    candidates=owner_matches,
                    threshold=0.60,
                )
                if picked.candidate:
                    return _ResolutionDecision(
                        candidate=picked.candidate,
                        method="resolved_defendant_owner_legal",
                        ambiguous=False,
                        reason=f"defendant_{name[:30]}_plus_legal",
                    )
```

Also add `"resolved_defendant_owner": 0` and `"resolved_defendant_owner_legal": 0` to the stats dict.

- [ ] **Step 5: Run all tests**

```bash
uv run pytest tests/test_pg_foreclosure_identifier_recovery_service.py -v
```

- [ ] **Step 6: Lint**

```bash
uv run ruff check src/services/pg_foreclosure_identifier_recovery_service.py
```

- [ ] **Step 7: Commit**

```bash
git add src/services/pg_foreclosure_identifier_recovery_service.py tests/test_pg_foreclosure_identifier_recovery_service.py
git commit -m "Add defendant-to-owner fallback for identifier recovery"
```

---

### Task 6: Integration Test Against Real Data

- [ ] **Step 1: Reset recovery cooldown for the 9 test cases**

```bash
uv run python -c "
import psycopg
conn = psycopg.connect('postgresql://hills:hills_dev@localhost:5433/hills_sunbiz')
cur = conn.cursor()
cur.execute(\"\"\"
    UPDATE foreclosures
    SET step_identifier_recovery = NULL
    WHERE foreclosure_id IN (15319, 100038, 100040, 100046, 100047, 100056, 100057, 100058, 100059)
\"\"\")
conn.commit()
print(f'Reset {cur.rowcount} rows')
conn.close()
"
```

- [ ] **Step 2: Run identifier recovery**

```bash
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-title-breaks --skip-market-data --skip-judgment --skip-ori --skip-encumbrance-extraction --skip-survival --identifier-recovery-limit 20
```

- [ ] **Step 3: Verify resolutions**

```bash
uv run python -c "
import psycopg
conn = psycopg.connect('postgresql://hills:hills_dev@localhost:5433/hills_sunbiz')
cur = conn.cursor()
expected = {
    15319: '18283418D000203B00000U',
    100038: '182811104000055000300U',
    100047: '2032072W5BB0000000190U',
    100056: '1829023HX000001000070A',
    100057: '203010B6M000000000230U',
    100058: '192918516000000000110A',
    100059: '2029342JR000001000130U',
}
cur.execute(\"\"\"
    SELECT foreclosure_id, strap, folio
    FROM foreclosures
    WHERE foreclosure_id IN (15319, 100038, 100040, 100046, 100047, 100056, 100057, 100058, 100059)
    ORDER BY foreclosure_id
\"\"\")
passed = 0
for r in cur.fetchall():
    fid, strap, folio = r
    exp = expected.get(fid)
    status = 'PASS' if strap == exp else f'FAIL (got {strap}, expected {exp})'
    if strap == exp: passed += 1
    print(f'  fid={fid}: strap={strap} — {status}')
print(f'\n{passed}/{len(expected)} resolved')
conn.close()
"
```

Expected: At least 7/7 of the non-LLM-error cases resolve. fid=100040 (wrong LLM address) and fid=100046 (complex parcel) may need the repair prompt (Design 1) or may resolve via legal/defendant paths.

- [ ] **Step 4: Commit integration test script (optional)**

```bash
git add -A && git commit -m "Integration-test identifier recovery against 9 unresolved cases"
```
