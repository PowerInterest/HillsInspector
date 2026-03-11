# Encumbrance Extraction Repair Prompt — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When an encumbrance extraction produces a property address that doesn't match any HCPA parcel, fire a targeted repair prompt with Hillsborough County zip codes and common-mistake guidance.

**Architecture:** Add a repair pass to `_process_one()` in `pg_encumbrance_extraction_service.py`. After validation succeeds, check the extracted address against HCPA. If no match, re-prompt the LLM with the original OCR text, the failed extraction, and error-specific guidance. One retry max. Existing prompts untouched.

**Tech Stack:** Python, PostgreSQL, SQLAlchemy `text()`, VisionService, pytest

**Spec:** `docs/superpowers/specs/2026-03-11-encumbrance-repair-prompt-design.md`

---

## Chunk 1: Address Check + Repair Prompt

### Task 1: HCPA Address Check

**Files:**
- Modify: `src/services/pg_encumbrance_extraction_service.py`
- Test: `tests/test_encumbrance_extraction_service.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_encumbrance_extraction_service.py`:

```python
def test_address_resolves_returns_false_for_non_hillsborough(mock_engine):
    """Out-of-county addresses should not resolve."""
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = None
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    svc.engine = mock_engine

    assert svc._address_resolves("951 Yamato Road, Suite 175, Boca Raton, FL 33431") is False


def test_address_resolves_returns_true_for_matching_hcpa(mock_engine):
    """Known HCPA address should resolve."""
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = (1,)
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    svc.engine = mock_engine

    assert svc._address_resolves("1202 E 15TH AVE, TAMPA, FL 33605") is True


def test_address_resolves_returns_false_for_null():
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    assert svc._address_resolves(None) is False
    assert svc._address_resolves("") is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_encumbrance_extraction_service.py -k "address_resolves" -v
```

- [ ] **Step 3: Implement `_address_resolves`**

Add to `PgEncumbranceExtractionService` class, after `_save_raw_to_pg`:

```python
def _address_resolves(self, address: str | None) -> bool:
    """Check if extracted address matches any HCPA parcel."""
    if not address or len(address.strip()) < 5:
        return False
    normalized = address.upper().strip().split(",")[0].strip()
    if not normalized:
        return False
    sql = text("""
        SELECT 1 FROM hcpa_bulk_parcels
        WHERE property_address = :addr
        LIMIT 1
    """)
    with self.engine.connect() as conn:
        return conn.execute(sql, {"addr": normalized}).fetchone() is not None
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_encumbrance_extraction_service.py -k "address_resolves" -v
```

- [ ] **Step 5: Commit**

```bash
git add src/services/pg_encumbrance_extraction_service.py tests/test_encumbrance_extraction_service.py
git commit -m "Add HCPA address resolution check to extraction service"
```

---

### Task 2: Repair Prompt Constant and Builder

**Files:**
- Modify: `src/services/pg_encumbrance_extraction_service.py`
- Test: `tests/test_encumbrance_extraction_service.py`

- [ ] **Step 1: Add the repair prompt constant**

Add after the existing imports/constants section (around line 95, before `EXTRACTION_DISPATCH`):

```python
_HILLSBOROUGH_ZIPS = (
    "33503, 33510, 33511, 33527, 33534, 33544, 33547, 33548, 33549, "
    "33556, 33558, 33559, 33563, 33565, 33566, 33567, 33569, 33570, "
    "33572, 33573, 33575, 33578, 33579, 33584, 33592, 33594, 33596, "
    "33598, 33601, 33602, 33603, 33604, 33605, 33606, 33607, 33609, "
    "33610, 33611, 33612, 33613, 33614, 33615, 33616, 33617, 33618, "
    "33619, 33624, 33625, 33626, 33629, 33634, 33635, 33636, 33637, 33647"
)

_REPAIR_PROMPT_TEMPLATE = """You previously extracted data from this Hillsborough County document, but the \
property address you returned does not match any known parcel in the county.

YOUR PREVIOUS EXTRACTION:
{previous_json}

THE ERROR:
{error_description}

VALID HILLSBOROUGH COUNTY ZIP CODES:
{zips}

COMMON MISTAKES TO CORRECT:
- The property address is in the GRANTING CLAUSE or LEGAL DESCRIPTION section, \
NOT the "Return To" / "Prepared By" / "After Recording" header block
- If the zip code is not in the list above, you have the WRONG address \
(likely the lender's, attorney's, or servicer's office)
- The BORROWER/MORTGAGOR grants the mortgage — the return-to contact is NOT the borrower
- The RECORDING DATE is on the clerk's stamp (top-right, "RECORDED" or "INSTR #"), \
not the form print date at the bottom of the page
- For UCC/Consensual Liens: always extract parties and amounts — \
secured party = creditor, debtor = lienee

ORIGINAL OCR TEXT:
{ocr_text}

Return a corrected JSON object with the same schema. Fix the property_address \
and any other fields that were wrong. Use null only if the information truly \
does not appear anywhere in the document."""
```

- [ ] **Step 2: Add `_build_repair_error_description`**

```python
def _build_repair_error_description(self, address: str | None) -> str:
    """Build a human-readable error for the repair prompt."""
    if not address:
        return (
            "No property address was extracted. Look for it in the granting "
            "clause, legal description, or sale paragraph."
        )
    # Check if zip is outside Hillsborough
    zip_match = re.search(r"\b(\d{5})\b", address)
    if zip_match:
        zip_code = zip_match.group(1)
        if zip_code not in _HILLSBOROUGH_ZIPS:
            return (
                f"You extracted address '{address}' with zip code {zip_code}, "
                f"which is not in Hillsborough County. This is likely the "
                f"lender's or attorney's office address."
            )
    return (
        f"Address '{address}' does not match any known parcel in "
        f"Hillsborough County. Check for OCR errors in the street name or number."
    )
```

- [ ] **Step 3: Write test for error description builder**

```python
def test_repair_error_description_detects_non_hillsborough_zip():
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    desc = svc._build_repair_error_description("951 Yamato Road, Boca Raton, FL 33431")
    assert "33431" in desc
    assert "not in Hillsborough County" in desc


def test_repair_error_description_handles_none():
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    desc = svc._build_repair_error_description(None)
    assert "No property address" in desc
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_encumbrance_extraction_service.py -k "repair_error" -v
```

- [ ] **Step 5: Commit**

```bash
git add src/services/pg_encumbrance_extraction_service.py tests/test_encumbrance_extraction_service.py
git commit -m "Add repair prompt template and error description builder"
```

---

### Task 3: Wire Repair Pass into `_process_one`

**Files:**
- Modify: `src/services/pg_encumbrance_extraction_service.py:945-960` (`_process_one`)

- [ ] **Step 1: Add `_attempt_repair` method**

Add to the service class:

```python
def _attempt_repair(
    self,
    ocr_text: str,
    validated: dict[str, Any],
    enc_type: str,
) -> dict[str, Any] | None:
    """Fire a repair prompt when the extracted address doesn't resolve."""
    address = validated.get("property_address")
    error_desc = self._build_repair_error_description(address)

    prompt = _REPAIR_PROMPT_TEMPLATE.format(
        previous_json=json.dumps(validated, indent=2, default=str),
        error_description=error_desc,
        zips=_HILLSBOROUGH_ZIPS,
        ocr_text=ocr_text,
    )

    _, model_cls = EXTRACTION_DISPATCH[enc_type]
    schema_guidance = _schema_contract_text(model_cls)
    full_prompt = f"{prompt}\n\n{schema_guidance}"

    raw = self._vision.analyze_text(full_prompt, max_tokens=4000)
    if not raw:
        return None

    parsed = robust_json_parse(raw, f"{enc_type}_repair")
    if not parsed:
        return None

    repaired, _ = self._validate(parsed, enc_type, row_context={}, source="repair")
    return repaired
```

- [ ] **Step 2: Insert repair check in `_process_one`**

In `_process_one`, after validation succeeds (line ~945) and before the cache write (line ~948), add:

Find:
```python
            # 6. Cache
            _write_cache(downloaded, validated)
```

Replace with:
```python
            # 6. Repair if address doesn't resolve
            address = validated.get("property_address")
            if not self._address_resolves(address):
                logger.info(
                    "Address '{}' for id={} type={} inst={} does not resolve; attempting repair",
                    address,
                    row["id"],
                    enc_type,
                    row.get("instrument_number"),
                )
                repaired = self._attempt_repair(ocr_text, validated, enc_type)
                if repaired and self._address_resolves(repaired.get("property_address")):
                    logger.info(
                        "Repair succeeded for id={}: '{}' -> '{}'",
                        row["id"],
                        address,
                        repaired.get("property_address"),
                    )
                    validated = repaired
                else:
                    logger.info(
                        "Repair did not improve address for id={}; keeping original",
                        row["id"],
                    )

            # 7. Cache
            _write_cache(downloaded, validated)
```

Also update the subsequent `_save_to_pg` and log comments to use step numbers 7/8 instead of 6/7.

- [ ] **Step 3: Add `_vision` attribute**

The service needs access to VisionService. Check if it's already available — find where `_extract_from_ocr_text` calls the vision service and use the same reference. It likely uses `self._vision` or calls `VisionService()`.

Look at `_extract_from_ocr_text` to find the pattern and ensure `_attempt_repair` uses the same instance.

- [ ] **Step 4: Run all tests**

```bash
uv run pytest tests/test_encumbrance_extraction_service.py -v
```

- [ ] **Step 5: Lint**

```bash
uv run ruff check src/services/pg_encumbrance_extraction_service.py
```

- [ ] **Step 6: Commit**

```bash
git add src/services/pg_encumbrance_extraction_service.py tests/test_encumbrance_extraction_service.py
git commit -m "Wire repair prompt into extraction pipeline for failed address resolution"
```

---

### Task 4: Integration Test

- [ ] **Step 1: Clear extracted_data for a few known-bad rows to force re-extraction**

```bash
uv run python -c "
import psycopg
conn = psycopg.connect('postgresql://hills:hills_dev@localhost:5433/hills_sunbiz')
cur = conn.cursor()
# Clear 3 rows we know have bad addresses
cur.execute(\"\"\"
    UPDATE ori_encumbrances
    SET extracted_data = NULL, raw = NULL
    WHERE id IN (140408, 138560, 137024)
\"\"\")
conn.commit()
print(f'Reset {cur.rowcount} rows for re-extraction')
conn.close()
"
```

- [ ] **Step 2: Run extraction with limit 5**

```bash
uv run Controller.py --skip-hcpa --skip-clerk-bulk --skip-clerk-criminal --skip-clerk-civil-alpha --skip-nal --skip-flr --skip-sunbiz-entity --skip-county-permits --skip-tampa-permits --skip-single-pin-permits --skip-foreclosure-refresh --skip-trust-accounts --skip-title-chain --skip-title-breaks --skip-market-data --skip-judgment --skip-ori --skip-survival --extraction-limit 5
```

- [ ] **Step 3: Check if repair was triggered in logs**

```bash
grep "does not resolve\|Repair succeeded\|Repair did not" logs/hills_inspector.log | tail -20
```

- [ ] **Step 4: Compare before/after extraction quality**

```bash
uv run python -c "
import psycopg, json
conn = psycopg.connect('postgresql://hills:hills_dev@localhost:5433/hills_sunbiz')
cur = conn.cursor()
cur.execute(\"\"\"
    SELECT id, encumbrance_type, instrument_number,
           extracted_data->>'property_address' AS addr,
           extracted_data->>'confidence' AS conf,
           raw IS NOT NULL AS has_raw
    FROM ori_encumbrances
    WHERE id IN (140408, 138560, 137024)
\"\"\")
for r in cur.fetchall():
    print(f'id={r[0]} type={r[1]} inst={r[2]} addr={r[3]} conf={r[4]} raw={r[5]}')
conn.close()
"
```

Expected: At least some addresses should improve (e.g., 140408 should no longer show Boca Raton).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "Test repair prompt against known-bad extractions"
```
