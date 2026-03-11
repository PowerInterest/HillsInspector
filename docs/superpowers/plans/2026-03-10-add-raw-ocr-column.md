# Add `raw` OCR Column to `ori_encumbrances`

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the raw OCR text on `ori_encumbrances` so every extraction has an auditable record of what the LLM actually saw.

**Architecture:** Add a `raw` TEXT column to `ori_encumbrances` via Alembic. Modify `_process_one()` in the extraction service to save OCR text to PG *before* the LLM call, so it's preserved even when extraction fails. The new flow is: PDF → OCR → **save `raw`** → LLM → save `extracted_data`.

**Tech Stack:** PostgreSQL, Alembic, SQLAlchemy ORM (mapped_column), pytesseract (existing)

---

## Chunk 1: Migration + ORM + Service + Tests

### Task 1: Alembic Migration

**Files:**
- Create: `alembic/versions/014_add_raw_ocr_column.py`

- [ ] **Step 1: Generate the migration**

```bash
cd /mnt/c/code/HillsInspector
uv run alembic revision -m "Add raw OCR text column to ori_encumbrances"
```

Then replace the generated file contents with:

```python
"""Add raw OCR text column to ori_encumbrances.

Stores the full pytesseract output (with --- PAGE N --- delimiters)
that was sent to the LLM for structured extraction. Persisted before
the LLM call so the text survives even when extraction fails.

Revision ID: 014_add_raw_ocr_column
Revises: 013_rename_to_extracted_data
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa

revision = "014_add_raw_ocr_column"
down_revision = "013_rename_to_extracted_data"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ori_encumbrances",
        sa.Column("raw", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    raise NotImplementedError("Forward-only migration policy")
```

- [ ] **Step 2: Run the migration**

```bash
uv run alembic upgrade head
```

Expected: `INFO  [alembic.runtime.migration] Running upgrade 013_rename_to_extracted_data -> 014_add_raw_ocr_column`

- [ ] **Step 3: Verify the column exists**

```bash
uv run python -c "
import psycopg
conn = psycopg.connect('postgresql://hills:hills_dev@localhost:5433/hills_sunbiz')
cur = conn.cursor()
cur.execute(\"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ori_encumbrances' AND column_name = 'raw'\")
print(cur.fetchone())
conn.close()
"
```

Expected: `('raw', 'text')`

- [ ] **Step 4: Commit**

```bash
git add alembic/versions/014_add_raw_ocr_column.py
git commit -m "Add raw OCR text column to ori_encumbrances"
```

---

### Task 2: ORM Model Update

**Files:**
- Modify: `sunbiz/models.py:1040`

- [ ] **Step 1: Add the column to the ORM**

In `sunbiz/models.py`, find line 1040:

```python
    extracted_data: Mapped[dict | None] = mapped_column(JSONB)
```

Add immediately after it:

```python
    raw: Mapped[str | None] = mapped_column(Text)
```

Note: `Text` is already imported in this file.

- [ ] **Step 2: Verify no import needed**

```bash
uv run ruff check sunbiz/models.py
```

Expected: clean (or only pre-existing warnings unrelated to this change)

- [ ] **Step 3: Commit**

```bash
git add sunbiz/models.py
git commit -m "Add raw column to ORI encumbrances ORM model"
```

---

### Task 3: Save Raw OCR Before LLM Call

**Files:**
- Modify: `src/services/pg_encumbrance_extraction_service.py:818-827` (`_save_to_pg`)
- Modify: `src/services/pg_encumbrance_extraction_service.py:885-958` (`_process_one`)

- [ ] **Step 1: Add `_save_raw_to_pg` method**

In `src/services/pg_encumbrance_extraction_service.py`, add a new method right after `_save_to_pg` (after line 827):

```python
    def _save_raw_to_pg(self, encumbrance_id: int, ocr_text: str) -> None:
        """Persist raw OCR text before LLM extraction."""
        sql = text("""
            UPDATE ori_encumbrances
            SET raw = :ocr_text,
                updated_at = NOW()
            WHERE id = :id
        """)
        with self.engine.begin() as conn:
            conn.execute(sql, {"ocr_text": ocr_text, "id": encumbrance_id})
```

- [ ] **Step 2: Insert the save call in `_process_one`**

In `_process_one`, find the block after the OCR empty check (after line 912, before line 914). Insert the raw save call so the OCR text is persisted *before* hitting the LLM:

Current code at lines 912-914:
```python
                return {
                    "_status": "error",
                    "_reason": "ocr_empty",
                }

            raw = self._extract_from_ocr_text(ocr_text, enc_type)
```

Change to:
```python
                return {
                    "_status": "error",
                    "_reason": "ocr_empty",
                }

            # Persist raw OCR text before LLM call
            self._save_raw_to_pg(row["id"], ocr_text)

            raw = self._extract_from_ocr_text(ocr_text, enc_type)
```

That's it — one new method, one new line in the orchestration. The existing `_save_to_pg` for `extracted_data` stays unchanged.

- [ ] **Step 3: Lint**

```bash
uv run ruff check src/services/pg_encumbrance_extraction_service.py
```

- [ ] **Step 4: Commit**

```bash
git add src/services/pg_encumbrance_extraction_service.py
git commit -m "Persist raw OCR text to PG before LLM extraction"
```

---

### Task 4: Tests

**Files:**
- Modify: `tests/test_encumbrance_extraction_service.py`

- [ ] **Step 1: Write test for `_save_raw_to_pg`**

Add a test that verifies the raw OCR text is written to PG. This test should go in the existing `TestEndToEnd` class. Find the pattern used by other DB-touching tests in that file — they use `self.engine` or mock `engine.begin()`. Add:

```python
def test_save_raw_to_pg_persists_ocr_text(self):
    """Raw OCR text is saved to the `raw` column before LLM extraction."""
    svc = PgEncumbranceExtractionService.__new__(PgEncumbranceExtractionService)
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    svc.engine = mock_engine

    svc._save_raw_to_pg(42, "--- PAGE 1 ---\nHello world")

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    params = call_args[0][1]
    assert params["id"] == 42
    assert params["ocr_text"] == "--- PAGE 1 ---\nHello world"
```

- [ ] **Step 2: Write test that raw is saved before LLM call**

Add a test that verifies the ordering — `_save_raw_to_pg` is called before `_extract_from_ocr_text`. Use `unittest.mock.call_args_list` or a side-effect list to track call order:

```python
def test_process_one_saves_raw_before_llm(self):
    """OCR text is persisted to PG before being sent to the LLM."""
    call_order = []

    original_save_raw = svc._save_raw_to_pg
    original_extract = svc._extract_from_ocr_text

    def mock_save_raw(*args, **kwargs):
        call_order.append("save_raw")

    def mock_extract(*args, **kwargs):
        call_order.append("extract")
        return {"instrument_number": "123"}

    # Patch both methods and verify save_raw comes first
    # (Adapt to match the existing test harness patterns in this file)
    assert call_order == ["save_raw", "extract"]
```

Adapt this skeleton to match the existing mock patterns in `tests/test_encumbrance_extraction_service.py` — the file already has full end-to-end mocks for `_process_one`.

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_encumbrance_extraction_service.py -v
```

Expected: all tests pass, including the two new ones.

- [ ] **Step 4: Full lint + type check**

```bash
uv run ruff check . && uv run ty check
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_encumbrance_extraction_service.py
git commit -m "Add tests for raw OCR persistence"
```
