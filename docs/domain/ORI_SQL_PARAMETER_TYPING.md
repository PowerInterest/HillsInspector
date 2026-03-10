## ORI SQL Parameter Typing

`PgOriService._save_documents()` updates existing `ori_encumbrances` rows before it
falls back to the `(folio, instrument_number, book, page, book_type)` upsert path.
That update intentionally skips pure no-op writes by checking whether any incoming
field differs from the stored row.

### Failure Mode

On March 10, 2026, the controller log showed repeated warnings like:

- `psycopg.errors.AmbiguousParameter: could not determine data type of parameter $3`

The failing statement was the `UPDATE ori_encumbrances ... WHERE ... AND (...)`
change-detection clause inside `_save_documents()`. The old SQL used raw bind
parameters directly in predicates such as:

```sql
(:book IS NOT NULL AND book IS DISTINCT FROM :book)
(:is_sat_update IS TRUE AND is_satisfied IS DISTINCT FROM TRUE)
```

Under psycopg v3 / PostgreSQL prepared statements, those standalone boolean/null
guards do not always provide enough type context for repeated parameters. The
result is a document-level rollback to the local savepoint and a logged
`Skip document ...` warning. The step continues, but the document update is lost.

### Fix

Every guard predicate in that change-detection block must cast the bind
parameter to the intended PostgreSQL type before checking `IS NOT NULL` or
`IS TRUE`.

Examples:

```sql
(CAST(:book AS TEXT) IS NOT NULL AND book IS DISTINCT FROM :book)
(CAST(:amount AS NUMERIC) IS NOT NULL AND amount IS DISTINCT FROM :amount)
(CAST(:is_sat_update AS BOOLEAN) IS TRUE AND is_satisfied IS DISTINCT FROM TRUE)
```

The `SET` expressions themselves were not the problem. The ambiguous branch was
the untyped guard predicate.

### Verification

- Reproduced against the local `hills_sunbiz` PostgreSQL database using the exact
  logged parameters for instrument `2018209753`.
- The pre-fix SQL raised `AmbiguousParameter`.
- The casted SQL succeeded and returned `rowcount=1`, proving the warning was
  hiding a real update rather than a harmless no-op.

### Regression Coverage

`tests/test_pg_ori_service.py::test_save_documents_casts_change_detection_params_for_pg_type_inference`
asserts that the generated update SQL keeps the typed guards in place.
