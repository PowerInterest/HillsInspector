# Pipeline Quality Thresholds

This document defines how to judge whether a `Controller.py` run produced
investment-grade foreclosure analysis.

The key distinction is:

- **coverage**: did we build the expected output objects?
- **classification quality**: are those outputs internally consistent and
  legally sensible?

Some properties are genuinely broken, missing a folio, or otherwise unresolved.
So the pipeline should not fail merely because every title chain is not
`COMPLETE`. It should fail when it cannot build a summary, cannot classify a
gap correctly, or misclassifies encumbrance outcomes.

## Hard Gates

These are pass/fail gates. A run is a failure if any of them are missed.

### 1. Final Judgment PDFs

Target:

- `90%+` of active foreclosures

Validation:

```sql
SELECT COUNT(*) FROM foreclosures WHERE archived_at IS NULL;
```

Filesystem count:

```bash
python - <<'PY'
from pathlib import Path
print(len(list(Path("data/Foreclosure").glob("*/documents/*.pdf"))))
PY
```

### 2. Extracted Judgment Data

Target:

- `90%+` of active PDF-backed foreclosures

Validation:

```sql
SELECT COUNT(*) FROM foreclosures
WHERE archived_at IS NULL
  AND pdf_path IS NOT NULL;

SELECT COUNT(*) FROM foreclosures
WHERE archived_at IS NULL
  AND judgment_data IS NOT NULL;
```

### 3. Title Summary Coverage

Target:

- `80%+` of active foreclosures with judgments

Validation:

```sql
SELECT COUNT(*) FROM foreclosures
WHERE archived_at IS NULL
  AND judgment_data IS NOT NULL;

SELECT COUNT(DISTINCT f.foreclosure_id)
FROM foreclosures f
JOIN foreclosure_title_summary ts
  ON ts.foreclosure_id = f.foreclosure_id
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL;
```

### 4. Title Gap Consistency

Target:

- `0` contradictions

Validation:

```sql
SELECT COUNT(*)
FROM (
    SELECT 1
    FROM foreclosure_title_chain tc
    JOIN foreclosures f
      ON f.foreclosure_id = tc.foreclosure_id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
      AND tc.link_status IN ('MISSING_PARTY', 'CHAINED_BY_FOLIO')
      AND COALESCE(tc.is_gap, FALSE) = FALSE

    UNION ALL

    SELECT 1
    FROM foreclosure_title_summary ts
    JOIN foreclosures f
      ON f.foreclosure_id = ts.foreclosure_id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
      AND COALESCE(ts.gap_count, 0) > 0
      AND ts.chain_status <> 'BROKEN'
) q;
```

### 5. Encumbrance Coverage

Target:

- `80%+` of active foreclosures with judgments

Validation:

```sql
SELECT COUNT(*) FROM foreclosures
WHERE archived_at IS NULL
  AND judgment_data IS NOT NULL;

SELECT COUNT(DISTINCT f.foreclosure_id)
FROM foreclosures f
JOIN ori_encumbrances oe
  ON oe.strap = f.strap
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL
  AND oe.encumbrance_type != 'noc';
```

### 6. Survival Coverage

Target:

- `80%+` of active foreclosures with judgments

Validation:

```sql
SELECT COUNT(DISTINCT f.foreclosure_id)
FROM foreclosures f
JOIN ori_encumbrances oe
  ON oe.strap = f.strap
LEFT JOIN foreclosure_encumbrance_survival fes
  ON fes.foreclosure_id = f.foreclosure_id
 AND fes.encumbrance_id = oe.id
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL
  AND oe.encumbrance_type != 'noc'
  AND COALESCE(fes.survival_status, oe.survival_status) IS NOT NULL;
```

### 7. Exactly One Foreclosing Lien

Target:

- `0` foreclosures with zero or multiple `FORECLOSING` rows

Validation:

```sql
SELECT COUNT(*)
FROM (
    SELECT
        f.foreclosure_id,
        COUNT(*) FILTER (
            WHERE COALESCE(fes.survival_status, oe.survival_status) = 'FORECLOSING'
        ) AS foreclosing_count
    FROM foreclosures f
    JOIN ori_encumbrances oe
      ON oe.strap = f.strap
     AND oe.encumbrance_type != 'noc'
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON fes.foreclosure_id = f.foreclosure_id
     AND fes.encumbrance_id = oe.id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
    GROUP BY f.foreclosure_id
) q
WHERE foreclosing_count <> 1;
```

### 8. Procedural-Document Sanity

Target:

- `0` misclassified procedural rows

Validation:

```sql
SELECT COUNT(*)
FROM (
    -- Same-case recorded judgments should not survive independently.
    SELECT 1
    FROM foreclosures f
    JOIN ori_encumbrances oe
      ON oe.strap = f.strap
     AND oe.encumbrance_type = 'judgment'
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON fes.foreclosure_id = f.foreclosure_id
     AND fes.encumbrance_id = oe.id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
      AND normalize_case_number_fn(oe.case_number) =
          normalize_case_number_fn(f.case_number_raw)
      AND COALESCE(fes.survival_status, oe.survival_status) IN (
          'SURVIVED', 'UNCERTAIN', 'FORECLOSING'
      )

    UNION ALL

    -- Assignments transfer lien ownership; they are not independent survivors.
    SELECT 1
    FROM foreclosures f
    JOIN ori_encumbrances oe
      ON oe.strap = f.strap
     AND oe.encumbrance_type = 'assignment'
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON fes.foreclosure_id = f.foreclosure_id
     AND fes.encumbrance_id = oe.id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
      AND COALESCE(fes.survival_status, oe.survival_status) IN (
          'SURVIVED', 'UNCERTAIN', 'FORECLOSING'
      )

    UNION ALL

    -- Lis pendens is procedural notice, not a survived lien.
    SELECT 1
    FROM foreclosures f
    JOIN ori_encumbrances oe
      ON oe.strap = f.strap
     AND oe.encumbrance_type = 'lis_pendens'
    LEFT JOIN foreclosure_encumbrance_survival fes
      ON fes.foreclosure_id = f.foreclosure_id
     AND fes.encumbrance_id = oe.id
    WHERE f.archived_at IS NULL
      AND f.judgment_data IS NOT NULL
      AND COALESCE(fes.survival_status, oe.survival_status) IN (
          'SURVIVED', 'UNCERTAIN'
      )
) q;
```

## Required Diagnostics

These are required report-out metrics. They are not all hard failure gates,
because some reflect real-world legal conditions rather than pipeline quality.

### A. Fully Complete Title Chains

This is the metric you want to read as “full complete title chains with no
breaks.”

Validation:

```sql
SELECT
    COUNT(*) FILTER (
        WHERE ts.chain_status = 'COMPLETE'
          AND COALESCE(ts.gap_count, 0) = 0
    ) AS complete_unbroken,
    COUNT(*) FILTER (WHERE ts.chain_status = 'BROKEN') AS broken,
    COUNT(*) FILTER (WHERE ts.chain_status = 'MISSING_FOLIO') AS missing_folio,
    COUNT(*) FILTER (WHERE ts.chain_status = 'NO_SALES') AS no_sales,
    COUNT(*) AS total_summaries
FROM foreclosure_title_summary ts
JOIN foreclosures f
  ON f.foreclosure_id = ts.foreclosure_id
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL;
```

Interpretation:

- a low `complete_unbroken` rate is not automatically a pipeline failure
- a low `total_summaries` rate is a pipeline failure
- a high `broken` count can be a correct result if the underlying chain is
  actually broken

### B. Mortgage-Case Foreclosing Anchor Quality

Validation:

```sql
SELECT COUNT(*)
FROM foreclosures f
WHERE f.archived_at IS NULL
  AND f.judgment_data IS NOT NULL
  AND LOWER(COALESCE(f.judgment_data->>'foreclosure_type', '')) = 'mortgage'
  AND EXISTS (
      SELECT 1
      FROM ori_encumbrances oe
      WHERE oe.strap = f.strap
        AND oe.encumbrance_type = 'mortgage'
        AND COALESCE(oe.is_satisfied, FALSE) = FALSE
  )
  AND EXISTS (
      SELECT 1
      FROM ori_encumbrances oe
      LEFT JOIN foreclosure_encumbrance_survival fes
        ON fes.foreclosure_id = f.foreclosure_id
       AND fes.encumbrance_id = oe.id
      WHERE oe.strap = f.strap
        AND COALESCE(fes.survival_status, oe.survival_status) = 'FORECLOSING'
        AND oe.encumbrance_type IN ('assignment', 'judgment')
  );
```

Target:

- `0`

### C. Satisfied Mortgage Linkage Quality

Validation:

```sql
SELECT
    COUNT(*) FILTER (WHERE oe.encumbrance_type = 'mortgage' AND oe.is_satisfied) AS satisfied_mortgages,
    COUNT(*) FILTER (
        WHERE oe.encumbrance_type = 'mortgage'
          AND oe.is_satisfied
          AND (
              oe.satisfaction_instrument IS NOT NULL
              OR oe.satisfaction_date IS NOT NULL
          )
    ) AS satisfied_with_linkage
FROM ori_encumbrances oe;
```

Interpretation:

- report the linkage rate
- trend it upward over time
- do not fail a run solely on this metric unless there is a sudden regression

### D. Staged Case-Only ORI Documents

When no parcel identity is known, ORI now stages unresolved docs instead of
writing bad parcel-scoped rows. Report the cases and staged counts after a run.

Filesystem check:

```bash
find data/Foreclosure -path '*/ori/case_only_unresolved*.json' -type f | sort
```

Interpretation:

- staged unresolved docs are safer than bad inserts
- they still represent unresolved analytical debt and should be reported
