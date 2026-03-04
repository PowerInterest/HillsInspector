## ORI Lis Pendens Recovery

This guide explains how the PostgreSQL-first ORI pipeline now recovers missing
`lis_pendens` rows without adding new schema.

### Problem

Historically, ORI coverage could miss a valid lis pendens for three reasons:

1. The normal ORI target selector required both `strap` and `folio`, so a case
   with a valid foreclosure case number but missing parcel identity could never
   get a case-based LP retry.
2. `foreclosures.step_ori_searched` was treated as "done" even when no LP had
   been persisted.
3. PAV negative cache entries could suppress a later live case-number retry for
   up to seven days.

That combination produced long-lived LP gaps even when the Clerk still returned
the document live.

### Current Behavior

`src/services/pg_ori_service.py` now separates two ORI workloads:

1. The standard parcel-backed ORI pass for unsearched foreclosures.
2. A lis-pendens recovery pass for active judged foreclosure cases that still
   have no persisted `ori_encumbrances.encumbrance_type = 'lis_pendens'`.

The recovery selector:

- does not require `strap` or `folio`,
- treats `CA` cases and `CC Real Property/Mortgage Foreclosure%` cases as LP
  relevant,
- and considers an LP satisfied when `ori_encumbrances` matches by either
  `strap` or persisted `case_number`.

### Write Semantics

The recovery flow stays within the existing schema:

- no new tables,
- no new columns,
- no migrations.

Instead, it relies on two write-path rules:

1. Case-query PAV results are tagged with the canonical foreclosure case number
   before save.
2. `_save_documents()` updates an existing `ori_encumbrances` row by
   `instrument_number` before attempting the legacy folio-based upsert.

That lets a case-only LP row be inserted first and later backfilled with
`strap` / `folio` when parcel identity becomes available, without creating a
second row for the same instrument.

### Cache Policy

LP recovery bypasses the disk PAV cache only for case-number searches on
explicit LP-gap targets. This keeps the normal ORI flow cached while preventing
stale negative case-search results from blocking LP recovery.

### Operational Command

Run the repeatable LP maintenance command with:

```bash
uv run python -m src.tools.pg_ori_lis_pendens_backfill --limit 100
```

Useful options:

- `--dry-run`: probe PAV without writing rows
- `--foreclosure-id <id>`: target a specific case
- `--include-never-searched`: include active LP-relevant cases where
  `step_ori_searched` is still `NULL`
- `--json`: emit the per-target result payload

### Guardrails

- LP recovery targets skip judgment-inferred fallback. A missing live LP should
  not be "resolved" by fabricating a placeholder encumbrance.
- LP recovery targets skip the live NOC fallback. That fallback is only for the
  NOC problem space and would waste Clerk calls here.
- The normal full ORI run still performs the broader parcel-backed discovery
  path, including deeds, adjacent instruments, and guarded fallback searches.
