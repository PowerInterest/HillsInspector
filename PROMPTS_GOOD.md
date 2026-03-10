# Good Prompts

## Persistence Audit Prompt

Use this when a service appears to run but the expected data does not show up in PostgreSQL, on disk, or in downstream views/pages.

### Paste-Ready Prompt

```md
Investigate this as a persistence and observability bug across this project, not just a code bug.

Core rule:
A service is not successful because it ran without exceptions or logged `success`. A service is successful only if it actually persisted the expected data to PostgreSQL and/or disk, and downstream consumers can see that data.

What I want you to do:
1. Trace the full write path for the affected workflow:
   - controller step
   - service method
   - helper/scraper calls
   - SQL writes, upserts, or file writes
   - downstream tables, views, functions, or web endpoints that should reflect the result
2. Verify whether the service found data but failed to persist it.
3. Compare:
   - what the logs say happened
   - what the code intended to write
   - what is actually present in PostgreSQL and/or data files after the run
4. Identify the exact failure mode. Check for things like:
   - stale API endpoint
   - wrong success/noop accounting
   - transaction not committed
   - write filtered out by dedupe logic
   - data written to an overlay table but ignored by rebuild logic
   - downstream rebuild wiping prior writes
   - wrong scope, wrong join, wrong UPSERT condition
   - parse succeeded but persistence payload was empty
   - data persisted but not consumed downstream
5. Fix the root cause, not just the symptom.
6. Add regression tests that would have caught this.
7. Update docs in `docs/` if the persistence contract or architecture was unclear or wrong.

Requirements:
- Treat every service as guilty until persistence is proven.
- Do not stop at logs. Query the actual DB state and inspect actual files.
- If logs say “found X” but inserts/updates are zero, explain exactly why.
- If a step reports `success` or `noop`, verify that status matches real persisted results.
- If a downstream table/view/materialized output should reflect a write, verify that too.
- If a rebuild step destroys or ignores persisted repair rows, fix that workflow.
- Preserve unrelated worktree changes.
- Use `apply_patch` for edits.

Failure-handling rules:
- Treat bare `except`, broad exception catches, and “log then continue” patterns as suspect until proven safe.
- Identify any place where errors are swallowed, downgraded, or converted into empty results like `None`, `[]`, `{}`, or `0` without preserving the reason.
- For SQL writes, do not accept `rowcount = 0` as benign unless the code proves that no change was expected.
- Verify whether important inserts/upserts should use `RETURNING`, follow-up reads, or explicit post-write validation.
- Distinguish among:
  - no candidate rows found
  - candidates found but filtered before write
  - write attempted but no rows affected
  - write failed and the failure was swallowed
  - write succeeded but downstream readers ignored it
- If a step continues after an internal failure, explain whether that is intentional degradation or a bug.

Validation you must perform:
- show the exact tables/files that should have changed
- show whether they changed before vs after
- run targeted tests for the touched logic
- run `ruff` on touched files and `ty check`
- if full-tree lint/type checks fail because of unrelated existing issues, say so explicitly and do not fix unrelated code unless necessary

Output I expect:
- root cause
- exact code path
- exact persistence point
- why data was or was not written
- fix made
- verification results
- any remaining risk
```

### Short Version

```md
Audit this workflow end-to-end as a persistence bug. Do not trust logs or step status. Trace the full path from controller -> service -> scraper/helper -> SQL/file write -> downstream consumer. Prove whether data was actually persisted. If the service found data but wrote nothing, explain exactly why and fix the root cause. Add regression tests and verify with DB/file-level evidence, targeted pytest, `ruff` on touched files, and `ty check`.
```

### Key Instruction

> A service is only successful if expected data is actually persisted and downstream consumers can see it.

## How To Use

- Replace "this workflow" with the actual step or service name.
- Include the affected case number, folio, foreclosure ID, URL, or log snippet.
- If the issue is web-visible, include the page or endpoint that should reflect the data.
- If the issue is pipeline-visible, include the controller step name and last run log.

## Good Targets

- `title_breaks` found repairs but `foreclosure_title_chain` did not change
- market-data scraper found listing data but no photos were persisted
- ORI search found documents but `ori_encumbrances` did not update
- permit service logged matches but no permits appeared in downstream review pages
- refresh step ran but summary tables still reflect stale data
