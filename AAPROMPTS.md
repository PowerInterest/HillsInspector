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

### How To Use

- Replace "this workflow" with the actual step or service name.
- Include the affected case number, folio, foreclosure ID, URL, or log snippet.
- If the issue is web-visible, include the page or endpoint that should reflect the data.
- If the issue is pipeline-visible, include the controller step name and last run log.

### Good Targets

- `title_breaks` found repairs but `foreclosure_title_chain` did not change
- market-data scraper found listing data but no photos were persisted
- ORI search found documents but `ori_encumbrances` did not update
- permit service logged matches but no permits appeared in downstream review pages
- refresh step ran but summary tables still reflect stale data

##  Forensic Case Study

Use this when one case looks wrong and you need a deep, evidence-driven
investigation rather than a quick bug hunt.

### Paste-Ready Prompt

Core rule:
Do not trust the extracted JSON, the database row, the controller status, or prior conclusions until each critical fact is proven from primary evidence.

Your job:
Take one case and reconstruct the truth end-to-end from the source documents, OCR text, database state, and downstream consumers. I want a field-by-field, evidence-backed determination of what is wrong, why it is wrong, whether the problem is extraction, linkage, persistence, or downstream interpretation, and what fix should be made.

Case context:
- Case number: {case_number}
- Primary document: {pdf_path}
- Related extracted JSON/cache: {json_path}
- Any known strap/folio/address: {known_identifiers}
- Any relevant log snippet / controller run: {log_context}

What you must do:
1. Build ground truth from primary evidence first.
   - Read the actual PDF or OCR text.
   - If the PDF is image-based, do not rely on `pypdf` text extraction. Use OCR on all pages then send to LLM
   - Quote or paraphrase the exact lines that prove each key fact.
2. Compare all major fields against each other:
   - PDF / OCR ground truth
   - extracted JSON
   - PostgreSQL foreclosure row
   - HCPA parcel data
   - ORI encumbrance data
   - any downstream page / API / summary using the result
3. Determine whether the failure is:
   - bad OCR / bad LLM extraction
   - wrong case linkage
   - wrong parcel linkage
   - persistence failure
   - downstream interpretation bug
   - stale historical state / later procedural update
   - some combination of the above
4. For final judgments and similar money documents, do the arithmetic yourself.
   - Do not trust the model's math.
   - Reconcile principal, interest, per-diem/accrued interest, late fees, escrow advances, court costs, title costs, attorney fees, credits/payments, subtotal, and total.
   - If the document contains subtractive lines like credits or payments, prove whether they were applied correctly.
5. Prove property identity from the document itself.
   - Address
   - legal description
   - subdivision
   - lot / block / unit
   - plat book / page
   - mortgage recording refs if present
   - then compare to HCPA / foreclosure linkage
6. Prove sale terms from the document itself.
   - sale date
   - sale location
   - online vs in-person sale
   - any reset / amended sale language if present
7. Identify the exact code path responsible for the bad result.
   - prompt/schema problem
   - parser/normalizer problem
   - merge/repair pass problem
   - persistence/load problem
   - downstream consumer problem
8. Fix the root cause if the case reveals a systematic bug.
9. Add regression tests using the concrete failure pattern from this case.
10. Update docs in `docs/` if the investigation uncovered an architectural or workflow rule that should be preserved.

Non-negotiable rules:
- No silent assumptions.
- No "looks right" judgments without evidence.
- No swallowed errors.
- Distinguish clearly between:
  - document says X
  - extraction says Y
  - database says Z
  - current linkage points to W
- If PG/HCPA/ORI disagree with the document, say which system is wrong and why.
- If the document and extraction are correct but the foreclosure is linked to the wrong parcel, do not call that a bad extraction.
- If the extraction is invalid but the linkage is also wrong, separate those failure classes explicitly.
- Confidence scores are not evidence. If the extraction is materially wrong, say so even if confidence is high.

Output I expect:
1. Ground-truth summary
   - the true facts of the case, sourced from the document
2. Field-by-field mismatch table
   - field
   - extracted value
   - proven value
   - evidence
   - failure class
3. Root cause
   - exact failure mode
   - exact code path
4. Fix made
   - code change
   - why it is safe
5. Verification
   - targeted tests
   - before/after evidence
   - DB/file recheck if relevant
6. Remaining risk
   - what this case proves
   - what it does not yet prove
```

### Short Version

```md
Run a forensic case study on {case_number}. Reconstruct ground truth from the source PDF/OCR first, then compare it field-by-field against the extracted JSON, PostgreSQL, HCPA, ORI, and downstream views. Do the arithmetic yourself. Prove whether the issue is extraction, linkage, persistence, or downstream interpretation. Fix the systematic root cause, add regression tests, and document any workflow rule the case reveals.
```

### Key Instruction

> One case can expose a whole class of failures. Treat the case as evidence, not as an anecdote.

### Good Targets

- a final judgment whose property identity or total amount looks wrong
- a lis pendens linked to the wrong parcel
- a title-break case where the chain result contradicts the deed evidence
- an ORI recovery case where the documents exist but downstream survival/UI still look wrong
- any case where the PDF, JSON, and PostgreSQL row disagree materially

### What Good Output Looks Like

- It proves the truth from the document first.
- It separates extraction bugs from linkage bugs.
- It names the exact broken code path.
- It turns one ugly case into a reusable regression test and a durable workflow rule.

## Code Review
Perform a detailed code review of the pa
