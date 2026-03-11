# Good Prompts

## Code Review

Use this when you want a real engineering review, not a summary.

### Paste-Ready Prompt

```md
Review this change like a strict senior engineer.

Priorities:
- Find bugs, regressions, bad assumptions, missing validation, missing tests, and data-integrity risks.
- Explain why each issue is a problem in production, not just that it looks wrong.
- Treat swallowed errors, broad exception handling, misleading success logging, and silent fallbacks as first-class defects.
- Verify behavior against the actual code path, not comments or intent.

Required output:
1. Findings first, ordered by severity.
2. Each finding must include:
   - file and line or code path
   - what is wrong
   - why it is a real problem
   - what scenario triggers it
3. Then list open questions or missing coverage.
4. Only after that, provide a short change summary.

Do not praise the code. Do not lead with a summary. Lead with defects.
```

## Forensic Case Study

Use this when one case looks wrong and you need ground truth from primary evidence.

### Paste-Ready Prompt

```md
Run a forensic case study on this case. Do not trust prior conclusions, extracted JSON, DB state, or logs until each key fact is proven from primary evidence.

Case context:
- Case number: {case_number}
- PDF or document path: {pdf_path}
- Extracted JSON path: {json_path}
- Known identifiers: {known_identifiers}
- Relevant log context: {log_context}

What to do:
1. Build ground truth from the actual PDF or OCR text first.
2. Quote or paraphrase the exact lines that prove the important facts.
3. Compare document truth against:
   - extracted JSON
   - PostgreSQL rows
   - parcel/linkage data
   - downstream UI/API output
4. Separate the failure class clearly:
   - extraction
   - linkage
   - persistence
   - downstream interpretation
   - stale historical state
5. Name the exact broken code path.
6. Fix the systematic root cause if this case exposes one.
7. Add a regression test based on the concrete failure pattern.

For money documents, do the arithmetic yourself. For parcel identity, prove address and legal description from the document itself.
```

## Excellent Logging
```md
Improve logging and error handling in this workflow.

Rules:
- No silent errors.
- No swallowed exceptions.
- No broad catch-and-continue without preserving the failure reason.
- No returning `None`, `[]`, `{}`, or success/noop status without explaining why.
- Every warning or error log must say why the condition is a problem, what was skipped or degraded, and what the downstream impact is.

What I want:
1. Find places where failures are hidden, downgraded, or logged without enough context.
2. Tighten exception handling so the real failure reason is preserved.
3. Improve logs so they include:
   - entity or case identifier
   - step or function
   - failed condition
   - why it matters
   - what happens next
4. Keep logs concise but diagnostic.
5. Add or update tests for the failure-handling path.

Treat missing context in warnings as a bug, not a style issue.
```

## Root Cause From Logs

```md
Investigate these warnings/errors to root cause.

Core rule:
Do not stop at the log line. Trace the warning or error to the real code path, triggering condition, and downstream effect.

What to do:
1. Find the exact source of each warning/error in code.
2. Reconstruct the conditions that trigger it.
3. Explain why the warning/error is happening.
4. Explain why it is a real problem, or prove that it is intentionally safe.
5. Check whether the code is hiding a deeper failure behind a warning.
6. Fix the root cause, not just the message text.
7. Add regression coverage if the issue is real.

Required output:
- warning/error text
- source file and code path
- root cause
- why it matters
- fix made
- verification
```
