# Learned Notes

## Data Contracts

- Normalize inputs before writing to the database.
- Prevent uncontrolled schema drift from LLM-generated code.
  Enforce a policy that new columns cannot be added without explicit approval.

## Reliability And Logging

- Never fail silently.
  Log all issues, including failed inserts/upserts and zero-result/zero-row operations.
- Route all log messages through `loguru`.

## Debugging Approach

- "That is not the issue, think deeper" is a useful review heuristic.

## Workflow

- Use Codex for code review and for checking/pushing/syncing Git changes.
- Improve unit-testing strategy for LLM-authored code.
