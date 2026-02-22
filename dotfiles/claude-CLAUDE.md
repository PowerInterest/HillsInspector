# Global Instructions

## Multi-LLM Workflow Rule (Coding Projects)

Do NOT rely on internal, hidden markdown artifacts (like walkthrough.md, task.md, or scratchpads) for architectural documentation. If you design a new system, fix a complex bug, or discover important project context, you MUST write that documentation directly into the repository's `docs/` folder and link it in the `README.md`. Other LLMs are reading this codebase, so all critical knowledge must be surfaced in the repository itself.

Furthermore, the Python source code file MUST also include a very clear, detailed docstring at the top of the file explaining its architectural purpose and how it fits into the broader system.
