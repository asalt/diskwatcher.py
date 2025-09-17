---
date: 2025-09-17T16:49:38Z
task: "Refresh agent guides for Diskwatcher"
branch: "main"
agent: "gpt-5-codex"
commit: "N/A"
tags: [doc]
---

**Summary.** Updated AGENTS.md and agents.txt so future contributors see Diskwatcher-specific guardrails for cataloging disks and extending the watcher CLI. Reframed the workflow notes around the actual core modules, database helpers, and logging stacks so new agents land on the right entry points without spelunking.

**Highlights.**
- Clarified where to look for CLI, watcher, and database code, plus how to keep UUID mapping deterministic across mounts.
- Added guidance for documenting schema changes and using structured logging helpers that feed the catalog telemetry.
- Captured expectations about creating dev logs and JSON reflections so the handoff ritual is spelled out in one place.

**Challenges.**
- No existing docs folder, so noted creation requirements while keeping instructions concrete for first contributors instead of inventing extra surfaces.
- Balancing brevity with actionable direction in agents.txt without repeating entire README copy.

**Suggestions.**
- Populate README.md with a high-level roadmap and quickstart before onboarding more help.
- Consider adding a stub `docs/agent_playbook.md` soon so future edits have a destination.

**Score.**
Novelty: low
Importance: medium
Difficulty: low

**Signature.** @codex
