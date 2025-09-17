# Agent workflow guide

Welcome to Diskwatcher. This project catalogs removable and fixed disks by a
stable volume identifier and records filesystem activity so we can build a
searchable map of every external drive in the lab. Use the notes below to stay
productive without breaking the catalog.

## Quick orientation
- Read `README.md` for the product goals; keep it updated as the cataloger evolves.
- Tour `src/diskwatcher/core/cli.py` for the Typer entrypoint, then skim `core/manager.py` and `core/watcher.py` to see how watcher threads and UUID resolution fit together.
- Storage lives in `src/diskwatcher/db/` and `src/diskwatcher/sql/`; these show how events land in SQLite and what schema we depend on.
- Hardware discovery and volume identity helpers are in `src/diskwatcher/utils/devices.py`; touch them carefully and prefer new pure helpers over shelling out inline.
- The expectations and guardrails in `agents.txt` are canonical—review them before shipping behaviour changes.

## Development workflow
- Install locally with `pip install -e .` (or `python -m pip install -e .`) so the `diskwatcher` CLI and shared utils resolve during iteration.
- Design features so the catalog stays deterministic: pure functions in `core` or `utils`, slim orchestrators for threading, and all effects routed through the existing wrappers.
- Reuse `diskwatcher.utils.logging` for structured records; never add bare `print` statements.
- When persisting new data, extend the helpers in `diskwatcher.db.events` or add well-tested wrappers instead of direct `sqlite3` calls sprinkled through the codebase.
- If you add a CLI surface, register it in `core/cli.py` and document expected usage in `README.md` so future operators know how to trigger the flow.

## Tests and checks
- Run `pytest -q` whenever you modify code under `src/diskwatcher` or anything in `tests/`.
- For faster feedback, target the affected tests (for example, `pytest tests/test_diskwatcher.py -q`), but finish with the full suite before you hand off work.
- Linters are configured via `pyproject.toml`; keep imports sorted and respect Black's defaults (line length 88).
- Manual CLI smoke tests (`diskwatcher run <path>`) are welcome, but record the scenario in the dev log so the next agent can reproduce symptoms.

## Leaving breadcrumbs
- Document new subsystems, mount-detection heuristics, or schema tweaks in `README.md` (and `docs/agent_playbook.md` once it exists) so the runtime surfaces stay discoverable.
- Keep SQL and code in sync: update `src/diskwatcher/sql/schema.sql` alongside database code and mention feature flags or config knobs in the docs.
- Prefer TODOs over silent assumptions. If you defer work, leave a pointer in code, the dev log, or an issue stub.

## Before you leave
Please write a brief log entry **at the top** of `docs/dev_log.md` (create the file if it is missing) using the template below.
- Keep it to 120–200 words unless the task was unusually complex.
- Be candid about what went well and what was challenging.
- Add any project-level suggestions.
- Use `related_to` for linking follow-up tasks when applicable (else null).
- Always append, never overwrite existing logs.
- Include a short signature (handle/nickname allowed).

Template:

---
date: <UTC ISO8601>
task: "<one-line task title>"
branch: "<current git branch or N/A>"
agent: "<model short name>"  # e.g., gpt-5-codex
commit: "<short sha if available or N/A>"
tags: [refactor, tests, doc]  # 1–3 topic tags
---

**Summary.** 1–3 sentences on what you did and why.

**Highlights.** Bullets of what worked.

**Challenges.** Bullets of blockers or tricky bits (with links/paths).

**Suggestions.** Project/process improvements (brief).

**Score.**
Novelty:
Importance:
Difficulty:

**Signature.** @your-nickname

Also append a JSONL record to `logs/agent_reflections.jsonl` (create the file if missing) using the structure below:

```jsonl
{
  "date": "2025-09-16",
  "task": "Short description of task",
  "branch": "work",
  "agent": "gpt-5-codex",
  "commit": "abc123",
  "tags": ["refactor", "tests", "doc"],
  "summary": "1–3 sentences on what you did and why.",
  "highlights": [
    "Bullet of what worked well",
    "Another highlight"
  ],
  "challenges": [
    "Bullet of tricky parts or blockers",
    "Another challenge"
  ],
  "suggestions": [
    "Project/process improvements (brief)"
  ],
  "signature": "@your-nickname",
  "related_to": null
}
```
