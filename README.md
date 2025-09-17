# Diskwatcher

Diskwatcher catalogues filesystem activity per volume so the studio always knows
which external drive holds which files. It monitors directories with watchdog,
logs events to a local SQLite catalog, and exposes a small CLI for ingesting and
querying the data.

## Features

- Per-volume activity tracking keyed by UUID/label/device fallback.
- Threaded watchers with a shared SQLite connection for low-overhead writes.
- Structured logging and JSON summaries for scripting.
- Alembic-powered migrations to evolve the catalog schema safely.

## Installation

```bash
python -m pip install -e .
```

This installs the `diskwatcher` console entrypoint and the Python package.

## CLI Usage

### Monitor directories

```bash
diskwatcher run /mnt/e /media/alex --log-level info
```

- If no directories are provided the CLI will try to auto-detect removable media.
- Pass `--no-scan` to skip the initial archival sweep of existing files.

### Inspect status

```bash
diskwatcher status
```

Outputs the latest events plus a "By volume" summary indicating total, created,
modified, and deleted counts per directory.

Structured output is available for automation:

```bash
diskwatcher status --json --limit 25 | jq
```

The JSON payload contains two keys: `events` (recent rows ordered by timestamp)
and `volumes` (aggregated metrics).

### Apply migrations

```bash
diskwatcher migrate --revision head
```

- Uses Alembic under the hood and defaults to the catalog at
  `~/.diskwatcher/diskwatcher.db`.
- Supply `--url sqlite:////tmp/test.db` when exercising the CLI in tests or
  staging environments.

## Programmatic API

```python
from diskwatcher.db.connection import init_db
from diskwatcher.db.events import summarize_by_volume

with init_db() as conn:
    rollup = summarize_by_volume(conn)
    for row in rollup:
        print(row["volume_id"], row["total_events"])
```

`init_db()` now routes through Alembic migrations for file-backed databases, so
new installations pick up schema changes automatically. In-memory connections
(for tests) still use the static schema defined in `src/diskwatcher/sql/schema.sql`.

## Device Identity

`diskwatcher.utils.devices.get_mount_info(path)` resolves the mount point,
`lsblk` metadata, and UUID/label/device fallbacks. For example:

```python
>>> get_mount_info("/mnt/e")
{'directory': '/mnt/e', 'mount_point': '/mnt/e', 'device': '/dev/sda', 'uuid': '961727af-2c2d-4e11-8d3e-c7508a3bed73', 'label': 'e'}
```

On platforms lacking `findmnt`/`lsblk`, the helper falls back to best-effort
identifiers so the catalog still records events.

## Development

- Run `pytest -q` before committing changes.
- Migrations live under `migrations/versions/`. Create new revisions via the CLI:
  `diskwatcher dev revision -m "description" --autogenerate` (or use
  `python scripts/new_revision.py` for scripting) and commit the generated file.
- Update `docs/dev_log.md` and `logs/agent_reflections.jsonl` at the end of each
  session to leave breadcrumbs for the next contributor.

### Developer utilities

```
diskwatcher dev revision -m "description" --autogenerate
diskwatcher dev vacuum
diskwatcher dev integrity
```

- `dev revision` wraps Alembic revision creation with optional autogenerate and URL overrides.
- `dev vacuum` runs SQLite VACUUM against the catalog (default `~/.diskwatcher/diskwatcher.db`).
- `dev integrity` executes `PRAGMA integrity_check` and reports the status.
