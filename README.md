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
- Derived metadata tables track per-volume disk usage and per-file attributes for richer reporting.

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
- Control the initial archival sweep with `--scan/--no-scan` or set
  `diskwatcher config set run.auto_scan false` to disable it by default.

### Inspect status

```bash
diskwatcher status
```

Outputs the latest events plus a "By volume" summary indicating total, created,
modified, and deleted counts per directory. The view now folds in the persisted
disk-usage snapshots so you can gauge free/used capacity per volume at a glance.

Structured output is available for automation:

```bash
diskwatcher status --json --limit 25 | jq
```

The JSON payload contains two keys: `events` (recent rows ordered by timestamp)
and `volumes` (aggregated metrics plus the persisted fields from the `volumes`
table, including usage bytes and refresh timestamps).

### Browse catalog contents

```bash
diskwatcher dashboard --limit 15
```

- Shows the most recently touched files with their volume, directory, and event counts.
- Pass `--json` to feed the aggregated `files`/`volumes` payload into notebooks or dashboards.

### Stream live events

```bash
diskwatcher stream --interval 2 | vd -f jsonl -
```

- Emits new catalog entries as NDJSON, perfect for piping into VisiData, `jq`, or custom scripts.
- Adjust `--limit` (per poll) and `--interval` (seconds between polls) to balance freshness and load.

### Apply migrations

```bash
diskwatcher migrate --revision head
```

- Uses Alembic under the hood and defaults to the catalog at
  `~/.diskwatcher/diskwatcher.db`.
- Supply `--url sqlite:////tmp/test.db` when exercising the CLI in tests or
  staging environments.

### Manage configuration

```bash
diskwatcher config show
diskwatcher config set log.level debug
diskwatcher config unset run.auto_scan
```

- Settings live in `~/.diskwatcher/config.json` (override with
  `$DISKWATCHER_CONFIG_DIR`).
- `config show --json` prints effective values, defaults, and allowed choices to
  speed up discovery of available knobs.
- Use `config path` to reveal the backing file.

### Storage layout

- The catalog database defaults to `~/.diskwatcher/diskwatcher.db`; override it by
  pointing `DISKWATCHER_CONFIG_DIR` at a different home or by running the CLI with
  `--url` for migration/status snapshots.
- Structured logs are written to `~/.diskwatcher/diskwatcher.log` via
  `diskwatcher.utils.logging`.
- CLI helpers such as `status --json` emit transient payloads to stdout, while
  integration tests archive artifacts under `logs/artifacts/` when run with
  `pytest --keep-artifacts` (or a custom `--artifact-dir`).


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

Integration tests that exercise real watcher threads are marked with
`@pytest.mark.integration` and are skipped unless you pass `pytest --integration`.
Use `pytest --keep-artifacts` to stash catalog/log files under `logs/artifacts`, or
`pytest --artifact-dir /path/to/folder` to control where persistent outputs land. The
integration suite now writes `events.json` and `status.json` next to the SQLite catalog
so you can inspect the recorded activity after a run.

When rehearsing with lab media (e.g. `/mnt/e`):
- Create or reuse a writable folder such as `/mnt/e/diskwatcher_artifacts` and pass it
  to `pytest tests/integration/test_end_to_end.py --integration --artifact-dir <path>`.
- Populate a practice directory with just a few files so the watcher finishes quickly;
  the test only needs to observe a small burst of activity.
- Drop any scratch artifacts under `tests/test_out/` (ignored by git) and leave a note in
  `docs/dev_log.md` when collaborators should review them.

### Developer utilities

```
diskwatcher dev revision -m "description" --autogenerate
diskwatcher dev vacuum
diskwatcher dev integrity
```

- `dev revision` wraps Alembic revision creation with optional autogenerate and URL overrides.
- `dev vacuum` runs SQLite VACUUM against the catalog (default `~/.diskwatcher/diskwatcher.db`).
- `dev integrity` executes `PRAGMA integrity_check` and reports the status.
