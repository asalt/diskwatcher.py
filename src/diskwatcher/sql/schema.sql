-- schema.sql
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    event_type TEXT NOT NULL,
    path TEXT NOT NULL,
    directory TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    process_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_path ON events (path);
CREATE INDEX IF NOT EXISTS idx_events_volume ON events (volume_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);

CREATE TABLE IF NOT EXISTS volumes (
    volume_id TEXT PRIMARY KEY,
    directory TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    created_count INTEGER NOT NULL DEFAULT 0,
    modified_count INTEGER NOT NULL DEFAULT 0,
    deleted_count INTEGER NOT NULL DEFAULT 0,
    last_event_timestamp TEXT,
    usage_total_bytes INTEGER,
    usage_used_bytes INTEGER,
    usage_free_bytes INTEGER,
    usage_refreshed_at TEXT,
    events_since_refresh INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    volume_id TEXT NOT NULL,
    path TEXT NOT NULL,
    directory TEXT NOT NULL,
    size_bytes INTEGER,
    modified_time TEXT,
    created_time TEXT,
    last_event_timestamp TEXT,
    last_event_type TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (volume_id, path)
);

CREATE INDEX IF NOT EXISTS idx_files_directory ON files (directory);
