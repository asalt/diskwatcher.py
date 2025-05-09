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
