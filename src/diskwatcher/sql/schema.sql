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
    mount_device TEXT,
    mount_point TEXT,
    mount_uuid TEXT,
    mount_label TEXT,
    mount_volume_id TEXT,
    lsblk_name TEXT,
    lsblk_path TEXT,
    lsblk_model TEXT,
    lsblk_serial TEXT,
    lsblk_vendor TEXT,
    lsblk_size TEXT,
    lsblk_fsver TEXT,
    lsblk_pttype TEXT,
    lsblk_ptuuid TEXT,
    lsblk_parttype TEXT,
    lsblk_partuuid TEXT,
    lsblk_parttypename TEXT,
    lsblk_wwn TEXT,
    lsblk_maj_min TEXT,
    lsblk_json TEXT,
    identity_refreshed_at TEXT,
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

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    path TEXT,
    volume_id TEXT,
    status TEXT NOT NULL,
    progress_json TEXT,
    owner_pid TEXT,
    owner_host TEXT,
    error_message TEXT,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_volume ON jobs (volume_id);
