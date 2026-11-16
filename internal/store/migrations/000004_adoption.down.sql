CREATE TABLE replications_new (
    hash TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'transferring', 'verifying', 'replicated', 'failed', 'blocked')),
    manifest TEXT NOT NULL,
    manifest_fingerprint TEXT NOT NULL,
    source_identity TEXT NOT NULL,
    destination_identity TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_attempt_at TEXT,
    next_attempt_at TEXT,
    verified_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    cache_state TEXT NOT NULL DEFAULT 'present' CHECK (cache_state IN ('present', 'evicting', 'evicted', 'restoring')),
    cache_operation TEXT NOT NULL DEFAULT '' CHECK (cache_operation IN ('', 'evict', 'restore', 'seed')),
    cache_updated_at TEXT NOT NULL,
    cache_error TEXT NOT NULL DEFAULT '',
    evicted_at TEXT,
    restored_at TEXT
);
INSERT INTO replications_new SELECT * FROM replications;
DROP TABLE replications;
ALTER TABLE replications_new RENAME TO replications;
