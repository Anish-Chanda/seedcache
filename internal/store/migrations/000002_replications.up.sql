CREATE TABLE replications (
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
    last_error TEXT NOT NULL DEFAULT ''
);
