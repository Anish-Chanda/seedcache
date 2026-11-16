ALTER TABLE replications ADD COLUMN cache_state TEXT NOT NULL DEFAULT 'present'
    CHECK (cache_state IN ('present', 'evicting', 'evicted', 'restoring'));
ALTER TABLE replications ADD COLUMN cache_operation TEXT NOT NULL DEFAULT ''
    CHECK (cache_operation IN ('', 'evict', 'restore', 'seed'));
ALTER TABLE replications ADD COLUMN cache_updated_at TEXT NOT NULL DEFAULT '';
ALTER TABLE replications ADD COLUMN cache_error TEXT NOT NULL DEFAULT '';
ALTER TABLE replications ADD COLUMN evicted_at TEXT;
ALTER TABLE replications ADD COLUMN restored_at TEXT;

UPDATE replications SET cache_updated_at = updated_at WHERE cache_updated_at = '';
