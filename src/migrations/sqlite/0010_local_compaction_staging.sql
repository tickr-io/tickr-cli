CREATE TABLE local_compaction_staging (
    workflow_instance_id UUID PRIMARY KEY,
    protocol_version INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload BLOB,
    state ENUM NOT NULL CHECK (state IN ('staged', 'complete', 'purged')),
    scope_id UUID,
    scope_digest TEXT,
    final_log_references JSON,
    staged_at TIMESTAMP_MICROS NOT NULL,
    completed_at TIMESTAMP_MICROS,
    purged_at TIMESTAMP_MICROS,
    CHECK (
        (state = 'staged' AND payload IS NOT NULL AND completed_at IS NULL AND purged_at IS NULL)
        OR (state = 'complete' AND payload IS NOT NULL AND completed_at IS NOT NULL AND purged_at IS NULL)
        OR (state = 'purged' AND payload IS NULL AND completed_at IS NOT NULL AND purged_at IS NOT NULL)
    )
);

CREATE INDEX local_compaction_staging_pending_idx
    ON local_compaction_staging (state, staged_at, workflow_instance_id);
