CREATE TABLE tickr_ctx_scopes (
    scope_id UUID NOT NULL PRIMARY KEY,
    namespace TEXT NOT NULL,
    run_id TEXT NOT NULL,
    protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
    creation_claim_id UUID NOT NULL UNIQUE,
    creation_request_digest TEXT NOT NULL,
    state ENUM NOT NULL CHECK (state IN ('active', 'snapshotted', 'cleaned', 'quarantined')),
    created_at TIMESTAMP_MICROS NOT NULL,
    updated_at TIMESTAMP_MICROS NOT NULL,
    snapshot BLOB,
    snapshot_digest TEXT,
    snapshot_row_count INTEGER,
    snapshot_value_bytes INTEGER,
    snapshotted_at TIMESTAMP_MICROS,
    cleaned_at TIMESTAMP_MICROS,
    quarantine_reason TEXT,
    UNIQUE (namespace, run_id),
    CHECK ((snapshot IS NULL AND snapshot_digest IS NULL AND snapshot_row_count IS NULL
            AND snapshot_value_bytes IS NULL AND snapshotted_at IS NULL)
        OR (snapshot IS NOT NULL AND snapshot_digest IS NOT NULL AND snapshot_row_count IS NOT NULL
            AND snapshot_value_bytes IS NOT NULL AND snapshotted_at IS NOT NULL))
);

CREATE TABLE tickr_ctx_scope_values (
    scope_id UUID NOT NULL,
    key TEXT NOT NULL,
    value_identity UUID NOT NULL UNIQUE,
    envelope BLOB NOT NULL,
    created_at TIMESTAMP_MICROS NOT NULL,
    updated_at TIMESTAMP_MICROS NOT NULL,
    PRIMARY KEY (scope_id, key),
    FOREIGN KEY (scope_id) REFERENCES tickr_ctx_scopes(scope_id) ON DELETE CASCADE
);

CREATE TABLE tickr_ctx_scope_claims (
    claim_id UUID NOT NULL PRIMARY KEY,
    scope_id UUID NOT NULL,
    request_digest TEXT NOT NULL,
    committed_at TIMESTAMP_MICROS NOT NULL,
    FOREIGN KEY (scope_id) REFERENCES tickr_ctx_scopes(scope_id) ON DELETE CASCADE
);
