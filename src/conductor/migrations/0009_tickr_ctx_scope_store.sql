CREATE TABLE tickr_ctx_scopes (
    scope_id uuid PRIMARY KEY,
    namespace text NOT NULL,
    run_id text NOT NULL,
    protocol_version bigint NOT NULL CHECK (protocol_version = 1),
    creation_claim_id uuid NOT NULL UNIQUE,
    creation_request_digest text NOT NULL,
    state text NOT NULL CHECK (state IN ('active', 'snapshotted', 'cleaned', 'quarantined')),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    snapshot bytea,
    snapshot_digest text,
    snapshot_row_count bigint,
    snapshot_value_bytes bigint,
    snapshotted_at timestamp with time zone,
    cleaned_at timestamp with time zone,
    quarantine_reason text,
    UNIQUE (namespace, run_id),
    CHECK ((snapshot IS NULL AND snapshot_digest IS NULL AND snapshot_row_count IS NULL
            AND snapshot_value_bytes IS NULL AND snapshotted_at IS NULL)
        OR (snapshot IS NOT NULL AND snapshot_digest IS NOT NULL AND snapshot_row_count IS NOT NULL
            AND snapshot_value_bytes IS NOT NULL AND snapshotted_at IS NOT NULL))
);

CREATE TABLE tickr_ctx_scope_values (
    scope_id uuid NOT NULL,
    key text NOT NULL,
    value_identity uuid NOT NULL UNIQUE,
    envelope bytea NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    PRIMARY KEY (scope_id, key),
    FOREIGN KEY (scope_id) REFERENCES tickr_ctx_scopes(scope_id) ON DELETE CASCADE
);

CREATE TABLE tickr_ctx_scope_claims (
    claim_id uuid PRIMARY KEY,
    scope_id uuid NOT NULL,
    request_digest text NOT NULL,
    committed_at timestamp with time zone NOT NULL,
    FOREIGN KEY (scope_id) REFERENCES tickr_ctx_scopes(scope_id) ON DELETE CASCADE
);
