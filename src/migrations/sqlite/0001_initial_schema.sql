-- Current Tickr data-plane schema collapsed for greenfield SQLite stores.
-- Historical objects removed from Postgres never enter this baseline.

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id UUID NOT NULL UNIQUE,
    ts TIMESTAMP_MICROS NOT NULL,
    event_type TEXT NOT NULL,
    payload JSON NOT NULL CHECK (json_valid(payload)),
    archived_at TIMESTAMP_MICROS NOT NULL
);

CREATE TABLE signal_cancels (
    signal_id UUID PRIMARY KEY NOT NULL,
    applied_count INTEGER NOT NULL,
    target JSON NOT NULL CHECK (json_valid(target)),
    note TEXT,
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER))
);

CREATE TABLE signal_captures (
    signal_id UUID PRIMARY KEY NOT NULL,
    workflow_id UUID NOT NULL,
    captures JSON NOT NULL CHECK (json_valid(captures)),
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    materialized_run_id UUID,
    terminal_at TIMESTAMP_MICROS,
    workflow_version INTEGER
);

CREATE TABLE signal_wakeups (
    signal_id UUID PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    matched_workflows INTEGER NOT NULL,
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER))
);

CREATE TABLE workflow_instances (
    id UUID PRIMARY KEY NOT NULL,
    workflow_id UUID NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    scheduled_at TIMESTAMP_MICROS,
    archived_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    instance JSON NOT NULL CHECK (json_valid(instance))
);

CREATE TABLE task_instances (
    id UUID PRIMARY KEY NOT NULL,
    workflow_instance_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    task_id UUID NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    archived_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    task_instance JSON NOT NULL CHECK (json_valid(task_instance)),
    attempt INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE
);

CREATE TABLE task_specs (
    task_id UUID PRIMARY KEY NOT NULL,
    routing_vars JSON NOT NULL DEFAULT '[]' CHECK (json_valid(routing_vars)),
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER))
);

CREATE TABLE workflow_patch_discrepancies (
    workflow_instance_id UUID NOT NULL,
    patch_key UUID NOT NULL,
    ledger_status TEXT NOT NULL,
    detail TEXT NOT NULL,
    detected_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    PRIMARY KEY (workflow_instance_id, patch_key)
);

CREATE TABLE workflow_patches (
    patch_key UUID PRIMARY KEY NOT NULL,
    patch_id UUID NOT NULL,
    workflow_instance_id UUID NOT NULL,
    status ENUM NOT NULL CHECK (status IN ('Validating', 'Building', 'Submitted', 'Applied', 'Rejected', 'BuildFailed')),
    ops JSON NOT NULL CHECK (json_valid(ops)),
    reason TEXT,
    outcome TEXT,
    applied_version INTEGER,
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    updated_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    provenance ENUM NOT NULL DEFAULT 'external' CHECK (provenance IN ('self', 'external')),
    source TEXT,
    source_format ENUM CHECK (source_format IN ('nickel', 'json')),
    operation JSON CHECK (operation IS NULL OR json_valid(operation))
);

CREATE TABLE workflow_patch_task_builds (
    patch_key UUID NOT NULL,
    task_id UUID NOT NULL,
    status ENUM NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'success', 'failure')),
    error TEXT,
    pending_since TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    built_at TIMESTAMP_MICROS,
    PRIMARY KEY (patch_key, task_id),
    FOREIGN KEY (patch_key) REFERENCES workflow_patches(patch_key) ON DELETE CASCADE
);

CREATE TABLE workflow_replays (
    replay_instance_id UUID PRIMARY KEY NOT NULL,
    source_instance_id UUID NOT NULL,
    signal_id UUID NOT NULL,
    idempotency_key TEXT,
    status ENUM NOT NULL CHECK (status IN ('Materializing', 'Released', 'VersionUnresolvable')),
    resume_from JSON NOT NULL DEFAULT '[]' CHECK (json_valid(resume_from)),
    pre_grounded JSON NOT NULL DEFAULT '[]' CHECK (json_valid(pre_grounded)),
    name TEXT,
    seed_sha256 TEXT,
    outcome TEXT,
    created_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    updated_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    shadowed_keys JSON NOT NULL DEFAULT '[]' CHECK (json_valid(shadowed_keys))
);

CREATE TABLE workflow_run_info (
    workflow_instance_id UUID PRIMARY KEY NOT NULL,
    ctx_envelope JSON NOT NULL DEFAULT '[]' CHECK (json_valid(ctx_envelope)),
    runtime_params JSON NOT NULL DEFAULT '{}' CHECK (json_valid(runtime_params)),
    log_uris JSON NOT NULL DEFAULT '{}' CHECK (json_valid(log_uris)),
    enriched_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    FOREIGN KEY (workflow_instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE
);

CREATE TABLE workflows (
    id UUID NOT NULL,
    name TEXT NOT NULL,
    definition JSON NOT NULL CHECK (json_valid(definition)),
    inserted_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    updated_at TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    version INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Building',
    nickel_source TEXT NOT NULL,
    namespace TEXT NOT NULL,
    slug TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    cosmetic_hash TEXT NOT NULL,
    PRIMARY KEY (id, version)
);

CREATE TABLE workflow_task_builds (
    workflow_id UUID NOT NULL,
    workflow_version INTEGER NOT NULL,
    task_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    pending_since TIMESTAMP_MICROS NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)),
    built_at TIMESTAMP_MICROS,
    PRIMARY KEY (workflow_id, workflow_version, task_id),
    FOREIGN KEY (workflow_id, workflow_version) REFERENCES workflows(id, version) ON DELETE CASCADE
);

CREATE INDEX events_archived_at_id_idx ON events (archived_at, id);
CREATE INDEX signal_captures_materialized_run_id_idx ON signal_captures (materialized_run_id) WHERE materialized_run_id IS NOT NULL;
CREATE INDEX signal_captures_terminal_at_idx ON signal_captures (terminal_at) WHERE terminal_at IS NOT NULL;
CREATE INDEX task_instances_wf_inst_task_attempt_idx ON task_instances (workflow_instance_id, task_id, attempt);
CREATE INDEX task_instances_workflow_id_idx ON task_instances (workflow_id);
CREATE INDEX task_instances_workflow_instance_id_idx ON task_instances (workflow_instance_id);
CREATE INDEX workflow_instances_state_archived_at_idx ON workflow_instances (state, archived_at DESC);
CREATE INDEX workflow_instances_workflow_scheduled_idx ON workflow_instances (workflow_id, scheduled_at);
CREATE INDEX workflow_patch_discrepancies_detected_idx ON workflow_patch_discrepancies (detected_at DESC);
CREATE INDEX workflow_patches_unsettled_idx ON workflow_patches (workflow_instance_id) WHERE status IN ('Validating', 'Building', 'Submitted');
CREATE UNIQUE INDEX workflow_replays_idempotency_idx ON workflow_replays (source_instance_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX workflow_replays_source_idx ON workflow_replays (source_instance_id, created_at DESC, replay_instance_id DESC);
CREATE INDEX workflow_replays_unsettled_idx ON workflow_replays (updated_at, replay_instance_id) WHERE status = 'Materializing';
CREATE INDEX workflow_task_builds_workflow_idx ON workflow_task_builds (workflow_id, workflow_version);
