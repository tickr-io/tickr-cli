use std::collections::{BTreeMap, BTreeSet};

use sqlx::{PgPool, Row, SqlitePool};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LogicalType {
    Integer,
    Bytes,
    Uuid,
    Timestamp,
    Text,
    Json,
    Enum,
}

#[derive(Debug, Clone, Copy)]
struct ColumnSpec {
    table: &'static str,
    name: &'static str,
    kind: LogicalType,
    nullable: bool,
}

#[derive(Debug, Clone, Copy)]
struct KeySpec {
    table: &'static str,
    columns: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
struct ForeignKeySpec {
    table: &'static str,
    columns: &'static [&'static str],
    referenced_table: &'static str,
    referenced_columns: &'static [&'static str],
    on_delete: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct EnumSpec {
    table: &'static str,
    column: &'static str,
    values: &'static [&'static str],
}

const TABLES: &[&str] = &[
    "events",
    "local_compaction_staging",
    "local_task_cancellation_ack_outbox",
    "local_task_cancellation_fences",
    "local_task_dispatch_quarantine",
    "local_task_dispatches",
    "local_task_event_outbox",
    "local_task_terminal_outcomes",
    "tickr_ctx_scope_claims",
    "tickr_ctx_scope_values",
    "tickr_ctx_scopes",
    "signal_cancels",
    "signal_captures",
    "signal_wakeups",
    "task_instances",
    "task_specs",
    "workflow_instances",
    "workflow_patch_discrepancies",
    "workflow_patch_task_builds",
    "workflow_patches",
    "workflow_replays",
    "workflow_run_info",
    "workflow_task_builds",
    "workflows",
];

macro_rules! column {
    ($table:literal, $name:literal, $kind:ident, $nullable:literal) => {
        ColumnSpec {
            table: $table,
            name: $name,
            kind: LogicalType::$kind,
            nullable: $nullable,
        }
    };
}

const COLUMNS: &[ColumnSpec] = &[
    column!("events", "seq", Integer, false),
    column!("events", "id", Uuid, false),
    column!("events", "ts", Timestamp, false),
    column!("events", "event_type", Text, false),
    column!("events", "payload", Json, false),
    column!("events", "archived_at", Timestamp, false),
    column!(
        "local_task_dispatch_quarantine",
        "dispatch_key",
        Text,
        false
    ),
    column!("local_task_dispatch_quarantine", "payload", Json, false),
    column!("local_task_dispatch_quarantine", "reason", Text, false),
    column!(
        "local_task_dispatch_quarantine",
        "quarantined_at",
        Timestamp,
        false
    ),
    column!(
        "local_task_cancellation_ack_outbox",
        "acknowledgement_identity",
        Text,
        false
    ),
    column!(
        "local_task_cancellation_ack_outbox",
        "acknowledgement",
        Json,
        false
    ),
    column!(
        "local_task_cancellation_ack_outbox",
        "staged_at",
        Timestamp,
        false
    ),
    column!(
        "local_task_cancellation_ack_outbox",
        "forwarded_at",
        Timestamp,
        true
    ),
    column!(
        "local_task_cancellation_fences",
        "acknowledgement_identity",
        Text,
        false
    ),
    column!(
        "local_task_cancellation_fences",
        "task_instance_id",
        Text,
        false
    ),
    column!(
        "local_task_cancellation_fences",
        "workflow_instance_id",
        Text,
        false
    ),
    column!("local_task_cancellation_fences", "dispatch_key", Text, true),
    column!(
        "local_task_cancellation_fences",
        "pickup_generation",
        Integer,
        true
    ),
    column!("local_task_cancellation_fences", "owner", Text, true),
    column!(
        "local_task_cancellation_fences",
        "committed_at",
        Timestamp,
        false
    ),
    column!(
        "local_task_cancellation_fences",
        "owner_notified_at",
        Timestamp,
        true
    ),
    column!(
        "local_task_cancellation_fences",
        "reconciliation",
        Text,
        true
    ),
    column!(
        "local_task_cancellation_fences",
        "settled_at",
        Timestamp,
        true
    ),
    column!("local_task_dispatches", "dispatch_key", Text, false),
    column!("local_task_dispatches", "payload", Json, false),
    column!("local_task_dispatches", "state", Text, false),
    column!("local_task_dispatches", "pickup_generation", Integer, false),
    column!("local_task_dispatches", "task_instance_id", Text, true),
    column!("local_task_dispatches", "workflow_instance_id", Text, true),
    column!("local_task_dispatches", "owner", Text, true),
    column!(
        "local_task_dispatches",
        "liveness_deadline",
        Timestamp,
        true
    ),
    column!(
        "local_task_dispatches",
        "liveness_armed_at",
        Timestamp,
        true
    ),
    column!("local_task_dispatches", "rejection_reason", Text, true),
    column!("local_task_dispatches", "created_at", Timestamp, false),
    column!("local_task_dispatches", "updated_at", Timestamp, false),
    column!("local_task_event_outbox", "dispatch_key", Text, false),
    column!(
        "local_task_event_outbox",
        "pickup_generation",
        Integer,
        false
    ),
    column!("local_task_event_outbox", "kind", Text, false),
    column!("local_task_event_outbox", "event", Json, false),
    column!("local_task_event_outbox", "staged_at", Timestamp, false),
    column!("local_task_event_outbox", "forwarded_at", Timestamp, true),
    column!("local_task_terminal_outcomes", "dispatch_key", Text, false),
    column!(
        "local_task_terminal_outcomes",
        "pickup_generation",
        Integer,
        false
    ),
    column!("local_task_terminal_outcomes", "outcome", Text, false),
    column!(
        "local_task_terminal_outcomes",
        "settled_at",
        Timestamp,
        false
    ),
    column!("signal_cancels", "signal_id", Uuid, false),
    column!("signal_cancels", "applied_count", Integer, false),
    column!("signal_cancels", "target", Json, false),
    column!("signal_cancels", "note", Text, true),
    column!("signal_cancels", "created_at", Timestamp, false),
    column!("signal_captures", "signal_id", Uuid, false),
    column!("signal_captures", "workflow_id", Uuid, false),
    column!("signal_captures", "captures", Json, false),
    column!("signal_captures", "created_at", Timestamp, false),
    column!("signal_captures", "materialized_run_id", Uuid, true),
    column!("signal_captures", "terminal_at", Timestamp, true),
    column!("signal_captures", "workflow_version", Integer, true),
    column!("signal_wakeups", "signal_id", Uuid, false),
    column!("signal_wakeups", "name", Text, false),
    column!("signal_wakeups", "matched_workflows", Integer, false),
    column!("signal_wakeups", "created_at", Timestamp, false),
    column!("task_instances", "id", Uuid, false),
    column!("task_instances", "workflow_instance_id", Uuid, false),
    column!("task_instances", "workflow_id", Uuid, false),
    column!("task_instances", "task_id", Uuid, false),
    column!("task_instances", "name", Text, false),
    column!("task_instances", "state", Text, false),
    column!("task_instances", "archived_at", Timestamp, false),
    column!("task_instances", "task_instance", Json, false),
    column!("task_instances", "attempt", Integer, false),
    column!("task_specs", "task_id", Uuid, false),
    column!("task_specs", "routing_vars", Json, false),
    column!("task_specs", "created_at", Timestamp, false),
    column!("workflow_instances", "id", Uuid, false),
    column!("workflow_instances", "workflow_id", Uuid, false),
    column!("workflow_instances", "name", Text, false),
    column!("workflow_instances", "state", Text, false),
    column!("workflow_instances", "scheduled_at", Timestamp, true),
    column!("workflow_instances", "archived_at", Timestamp, false),
    column!("workflow_instances", "instance", Json, false),
    column!(
        "workflow_patch_discrepancies",
        "workflow_instance_id",
        Uuid,
        false
    ),
    column!("workflow_patch_discrepancies", "patch_key", Uuid, false),
    column!("workflow_patch_discrepancies", "ledger_status", Text, false),
    column!("workflow_patch_discrepancies", "detail", Text, false),
    column!(
        "workflow_patch_discrepancies",
        "detected_at",
        Timestamp,
        false
    ),
    column!("workflow_patch_task_builds", "patch_key", Uuid, false),
    column!("workflow_patch_task_builds", "task_id", Uuid, false),
    column!("workflow_patch_task_builds", "status", Enum, false),
    column!("workflow_patch_task_builds", "error", Text, true),
    column!(
        "workflow_patch_task_builds",
        "pending_since",
        Timestamp,
        false
    ),
    column!("workflow_patch_task_builds", "built_at", Timestamp, true),
    column!("workflow_patch_task_builds", "lease_owner", Text, true),
    column!("workflow_patch_task_builds", "lease_token", Uuid, true),
    column!(
        "workflow_patch_task_builds",
        "lease_expires_at",
        Timestamp,
        true
    ),
    column!("workflow_patches", "patch_key", Uuid, false),
    column!("workflow_patches", "patch_id", Uuid, false),
    column!("workflow_patches", "workflow_instance_id", Uuid, false),
    column!("workflow_patches", "status", Enum, false),
    column!("workflow_patches", "ops", Json, false),
    column!("workflow_patches", "reason", Text, true),
    column!("workflow_patches", "outcome", Text, true),
    column!("workflow_patches", "applied_version", Integer, true),
    column!("workflow_patches", "created_at", Timestamp, false),
    column!("workflow_patches", "updated_at", Timestamp, false),
    column!("workflow_patches", "provenance", Enum, false),
    column!("workflow_patches", "source", Text, true),
    column!("workflow_patches", "source_format", Enum, true),
    column!("workflow_patches", "operation", Json, true),
    column!("workflow_patches", "lifecycle_lease_owner", Text, true),
    column!("workflow_patches", "lifecycle_lease_token", Uuid, true),
    column!(
        "workflow_patches",
        "lifecycle_lease_expires_at",
        Timestamp,
        true
    ),
    column!("workflow_replays", "replay_instance_id", Uuid, false),
    column!("workflow_replays", "source_instance_id", Uuid, false),
    column!("workflow_replays", "signal_id", Uuid, false),
    column!("workflow_replays", "idempotency_key", Text, true),
    column!("workflow_replays", "status", Enum, false),
    column!("workflow_replays", "resume_from", Json, false),
    column!("workflow_replays", "pre_grounded", Json, false),
    column!("workflow_replays", "name", Text, true),
    column!("workflow_replays", "seed_sha256", Text, true),
    column!("workflow_replays", "outcome", Text, true),
    column!("workflow_replays", "created_at", Timestamp, false),
    column!("workflow_replays", "updated_at", Timestamp, false),
    column!("workflow_replays", "shadowed_keys", Json, false),
    column!("workflow_replays", "lease_owner", Text, true),
    column!("workflow_replays", "lease_token", Uuid, true),
    column!("workflow_replays", "lease_expires_at", Timestamp, true),
    column!("tickr_ctx_scope_claims", "claim_id", Uuid, false),
    column!("tickr_ctx_scope_claims", "scope_id", Uuid, false),
    column!("tickr_ctx_scope_claims", "request_digest", Text, false),
    column!("tickr_ctx_scope_claims", "committed_at", Timestamp, false),
    column!("tickr_ctx_scope_values", "scope_id", Uuid, false),
    column!("tickr_ctx_scope_values", "key", Text, false),
    column!("tickr_ctx_scope_values", "value_identity", Uuid, false),
    column!("tickr_ctx_scope_values", "envelope", Bytes, false),
    column!("tickr_ctx_scope_values", "created_at", Timestamp, false),
    column!("tickr_ctx_scope_values", "updated_at", Timestamp, false),
    column!(
        "local_compaction_staging",
        "workflow_instance_id",
        Uuid,
        false
    ),
    column!(
        "local_compaction_staging",
        "protocol_version",
        Integer,
        false
    ),
    column!("local_compaction_staging", "payload_digest", Text, false),
    column!("local_compaction_staging", "payload", Bytes, true),
    column!("local_compaction_staging", "state", Enum, false),
    column!("local_compaction_staging", "scope_id", Uuid, true),
    column!("local_compaction_staging", "scope_digest", Text, true),
    column!(
        "local_compaction_staging",
        "final_log_references",
        Json,
        true
    ),
    column!("local_compaction_staging", "staged_at", Timestamp, false),
    column!("local_compaction_staging", "completed_at", Timestamp, true),
    column!("local_compaction_staging", "purged_at", Timestamp, true),
    column!("tickr_ctx_scopes", "scope_id", Uuid, false),
    column!("tickr_ctx_scopes", "namespace", Text, false),
    column!("tickr_ctx_scopes", "run_id", Text, false),
    column!("tickr_ctx_scopes", "protocol_version", Integer, false),
    column!("tickr_ctx_scopes", "creation_claim_id", Uuid, false),
    column!("tickr_ctx_scopes", "creation_request_digest", Text, false),
    column!("tickr_ctx_scopes", "state", Enum, false),
    column!("tickr_ctx_scopes", "created_at", Timestamp, false),
    column!("tickr_ctx_scopes", "updated_at", Timestamp, false),
    column!("tickr_ctx_scopes", "snapshot", Bytes, true),
    column!("tickr_ctx_scopes", "snapshot_digest", Text, true),
    column!("tickr_ctx_scopes", "snapshot_row_count", Integer, true),
    column!("tickr_ctx_scopes", "snapshot_value_bytes", Integer, true),
    column!("tickr_ctx_scopes", "snapshotted_at", Timestamp, true),
    column!("tickr_ctx_scopes", "cleaned_at", Timestamp, true),
    column!("tickr_ctx_scopes", "quarantine_reason", Text, true),
    column!("workflow_run_info", "workflow_instance_id", Uuid, false),
    column!("workflow_run_info", "ctx_envelope", Json, false),
    column!("workflow_run_info", "runtime_params", Json, false),
    column!("workflow_run_info", "log_uris", Json, false),
    column!("workflow_run_info", "enriched_at", Timestamp, false),
    column!("workflow_task_builds", "workflow_id", Uuid, false),
    column!("workflow_task_builds", "workflow_version", Integer, false),
    column!("workflow_task_builds", "task_id", Uuid, false),
    column!("workflow_task_builds", "status", Text, false),
    column!("workflow_task_builds", "error", Text, true),
    column!("workflow_task_builds", "pending_since", Timestamp, false),
    column!("workflow_task_builds", "built_at", Timestamp, true),
    column!("workflow_task_builds", "lease_owner", Text, true),
    column!("workflow_task_builds", "lease_token", Uuid, true),
    column!("workflow_task_builds", "lease_expires_at", Timestamp, true),
    column!("workflows", "id", Uuid, false),
    column!("workflows", "name", Text, false),
    column!("workflows", "definition", Json, false),
    column!("workflows", "inserted_at", Timestamp, false),
    column!("workflows", "updated_at", Timestamp, false),
    column!("workflows", "version", Integer, false),
    column!("workflows", "status", Text, false),
    column!("workflows", "nickel_source", Text, false),
    column!("workflows", "namespace", Text, false),
    column!("workflows", "slug", Text, false),
    column!("workflows", "content_hash", Text, false),
    column!("workflows", "cosmetic_hash", Text, false),
    column!("workflows", "submission_lease_owner", Text, true),
    column!("workflows", "submission_lease_token", Uuid, true),
    column!("workflows", "submission_lease_expires_at", Timestamp, true),
];

const PRIMARY_KEYS: &[KeySpec] = &[
    KeySpec {
        table: "events",
        columns: &["seq"],
    },
    KeySpec {
        table: "local_compaction_staging",
        columns: &["workflow_instance_id"],
    },
    KeySpec {
        table: "local_task_dispatch_quarantine",
        columns: &["dispatch_key"],
    },
    KeySpec {
        table: "local_task_cancellation_ack_outbox",
        columns: &["acknowledgement_identity"],
    },
    KeySpec {
        table: "local_task_cancellation_fences",
        columns: &["acknowledgement_identity"],
    },
    KeySpec {
        table: "local_task_dispatches",
        columns: &["dispatch_key"],
    },
    KeySpec {
        table: "local_task_event_outbox",
        columns: &["dispatch_key", "pickup_generation", "kind"],
    },
    KeySpec {
        table: "local_task_terminal_outcomes",
        columns: &["dispatch_key", "pickup_generation"],
    },
    KeySpec {
        table: "tickr_ctx_scope_claims",
        columns: &["claim_id"],
    },
    KeySpec {
        table: "tickr_ctx_scope_values",
        columns: &["scope_id", "key"],
    },
    KeySpec {
        table: "tickr_ctx_scopes",
        columns: &["scope_id"],
    },
    KeySpec {
        table: "signal_cancels",
        columns: &["signal_id"],
    },
    KeySpec {
        table: "signal_captures",
        columns: &["signal_id"],
    },
    KeySpec {
        table: "signal_wakeups",
        columns: &["signal_id"],
    },
    KeySpec {
        table: "task_instances",
        columns: &["id"],
    },
    KeySpec {
        table: "task_specs",
        columns: &["task_id"],
    },
    KeySpec {
        table: "workflow_instances",
        columns: &["id"],
    },
    KeySpec {
        table: "workflow_patch_discrepancies",
        columns: &["workflow_instance_id", "patch_key"],
    },
    KeySpec {
        table: "workflow_patch_task_builds",
        columns: &["patch_key", "task_id"],
    },
    KeySpec {
        table: "workflow_patches",
        columns: &["patch_key"],
    },
    KeySpec {
        table: "workflow_replays",
        columns: &["replay_instance_id"],
    },
    KeySpec {
        table: "workflow_run_info",
        columns: &["workflow_instance_id"],
    },
    KeySpec {
        table: "workflow_task_builds",
        columns: &["workflow_id", "workflow_version", "task_id"],
    },
    KeySpec {
        table: "workflows",
        columns: &["id", "version"],
    },
];

const UNIQUE_KEYS: &[KeySpec] = &[
    KeySpec {
        table: "events",
        columns: &["id"],
    },
    KeySpec {
        table: "local_task_dispatches",
        columns: &["task_instance_id", "workflow_instance_id"],
    },
    KeySpec {
        table: "local_task_cancellation_fences",
        columns: &["task_instance_id", "workflow_instance_id"],
    },
    KeySpec {
        table: "local_task_event_outbox",
        columns: &["dispatch_key", "pickup_generation"],
    },
    KeySpec {
        table: "tickr_ctx_scope_values",
        columns: &["value_identity"],
    },
    KeySpec {
        table: "tickr_ctx_scopes",
        columns: &["creation_claim_id"],
    },
    KeySpec {
        table: "tickr_ctx_scopes",
        columns: &["namespace", "run_id"],
    },
    KeySpec {
        table: "workflow_replays",
        columns: &["source_instance_id", "idempotency_key"],
    },
];

const FOREIGN_KEYS: &[ForeignKeySpec] = &[
    ForeignKeySpec {
        table: "task_instances",
        columns: &["workflow_instance_id"],
        referenced_table: "workflow_instances",
        referenced_columns: &["id"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "local_task_cancellation_ack_outbox",
        columns: &["acknowledgement_identity"],
        referenced_table: "local_task_cancellation_fences",
        referenced_columns: &["acknowledgement_identity"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "local_task_cancellation_fences",
        columns: &["dispatch_key"],
        referenced_table: "local_task_dispatches",
        referenced_columns: &["dispatch_key"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "local_task_dispatch_quarantine",
        columns: &["dispatch_key"],
        referenced_table: "local_task_dispatches",
        referenced_columns: &["dispatch_key"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "local_task_event_outbox",
        columns: &["dispatch_key"],
        referenced_table: "local_task_dispatches",
        referenced_columns: &["dispatch_key"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "local_task_terminal_outcomes",
        columns: &["dispatch_key"],
        referenced_table: "local_task_dispatches",
        referenced_columns: &["dispatch_key"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "tickr_ctx_scope_claims",
        columns: &["scope_id"],
        referenced_table: "tickr_ctx_scopes",
        referenced_columns: &["scope_id"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "tickr_ctx_scope_values",
        columns: &["scope_id"],
        referenced_table: "tickr_ctx_scopes",
        referenced_columns: &["scope_id"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "workflow_patch_task_builds",
        columns: &["patch_key"],
        referenced_table: "workflow_patches",
        referenced_columns: &["patch_key"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "workflow_run_info",
        columns: &["workflow_instance_id"],
        referenced_table: "workflow_instances",
        referenced_columns: &["id"],
        on_delete: "CASCADE",
    },
    ForeignKeySpec {
        table: "workflow_task_builds",
        columns: &["workflow_id", "workflow_version"],
        referenced_table: "workflows",
        referenced_columns: &["id", "version"],
        on_delete: "CASCADE",
    },
];

const ENUMS: &[EnumSpec] = &[
    EnumSpec {
        table: "workflow_patch_task_builds",
        column: "status",
        values: &["pending", "success", "failure"],
    },
    EnumSpec {
        table: "workflow_patches",
        column: "status",
        values: &[
            "Validating",
            "Building",
            "Submitted",
            "Applied",
            "Rejected",
            "BuildFailed",
        ],
    },
    EnumSpec {
        table: "workflow_patches",
        column: "provenance",
        values: &["self", "external"],
    },
    EnumSpec {
        table: "workflow_patches",
        column: "source_format",
        values: &["nickel", "json"],
    },
    EnumSpec {
        table: "workflow_replays",
        column: "status",
        values: &["Materializing", "Released", "VersionUnresolvable"],
    },
    EnumSpec {
        table: "tickr_ctx_scopes",
        column: "state",
        values: &["active", "cleaned", "quarantined", "snapshotted"],
    },
    EnumSpec {
        table: "local_compaction_staging",
        column: "state",
        values: &["complete", "purged", "staged"],
    },
];

#[derive(Debug, thiserror::Error)]
pub enum SchemaVerificationError {
    #[error("failed to inspect {backend} logical schema: {source}")]
    Query {
        backend: &'static str,
        #[source]
        source: sqlx::Error,
    },
    #[error("{backend} logical schema is incompatible: {detail}")]
    Incompatible {
        backend: &'static str,
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ActualColumn {
    table: String,
    name: String,
    kind: LogicalType,
    nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ActualForeignKey {
    table: String,
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    on_delete: String,
}

pub async fn verify_postgres_schema(pool: &PgPool) -> Result<(), SchemaVerificationError> {
    let backend = "postgres";
    let tables = sqlx::query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?
    .into_iter()
    .map(|row| row.get::<String, _>("table_name"))
    .filter(|table| table != "_sqlx_migrations")
    .collect();
    verify_tables(backend, tables)?;

    let column_rows = sqlx::query(
        "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = 'public'",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?;
    let mut columns = BTreeSet::new();
    for row in column_rows {
        let table = row.get::<String, _>("table_name");
        if table == "_sqlx_migrations" {
            continue;
        }
        columns.insert(ActualColumn {
            table,
            name: row.get("column_name"),
            kind: postgres_type(row.get::<String, _>("data_type").as_str())?,
            nullable: row.get::<String, _>("is_nullable") == "YES",
        });
    }
    verify_columns(backend, columns, false)?;

    let key_rows = sqlx::query(
        "SELECT tc.constraint_name, tc.table_name, tc.constraint_type, kcu.column_name, kcu.ordinal_position FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name WHERE tc.table_schema = 'public' AND tc.table_name <> '_sqlx_migrations' AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') ORDER BY tc.constraint_name, kcu.ordinal_position",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?;
    let (primary, mut unique) = collect_postgres_keys(key_rows);
    let unique_index_rows = sqlx::query(
        "SELECT tab.relname AS table_name, idx.relname AS index_name, att.attname AS column_name, ord.ordinality AS ordinal_position FROM pg_index ix JOIN pg_class tab ON tab.oid = ix.indrelid JOIN pg_namespace ns ON ns.oid = tab.relnamespace JOIN pg_class idx ON idx.oid = ix.indexrelid JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY ord(attnum, ordinality) ON TRUE JOIN pg_attribute att ON att.attrelid = tab.oid AND att.attnum = ord.attnum WHERE ns.nspname = 'public' AND tab.relname <> '_sqlx_migrations' AND ix.indisunique AND NOT ix.indisprimary ORDER BY idx.relname, ord.ordinality",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?;
    unique.extend(collect_named_keys(unique_index_rows));
    verify_keys(backend, primary, unique)?;

    let foreign_rows = sqlx::query(
        "SELECT tc.constraint_name, tc.table_name, kcu.column_name, kcu.ordinal_position, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column, rc.delete_rule FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name JOIN information_schema.referential_constraints rc ON tc.constraint_schema = rc.constraint_schema AND tc.constraint_name = rc.constraint_name JOIN information_schema.key_column_usage ccu ON ccu.constraint_schema = rc.unique_constraint_schema AND ccu.constraint_name = rc.unique_constraint_name AND ccu.ordinal_position = kcu.position_in_unique_constraint WHERE tc.table_schema = 'public' AND tc.constraint_type = 'FOREIGN KEY' ORDER BY tc.constraint_name, kcu.ordinal_position",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?;
    verify_foreign_keys(backend, collect_postgres_foreign_keys(foreign_rows))?;

    let checks = sqlx::query(
        "SELECT rel.relname AS table_name, pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con JOIN pg_class rel ON rel.oid = con.conrelid JOIN pg_namespace ns ON ns.oid = rel.relnamespace WHERE ns.nspname = 'public' AND con.contype = 'c'",
    )
    .fetch_all(pool)
    .await
    .map_err(|source| SchemaVerificationError::Query { backend, source })?
    .into_iter()
    .map(|row| (row.get::<String, _>("table_name"), row.get::<String, _>("definition")))
    .collect::<Vec<_>>();
    verify_enum_checks(backend, &checks)
}

pub async fn verify_sqlite_schema(pool: &SqlitePool) -> Result<(), SchemaVerificationError> {
    let backend = "sqlite";
    let table_rows = sqlx::query("SELECT name, sql FROM sqlite_schema WHERE type = 'table'")
        .fetch_all(pool)
        .await
        .map_err(|source| SchemaVerificationError::Query { backend, source })?;
    let tables = table_rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .filter(|table| table != "_sqlx_migrations" && !table.starts_with("sqlite_"))
        .collect();
    verify_tables(backend, tables)?;

    let primary_columns = primary_key_columns();
    let mut columns = BTreeSet::new();
    let mut primary = BTreeSet::new();
    let mut unique = BTreeSet::new();
    let mut foreign = BTreeSet::new();
    for table in TABLES {
        let rows = sqlx::query(
            "SELECT name, type, \"notnull\", pk FROM pragma_table_info(?) ORDER BY cid",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .map_err(|source| SchemaVerificationError::Query { backend, source })?;
        let mut table_primary = Vec::new();
        for row in rows {
            let name = row.get::<String, _>("name");
            let primary_position = row.get::<i64, _>("pk");
            if primary_position > 0 {
                table_primary.push((primary_position, name.clone()));
            }
            columns.insert(ActualColumn {
                table: (*table).to_owned(),
                kind: sqlite_type(row.get::<String, _>("type").as_str())?,
                nullable: row.get::<i64, _>("notnull") == 0
                    && !primary_columns.contains(&((*table).to_owned(), name.clone())),
                name,
            });
        }
        table_primary.sort_by_key(|(position, _)| *position);
        primary.insert((
            (*table).to_owned(),
            table_primary.into_iter().map(|(_, name)| name).collect(),
        ));

        let indexes = sqlx::query("SELECT name, \"unique\" FROM pragma_index_list(?)")
            .bind(table)
            .fetch_all(pool)
            .await
            .map_err(|source| SchemaVerificationError::Query { backend, source })?;
        for index in indexes {
            if index.get::<i64, _>("unique") == 0 {
                continue;
            }
            let name = index.get::<String, _>("name");
            let index_columns = sqlx::query("SELECT name FROM pragma_index_info(?) ORDER BY seqno")
                .bind(name)
                .fetch_all(pool)
                .await
                .map_err(|source| SchemaVerificationError::Query { backend, source })?
                .into_iter()
                .map(|row| row.get::<String, _>("name"))
                .collect::<Vec<_>>();
            if !primary.contains(&((*table).to_owned(), index_columns.clone())) {
                unique.insert(((*table).to_owned(), index_columns));
            }
        }

        let fk_rows = sqlx::query("SELECT id, seq, \"table\", \"from\", \"to\", on_delete FROM pragma_foreign_key_list(?) ORDER BY id, seq")
            .bind(table)
            .fetch_all(pool)
            .await
            .map_err(|source| SchemaVerificationError::Query { backend, source })?;
        foreign.extend(collect_sqlite_foreign_keys(table, fk_rows));
    }
    verify_columns(backend, columns, true)?;
    verify_keys(backend, primary, unique)?;
    verify_foreign_keys(backend, foreign)?;

    let checks = table_rows
        .into_iter()
        .filter_map(|row| {
            let table = row.get::<String, _>("name");
            row.try_get::<Option<String>, _>("sql")
                .ok()
                .flatten()
                .map(|sql| (table, sql))
        })
        .collect::<Vec<_>>();
    verify_enum_checks(backend, &checks)
}

fn verify_tables(
    backend: &'static str,
    actual: BTreeSet<String>,
) -> Result<(), SchemaVerificationError> {
    let expected = TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<BTreeSet<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(incompatible(backend, "tables", expected, actual))
    }
}

fn verify_columns(
    backend: &'static str,
    actual: BTreeSet<ActualColumn>,
    sqlite: bool,
) -> Result<(), SchemaVerificationError> {
    let expected = COLUMNS
        .iter()
        .map(|column| ActualColumn {
            table: column.table.to_owned(),
            name: column.name.to_owned(),
            kind: column.kind,
            nullable: column.nullable,
        })
        .collect::<BTreeSet<_>>();
    let normalized = actual
        .into_iter()
        .map(|mut column| {
            if !sqlite
                && column.kind == LogicalType::Text
                && ENUMS
                    .iter()
                    .any(|spec| spec.table == column.table && spec.column == column.name)
            {
                column.kind = LogicalType::Enum;
            }
            column
        })
        .collect::<BTreeSet<_>>();
    if normalized == expected {
        Ok(())
    } else {
        Err(incompatible(backend, "columns", expected, normalized))
    }
}

fn verify_keys(
    backend: &'static str,
    actual_primary: BTreeSet<(String, Vec<String>)>,
    actual_unique: BTreeSet<(String, Vec<String>)>,
) -> Result<(), SchemaVerificationError> {
    let expected_primary = key_set(PRIMARY_KEYS);
    if actual_primary != expected_primary {
        return Err(incompatible(
            backend,
            "primary keys",
            expected_primary,
            actual_primary,
        ));
    }
    let expected_unique = key_set(UNIQUE_KEYS);
    if actual_unique != expected_unique {
        return Err(incompatible(
            backend,
            "unique keys",
            expected_unique,
            actual_unique,
        ));
    }
    Ok(())
}

fn verify_foreign_keys(
    backend: &'static str,
    actual: BTreeSet<ActualForeignKey>,
) -> Result<(), SchemaVerificationError> {
    let expected = FOREIGN_KEYS
        .iter()
        .map(|key| ActualForeignKey {
            table: key.table.to_owned(),
            columns: strings(key.columns),
            referenced_table: key.referenced_table.to_owned(),
            referenced_columns: strings(key.referenced_columns),
            on_delete: key.on_delete.to_owned(),
        })
        .collect::<BTreeSet<_>>();
    if actual == expected {
        Ok(())
    } else {
        Err(incompatible(backend, "foreign keys", expected, actual))
    }
}

fn verify_enum_checks(
    backend: &'static str,
    checks: &[(String, String)],
) -> Result<(), SchemaVerificationError> {
    for spec in ENUMS {
        let expected = spec
            .values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<BTreeSet<_>>();
        let found = checks
            .iter()
            .filter(|(table, _)| table == spec.table)
            .flat_map(|(_, definition)| check_definitions(definition))
            .filter(|definition| definition.contains(spec.column))
            .map(quoted_values)
            .any(|values| values == expected);
        if !found {
            return Err(SchemaVerificationError::Incompatible {
                backend,
                detail: format!(
                    "{}.{} must constrain enum values to {:?}",
                    spec.table, spec.column, spec.values
                ),
            });
        }
    }
    Ok(())
}

fn postgres_type(value: &str) -> Result<LogicalType, SchemaVerificationError> {
    match value {
        "bigint" | "integer" => Ok(LogicalType::Integer),
        "uuid" => Ok(LogicalType::Uuid),
        "timestamp with time zone" => Ok(LogicalType::Timestamp),
        "text" => Ok(LogicalType::Text),
        "jsonb" => Ok(LogicalType::Json),
        "bytea" => Ok(LogicalType::Bytes),
        other => Err(SchemaVerificationError::Incompatible {
            backend: "postgres",
            detail: format!("unsupported column type `{other}`"),
        }),
    }
}

fn sqlite_type(value: &str) -> Result<LogicalType, SchemaVerificationError> {
    match value.to_ascii_uppercase().as_str() {
        "INTEGER" => Ok(LogicalType::Integer),
        "UUID" => Ok(LogicalType::Uuid),
        "TIMESTAMP_MICROS" => Ok(LogicalType::Timestamp),
        "TEXT" => Ok(LogicalType::Text),
        "JSON" => Ok(LogicalType::Json),
        "ENUM" => Ok(LogicalType::Enum),
        "BLOB" => Ok(LogicalType::Bytes),
        other => Err(SchemaVerificationError::Incompatible {
            backend: "sqlite",
            detail: format!("unsupported declared column type `{other}`"),
        }),
    }
}

fn collect_postgres_keys(
    rows: Vec<sqlx::postgres::PgRow>,
) -> (
    BTreeSet<(String, Vec<String>)>,
    BTreeSet<(String, Vec<String>)>,
) {
    let mut grouped: BTreeMap<(String, String, String), Vec<(i32, String)>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((
                row.get("constraint_name"),
                row.get("table_name"),
                row.get("constraint_type"),
            ))
            .or_default()
            .push((row.get("ordinal_position"), row.get("column_name")));
    }
    let mut primary = BTreeSet::new();
    let mut unique = BTreeSet::new();
    for ((_, table, kind), mut columns) in grouped {
        columns.sort_by_key(|(position, _)| *position);
        let key = (
            table,
            columns.into_iter().map(|(_, column)| column).collect(),
        );
        if kind == "PRIMARY KEY" {
            primary.insert(key);
        } else {
            unique.insert(key);
        }
    }
    (primary, unique)
}

fn collect_named_keys(rows: Vec<sqlx::postgres::PgRow>) -> BTreeSet<(String, Vec<String>)> {
    let mut grouped: BTreeMap<(String, String), Vec<(i64, String)>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry((row.get("index_name"), row.get("table_name")))
            .or_default()
            .push((row.get("ordinal_position"), row.get("column_name")));
    }
    grouped
        .into_iter()
        .map(|((_, table), mut columns)| {
            columns.sort_by_key(|(position, _)| *position);
            (
                table,
                columns.into_iter().map(|(_, column)| column).collect(),
            )
        })
        .collect()
}

fn collect_postgres_foreign_keys(rows: Vec<sqlx::postgres::PgRow>) -> BTreeSet<ActualForeignKey> {
    let mut grouped: BTreeMap<String, (String, String, String, Vec<(i32, String, String)>)> =
        BTreeMap::new();
    for row in rows {
        let constraint = row.get("constraint_name");
        let entry = grouped.entry(constraint).or_insert_with(|| {
            (
                row.get("table_name"),
                row.get("referenced_table"),
                row.get("delete_rule"),
                Vec::new(),
            )
        });
        entry.3.push((
            row.get("ordinal_position"),
            row.get("column_name"),
            row.get("referenced_column"),
        ));
    }
    grouped
        .into_values()
        .map(|(table, referenced_table, on_delete, mut columns)| {
            columns.sort_by_key(|(position, _, _)| *position);
            ActualForeignKey {
                table,
                columns: columns
                    .iter()
                    .map(|(_, column, _)| column.clone())
                    .collect(),
                referenced_table,
                referenced_columns: columns.into_iter().map(|(_, _, column)| column).collect(),
                on_delete,
            }
        })
        .collect()
}

fn collect_sqlite_foreign_keys(
    table: &str,
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> BTreeSet<ActualForeignKey> {
    let mut grouped: BTreeMap<i64, (String, String, Vec<(i64, String, String)>)> = BTreeMap::new();
    for row in rows {
        let id = row.get("id");
        let entry = grouped
            .entry(id)
            .or_insert_with(|| (row.get("table"), row.get("on_delete"), Vec::new()));
        entry
            .2
            .push((row.get("seq"), row.get("from"), row.get("to")));
    }
    grouped
        .into_values()
        .map(|(referenced_table, on_delete, mut columns)| {
            columns.sort_by_key(|(position, _, _)| *position);
            ActualForeignKey {
                table: table.to_owned(),
                columns: columns
                    .iter()
                    .map(|(_, column, _)| column.clone())
                    .collect(),
                referenced_table,
                referenced_columns: columns.into_iter().map(|(_, _, column)| column).collect(),
                on_delete,
            }
        })
        .collect()
}

fn primary_key_columns() -> BTreeSet<(String, String)> {
    PRIMARY_KEYS
        .iter()
        .flat_map(|key| {
            key.columns
                .iter()
                .map(move |column| (key.table.to_owned(), (*column).to_owned()))
        })
        .collect()
}

fn key_set(keys: &[KeySpec]) -> BTreeSet<(String, Vec<String>)> {
    keys.iter()
        .map(|key| (key.table.to_owned(), strings(key.columns)))
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn check_definitions(definition: &str) -> Vec<&str> {
    let bytes = definition.as_bytes();
    let mut checks = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = definition[cursor..].find("CHECK") {
        let start = cursor + relative;
        let Some(open_relative) = definition[start..].find('(') else {
            break;
        };
        let open = start + open_relative;
        let mut depth = 0_u32;
        let mut end = None;
        for (index, byte) in bytes.iter().enumerate().skip(open) {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(index + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(end) = end else {
            break;
        };
        checks.push(&definition[start..end]);
        cursor = end;
    }
    checks
}

fn quoted_values(definition: &str) -> BTreeSet<String> {
    let mut values = BTreeSet::new();
    let mut chars = definition.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '\'' {
            continue;
        }
        let mut value = String::new();
        while let Some(character) = chars.next() {
            if character == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    value.push('\'');
                } else {
                    break;
                }
            } else {
                value.push(character);
            }
        }
        values.insert(value);
    }
    values
}

fn incompatible<T: std::fmt::Debug>(
    backend: &'static str,
    surface: &str,
    expected: T,
    actual: T,
) -> SchemaVerificationError {
    SchemaVerificationError::Incompatible {
        backend,
        detail: format!("{surface} differ; expected {expected:?}, got {actual:?}"),
    }
}
