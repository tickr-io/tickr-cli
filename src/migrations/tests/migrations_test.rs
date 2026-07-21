#![cfg(not(madsim))]

use std::path::Path;

use chrono::{TimeZone, Timelike, Utc};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{PgPool, Row, SqlitePool};
use tempfile::TempDir;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_migrations::encoding::{
    decode_boolean, decode_enum, decode_json, decode_timestamp, decode_uuid, encode_boolean,
    encode_enum, encode_json, encode_timestamp, encode_uuid,
};
use tickr_migrations::{
    apply_sqlite, apply_target, sqlite_writer_options, validate_migration_registration,
    verify_current, verify_postgres_schema, verify_sqlite_current, verify_sqlite_schema,
    MigrationTarget, MigrationsDriftError, SchemaVerificationError,
};
use uuid::Uuid;

async fn postgres_pool() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    PgPool,
)> {
    let container = Postgres::default().start().await.ok()?;
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new().connect(&url).await.ok()?;
    Some((container, pool))
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

async fn sqlite_pool(path: &Path, create: bool) -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(sqlite_writer_options(&sqlite_url(path), create).unwrap())
        .await
        .unwrap()
}

async fn migrated_sqlite() -> (TempDir, SqlitePool) {
    let directory = tempfile::tempdir().unwrap();
    let pool = sqlite_pool(&directory.path().join("tickr.db"), true).await;
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    (directory, pool)
}

fn assert_incompatible(result: Result<(), SchemaVerificationError>) {
    assert!(
        matches!(result, Err(SchemaVerificationError::Incompatible { .. })),
        "expected incompatible schema, got {result:?}"
    );
}

#[test]
fn migration_registration_pairs_both_backends() {
    validate_migration_registration().unwrap();
}

#[tokio::test]
async fn conductor_postgres_migrations_are_fresh_repeatable_and_verified() {
    let Some((_container, pool)) = postgres_pool().await else {
        return;
    };
    assert!(matches!(
        verify_current(MigrationTarget::Conductor, &pool).await,
        Err(MigrationsDriftError::Behind { .. })
    ));
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_current(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_postgres_schema(&pool).await.unwrap();

    let version: i64 =
        sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn postgres_logical_verification_rejects_an_incompatible_schema() {
    let Some((_container, pool)) = postgres_pool().await else {
        return;
    };
    apply_target(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE signal_wakeups ALTER COLUMN name DROP NOT NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert_incompatible(verify_postgres_schema(&pool).await);
}

#[tokio::test]
async fn sqlite_migration_reopens_without_identity_or_data_drift() {
    const STATES: &[&str] = &["Building", "Submitted"];

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tickr.db");
    let pool = sqlite_pool(&path, true).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "wal"
    );
    assert!(matches!(
        verify_sqlite_current(MigrationTarget::Conductor, &pool).await,
        Err(MigrationsDriftError::Behind { .. })
    ));
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_sqlite_current(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_sqlite_schema(&pool).await.unwrap();

    let patch_key = Uuid::parse_str("00000000-0000-0000-0000-00000000000a").unwrap();
    let patch_id = Uuid::parse_str("00000000-0000-0000-0000-00000000000b").unwrap();
    let workflow_instance_id = Uuid::parse_str("00000000-0000-0000-0000-00000000000c").unwrap();
    let instant = Utc
        .with_ymd_and_hms(2026, 7, 21, 12, 34, 56)
        .single()
        .unwrap()
        .with_nanosecond(987_654_321)
        .unwrap();
    let payload = json!({"z": 2, "a": [{"d": 4, "c": 3}]});
    sqlx::query(
        "INSERT INTO workflow_patches (patch_key, patch_id, workflow_instance_id, status, ops, applied_version, created_at, updated_at, provenance, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(encode_uuid(patch_key))
    .bind(encode_uuid(patch_id))
    .bind(encode_uuid(workflow_instance_id))
    .bind(encode_enum("Building", STATES).unwrap())
    .bind(encode_json(&payload))
    .bind(Option::<i64>::None)
    .bind(encode_timestamp(instant))
    .bind(encode_timestamp(instant))
    .bind("external")
    .bind(Option::<String>::None)
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TEMP TABLE encoding_probe (id UUID NOT NULL, instant TIMESTAMP_MICROS NOT NULL, payload JSON NOT NULL, flag BOOLEAN NOT NULL CHECK (flag IN (0, 1)), state ENUM NOT NULL, optional TEXT)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (id, offset, flag, optional) in [
        ("00000000-0000-0000-0000-000000000002", 2_i64, true, None),
        (
            "00000000-0000-0000-0000-000000000001",
            1_i64,
            false,
            Some("present"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO encoding_probe (id, instant, payload, flag, state, optional) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(encode_uuid(Uuid::parse_str(id).unwrap()))
        .bind(encode_timestamp(instant) + offset)
        .bind(encode_json(&payload))
        .bind(encode_boolean(flag))
        .bind(encode_enum("Building", STATES).unwrap())
        .bind(optional)
        .execute(&pool)
        .await
        .unwrap();
    }
    let rows = sqlx::query(
        "SELECT id, instant, payload, flag, state, optional FROM encoding_probe ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        decode_uuid(rows[0].get::<String, _>("id").as_str()).unwrap(),
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    );
    assert_eq!(
        decode_timestamp(rows[0].get("instant")).unwrap(),
        instant.with_nanosecond(987_654_000).unwrap() + chrono::Duration::microseconds(1)
    );
    assert_eq!(
        decode_json(rows[0].get::<String, _>("payload").as_str()).unwrap(),
        payload
    );
    assert!(!decode_boolean(rows[0].get("flag")).unwrap());
    assert_eq!(
        decode_enum(rows[0].get::<String, _>("state").as_str(), STATES).unwrap(),
        "Building"
    );
    assert_eq!(
        rows[0].get::<Option<String>, _>("optional").as_deref(),
        Some("present")
    );
    let timestamp_order: Vec<String> =
        sqlx::query_scalar("SELECT id FROM encoding_probe ORDER BY instant")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        timestamp_order,
        [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
    );
    pool.close().await;

    let pool = sqlite_pool(&path, false).await;
    apply_sqlite(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_sqlite_current(MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    verify_sqlite_schema(&pool).await.unwrap();
    let stored: (String, String, i64, Option<String>) =
        sqlx::query_as("SELECT patch_key, ops, created_at, source FROM workflow_patches")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(decode_uuid(&stored.0).unwrap(), patch_key);
    assert_eq!(decode_json(&stored.1).unwrap(), payload);
    assert_eq!(
        decode_timestamp(stored.2).unwrap(),
        instant.with_nanosecond(987_654_000).unwrap()
    );
    assert_eq!(stored.3, None);
    let version: i64 =
        sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success = TRUE")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(version, 1);
}

#[tokio::test]
async fn sqlite_verification_detects_missing_tables_and_columns() {
    let (_directory, pool) = migrated_sqlite().await;
    sqlx::query("ALTER TABLE signal_wakeups DROP COLUMN name")
        .execute(&pool)
        .await
        .unwrap();
    assert_incompatible(verify_sqlite_schema(&pool).await);

    let (_directory, pool) = migrated_sqlite().await;
    sqlx::query("DROP TABLE signal_wakeups")
        .execute(&pool)
        .await
        .unwrap();
    assert_incompatible(verify_sqlite_schema(&pool).await);
}

#[tokio::test]
async fn sqlite_verification_detects_type_nullability_and_primary_key_drift() {
    for definition in [
        "CREATE TABLE signal_wakeups (signal_id UUID PRIMARY KEY NOT NULL, name INTEGER NOT NULL, matched_workflows INTEGER NOT NULL, created_at TIMESTAMP_MICROS NOT NULL)",
        "CREATE TABLE signal_wakeups (signal_id UUID PRIMARY KEY NOT NULL, name TEXT, matched_workflows INTEGER NOT NULL, created_at TIMESTAMP_MICROS NOT NULL)",
        "CREATE TABLE signal_wakeups (signal_id UUID NOT NULL, name TEXT NOT NULL, matched_workflows INTEGER NOT NULL, created_at TIMESTAMP_MICROS NOT NULL)",
    ] {
        let (_directory, pool) = migrated_sqlite().await;
        sqlx::query("DROP TABLE signal_wakeups")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(definition).execute(&pool).await.unwrap();
        assert_incompatible(verify_sqlite_schema(&pool).await);
    }
}

#[tokio::test]
async fn sqlite_verification_detects_unique_and_foreign_key_drift() {
    let (_directory, pool) = migrated_sqlite().await;
    sqlx::query("DROP INDEX workflow_replays_idempotency_idx")
        .execute(&pool)
        .await
        .unwrap();
    assert_incompatible(verify_sqlite_schema(&pool).await);

    let (_directory, pool) = migrated_sqlite().await;
    sqlx::query("DROP TABLE workflow_run_info")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE workflow_run_info (workflow_instance_id UUID PRIMARY KEY NOT NULL, ctx_envelope JSON NOT NULL, runtime_params JSON NOT NULL, log_uris JSON NOT NULL, enriched_at TIMESTAMP_MICROS NOT NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_incompatible(verify_sqlite_schema(&pool).await);
}

#[tokio::test]
async fn sqlite_verification_detects_incompatible_enum_constraints() {
    let (_directory, pool) = migrated_sqlite().await;
    sqlx::query("DROP TABLE workflow_patch_task_builds")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE workflow_patch_task_builds (patch_key UUID NOT NULL, task_id UUID NOT NULL, status ENUM NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'success', 'failure', 'unknown')), error TEXT, pending_since TIMESTAMP_MICROS NOT NULL, built_at TIMESTAMP_MICROS, PRIMARY KEY (patch_key, task_id), FOREIGN KEY (patch_key) REFERENCES workflow_patches(patch_key) ON DELETE CASCADE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_incompatible(verify_sqlite_schema(&pool).await);
}
