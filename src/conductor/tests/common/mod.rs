//! Shared fixtures for the conductor integration tests.
//!
//! Each test gets its OWN database on the repository formation's shared
//! Postgres, which `just test` starts and verifies before Cargo runs, rather
//! than a dedicated postgres testcontainer per test. That old pattern booted ~30 containers per run and
//! leaked them (testcontainers 0.23 has no reaper and cleans up only on Drop,
//! which races nextest's per-test process exit), starving Docker and flaking
//! the gate. Sharing one engine with a per-test database keeps tests isolated
//! (a database is a hard namespace boundary) while spinning nothing per test.
//!
//! NATS is intentionally NOT shared: its streams, consumers and KV buckets are
//! server-global named objects and its subjects are one flat namespace, so a
//! shared server would let one test's state meddle with another's. NATS tests
//! keep their per-test container (few of them, and `just test`'s reap trap
//! stops those from leaking).
#![allow(dead_code)]

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

/// Base connection string (no database) for the disposable PostgreSQL instance
/// that `just test` starts. Direct Cargo invocations must provide it explicitly.
fn admin_base() -> String {
    std::env::var("TICKR_TEST_PG_URL")
        .expect("TICKR_TEST_PG_URL is required; run `just test` or set an isolated PostgreSQL URL")
}

/// Owns a per-test database on the shared Postgres and drops it when the test
/// ends. The drop runs on a dedicated thread with its own runtime so cleanup
/// does not depend on the test's tokio runtime still being alive.
pub struct DbGuard {
    admin_url: String,
    db_name: String,
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        let admin_url = self.admin_url.clone();
        let db_name = self.db_name.clone();
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(mut conn) = PgConnection::connect(&admin_url).await {
                    let _ = sqlx::query(&format!(
                        "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
                    ))
                    .execute(&mut conn)
                    .await;
                }
            });
        })
        .join();
    }
}

/// Create a fresh, migrated conductor database on the shared Postgres and return
/// a pool connected to it. Infrastructure failure is fatal: integration tests
/// must never report success without executing their database assertions.
pub async fn test_db() -> Option<(DbGuard, PgPool)> {
    let base = admin_base();
    let admin_url = format!("{base}/postgres");
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .unwrap_or_else(|error| panic!("shared test Postgres unavailable: {error}"));
    let db_name = format!("t_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");

    let db_url = format!("{base}/{db_name}");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("connect to isolated test database");
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .expect("migrate isolated test database");
    Some((DbGuard { admin_url, db_name }, pool))
}

/// Like [`test_db`] but returns only the pool, for call sites that kept just a
/// pool and no container handle. The per-test database is left in place for the
/// next run's setup-script sweep instead of being dropped when this returns.
pub async fn test_db_pool() -> Option<PgPool> {
    let (guard, pool) = test_db().await?;
    std::mem::forget(guard);
    Some(pool)
}

/// Backend-specific assertion helper kept in test setup so production Patch
/// callers never receive a pool, SQL row, query, or storage encoding.
pub async fn fetch_patch_row(
    pool: &PgPool,
    key: Uuid,
) -> Result<Option<tickr_conductor::patch_pipeline::PatchRow>, sqlx::Error> {
    type PatchRowTuple = (
        Uuid,
        Uuid,
        Uuid,
        String,
        serde_json::Value,
        Option<String>,
        Option<String>,
        Option<i64>,
        String,
        Option<serde_json::Value>,
    );

    let row = sqlx::query_as::<_, PatchRowTuple>(
        "SELECT patch_key, patch_id, workflow_instance_id, status, ops, reason, outcome, applied_version, provenance, operation
           FROM workflow_patches WHERE patch_key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| tickr_conductor::patch_pipeline::PatchRow {
        patch_key: row.0,
        patch_id: row.1,
        workflow_instance_id: row.2,
        status: row.3,
        ops: row.4,
        reason: row.5,
        outcome: row.6,
        applied_version: row.7,
        provenance: tickr_conductor::patch_pipeline::PatchProvenance::from_wire(&row.8),
        operation: row.9.and_then(|value| serde_json::from_value(value).ok()),
    }))
}
