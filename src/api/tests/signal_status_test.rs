//! Integration test for the signal-audit read layer behind
//! `GET /api/signals/{signal_id}`. The three signal-archive tables share the
//! `signal_id` keyspace without overlap, and the endpoint's fallback chain
//! relies on that: wakeups → captures → cancels → 404. This test exercises the
//! four lookup cases against an ephemeral Postgres:
//!
//!   1. signal present in `signal_wakeups` only
//!   2. signal present in `signal_cancels` only
//!   3. signal present in `signal_captures` only
//!   4. unknown signal — absent from all three
//!
//! Requires Docker (testcontainers Postgres). Skipped automatically when Docker
//! is unavailable.

#![cfg(not(madsim))]

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// Stand up an ephemeral Postgres with the conductor's migrations applied.
/// Returns `None` when Docker isn't available so the test skips cleanly.
async fn pg_pool() -> Option<sqlx::PgPool> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: testcontainers Postgres unavailable: {}", e);
            return None;
        }
    };
    // Leak the container so it lives for the duration of the test process; the
    // pool's connections keep the port valid.
    let container = Box::leak(Box::new(container));
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .ok()?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .ok()?;
    Some(pool)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wakeup_row_resolves_from_wakeups_table_only() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pg_pool().await else {
        return Ok(());
    };

    let sid = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO signal_wakeups (signal_id, name, matched_workflows) VALUES ($1, $2, $3)",
    )
    .bind(sid)
    .bind("user-paid")
    .bind(3_i32)
    .execute(&pool)
    .await?;

    let wakeup = tickr_api::signal_wakeups::read(&pool, sid).await?;
    let wakeup = wakeup.expect("wakeup row must be returned");
    assert_eq!(wakeup.name, "user-paid");
    assert_eq!(wakeup.matched_workflows, 3);

    // Disjoint keyspace: the same id resolves in neither other table.
    assert!(tickr_api::signal_captures::read(&pool, sid)
        .await?
        .is_none());
    assert!(tickr_api::signal_cancels::read(&pool, sid).await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_row_resolves_from_cancels_table_only() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pg_pool().await else {
        return Ok(());
    };

    let sid = Uuid::new_v4();
    let target = serde_json::json!({"kind": "instance", "workflow_instance_id": Uuid::new_v4()});
    sqlx::query(
        "INSERT INTO signal_cancels (signal_id, applied_count, target, note) VALUES ($1, $2, $3, $4)",
    )
    .bind(sid)
    .bind(2_i32)
    .bind(&target)
    .bind(Some("operator stop"))
    .execute(&pool)
    .await?;

    let cancel = tickr_api::signal_cancels::read(&pool, sid).await?;
    let cancel = cancel.expect("cancel row must be returned");
    assert_eq!(cancel.applied_count, 2);
    assert_eq!(cancel.note.as_deref(), Some("operator stop"));

    assert!(tickr_api::signal_wakeups::read(&pool, sid).await?.is_none());
    assert!(tickr_api::signal_captures::read(&pool, sid)
        .await?
        .is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_row_resolves_from_captures_table_only() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pg_pool().await else {
        return Ok(());
    };

    let sid = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    // Empty captures array: a freshly-triggered, not-yet-materialized signal.
    // The endpoint projects this as status `pending` with an empty
    // `captures_summary`.
    sqlx::query("INSERT INTO signal_captures (signal_id, workflow_id, captures) VALUES ($1, $2, '[]'::jsonb)")
        .bind(sid)
        .bind(workflow_id)
        .execute(&pool)
        .await?;

    let captures = tickr_api::signal_captures::read(&pool, sid).await?;
    let captures = captures.expect("captures row must be returned");
    assert_eq!(captures.workflow_id, workflow_id);
    assert!(
        captures.materialized_run_id.is_none(),
        "not yet materialized"
    );
    assert!(captures.terminal_at.is_none(), "not yet terminal");
    assert!(captures.captures.is_empty(), "no named captures");

    assert!(tickr_api::signal_wakeups::read(&pool, sid).await?.is_none());
    assert!(tickr_api::signal_cancels::read(&pool, sid).await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_signal_is_absent_from_all_tables() -> Result<(), Box<dyn std::error::Error>> {
    let Some(pool) = pg_pool().await else {
        return Ok(());
    };

    let sid = Uuid::new_v4();
    assert!(tickr_api::signal_wakeups::read(&pool, sid).await?.is_none());
    assert!(tickr_api::signal_captures::read(&pool, sid)
        .await?
        .is_none());
    assert!(tickr_api::signal_cancels::read(&pool, sid).await?.is_none());
    Ok(())
}
