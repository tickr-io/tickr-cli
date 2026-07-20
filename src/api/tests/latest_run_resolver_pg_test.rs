//! Integration test for the latest-run-state resolver's PG-side query against
//! a real ephemeral Postgres (`testcontainers`), with the conductor migrations
//! applied. The live cluster-query side is exercised at the resolver's seam by
//! pointing the `CoordinatorClient` at an unreachable address, so the result
//! reflects the archive query alone (the resolver degrades to archive-only when
//! the live read fails).
//!
//! Requires Docker. Skipped automatically when Docker isn't available.

#![cfg(not(madsim))]

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::coordinator_client::CoordinatorClient;
use tickr_api::http::latest_run_resolver::resolve_latest_run_states;
use uuid::Uuid;

async fn insert_terminal(
    pool: &sqlx::PgPool,
    id: Uuid,
    workflow_id: Uuid,
    state: &str,
    scheduled_at: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, instance)
        VALUES ($1, $2, 'wf', $3, ($4)::timestamptz, '{}'::jsonb)
        "#,
    )
    .bind(id)
    .bind(workflow_id)
    .bind(state)
    .bind(scheduled_at)
    .execute(pool)
    .await
    .expect("insert terminal instance");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolves_latest_terminal_per_workflow_from_archive(
) -> Result<(), Box<dyn std::error::Error>> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: testcontainers Postgres unavailable: {}", e);
            return Ok(());
        }
    };
    let port = container.get_host_port_ipv4(5432).await?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool).await?;

    let wf_a = Uuid::new_v4();
    let wf_b = Uuid::new_v4();
    // Workflow A: two terminal runs — the later-scheduled `Completed` wins over
    // the earlier `Failed`.
    insert_terminal(
        &pool,
        Uuid::new_v4(),
        wf_a,
        "Failed",
        "2026-01-01T00:00:00Z",
    )
    .await;
    insert_terminal(
        &pool,
        Uuid::new_v4(),
        wf_a,
        "Completed",
        "2026-01-02T00:00:00Z",
    )
    .await;
    // Workflow B: a single terminal run.
    insert_terminal(
        &pool,
        Uuid::new_v4(),
        wf_b,
        "Failed",
        "2026-01-01T00:00:00Z",
    )
    .await;

    // Unreachable coordinator → the resolver degrades to archive-only, so the
    // result is purely the PG-side query under test.
    let coordinator =
        CoordinatorClient::with_timeout("http://127.0.0.1:1", Duration::from_millis(150));

    let wf_never = Uuid::new_v4();
    let out = resolve_latest_run_states(&pool, &coordinator, &[wf_a, wf_b, wf_never]).await;

    assert_eq!(
        out.get(&wf_a),
        Some(&Some("Completed".to_string())),
        "A's latest terminal by scheduled_at is Completed"
    );
    assert_eq!(out.get(&wf_b), Some(&Some("Failed".to_string())));
    assert_eq!(
        out.get(&wf_never),
        Some(&None),
        "a workflow with no archived instance resolves to None"
    );
    Ok(())
}
