//! Integration test for the Event log read path:
//! `archive_queries::list_events` over the tenant events projection.
//!
//! Verifies the read contract the Event log page polls against:
//!   - First load (no cursor) returns the latest rows newest-first by `seq`,
//!     capped.
//!   - `after=<seq>` returns only strictly newer rows — no duplicates across
//!     consecutive polls.
//!   - The batch cap holds on both shapes.
//!
//! Requires Docker (testcontainers Postgres with the conductor migrations,
//! which own the projection's schema).

#![cfg(not(madsim))]

use chrono::Utc;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_api::http::archive_queries::{
    list_events, list_task_instance_events, list_workflow_instance_events,
};
use uuid::Uuid;

/// Seed `n` projection rows in insertion order (BIGSERIAL assigns `seq`).
async fn seed_events(pool: &PgPool, n: usize, event_type: &str) {
    for _ in 0..n {
        sqlx::query(
            "INSERT INTO events (id, ts, event_type, payload, archived_at)
             VALUES ($1, now(), $2, '{}'::jsonb, now())",
        )
        .bind(Uuid::new_v4())
        .bind(event_type)
        .execute(pool)
        .await
        .expect("seed event");
    }
}

/// Seed one projection row carrying the real externally-tagged payload shape
/// `{ "<EventType>": { ...ids } }`, so the per-instance filters exercise the
/// same nesting production writes.
async fn seed_tagged_event(pool: &PgPool, event_type: &str, inner: serde_json::Value) {
    let payload = json!({ event_type: inner });
    sqlx::query(
        "INSERT INTO events (id, ts, event_type, payload, archived_at)
         VALUES ($1, now(), $2, $3::jsonb, now())",
    )
    .bind(Uuid::new_v4())
    .bind(event_type)
    .bind(payload)
    .execute(pool)
    .await
    .expect("seed tagged event");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_load_is_newest_first_capped_and_after_returns_only_newer(
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

    seed_events(&pool, 250, "TaskStarted").await;

    // First load: latest 200, newest-first by seq.
    let first = list_events(&pool, None, 200).await?;
    assert_eq!(first.len(), 200, "first load fills exactly the cap");
    assert!(
        first.windows(2).all(|w| w[0].seq > w[1].seq),
        "rows are newest-first by seq"
    );
    assert_eq!(first[0].seq, 250, "first load starts at the newest row");
    assert_eq!(
        first[199].seq, 51,
        "cap drops the oldest rows, not the newest"
    );

    // Poll with the highest seen seq: nothing newer yet.
    let highest = first[0].seq;
    let poll = list_events(&pool, Some(highest), 200).await?;
    assert!(poll.is_empty(), "no new rows ⇒ empty poll, not a re-send");

    // New activity arrives; the poll returns exactly the strictly-newer rows.
    seed_events(&pool, 7, "WorkflowCompleted").await;
    let poll = list_events(&pool, Some(highest), 200).await?;
    assert_eq!(poll.len(), 7);
    assert!(
        poll.iter().all(|r| r.seq > highest),
        "only rows strictly after the cursor"
    );
    assert!(
        poll.iter().all(|r| r.event_type == "WorkflowCompleted"),
        "the new rows are the new activity"
    );

    // Occurrence time and payload ride through.
    let now = Utc::now();
    assert!(poll.iter().all(|r| (now - r.ts).num_seconds() < 60));
    assert!(poll.iter().all(|r| r.payload.is_object()));

    Ok(())
}

/// The per-instance filters return only the target instance's rows, paginate
/// gap-free on the `seq` cursor, and the nested task endpoint mirrors the
/// instance endpoint's filter scoped to the task-instance id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_instance_filters_scope_to_one_instance_and_paginate_by_seq(
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

    let wi_a = Uuid::new_v4();
    let wi_b = Uuid::new_v4();
    let ti_a1 = Uuid::new_v4();
    let ti_a2 = Uuid::new_v4();

    // Instance A: a delivered task (carries both ids), a parked turn, and a
    // workflow-level completion. Plus a pre-delivery mint for ti_a2 that
    // carries only the task id — the documented rollup gap.
    seed_tagged_event(
        &pool,
        "TaskDelivered",
        json!({ "workflow_instance_id": wi_a, "task_instance_id": ti_a1 }),
    )
    .await;
    seed_tagged_event(
        &pool,
        "TaskParked",
        json!({ "workflow_instance_id": wi_a, "task_instance_id": ti_a1 }),
    )
    .await;
    seed_tagged_event(
        &pool,
        "TaskInstanceCreated",
        json!({ "task_instance_id": ti_a2 }),
    )
    .await;
    seed_tagged_event(
        &pool,
        "WorkflowCompleted",
        json!({ "workflow_instance_id": wi_a }),
    )
    .await;
    // Instance B: a single delivered task — must never leak into A's reads.
    seed_tagged_event(
        &pool,
        "TaskDelivered",
        json!({ "workflow_instance_id": wi_b, "task_instance_id": Uuid::new_v4() }),
    )
    .await;

    // Workflow-instance filter: only A's rows that carry its workflow id.
    // The pre-delivery `TaskInstanceCreated` (task id only) is the accepted
    // gap and is correctly absent.
    let a_events = list_workflow_instance_events(&pool, wi_a, None, 200).await?;
    assert_eq!(
        a_events.len(),
        3,
        "TaskDelivered + TaskParked + WorkflowCompleted"
    );
    assert!(
        a_events
            .iter()
            .all(|r| r.payload.get(&r.event_type).is_some()),
        "rows keep their externally-tagged payload"
    );
    assert!(
        a_events
            .iter()
            .all(|r| r.event_type != "TaskInstanceCreated"),
        "pre-delivery mint (task id only) is the documented rollup gap — absent"
    );
    assert!(
        a_events.windows(2).all(|w| w[0].seq > w[1].seq),
        "newest-first by seq"
    );

    let b_events = list_workflow_instance_events(&pool, wi_b, None, 200).await?;
    assert_eq!(b_events.len(), 1, "instance B is fully isolated from A");

    // `seq` cursor paginates gap-free (the 5s-poll model): polling from the
    // newest seq seen returns nothing until new activity arrives, then returns
    // exactly the strictly-newer rows — no duplicates, no skips.
    let newest = a_events[0].seq;
    let poll = list_workflow_instance_events(&pool, wi_a, Some(newest), 200).await?;
    assert!(poll.is_empty(), "no new rows ⇒ empty poll");
    seed_tagged_event(
        &pool,
        "WorkflowFailed",
        json!({ "workflow_instance_id": wi_a }),
    )
    .await;
    let poll = list_workflow_instance_events(&pool, wi_a, Some(newest), 200).await?;
    assert_eq!(poll.len(), 1, "only the strictly-newer row");
    assert!(poll[0].seq > newest);
    assert_eq!(poll[0].event_type, "WorkflowFailed");

    // Task-instance filter: ti_a1 carries TaskDelivered + TaskParked; the
    // pre-delivery mint for ti_a2 is served here (it carries the task id).
    let t1 = list_task_instance_events(&pool, ti_a1, None, 200).await?;
    assert_eq!(t1.len(), 2, "TaskDelivered + TaskParked for ti_a1");
    assert!(t1
        .iter()
        .all(|r| r.event_type == "TaskDelivered" || r.event_type == "TaskParked"),);
    let t2 = list_task_instance_events(&pool, ti_a2, None, 200).await?;
    assert_eq!(
        t2.len(),
        1,
        "the pre-delivery mint is served on the task endpoint"
    );
    assert_eq!(t2[0].event_type, "TaskInstanceCreated");

    Ok(())
}
