//! Integration test for the pending-schedule read-side linkage back-fill
//! (`instance_creation_linkage::backfill_pending_schedule_linkage`).
//!
//! A future-dated trigger persists a `Scheduled` workflow instance that does
//! not fire until its timer expires. So an operator can call the run back
//! *before* it fires, the signals read-path must surface the instance's id
//! while it is still pending — but the fire-time linkage (`link_and_rehydrate`,
//! keyed off the first dispatched task) hasn't run yet. This back-fill records
//! the deterministic `(signal_id -> run_id)` linkage up front, so the read
//! side exposes a target immediately.
//!
//! The instance id is deterministic in `(workflow_id, scheduled_at)` — the
//! same seam the server mints the real instance with — so the back-filled id
//! is exactly the one the run will later carry.
//!
//! Requires Docker (testcontainers Postgres). Skipped automatically when
//! Docker is unavailable.

#![cfg(not(madsim))]

use uuid::Uuid;

mod common;

/// Stand up a migrated conductor database on the shared Postgres.
/// Returns `None` when Postgres isn't available so the test skips cleanly.
async fn pg_pool() -> Option<sqlx::PgPool> {
    common::test_db_pool().await
}

/// A pending scheduled run exposes its instance id on the read side: the
/// back-fill records the deterministic `materialized_run_id` before the run
/// fires, so the signals read-path can surface a cancel target.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_marks_pending_scheduled_run_targetable() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(pool) = pg_pool().await else {
        return Ok(());
    };

    let signal_id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    let scheduled_at = chrono::Utc::now() + chrono::Duration::seconds(3600);

    // A freshly-triggered signal starts un-materialized (the trigger pipeline
    // inserts the captures row before the run is minted).
    tickr_conductor::signal_captures::insert(&pool, signal_id, workflow_id, None, &[]).await?;
    let before = tickr_conductor::signal_captures::read(&pool, signal_id)
        .await?
        .expect("captures row must exist after insert");
    assert!(
        before.materialized_run_id.is_none(),
        "a pending scheduled run must start un-materialized on the read side"
    );

    // Back-fill the linkage as the trigger pipeline does for a future-dated run.
    let run_id = tickr_conductor::instance_creation_linkage::backfill_pending_schedule_linkage(
        &pool,
        signal_id,
        workflow_id,
        scheduled_at,
    )
    .await?;

    // The returned id is exactly the deterministic id the server will mint.
    let expected = tickr_proto::derive_scheduled_workflow_instance_id(workflow_id, scheduled_at);
    assert_eq!(
        run_id, expected,
        "back-fill must record the deterministic instance id the run will carry"
    );

    // The read side now surfaces the pending run's id — an operator has a
    // target before it fires.
    let after = tickr_conductor::signal_captures::read(&pool, signal_id)
        .await?
        .expect("captures row still present");
    assert_eq!(
        after.materialized_run_id,
        Some(expected),
        "the pending scheduled run's instance id must be surfaced on the read side"
    );

    // Idempotent with the fire-time back-fill: re-marking with the same id is a
    // no-op under the `IS NULL` guard, and a stray different id never overwrites.
    tickr_conductor::signal_captures::mark_materialized(&pool, signal_id, expected).await?;
    tickr_conductor::signal_captures::mark_materialized(&pool, signal_id, Uuid::new_v4()).await?;
    let final_row = tickr_conductor::signal_captures::read(&pool, signal_id)
        .await?
        .expect("captures row still present");
    assert_eq!(
        final_row.materialized_run_id,
        Some(expected),
        "the recorded linkage is stable once set"
    );

    Ok(())
}
