//! Last-one-out finalizer.
//!
//! After a worker commits its per-task PG row to `success` or `failure`,
//! it calls [`finalize_after_task_outcome`]. The function dispatches on
//! the outcome:
//!
//! - `Failure`: short-circuit the workflow row to `BuildFailed` via a
//!   conditional UPDATE that succeeds only when the row is still
//!   `Building`. No outbox row is created. Subsequent finalizer calls
//!   for the same `(workflow_id, version)` are no-ops because the
//!   conditional guard fails.
//!
//! - `Success`: try to flip `Building → Ready` only when every per-task
//!   row for the workflow is `success`. The UPDATE is the locking
//!   mechanism — under concurrent finalizers, at most one wins; the
//!   losers' UPDATE matches zero rows and they exit silently. The
//!   winning finalizer inserts the outbox row in the same transaction
//!   so the cross-plane hand-off is gated on the lifecycle transition,
//!   not the build worker's wall-clock ordering.

use crate::build_pipeline::executor::BuildOutcome;
use anyhow::{Context, Result};
use sqlx::PgPool;
use tickr_proto::workflow as wf;
use uuid::Uuid;

/// The result of running a finalizer pass. Tests assert on this to
/// distinguish the "this worker turned off the lights" case from the
/// "another worker already did" no-op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FinalizerOutcome {
    /// The workflow row was just flipped to `Ready`; the outbox row was
    /// inserted in the same transaction. The waits-on-signal subscription
    /// index refresh should run after this returns.
    FlippedToReady,
    /// The workflow row was just flipped to `BuildFailed` (terminal).
    FlippedToBuildFailed,
    /// Nothing to do — the workflow row was no longer at `Building`, or
    /// other tasks were still pending. The conditional UPDATE matched
    /// zero rows.
    AlreadyTerminalOrNotReady,
}

/// Run the finalizer pass for `(workflow_id, workflow_version)` after a
/// per-task build outcome committed to PG. Idempotent under concurrent
/// callers: the conditional UPDATEs guarantee at-most-one flip.
pub async fn finalize_after_task_outcome(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
    outcome: &BuildOutcome,
) -> Result<FinalizerOutcome> {
    match outcome {
        BuildOutcome::Failure { .. } => finalize_failed(pool, workflow_id, workflow_version).await,
        BuildOutcome::Success => finalize_if_last(pool, workflow_id, workflow_version).await,
    }
}

async fn finalize_failed(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<FinalizerOutcome> {
    // BuildFailed is terminal: only flip from Building so a subsequent
    // failure (or a late finalizer racing a success) can't overwrite it.
    let res = sqlx::query(
        r#"
        UPDATE workflows
           SET status = 'BuildFailed', updated_at = now()
         WHERE id = $1 AND version = $2 AND status = 'Building'
        "#,
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .execute(pool)
    .await
    .context("flip workflow to BuildFailed")?;

    if res.rows_affected() == 1 {
        Ok(FinalizerOutcome::FlippedToBuildFailed)
    } else {
        Ok(FinalizerOutcome::AlreadyTerminalOrNotReady)
    }
}

async fn finalize_if_last(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<FinalizerOutcome> {
    let mut tx = pool.begin().await?;

    // The UPDATE both checks (every per-task row is `success`) AND flips
    // the workflow row in one atomic statement. Concurrent finalizers
    // race the WHERE clause; at most one wins (the others see
    // `Building` is no longer the row's status because the winner
    // already moved it past).
    let updated = sqlx::query(
        r#"
        UPDATE workflows w
           SET status = 'Ready',
               updated_at = now()
         WHERE w.id = $1 AND w.version = $2 AND w.status = 'Building'
           AND NOT EXISTS (
               SELECT 1 FROM workflow_task_builds b
                WHERE b.workflow_id = w.id
                  AND b.workflow_version = w.version
                  AND b.status <> 'success'
           )
        "#,
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .execute(&mut *tx)
    .await
    .context("conditional flip Building -> Ready")?;

    if updated.rows_affected() == 0 {
        // Either the workflow is no longer at `Building` (another
        // finalizer already flipped it) or there are still per-task
        // rows not yet at `success`.
        tx.rollback().await?;
        return Ok(FinalizerOutcome::AlreadyTerminalOrNotReady);
    }

    // We won the race. Commit the `Building -> Ready` flip; the
    // submission consumer picks the row up via its NATS subscription
    // (and via the boot-time reconciliation scan if the post-commit
    // publish drops).
    tx.commit().await?;
    Ok(FinalizerOutcome::FlippedToReady)
}

/// Load a freshly finalized workflow definition from PostgreSQL as the
/// canonical protobuf shape.
pub async fn load_workflow_definition(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<wf::WorkflowDefinition> {
    let (definition,): (serde_json::Value,) =
        sqlx::query_as("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(workflow_version)
            .fetch_one(pool)
            .await
            .context("load workflow definition")?;
    tickr_proto::codec::definition::definition_proto_from_json(definition)
        .context("rehydrate proto workflow definition from JSONB")
}

/// Commit a per-task build outcome to the `workflow_task_builds` row.
/// The HTTP handler pre-inserts a `pending` row for every task; this
/// function transitions that row to `success` or `failure` along with
/// timestamp / error metadata.
pub async fn record_task_outcome(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
    task_id: Uuid,
    outcome: &BuildOutcome,
) -> Result<()> {
    let (status, error) = match outcome {
        BuildOutcome::Success => ("success", None),
        BuildOutcome::Failure { error } => ("failure", Some(error.as_str())),
    };
    sqlx::query(
        r#"
        UPDATE workflow_task_builds
           SET status = $4, error = $5, built_at = now()
         WHERE workflow_id = $1 AND workflow_version = $2 AND task_id = $3
        "#,
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .bind(task_id)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await
    .context("update workflow_task_builds row")?;
    Ok(())
}
