//! Build worker entry point.
//!
//! Each worker subscribes to the build queue with NATS queue-group
//! semantics so a job is processed by exactly one worker across the
//! cluster of conductor replicas. For each job: deserialize, invoke
//! the injected [`BuildExecutor`], record the per-task outcome to PG,
//! run the finalizer pass. On finalizer transition to `Ready`, publish
//! a `SubmissionMessage` onto the submission queue so the submission
//! consumer ships the SubmitWorkflow envelope cross-plane, then
//! refresh the in-process waits-on-signal subscription index so the
//! next external wakeup can dispatch.
//!
//! The worker terminates cleanly when the supplied `CancellationToken`
//! is cancelled, after a brief grace window for an in-flight finalizer
//! publish.

use crate::build_pipeline::executor::BuildExecutor;
use crate::build_pipeline::finalizer::{
    finalize_after_task_outcome, record_task_outcome, FinalizerOutcome,
};
use crate::build_pipeline::job::TaskBuildJob;
use crate::build_pipeline::BUILD_QUEUE_SUBJECT;
use crate::submission_consumer::{publish_submission, SubmissionMessage};
use crate::waits_on_signal_lifecycle::apply_workflow_state;
use anyhow::Result;
use async_nats::Client as NatsClient;
use futures::StreamExt;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Grace window the build worker gives in-flight nix builds + the
/// finalizer's post-PG-commit NATS publish to drain after the
/// cancellation token fires. Placeholder for the final tuning pass.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(30);

/// The queue-group name shared across replicas. NATS guarantees one
/// delivery per message across the group; jobs fan out evenly.
pub const BUILD_QUEUE_GROUP: &str = "conductor-build-workers";

/// Spawn a build worker bound to the supplied executor. The worker
/// runs until the cancellation token fires; the returned future
/// resolves when the subscription stream ends or the token cancels.
pub async fn start_build_worker(
    nats: NatsClient,
    pg_pool: Arc<PgPool>,
    executor: Arc<dyn BuildExecutor>,
    cancel: CancellationToken,
) -> Result<()> {
    println!(
        "Starting conductor per-task build worker on {}",
        BUILD_QUEUE_SUBJECT
    );
    let mut sub = nats
        .queue_subscribe(BUILD_QUEUE_SUBJECT, BUILD_QUEUE_GROUP.into())
        .await?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("Build worker received shutdown signal; draining in-flight builds.");
                let _ = tokio::time::timeout(SHUTDOWN_DRAIN, async {
                    while let Some(msg) = sub.next().await {
                        process_job(&nats, &pg_pool, executor.as_ref(), msg).await;
                    }
                })
                .await;
                println!("Build worker drained.");
                break;
            }
            Some(msg) = sub.next() => {
                process_job(&nats, &pg_pool, executor.as_ref(), msg).await;
            }
            else => {
                println!("Build queue subscription ended.");
                break;
            }
        }
    }
    Ok(())
}

async fn process_job(
    nats: &NatsClient,
    pg_pool: &PgPool,
    executor: &dyn BuildExecutor,
    msg: async_nats::Message,
) {
    let job: TaskBuildJob = match bincode::deserialize(&msg.payload) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("build worker: malformed TaskBuildJob: {}", e);
            return;
        }
    };
    let outcome = executor.build(&job).await;
    if let Err(e) = record_task_outcome(
        pg_pool,
        job.workflow_id,
        job.workflow_version,
        job.task_id,
        &outcome,
    )
    .await
    {
        eprintln!(
            "build worker: failed to record outcome for task {}: {}",
            job.task_id, e
        );
        return;
    }
    match finalize_after_task_outcome(pg_pool, job.workflow_id, job.workflow_version, &outcome)
        .await
    {
        Ok(FinalizerOutcome::FlippedToReady) => {
            // Publish the submission pointer onto the submission queue
            // so the submission consumer ships the SubmitWorkflow
            // envelope cross-plane. A publish failure here is bounded
            // by the boot-time reconciliation scan — the workflow row
            // stays at Ready and gets republished on next conductor
            // start.
            let pointer = SubmissionMessage {
                workflow_id: job.workflow_id,
                workflow_version: job.workflow_version,
            };
            if let Err(e) = publish_submission(nats, &pointer).await {
                eprintln!(
                    "build worker: submission queue publish failed for ({}, {}): {} (boot reconciliation will retry)",
                    job.workflow_id, job.workflow_version, e
                );
            }
            // Refresh the in-process waits-on-signal index so a
            // subsequent wakeup can dispatch without waiting for a
            // conductor restart's reconciliation scan.
            if let Err(e) =
                refresh_subscription_index(pg_pool, job.workflow_id, job.workflow_version).await
            {
                eprintln!(
                    "build worker: waits-on-signal refresh failed for {}: {}",
                    job.workflow_id, e
                );
            }
        }
        Ok(FinalizerOutcome::FlippedToBuildFailed) => {
            // FlippedToBuildFailed only comes from a Failure outcome, which
            // carries the captured `nix build` stderr. Surface it so an
            // author sees why the build failed without re-running nix.
            let diagnostic = match &outcome {
                crate::build_pipeline::executor::BuildOutcome::Failure { error } => error.as_str(),
                _ => "",
            };
            eprintln!(
                "build worker: workflow {} v{} task {} flipped to BuildFailed; nix build stderr:\n{}",
                job.workflow_id, job.workflow_version, job.task_id, diagnostic
            );
        }
        Ok(FinalizerOutcome::AlreadyTerminalOrNotReady) => {
            // Another worker handled the transition or other tasks are
            // still pending. No-op.
        }
        Err(e) => {
            eprintln!(
                "build worker: finalizer pass failed for {} v{}: {}",
                job.workflow_id, job.workflow_version, e
            );
        }
    }
}

async fn refresh_subscription_index(
    pool: &PgPool,
    workflow_id: uuid::Uuid,
    workflow_version: i64,
) -> Result<()> {
    let (definition,): (serde_json::Value,) =
        sqlx::query_as("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(workflow_id)
            .bind(workflow_version)
            .fetch_one(pool)
            .await?;
    let workflow = crate::definition_store::proto_from_stored_definition(definition)?;
    apply_workflow_state(&workflow)?;
    Ok(())
}
