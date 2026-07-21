//! Build worker entry point.
//!
//! Each worker subscribes to the build queue with NATS queue-group
//! semantics so a job is processed by exactly one worker across the
//! cluster of conductor replicas. For each job, it invokes the executor and
//! asks the selected repository to settle the Task plus aggregate lifecycle
//! atomically. The single winning `Ready` outcome publishes a
//! `SubmissionMessage`; the committed row remains the recovery anchor.
//!
//! The worker terminates cleanly when the supplied `CancellationToken`
//! is cancelled, after a brief grace window for an in-flight finalizer
//! publish.

use crate::build_pipeline::executor::{BuildExecutor, BuildOutcome};
use crate::build_pipeline::job::TaskBuildJob;
use crate::build_pipeline::BUILD_QUEUE_SUBJECT;
use crate::submission_consumer::{publish_submission, SubmissionMessage};
use crate::waits_on_signal_lifecycle::apply_workflow_state;
use anyhow::Result;
use async_nats::Client as NatsClient;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionBuildSettlementOutcome, DefinitionTaskBuildResult,
};
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
    repositories: Arc<WriterRepositoryBundle>,
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
                        process_job(&nats, &repositories, executor.as_ref(), msg).await;
                    }
                })
                .await;
                println!("Build worker drained.");
                break;
            }
            Some(msg) = sub.next() => {
                process_job(&nats, &repositories, executor.as_ref(), msg).await;
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
    repositories: &WriterRepositoryBundle,
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
    let build_result = match &outcome {
        BuildOutcome::Success => DefinitionTaskBuildResult::Success,
        BuildOutcome::Failure { error } => DefinitionTaskBuildResult::Failure {
            error: error.as_str(),
        },
    };
    match repositories
        .settle_definition_task_build(
            job.workflow_id,
            job.workflow_version,
            job.task_id,
            build_result,
        )
        .await
    {
        Ok(DefinitionBuildSettlementOutcome::Ready(intent)) => {
            let pointer = SubmissionMessage {
                workflow_id: intent.workflow_id,
                workflow_version: intent.workflow_version,
            };
            if let Err(e) = publish_submission(nats, &pointer).await {
                eprintln!(
                    "build worker: submission queue publish failed for ({}, {}): {} (boot reconciliation will retry)",
                    intent.workflow_id, intent.workflow_version, e
                );
            }
            if let Err(e) = apply_workflow_state(&intent.definition) {
                eprintln!(
                    "build worker: waits-on-signal refresh failed for {}: {}",
                    intent.workflow_id, e
                );
            }
        }
        Ok(DefinitionBuildSettlementOutcome::BuildFailed) => {
            let diagnostic = match &outcome {
                BuildOutcome::Failure { error } => error.as_str(),
                BuildOutcome::Success => "",
            };
            eprintln!(
                "build worker: workflow {} v{} task {} flipped to BuildFailed; nix build stderr:\n{}",
                job.workflow_id, job.workflow_version, job.task_id, diagnostic
            );
        }
        Ok(
            DefinitionBuildSettlementOutcome::AwaitingTasks
            | DefinitionBuildSettlementOutcome::AlreadySettled(_)
            | DefinitionBuildSettlementOutcome::TaskAlreadySettled,
        ) => {}
        Ok(DefinitionBuildSettlementOutcome::Absent) => {
            eprintln!(
                "build worker: definition or task absent for {} v{} task {}",
                job.workflow_id, job.workflow_version, job.task_id
            );
        }
        Err(e) => {
            eprintln!(
                "build worker: settlement failed for {} v{}: {}",
                job.workflow_id, job.workflow_version, e
            );
        }
    }
}
