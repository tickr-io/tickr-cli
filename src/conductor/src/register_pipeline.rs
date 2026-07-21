//! Shared workflow-registration pipeline.
//!
//! Registration has two transports today: the HTTP `POST
//! /api/workflows/register` route and the API component's command bus. They
//! differ only at the transport edge (request body shape, response
//! projection); the work in between is identical — Nickel parse with a 30s
//! timeout, version-novelty check, the single repository transaction that
//! inserts the `workflows` row at `Building` plus one `workflow_task_builds`
//! row per task, and the publish-after-commit of one `TaskBuildJob` per task
//! onto the build queue.
//!
//! This module is that shared middle layer, mirroring `trigger_pipeline`.
//! Callers build a [`RegisterRequest`], invoke [`process_register`], and adapt
//! the resulting [`RegisterOutcome`] / [`RegisterError`] to their response
//! shape.

use async_nats::Client as NatsClient;
use std::time::Duration;
use tickr_migrations::backend::{RepositoryError, WriterRepositoryBundle};
use tickr_migrations::definition_repository::{
    DefinitionBuildTask, DefinitionRegistrationInput, DefinitionRegistrationOutcome,
};
use tickr_proto::TenantId;
use tokio::time::timeout;
use uuid::Uuid;

use crate::parser::Parser;

/// Nickel evaluation budget. A submitted source that doesn't evaluate within
/// this window surfaces as [`RegisterError::Timeout`] (HTTP 408).
const NICKEL_EVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Producer intent the transport-specific caller assembles.
pub struct RegisterRequest {
    pub nickel_source: String,
    /// Grouping segment supplied at registration (not in the source). Empty
    /// normalises to `default` before identity derivation. Qualifies the
    /// author's slug so `namespace.slug` is the workflow's identity.
    pub namespace: String,
}

/// Outcome of [`process_register`]. Both arms carry the fully-rendered
/// `message` string so the HTTP wrapper and the command-bus arm project
/// byte-identical bodies without re-deriving it.
///
/// The version is system-assigned: the conductor derives the workflow's content
/// hash and compares it to the latest stored version. Re-submitting identical
/// content is a clean [`NoOp`](RegisterOutcome::NoOp), not a conflict — so a CD
/// pipeline can blindly re-submit on every merge.
pub enum RegisterOutcome {
    /// A new version row + per-task rows committed and the `TaskBuildJob`s were
    /// published. `workflow_version` is the system-assigned integer. Maps to
    /// HTTP 202.
    Inserted {
        workflow_id: Uuid,
        workflow_version: i64,
        task_count: usize,
        message: String,
    },
    /// Content matched the latest successfully-built version but a cosmetic
    /// field changed; the latest row's cosmetic columns + archived source were
    /// updated in place. No version bump. Maps to HTTP 200.
    Refreshed {
        workflow_id: Uuid,
        workflow_version: i64,
        message: String,
    },
    /// Content matched the latest version whose build had failed; that version's
    /// failed task builds were re-enqueued on the same row. No version bump.
    /// Maps to HTTP 202.
    BuildRequeued {
        workflow_id: Uuid,
        workflow_version: i64,
        task_count: usize,
        message: String,
    },
    /// The incoming content matched the latest stored version's hash (and a
    /// build is settled/in-flight); no storage mutation. Maps to HTTP 200 — a
    /// success, not a conflict.
    NoOp {
        workflow_id: Uuid,
        workflow_version: i64,
        message: String,
    },
}

/// Failure modes the pipeline distinguishes for the caller. The `Display`
/// strings are the exact HTTP messages today's handler returns, so both
/// callers reproduce them by rendering `err.to_string()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("Failed to parse workflow: {0}")]
    Parse(String),
    #[error("Timeout while evaluating Nickel source")]
    Timeout,
    #[error("Workflow parsed successfully, but failed to persist: {0}")]
    Persist(#[source] RepositoryError),
}

/// Run the shared registration pipeline. On [`RegisterOutcome::Inserted`] the
/// repository transaction has committed and the per-task `TaskBuildJob`s have
/// been published onto the build queue (best-effort, after commit).
pub async fn process_register(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    req: RegisterRequest,
) -> Result<RegisterOutcome, RegisterError> {
    let tenant = TenantId::from_env();
    let workflow = match timeout(
        NICKEL_EVAL_TIMEOUT,
        Parser::parse_workflow(&req.nickel_source, tenant, &req.namespace),
    )
    .await
    {
        Ok(Ok(workflow)) => workflow,
        Ok(Err(error)) => return Err(RegisterError::Parse(error.to_string())),
        Err(_) => return Err(RegisterError::Timeout),
    };

    let name = workflow.name.clone();
    let incoming_hash = crate::content_hash::content_hash(&workflow);
    let incoming_cosmetic = crate::content_hash::cosmetic_hash(&workflow);
    let outcome = repositories
        .register_definition(DefinitionRegistrationInput {
            definition: workflow,
            content_hash: incoming_hash,
            cosmetic_hash: incoming_cosmetic,
            nickel_source: req.nickel_source,
        })
        .await
        .map_err(RegisterError::Persist)?;

    match outcome {
        DefinitionRegistrationOutcome::Inserted {
            workflow_id,
            workflow_version,
            tasks,
        } => {
            let task_count = tasks.len();
            publish_build_tasks(nats, tasks).await;
            Ok(RegisterOutcome::Inserted {
                workflow_id,
                workflow_version,
                task_count,
                message: format!(
                    "Workflow '{}' v{} accepted; {} per-task builds queued",
                    name, workflow_version, task_count
                ),
            })
        }
        DefinitionRegistrationOutcome::Refreshed {
            workflow_id,
            workflow_version,
        } => Ok(RegisterOutcome::Refreshed {
            workflow_id,
            workflow_version,
            message: format!(
                "Workflow '{}' v{} refreshed; display fields updated in place (no version bump)",
                name, workflow_version
            ),
        }),
        DefinitionRegistrationOutcome::BuildRequeued {
            workflow_id,
            workflow_version,
            tasks,
        } => {
            let task_count = tasks.len();
            publish_build_tasks(nats, tasks).await;
            Ok(RegisterOutcome::BuildRequeued {
                workflow_id,
                workflow_version,
                task_count,
                message: format!(
                    "Workflow '{}' v{} build requeued; {} failed task build(s) re-enqueued",
                    name, workflow_version, task_count
                ),
            })
        }
        DefinitionRegistrationOutcome::NoOp {
            workflow_id,
            workflow_version,
        } => Ok(RegisterOutcome::NoOp {
            workflow_id,
            workflow_version,
            message: format!(
                "Workflow '{}' v{} unchanged; no-op (content matches the latest version)",
                name, workflow_version
            ),
        }),
    }
}

async fn publish_build_tasks(nats: &NatsClient, tasks: Vec<DefinitionBuildTask>) {
    for task in tasks {
        let job = crate::build_pipeline::TaskBuildJob {
            workflow_id: task.workflow_id,
            workflow_version: task.workflow_version,
            task_id: task.task_id,
            nix_expression_path: task.nix_expression_path,
        };
        match bincode::serialize(&job) {
            Ok(payload) => {
                if let Err(error) = nats
                    .publish(crate::build_pipeline::BUILD_QUEUE_SUBJECT, payload.into())
                    .await
                {
                    eprintln!(
                        "Failed to publish TaskBuildJob for task {}: {}",
                        job.task_id, error
                    );
                }
            }
            Err(error) => eprintln!("Failed to serialize TaskBuildJob: {}", error),
        }
    }
}
