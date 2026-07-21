//! Submission consumer: NATS queue-group subscriber that ships a
//! freshly-built workflow over the relay and flips the workflow row
//! `Ready -> Submitted`.

use crate::relay::forward_workflow_registration_bytes;
use crate::submission_consumer::message::SubmissionMessage;
use crate::submission_consumer::{SUBMISSION_QUEUE_GROUP, SUBMISSION_QUEUE_SUBJECT};
use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::{
    DefinitionSubmissionCandidate, DefinitionSubmissionSettlementOutcome,
};
use tokio_util::sync::CancellationToken;

/// Grace window the submission consumer gives an in-flight relay send
/// to complete after the cancellation token fires. Placeholder for the
/// final tuning pass.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);

/// Publish a submission pointer onto the JetStream durable subject.
/// Used by both the build pipeline's finalizer (post-commit publish)
/// and the boot-time reconciliation scan.
pub async fn publish_submission(nats: &NatsClient, msg: &SubmissionMessage) -> Result<()> {
    let payload = bincode::serialize(msg).context("serialize submission message")?;
    nats.publish(SUBMISSION_QUEUE_SUBJECT, payload.into())
        .await
        .context("publish to submission queue")?;
    Ok(())
}

/// Start a submission-consumer task bound to the selected definition
/// repository and NATS client. The returned future resolves when the
/// cancellation token fires (after the in-flight grace window) or the
/// subscription stream ends.
pub async fn start_submission_consumer(
    nats: NatsClient,
    repositories: Arc<WriterRepositoryBundle>,
    cancel: CancellationToken,
) -> Result<()> {
    println!(
        "Starting conductor submission consumer on {}",
        SUBMISSION_QUEUE_SUBJECT
    );
    let mut sub = nats
        .queue_subscribe(SUBMISSION_QUEUE_SUBJECT, SUBMISSION_QUEUE_GROUP.into())
        .await
        .context("subscribe to submission queue")?;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("Submission consumer received shutdown signal; draining in-flight sends.");
                // Brief drain window: continue processing pulled-but-not-shipped
                // messages, then exit. Anything still in flight after the window
                // will be redelivered post-restart by JetStream's queue-group
                // semantics.
                let _ = tokio::time::timeout(SHUTDOWN_DRAIN, async {
                    while let Some(msg) = sub.next().await {
                        handle_message(&repositories, msg).await;
                    }
                })
                .await;
                println!("Submission consumer drained.");
                break;
            }
            Some(msg) = sub.next() => {
                handle_message(&repositories, msg).await;
            }
            else => {
                println!("Submission queue stream ended.");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_message(repositories: &WriterRepositoryBundle, msg: async_nats::Message) {
    let parsed: SubmissionMessage = match bincode::deserialize(&msg.payload) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("submission consumer: malformed SubmissionMessage: {}", e);
            return;
        }
    };
    if let Err(e) = process(repositories, &parsed).await {
        eprintln!(
            "submission consumer: failed to process ({}, {}): {}",
            parsed.workflow_id, parsed.workflow_version, e
        );
    }
}

async fn process(repositories: &WriterRepositoryBundle, msg: &SubmissionMessage) -> Result<()> {
    let intent = match repositories
        .definition_submission_candidate(msg.workflow_id, msg.workflow_version)
        .await
        .context("read submission candidate")?
    {
        DefinitionSubmissionCandidate::Ready(intent) => intent,
        DefinitionSubmissionCandidate::NotReady(_) => return Ok(()),
        DefinitionSubmissionCandidate::Absent => {
            eprintln!(
                "submission consumer: workflow ({}, {}) absent at submission time; ACKing",
                msg.workflow_id, msg.workflow_version
            );
            return Ok(());
        }
    };

    let payload = prost::Message::encode_to_vec(&intent.definition);
    forward_workflow_registration_bytes(payload)
        .await
        .context("relay send")?;

    match repositories
        .settle_definition_submission(intent.workflow_id, intent.workflow_version)
        .await
        .context("settle workflow submission")?
    {
        DefinitionSubmissionSettlementOutcome::Submitted => {}
        DefinitionSubmissionSettlementOutcome::AlreadySettled(_) => {
            eprintln!(
                "submission consumer: ({}, {}) raced another consumer to Submitted; ACKing",
                intent.workflow_id, intent.workflow_version
            );
        }
        DefinitionSubmissionSettlementOutcome::Absent => {
            eprintln!(
                "submission consumer: ({}, {}) disappeared after relay; ACKing",
                intent.workflow_id, intent.workflow_version
            );
        }
    }
    Ok(())
}
