//! Submission consumer: NATS queue-group subscriber that ships a
//! freshly-built workflow over the relay and flips the workflow row
//! `Ready -> Submitted`.

use crate::build_pipeline::load_workflow_definition;
use crate::relay::forward_workflow_registration_bytes;
use crate::submission_consumer::message::SubmissionMessage;
use crate::submission_consumer::{SUBMISSION_QUEUE_GROUP, SUBMISSION_QUEUE_SUBJECT};
use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use futures::StreamExt;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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

/// Start a submission-consumer task bound to the supplied PG pool +
/// NATS client. The returned future resolves when the cancellation
/// token fires (after the in-flight grace window) or the subscription
/// stream ends.
pub async fn start_submission_consumer(
    nats: NatsClient,
    pg_pool: Arc<PgPool>,
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
                        handle_message(&pg_pool, msg).await;
                    }
                })
                .await;
                println!("Submission consumer drained.");
                break;
            }
            Some(msg) = sub.next() => {
                handle_message(&pg_pool, msg).await;
            }
            else => {
                println!("Submission queue stream ended.");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_message(pg_pool: &PgPool, msg: async_nats::Message) {
    let parsed: SubmissionMessage = match bincode::deserialize(&msg.payload) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("submission consumer: malformed SubmissionMessage: {}", e);
            return;
        }
    };
    if let Err(e) = process(pg_pool, &parsed).await {
        eprintln!(
            "submission consumer: failed to process ({}, {}): {}",
            parsed.workflow_id, parsed.workflow_version, e
        );
    }
}

async fn process(pg_pool: &PgPool, msg: &SubmissionMessage) -> Result<()> {
    // Idempotency anchor: read the workflow row's lifecycle status. We
    // only ship when the row is at `Ready`. Anything else (already
    // `Submitted`, still `Building`, terminal `BuildFailed`) is a no-op
    // ACK — covers JetStream redelivery, boot-time reconciliation
    // duplicates, and out-of-order cases.
    let row: Option<(String,)> =
        sqlx::query_as("SELECT status FROM workflows WHERE id = $1 AND version = $2")
            .bind(msg.workflow_id)
            .bind(msg.workflow_version)
            .fetch_optional(pg_pool)
            .await
            .context("read workflow status")?;

    let Some((status,)) = row else {
        eprintln!(
            "submission consumer: workflow ({}, {}) absent at submission time; ACKing",
            msg.workflow_id, msg.workflow_version
        );
        return Ok(());
    };
    if status != "Ready" {
        // Not eligible — already shipped or never reached Ready.
        return Ok(());
    }

    // Load the full definition and ship it over the relay as a
    // SubmitWorkflow envelope. The relay client encapsulates the
    // entity-type tag.
    let definition = load_workflow_definition(pg_pool, msg.workflow_id, msg.workflow_version)
        .await
        .context("load workflow definition")?;
    // Registration persists the protobuf definition, which is relayed without
    // introducing another representation.
    let payload = prost::Message::encode_to_vec(&definition);
    forward_workflow_registration_bytes(payload)
        .await
        .context("relay send")?;

    // Transition Ready -> Submitted. The conditional UPDATE protects
    // against a race where two consumers observed Ready simultaneously
    // — the loser sees zero rows affected and does nothing (the winner
    // already shipped; the duplicate Submitted on the server side is
    // absorbed by the manager's idempotent admission).
    let flipped = flip_ready_to_submitted(pg_pool, msg.workflow_id, msg.workflow_version).await?;
    if !flipped {
        eprintln!(
            "submission consumer: ({}, {}) raced another consumer to Submitted; ACKing",
            msg.workflow_id, msg.workflow_version
        );
    }
    Ok(())
}

async fn flip_ready_to_submitted(
    pool: &PgPool,
    workflow_id: Uuid,
    workflow_version: i64,
) -> Result<bool> {
    let res = sqlx::query(
        r#"
        UPDATE workflows
           SET status = 'Submitted', updated_at = now()
         WHERE id = $1 AND version = $2 AND status = 'Ready'
        "#,
    )
    .bind(workflow_id)
    .bind(workflow_version)
    .execute(pool)
    .await
    .context("flip workflow Ready -> Submitted")?;
    Ok(res.rows_affected() == 1)
}
