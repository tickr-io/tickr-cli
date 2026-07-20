//! Conductor-side compaction staging and drain — the stage-then-drain
//! half of compaction.
//!
//! The relay handler **stages** an inbound `CompactionEnvelope` (proto: the
//! archive-grade projection + an opaque correlation) durably in the
//! per-tenant NATS work queue (`tickr.compaction.jobs`) and ACKs the
//! server immediately — the ACK means "durably staged", not "archived",
//! so live-state retirement is never gated on object-storage or Postgres
//! latency. The staged message is the only copy of the payload in the gap
//! between ACK and archive; that is why staging awaits the JetStream
//! publish acknowledgement before the relay ACK is sent.
//!
//! The **compaction drain** (this module's worker) consumes the queue and
//! performs the archival per job: upload every task's logs from the Log
//! staging stream (every outcome — failed attempts included), tickr-ctx
//! scope read + the three-table archive transaction + signal-captures
//! cleanup, then purge the log subjects. Every conductor instance runs a
//! drain against the same durable consumer, so any instance can drain any
//! staged job and throughput scales with the stateless tier. The drain
//! ACKs the work-queue message only after the whole job completes;
//! at-least-once redelivery of a half-finished job converges: the archive
//! transaction upserts, the blob overwrite is same-key, and the subject
//! purge is idempotent (an already-purged subject skips the upload rather
//! than overwriting the blob with emptiness).

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::consumer::{pull, PullConsumer};
use async_nats::jetstream::{self, stream};
use async_nats::Client as NatsClient;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use opendal::Operator;
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tickr_proto::archive as ap;
use tickr_proto::codec::compaction::decode_envelope;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::system_tasks::compaction_receiver::{
    audit_patch_settlement, persist_compaction_projection,
};
use crate::system_tasks::log_uploader::{purge_task_log_subject, upload_task_logs};

/// The archive-grade content of one staged compaction job, decoded from the
/// proto envelope. The `shipped_at` enrichment rides the wire wrapper, not the
/// projection.
struct DecodedJob {
    projection: ap::ArchiveProjection,
    shipped_at: Option<DateTime<Utc>>,
}

/// Decode a staged job from the proto envelope. Returns `None` when the bytes
/// are not a compaction envelope — a genuine poison job. The drain accepts
/// exactly the current staged encoding.
fn decode_job(bytes: &[u8]) -> Option<DecodedJob> {
    let envelope = decode_envelope(bytes).ok()?;
    let shipped_at = envelope
        .shipped_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc));
    // `decode_envelope` guarantees a projection.
    envelope.projection.map(|projection| DecodedJob {
        projection,
        shipped_at,
    })
}

/// Subject the relay handler stages compaction payloads onto. Dotted
/// hierarchy matches the conductor's other tenant-local subjects
/// (`tickr.external.signals`, `tickr.api.commands`).
pub const SUBJECT: &str = "tickr.compaction.jobs";

/// JetStream stream backing the subject. WorkQueue retention: an acked
/// (drained) job auto-deletes; an unacked job survives conductor death
/// and redelivers. NATS stream names cannot contain dots, so the stream
/// uses underscores while the subject keeps the dotted form.
pub const STREAM_NAME: &str = "tickr_compaction_jobs";

/// Durable pull-consumer name shared by every conductor instance — NATS
/// load-balances staged jobs across whichever instances are pulling.
pub const CONSUMER_NAME: &str = "tickr-conductor-compaction-drain";

/// Create or fetch the work-queue stream. Idempotent — an existing stream
/// is returned without reconciliation, matching the conductor's
/// create-if-absent posture for its other JetStream surfaces.
async fn ensure_stream(js: &jetstream::Context) -> Result<stream::Stream> {
    let stream_cfg = stream::Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![SUBJECT.to_string()],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    };
    js.get_or_create_stream(stream_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create stream {}: {}", STREAM_NAME, e))
}

/// Durably stage a raw compaction payload (the proto envelope bytes, exactly
/// as received off the relay) into the work queue. Returns only after
/// JetStream acknowledges the publish — this is the durability boundary
/// the `CompactionAck` reply to the server rests on, so a `Ok(())` here
/// is the precondition for sending that ACK.
pub async fn stage_compaction_payload(nats: &NatsClient, payload_bytes: Vec<u8>) -> Result<()> {
    let js = jetstream::new(nats.clone());
    ensure_stream(&js).await?;
    js.publish(SUBJECT, payload_bytes.into())
        .await
        .context("publishing compaction job to work queue")?
        .await
        .context("awaiting JetStream publish ack for compaction job")?;
    Ok(())
}

/// Create stream + durable pull consumer if absent. Idempotent.
pub async fn init_stream_and_consumer(nats: &NatsClient) -> Result<PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = ensure_stream(&js).await?;

    let consumer_cfg = pull::Config {
        durable_name: Some(CONSUMER_NAME.to_string()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ..Default::default()
    };
    stream
        .get_or_create_consumer(CONSUMER_NAME, consumer_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create consumer {}: {}", CONSUMER_NAME, e))
}

/// Archive one staged job end to end: log upload for every task instance,
/// the three-table archive transaction (with tickr-ctx scope read and
/// signal-captures cleanup), then log-subject purge. Each step is
/// idempotent, so a re-run after a mid-job crash converges.
async fn drain_one(
    pg_pool: &PgPool,
    nats: &NatsClient,
    log_storage: &Operator,
    job: &DecodedJob,
) -> Result<()> {
    let projection = &job.projection;
    // The workflow/instance ids the archived task-instance rows nest under —
    // parsed once from the projection's own identity.
    let workflow_instance_id = Uuid::parse_str(&projection.id).with_context(|| {
        format!(
            "archive projection carried an unparseable id `{}`",
            projection.id
        )
    })?;
    let workflow_id = Uuid::parse_str(&projection.workflow_id).with_context(|| {
        format!(
            "archive projection carried an unparseable workflow_id `{}`",
            projection.workflow_id
        )
    })?;
    let task_instance_ids: Vec<Uuid> = projection
        .task_instances
        .iter()
        .map(|ti| {
            Uuid::parse_str(&ti.id).with_context(|| {
                format!(
                    "archived task-instance carried an unparseable id `{}`",
                    ti.id
                )
            })
        })
        .collect::<Result<_>>()?;

    // Upload before the archive transaction so the log-URI rows the
    // enrichment derives always point at a blob that exists (or at a task
    // that never logged). Every outcome uploads — failed attempts included.
    for ti_id in &task_instance_ids {
        upload_task_logs(
            nats,
            log_storage,
            &workflow_id,
            &workflow_instance_id,
            ti_id,
        )
        .await
        .with_context(|| format!("log upload for task instance {}", ti_id))?;
    }

    persist_compaction_projection(pg_pool, projection, job.shipped_at, Some(nats)).await?;

    // Terminal-time patch settlement audit: reconcile the durable patch ledger
    // against this terminal instance's applied-patch log and record a loud
    // discrepancy for anything that never settled. Runs alongside the archive
    // write; a transient failure NAKs the whole job and redelivery re-runs the
    // idempotent audit. This never mutates a patch — it is the belt-and-
    // suspenders net for the in-flight re-drive loop in `patch_pipeline`.
    let applied_patch_keys: HashSet<Uuid> = projection
        .applied_patches
        .iter()
        .filter_map(|p| Uuid::parse_str(&p.patch_key).ok())
        .collect();
    let discrepancies = audit_patch_settlement(pg_pool, workflow_instance_id, &applied_patch_keys)
        .await
        .with_context(|| format!("patch settlement audit for {}", workflow_instance_id))?;
    if discrepancies > 0 {
        eprintln!(
            "compaction_drain: patch settlement audit for {} recorded {} discrepancy record(s)",
            workflow_instance_id, discrepancies
        );
    }

    // Purge only after the archive commit: a job that dies before this
    // point redelivers with its batches intact, so the re-run's upload
    // overwrites the same key instead of skipping.
    for ti_id in &task_instance_ids {
        purge_task_log_subject(nats, &workflow_id, &workflow_instance_id, ti_id)
            .await
            .with_context(|| format!("log purge for task instance {}", ti_id))?;
    }

    Ok(())
}

/// Run the compaction drain worker until shutdown. Per job: `drain_one`,
/// then ack. A failed job is NAK'd so the queue redelivers it (to this or
/// any other conductor instance); a job whose payload no longer
/// deserializes is dropped with a loud error — staged bytes were
/// deserialized once already on the relay path, so a poison job here means
/// corruption, and NAK-forever would only redeliver it eternally.
pub async fn run_compaction_drain(
    nats: NatsClient,
    pg_pool: Arc<PgPool>,
    log_storage: Operator,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let consumer = init_stream_and_consumer(&nats).await?;
    println!(
        "compaction_drain: worker started, subject={}, stream={}, consumer={}",
        SUBJECT, STREAM_NAME, CONSUMER_NAME
    );

    let mut messages = consumer
        .stream()
        .max_messages_per_batch(4)
        .messages()
        .await
        .context("opening compaction drain message stream")?;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                println!("compaction_drain: shutdown signal received");
                break;
            }
            next = messages.next() => {
                match next {
                    Some(Ok(msg)) => {
                        // Decode the proto envelope; a job that does not decode
                        // is a poison drop.
                        let job = match decode_job(&msg.payload) {
                            Some(j) => j,
                            None => {
                                eprintln!(
                                    "compaction_drain: dropping undeserializable job ({} bytes)",
                                    msg.payload.len(),
                                );
                                if let Err(e) = msg.ack().await {
                                    eprintln!("compaction_drain: ack of poison job failed: {}", e);
                                }
                                continue;
                            }
                        };
                        let wfi_id = job.projection.id.clone();
                        match drain_one(&pg_pool, &nats, &log_storage, &job).await {
                            Ok(()) => {
                                if let Err(e) = msg.ack().await {
                                    // The archive write committed but the queue keeps the
                                    // job; redelivery re-runs the idempotent archival and
                                    // converges.
                                    eprintln!(
                                        "compaction_drain: ack failed for {}: {} (redelivery converges)",
                                        wfi_id, e
                                    );
                                } else {
                                    println!(
                                        "compaction_drain: archived terminal workflow {} (state={})",
                                        wfi_id, job.projection.state
                                    );
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "compaction_drain: archival failed for {}: {} (NAK; queue redelivers)",
                                    wfi_id, e
                                );
                                if let Err(e) = msg
                                    .ack_with(jetstream::AckKind::Nak(None))
                                    .await
                                {
                                    eprintln!("compaction_drain: NAK failed: {}", e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("compaction_drain: pull error: {}", e);
                        // Brief sleep so a persistent NATS-side fault doesn't tight-loop.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    None => {
                        // Stream ended cleanly — happens only when the NATS
                        // connection drops; the consumer's reconnect machinery
                        // handles re-establishment on the next worker start.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
