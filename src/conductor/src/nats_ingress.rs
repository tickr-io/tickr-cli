//! NATS-side external-signals ingress translator.
//!
//! External systems (webhook gateways, message-bus bridges, scheduled
//! forwarders) publish v=1 JSON signal envelopes onto the per-tenant NATS
//! JetStream subject `tickr.external.signals`. The conductor runs a
//! durable pull consumer that decodes each envelope, mints `signal_id`,
//! applies the same idempotency cache the HTTP-trigger path uses, and
//! forwards a wire `Signal` over the existing relay outbound channel.
//!
//! Slice scope (this file's first revision): the Trigger variant is wired
//! end-to-end through the same captures-extraction + Postgres + NATS-KV
//! pipeline the HTTP path already runs. Cancel and Wakeup variants parse
//! successfully (so the envelope contract is stable for publishers) but
//! the translator logs and acks them without forwarding — their consumer
//! wiring lands in subsequent slices.
//!
//! Tenant boundary is implicit via the NATS cluster — each tenant runs
//! its own NATS, so the subject `tickr.external.signals` is single-tenant
//! by infrastructure invariant; no tenant identifier is encoded on the
//! envelope or in the subject name.

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::consumer::{pull, PullConsumer};
use async_nats::jetstream::{self, stream};
use async_nats::Client as NatsClient;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tickr_ctx::envelope::SignalSource;
use tickr_proto::signal as sp;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::canonical_json;
use crate::idempotency;

/// Boundary for relay-forward dispatch. Production wires `GlobalRelaySender`
/// against the conductor's existing relay outbound channel; integration
/// tests inject a capturing sender so concurrent test runs don't share the
/// process-global relay state.
///
/// `try_send` is the saturation-aware path: returns `Saturated` when the
/// outbound buffer is full so the translator can NAK rather than ack the
/// NATS message, letting NATS hold it for redelivery once the relay drains.
#[async_trait]
pub trait RelaySender: Send + Sync + 'static {
    async fn try_send(&self, signal: &sp::Signal) -> RelaySendOutcome;
}

/// External-signals Cancel `target` decode shape. It preserves the accepted JSON
/// shape and idempotency-hash bytes before mapping onto the protobuf `Target`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum ExtTarget {
    Instance {
        workflow_instance_id: Uuid,
        #[serde(default)]
        node_id: Option<Uuid>,
    },
    ByTag {
        filter: std::collections::HashMap<String, String>,
    },
}

impl ExtTarget {
    fn into_proto(self) -> sp::Target {
        let addressing = match self {
            ExtTarget::Instance {
                workflow_instance_id,
                node_id,
            } => sp::target::Addressing::Instance(sp::target::Instance {
                workflow_instance_id: workflow_instance_id.to_string(),
                node_id: node_id.map(|n| n.to_string()),
            }),
            ExtTarget::ByTag { filter } => {
                sp::target::Addressing::ByTag(sp::target::ByTag { filter })
            }
        };
        sp::Target {
            addressing: Some(addressing),
        }
    }
}

/// External-signals Cancel `reason` decode shape. Externally-tagged JSON like
/// [`ExtTarget`]; mapped onto the proto `CancelReason`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum ExtCancelReason {
    Timeout,
    UserRequested { actor: Option<String> },
    External { source: String },
}

impl ExtCancelReason {
    fn into_proto(self) -> sp::CancelReason {
        let reason = match self {
            ExtCancelReason::Timeout => {
                sp::cancel_reason::Reason::Timeout(sp::cancel_reason::Timeout {})
            }
            ExtCancelReason::UserRequested { actor } => {
                sp::cancel_reason::Reason::UserRequested(sp::cancel_reason::UserRequested { actor })
            }
            ExtCancelReason::External { source } => {
                sp::cancel_reason::Reason::External(sp::cancel_reason::External { source })
            }
        };
        sp::CancelReason {
            reason: Some(reason),
        }
    }
}

/// Outcome of `RelaySender::try_send`. The translator branches on this
/// directly: `Sent` → ack NATS; `Saturated` → NAK so NATS redelivers;
/// `Error` → log and ack (drop), matching the ADR's drop-on-failure
/// philosophy for non-saturation forward failures.
#[derive(Debug)]
pub enum RelaySendOutcome {
    /// Message accepted into the relay outbound queue.
    Sent,
    /// Outbound buffer saturated; NAK to NATS so it redelivers.
    Saturated,
    /// Non-saturation forward failure (relay uninitialized, channel closed).
    /// Log + ack-drop is the action; the message is lost in this slice, the
    /// future DLQ-subject path can capture it.
    Error(anyhow::Error),
}

/// Default production sender — forwards via the conductor's existing
/// global relay outbound channel set up by `relay::run_streaming`.
pub struct GlobalRelaySender;

#[async_trait]
impl RelaySender for GlobalRelaySender {
    async fn try_send(&self, signal: &sp::Signal) -> RelaySendOutcome {
        match crate::relay::try_send_signal(signal).await {
            Ok(crate::relay::TrySendOutcome::Sent) => RelaySendOutcome::Sent,
            Ok(crate::relay::TrySendOutcome::Saturated) => RelaySendOutcome::Saturated,
            Err(e) => RelaySendOutcome::Error(e),
        }
    }
}

/// Subject external publishers write to. Dots delimit a NATS subject hierarchy
/// (`tickr` → `external` → `signals`); future per-variant routing can
/// extend `tickr.external.signals.>` without renaming the v1 subject.
pub const SUBJECT: &str = "tickr.external.signals";

/// JetStream stream backing the subject. NATS stream names cannot contain
/// dots, so the stream uses underscores while the subject keeps the dotted
/// form.
pub const STREAM_NAME: &str = "tickr_external_signals";

/// Durable pull-consumer name. One consumer per conductor — serial
/// processing matches the conductor's other ingress shapes and is trivially
/// upgradable to a consumer group if throughput demands it.
pub const CONSUMER_NAME: &str = "tickr-conductor-external";

/// Counters surfacing reject / drop reasons to operators. Exposed via the
/// public getters so integration tests can assert the rejection paths
/// fired the expected counter rather than only inspecting the absence of
/// observable side effects.
static REJECTED_MISSING_VERSION: AtomicU64 = AtomicU64::new(0);
static REJECTED_UNKNOWN_VERSION: AtomicU64 = AtomicU64::new(0);
static REJECTED_MISSING_VARIANT: AtomicU64 = AtomicU64::new(0);
static REJECTED_UNKNOWN_VARIANT: AtomicU64 = AtomicU64::new(0);
static REJECTED_MISSING_IDEMPOTENCY_KEY: AtomicU64 = AtomicU64::new(0);
static REJECTED_MALFORMED_JSON: AtomicU64 = AtomicU64::new(0);
static REJECTED_TRIGGER_PROCESSING: AtomicU64 = AtomicU64::new(0);
static REJECTED_CANCEL_PROCESSING: AtomicU64 = AtomicU64::new(0);
static SIGNALS_DROPPED_IDEMPOTENCY_COLLISION: AtomicU64 = AtomicU64::new(0);
static SIGNALS_DEDUPLICATED: AtomicU64 = AtomicU64::new(0);
static SIGNALS_RELAY_OUTBOUND_SATURATION: AtomicU64 = AtomicU64::new(0);

pub fn rejected_missing_version() -> u64 {
    REJECTED_MISSING_VERSION.load(Ordering::Relaxed)
}
pub fn rejected_unknown_version() -> u64 {
    REJECTED_UNKNOWN_VERSION.load(Ordering::Relaxed)
}
pub fn rejected_missing_variant() -> u64 {
    REJECTED_MISSING_VARIANT.load(Ordering::Relaxed)
}
pub fn rejected_unknown_variant() -> u64 {
    REJECTED_UNKNOWN_VARIANT.load(Ordering::Relaxed)
}
pub fn rejected_missing_idempotency_key() -> u64 {
    REJECTED_MISSING_IDEMPOTENCY_KEY.load(Ordering::Relaxed)
}
pub fn rejected_malformed_json() -> u64 {
    REJECTED_MALFORMED_JSON.load(Ordering::Relaxed)
}
pub fn rejected_trigger_processing() -> u64 {
    REJECTED_TRIGGER_PROCESSING.load(Ordering::Relaxed)
}
pub fn rejected_cancel_processing() -> u64 {
    REJECTED_CANCEL_PROCESSING.load(Ordering::Relaxed)
}
pub fn signals_dropped_idempotency_collision() -> u64 {
    SIGNALS_DROPPED_IDEMPOTENCY_COLLISION.load(Ordering::Relaxed)
}
pub fn signals_deduplicated() -> u64 {
    SIGNALS_DEDUPLICATED.load(Ordering::Relaxed)
}
pub fn signals_relay_outbound_saturation() -> u64 {
    SIGNALS_RELAY_OUTBOUND_SATURATION.load(Ordering::Relaxed)
}

/// Failure modes of envelope decoding. Each variant maps onto a distinct
/// drop reason and a distinct counter so operators can attribute publisher
/// misbehavior without log diving.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("malformed JSON: {0}")]
    MalformedJson(String),
    #[error("envelope missing required field `version`")]
    MissingVersion,
    #[error("unknown envelope version `{0}` (only `1` accepted today)")]
    UnknownVersion(u64),
    #[error("envelope missing required field `variant`")]
    MissingVariant,
    #[error("unknown variant `{0}` (expected one of Cancel | Trigger | Wakeup)")]
    UnknownVariant(String),
    #[error("envelope missing required field `idempotency_key`")]
    MissingIdempotencyKey,
    #[error("variant `{variant}` failed to deserialize: {message}")]
    VariantDeserialize { variant: String, message: String },
}

/// Decoded envelope. The variant-specific fields are exactly what the
/// downstream translator needs to construct a wire `Signal`. The conductor
/// extracts the required metadata and emits a compact signal envelope.
#[derive(Debug, Clone)]
pub enum Envelope {
    Trigger {
        idempotency_key: String,
        workflow_id: Uuid,
        scheduled_at: Option<DateTime<Utc>>,
        inputs: Option<Value>,
    },
    Cancel {
        idempotency_key: String,
        // Reserved-shape fields; full Cancel routing lands with the next slice.
        // Held as raw JSON Value so v2 envelope refinements can drift the
        // typed shape without breaking the parser surface.
        target: Value,
        reason: Value,
        note: Option<String>,
    },
    Wakeup {
        idempotency_key: String,
        // Reserved-shape fields; full Wakeup downstream routing lands with
        // a later slice.
        name: String,
        target: Value,
        payload: Option<Value>,
    },
}

/// Decode bytes as a v=1 envelope. Errors are classified so callers can
/// increment the matching counter. Done in two passes: first a generic
/// JSON parse to distinguish malformed-JSON from missing-fields; then a
/// version + variant check; finally a typed deserialization of the
/// variant-specific fields.
pub fn parse_envelope(bytes: &[u8]) -> Result<Envelope, EnvelopeError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| EnvelopeError::MalformedJson(e.to_string()))?;

    let obj = value.as_object().ok_or_else(|| {
        EnvelopeError::MalformedJson("envelope must be a JSON object".to_string())
    })?;

    let version = match obj.get("version") {
        None => return Err(EnvelopeError::MissingVersion),
        Some(v) => v.as_u64().ok_or_else(|| {
            EnvelopeError::MalformedJson("`version` must be an unsigned integer".to_string())
        })?,
    };
    if version != 1 {
        return Err(EnvelopeError::UnknownVersion(version));
    }

    let idempotency_key = obj
        .get("idempotency_key")
        .ok_or(EnvelopeError::MissingIdempotencyKey)?
        .as_str()
        .ok_or_else(|| {
            EnvelopeError::MalformedJson("`idempotency_key` must be a string".to_string())
        })?
        .to_string();

    let variant = obj
        .get("variant")
        .ok_or(EnvelopeError::MissingVariant)?
        .as_str()
        .ok_or_else(|| EnvelopeError::MalformedJson("`variant` must be a string".to_string()))?;

    match variant {
        "Trigger" => {
            #[derive(Deserialize)]
            struct TriggerFields {
                workflow_id: Uuid,
                #[serde(default)]
                scheduled_at: Option<DateTime<Utc>>,
                #[serde(default)]
                inputs: Option<Value>,
            }
            let fields: TriggerFields = serde_json::from_value(Value::Object(obj.clone()))
                .map_err(|e| EnvelopeError::VariantDeserialize {
                    variant: "Trigger".to_string(),
                    message: e.to_string(),
                })?;
            Ok(Envelope::Trigger {
                idempotency_key,
                workflow_id: fields.workflow_id,
                scheduled_at: fields.scheduled_at,
                inputs: fields.inputs,
            })
        }
        "Cancel" => {
            #[derive(Deserialize)]
            struct CancelFields {
                target: Value,
                #[serde(default = "default_null")]
                reason: Value,
                #[serde(default)]
                note: Option<String>,
            }
            fn default_null() -> Value {
                Value::Null
            }
            let fields: CancelFields =
                serde_json::from_value(Value::Object(obj.clone())).map_err(|e| {
                    EnvelopeError::VariantDeserialize {
                        variant: "Cancel".to_string(),
                        message: e.to_string(),
                    }
                })?;
            Ok(Envelope::Cancel {
                idempotency_key,
                target: fields.target,
                reason: fields.reason,
                note: fields.note,
            })
        }
        "Wakeup" => {
            #[derive(Deserialize)]
            struct WakeupFields {
                name: String,
                target: Value,
                #[serde(default)]
                payload: Option<Value>,
            }
            let fields: WakeupFields =
                serde_json::from_value(Value::Object(obj.clone())).map_err(|e| {
                    EnvelopeError::VariantDeserialize {
                        variant: "Wakeup".to_string(),
                        message: e.to_string(),
                    }
                })?;
            Ok(Envelope::Wakeup {
                idempotency_key,
                name: fields.name,
                target: fields.target,
                payload: fields.payload,
            })
        }
        other => Err(EnvelopeError::UnknownVariant(other.to_string())),
    }
}

/// Increment the counter that matches the rejection reason.
fn record_envelope_error(err: &EnvelopeError) {
    let counter = match err {
        EnvelopeError::MalformedJson(_) => &REJECTED_MALFORMED_JSON,
        EnvelopeError::MissingVersion => &REJECTED_MISSING_VERSION,
        EnvelopeError::UnknownVersion(_) => &REJECTED_UNKNOWN_VERSION,
        EnvelopeError::MissingVariant => &REJECTED_MISSING_VARIANT,
        EnvelopeError::UnknownVariant(_) => &REJECTED_UNKNOWN_VARIANT,
        EnvelopeError::MissingIdempotencyKey => &REJECTED_MISSING_IDEMPOTENCY_KEY,
        EnvelopeError::VariantDeserialize { .. } => &REJECTED_MALFORMED_JSON,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Create stream + durable pull consumer if absent. Idempotent — existing
/// stream/consumer instances are returned without reconciliation, matching
/// the create-if-absent posture documented for the NATS ingress.
pub async fn init_stream_and_consumer(nats: &NatsClient) -> Result<PullConsumer> {
    let js = jetstream::new(nats.clone());

    // Create or fetch the stream backing `tickr.external.signals`. Default
    // retention is `WorkQueue` so acked messages auto-delete; storage and
    // replicas use NATS defaults until production hardening demands tuning.
    let stream_cfg = stream::Config {
        name: STREAM_NAME.to_string(),
        subjects: vec![SUBJECT.to_string()],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    };
    let stream = js
        .get_or_create_stream(stream_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create stream {}: {}", STREAM_NAME, e))?;

    // Durable pull consumer named after the conductor. Manual ack — the
    // translator-loop acks once it has either successfully translated and
    // forwarded the message or decided to drop it (parser reject, processing
    // failure that's not transient).
    let consumer_cfg = pull::Config {
        durable_name: Some(CONSUMER_NAME.to_string()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ..Default::default()
    };
    let consumer = stream
        .get_or_create_consumer(CONSUMER_NAME, consumer_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create consumer {}: {}", CONSUMER_NAME, e))?;

    Ok(consumer)
}

/// Run the translator-loop with the production `GlobalRelaySender`. Thin
/// wrapper that the conductor's startup code calls; tests use
/// `run_translator_with_sender` to inject a capturing sender.
pub async fn run_translator(
    nats: NatsClient,
    pg_pool: Arc<PgPool>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    run_translator_with_sender(nats, pg_pool, Arc::new(GlobalRelaySender), shutdown_token).await
}

/// Run the translator-loop with an explicit relay sender. Pulls messages
/// from the durable consumer, decodes each, and processes Trigger envelopes
/// end-to-end; Cancel and Wakeup envelopes are parser-reserved and logged
/// + acked without forwarding in this slice.
pub async fn run_translator_with_sender(
    nats: NatsClient,
    pg_pool: Arc<PgPool>,
    relay_sender: Arc<dyn RelaySender>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let consumer = init_stream_and_consumer(&nats).await?;
    println!(
        "nats_ingress: translator started, subject={}, stream={}, consumer={}",
        SUBJECT, STREAM_NAME, CONSUMER_NAME
    );

    let mut messages = consumer
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .context("opening pull consumer message stream")?;

    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                println!("nats_ingress: translator shutdown signal received");
                break;
            }
            next = messages.next() => {
                match next {
                    Some(Ok(msg)) => {
                        let disposition = process_one(
                            &nats,
                            pg_pool.as_ref(),
                            relay_sender.as_ref(),
                            &msg,
                        )
                        .await;
                        match disposition {
                            MessageDisposition::Ack => {
                                if let Err(e) = msg.ack().await {
                                    eprintln!("nats_ingress: ack failed: {}", e);
                                }
                            }
                            MessageDisposition::Nak => {
                                // NAK so NATS holds the message and
                                // redelivers per `ack_wait` / `max_deliver`.
                                // Increment the saturation counter so
                                // operators see sustained backpressure
                                // rather than guessing from drop rates.
                                SIGNALS_RELAY_OUTBOUND_SATURATION
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Err(e) = msg.ack_with(async_nats::jetstream::AckKind::Nak(None)).await {
                                    eprintln!("nats_ingress: NAK failed: {}", e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("nats_ingress: pull error: {}", e);
                        // Brief sleep so a persistent NATS-side fault doesn't tight-loop.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    None => {
                        // Stream ended cleanly — reopen on the next loop iteration.
                        // Practically this happens only when the NATS connection
                        // drops; the consumer's reconnect machinery handles it.
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Disposition the translator-loop applies to a NATS message after
/// processing: `Ack` removes the message from the queue; `Nak` returns it
/// so NATS redelivers when the conditions that caused the NAK clear
/// (typically: relay outbound buffer saturation).
#[derive(Debug)]
enum MessageDisposition {
    Ack,
    Nak,
}

/// Decode + dispatch a single inbound NATS message. Variant-specific routing
/// happens inside; parser errors and processing errors are logged with
/// structured details (subject, payload size, error reason). Returns the
/// disposition the loop should apply to the NATS message.
async fn process_one(
    nats: &NatsClient,
    pg_pool: &PgPool,
    relay_sender: &dyn RelaySender,
    msg: &async_nats::jetstream::Message,
) -> MessageDisposition {
    let envelope = match parse_envelope(&msg.payload) {
        Ok(env) => env,
        Err(err) => {
            record_envelope_error(&err);
            eprintln!(
                "nats_ingress: envelope rejected: subject={}, bytes={}, reason={}",
                msg.subject,
                msg.payload.len(),
                err
            );
            return MessageDisposition::Ack;
        }
    };

    match envelope {
        Envelope::Trigger {
            idempotency_key,
            workflow_id,
            scheduled_at,
            inputs,
        } => {
            match process_trigger(
                nats,
                pg_pool,
                relay_sender,
                idempotency_key,
                workflow_id,
                scheduled_at,
                inputs,
            )
            .await
            {
                Ok(disposition) => disposition,
                Err(e) => {
                    REJECTED_TRIGGER_PROCESSING.fetch_add(1, Ordering::Relaxed);
                    eprintln!("nats_ingress: trigger processing failed: {}", e);
                    MessageDisposition::Ack
                }
            }
        }
        Envelope::Cancel {
            idempotency_key,
            target,
            reason,
            note,
        } => {
            match process_cancel(nats, relay_sender, idempotency_key, target, reason, note).await {
                Ok(disposition) => disposition,
                Err(e) => {
                    REJECTED_CANCEL_PROCESSING.fetch_add(1, Ordering::Relaxed);
                    eprintln!("nats_ingress: cancel processing failed: {}", e);
                    MessageDisposition::Ack
                }
            }
        }
        Envelope::Wakeup {
            idempotency_key,
            name,
            target,
            payload,
        } => match process_wakeup(nats, pg_pool, idempotency_key, name, target, payload).await {
            Ok(disposition) => disposition,
            Err(e) => {
                eprintln!("nats_ingress: wakeup processing failed: {}", e);
                MessageDisposition::Ack
            }
        },
    }
}

/// Process a parsed Trigger envelope. Delegates the workflow-lookup +
/// captures-extraction + Postgres/NATS write + idempotency-cache
/// orchestration to the shared `trigger_pipeline` module; this function is
/// the NATS-transport adapter that translates between envelope shape and
/// pipeline-Outcome, applies the counter increments specific to the NATS
/// path, and handles relay forwarding with NAK-on-saturation semantics.
async fn process_trigger(
    nats: &NatsClient,
    pg_pool: &PgPool,
    relay_sender: &dyn RelaySender,
    idempotency_key: String,
    workflow_id: Uuid,
    scheduled_at: Option<DateTime<Utc>>,
    inputs: Option<Value>,
) -> Result<MessageDisposition> {
    // The NATS path hashes a wider tuple than HTTP: workflow_id is in-band
    // on the envelope (not on a URL), so two envelopes with the same key
    // but different workflow_ids are different logical requests and must
    // surface as a Collision rather than a Dedup.
    let payload_for_hash = serde_json::json!({
        "workflow_id": workflow_id,
        "scheduled_at": scheduled_at,
        "inputs": &inputs.clone().unwrap_or(Value::Object(Default::default())),
    });

    let pipeline_req = crate::trigger_pipeline::TriggerRequest {
        workflow_id,
        scheduled_at,
        inputs,
        idempotency_key: Some(idempotency_key.clone()),
        source: SignalSource::ExternalNats {
            subject: SUBJECT.to_string(),
        },
        hash_payload: payload_for_hash,
        // External NATS-ingress triggers carry no Run name in this slice;
        // those instances show the server default.
        name: None,
    };

    let outcome = match crate::trigger_pipeline::process_trigger(pg_pool, nats, pipeline_req).await
    {
        Ok(o) => o,
        Err(e) => return Err(anyhow!("{}", e)),
    };

    match outcome {
        crate::trigger_pipeline::TriggerOutcome::Fresh { signal, .. } => {
            match relay_sender.try_send(&signal).await {
                RelaySendOutcome::Sent => Ok(MessageDisposition::Ack),
                RelaySendOutcome::Saturated => {
                    eprintln!(
                        "nats_ingress: relay outbound saturated; NAK trigger to let NATS redeliver"
                    );
                    Ok(MessageDisposition::Nak)
                }
                RelaySendOutcome::Error(e) => Err(e.context("relay sender forward")),
            }
        }
        crate::trigger_pipeline::TriggerOutcome::Deduplicated { original_signal_id } => {
            SIGNALS_DEDUPLICATED.fetch_add(1, Ordering::Relaxed);
            println!(
                "nats_ingress: trigger deduplicated: key={}, original_signal_id={}",
                idempotency_key, original_signal_id
            );
            Ok(MessageDisposition::Ack)
        }
        crate::trigger_pipeline::TriggerOutcome::Conflict {
            original_signal_id,
            original_hash,
            your_hash,
        } => {
            SIGNALS_DROPPED_IDEMPOTENCY_COLLISION.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "nats_ingress: trigger idempotency collision: key={}, original_signal_id={}, original_hash={}, your_hash={}",
                idempotency_key, original_signal_id, original_hash, your_hash,
            );
            Ok(MessageDisposition::Ack)
        }
    }
}

/// Process a parsed Cancel envelope. Shares the idempotency-cache check
/// with the Trigger path: Fresh forwards, Deduplicated short-circuits
/// silently, Collision drops with the matching counter. Cancel has no
/// captures pipeline and no PostgreSQL archive write, so relay forwarding is
/// its only side effect.
async fn process_cancel(
    nats: &NatsClient,
    relay_sender: &dyn RelaySender,
    idempotency_key: String,
    target_value: Value,
    reason_value: Value,
    note: Option<String>,
) -> Result<MessageDisposition> {
    // Decode the raw `target` and `reason` Value into the typed wire
    // shapes. Errors here mean the publisher sent a malformed Cancel —
    // surface as a processing error so the matching counter increments
    // and the message is dropped.
    let target: ExtTarget =
        serde_json::from_value(target_value).context("decode Cancel.target as Signal::Target")?;
    let reason: ExtCancelReason = serde_json::from_value(reason_value)
        .context("decode Cancel.reason as Signal::CancelReason")?;

    // Compute payload hash over the canonical-JSON of (target, reason,
    // note) so the idempotency cache distinguishes "same key, byte-
    // identical cancel" (dedupe) from "same key, different cancel intent"
    // (collision). The HTTP path doesn't currently expose a Cancel route
    // so we don't share the canonical-JSON shape with HTTP — when the
    // HTTP route lands, both paths converge onto this same hashing rule.
    let payload_for_hash = serde_json::json!({
        "target": &target,
        "reason": &reason,
        "note": &note,
    });
    let input_hash = canonical_json::hash(Some(&payload_for_hash));

    let signal_id = Uuid::new_v4();

    let bucket = idempotency::open_bucket(nats)
        .await
        .context("opening idempotency bucket")?;
    match idempotency::check_or_insert(&bucket, &idempotency_key, signal_id, &input_hash)
        .await
        .context("idempotency check")?
    {
        idempotency::CacheOutcome::Fresh => {
            // Fall through to relay forward.
        }
        idempotency::CacheOutcome::DeduplicatedSameHash { original_signal_id } => {
            SIGNALS_DEDUPLICATED.fetch_add(1, Ordering::Relaxed);
            println!(
                "nats_ingress: cancel deduplicated: key={}, original_signal_id={}",
                idempotency_key, original_signal_id
            );
            return Ok(MessageDisposition::Ack);
        }
        idempotency::CacheOutcome::ConflictDifferentHash {
            original_signal_id,
            original_hash,
        } => {
            SIGNALS_DROPPED_IDEMPOTENCY_COLLISION.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "nats_ingress: cancel idempotency collision: key={}, original_signal_id={}, original_hash={}, your_hash={}",
                idempotency_key,
                original_signal_id,
                original_hash,
                hex::encode(input_hash),
            );
            return Ok(MessageDisposition::Ack);
        }
    }

    let signal = sp::Signal {
        signal_id: signal_id.to_string(),
        idempotency_key: Some(idempotency_key),
        variant: Some(sp::signal::Variant::Cancel(sp::Cancel {
            target: Some(target.into_proto()),
            reason: Some(reason.into_proto()),
            note,
        })),
    };
    match relay_sender.try_send(&signal).await {
        RelaySendOutcome::Sent => Ok(MessageDisposition::Ack),
        RelaySendOutcome::Saturated => {
            eprintln!("nats_ingress: relay outbound saturated; NAK cancel to let NATS redeliver");
            Ok(MessageDisposition::Nak)
        }
        RelaySendOutcome::Error(e) => Err(e.context("relay sender forward")),
    }
}

/// Process a parsed Wakeup envelope. The envelope contract is reserved for
/// publishers today, but the downstream consumers — the conductor's
/// translator arm that maps a Wakeup to either a workflow-trigger
/// `Signal::Trigger` emission or a gate-satisfaction event — haven't
/// shipped yet. Until they do, the translator logs the arrival with the
/// envelope's name/target/payload so operators can observe inbound traffic
/// without forwarding anything onto the wire. Wakeup envelopes still flow
/// through the same idempotency cache as Trigger and Cancel so a redelivery
/// Process a parsed Wakeup envelope. Delegates to the shared
/// `wakeup_translator::process_wakeup` so the NATS path and the HTTP
/// path produce identical downstream behaviour: idempotency cache check,
/// subscription-index lookup, per-subscriber predicate eval, captures
/// extraction + persistence, fan-out `Signal::Trigger` emission.
///
/// `target_value` is decoded for shape validation only; the
/// waits-on-signal translator ignores it (target is reserved for the
/// deferred in-graph gate consumer). Decoding here keeps the envelope
/// validation symmetric across HTTP and NATS so a malformed target on
/// the wire surfaces as an envelope-level reject regardless of which
/// transport delivered it.
async fn process_wakeup(
    nats: &NatsClient,
    pg_pool: &PgPool,
    idempotency_key: String,
    name: String,
    target_value: Value,
    payload: Option<Value>,
) -> Result<MessageDisposition> {
    // Shape-validate the optional target. The translator itself ignores
    // it; this validation just keeps the wire-shape contract honest.
    if !target_value.is_null() {
        let _: ExtTarget = serde_json::from_value(target_value)
            .context("decode Wakeup.target as Signal::Target")?;
    }

    let pipeline_req = crate::wakeup_translator::WakeupRequest {
        name,
        payload,
        idempotency_key: Some(idempotency_key.clone()),
    };

    let gate_index = crate::gate_index_lifecycle::gate_index();
    let outcome = crate::wakeup_translator::process_wakeup(
        pg_pool,
        nats,
        &crate::wakeup_translator::DefaultRelaySender,
        &gate_index,
        pipeline_req,
    )
    .await?;

    match outcome {
        crate::wakeup_translator::WakeupOutcome::Fresh { .. } => Ok(MessageDisposition::Ack),
        crate::wakeup_translator::WakeupOutcome::Deduplicated { original_signal_id } => {
            SIGNALS_DEDUPLICATED.fetch_add(1, Ordering::Relaxed);
            println!(
                "nats_ingress: wakeup deduplicated: key={}, original_signal_id={}",
                idempotency_key, original_signal_id
            );
            Ok(MessageDisposition::Ack)
        }
        crate::wakeup_translator::WakeupOutcome::Conflict {
            original_signal_id,
            original_hash,
            your_hash,
        } => {
            SIGNALS_DROPPED_IDEMPOTENCY_COLLISION.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "nats_ingress: wakeup idempotency collision: key={}, original_signal_id={}, original_hash={}, your_hash={}",
                idempotency_key, original_signal_id, original_hash, your_hash,
            );
            Ok(MessageDisposition::Ack)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ok_trigger_bytes() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Trigger",
            "idempotency_key": "ext-1",
            "workflow_id": Uuid::new_v4().to_string(),
        }))
        .unwrap()
    }

    #[test]
    fn parses_minimal_trigger_envelope() {
        let bytes = ok_trigger_bytes();
        let env = parse_envelope(&bytes).expect("trigger parses");
        match env {
            Envelope::Trigger {
                idempotency_key, ..
            } => assert_eq!(idempotency_key, "ext-1"),
            _ => panic!("expected Trigger variant"),
        }
    }

    #[test]
    fn parses_trigger_with_scheduled_at_and_inputs() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Trigger",
            "idempotency_key": "ext-2",
            "workflow_id": Uuid::new_v4().to_string(),
            "scheduled_at": "2026-05-14T10:00:00Z",
            "inputs": { "k": "v" },
        }))
        .unwrap();
        let env = parse_envelope(&bytes).expect("trigger with optional fields parses");
        match env {
            Envelope::Trigger {
                scheduled_at,
                inputs,
                ..
            } => {
                assert!(scheduled_at.is_some());
                assert!(inputs.is_some());
            }
            _ => panic!("expected Trigger"),
        }
    }

    /// No-smuggle tripwire (permanent). A replay's carried state is minted
    /// conductor-side from the archive, never accepted from a client. The
    /// hand-rolled Trigger decoder extracts only its declared fields into a
    /// constrained `Envelope::Trigger`, so a client-supplied `replay` / `seed`
    /// field is silently ignored — it cannot ride the ingress into a
    /// `ReplaySeed`. The structural absence of any seed field on the envelope
    /// *is* the guarantee; this test asserts a forged field is non-fatal and
    /// carries nothing forward. If a future change adds a pass-through seed
    /// field here, this test must be revisited — that is the tripwire.
    #[test]
    fn trigger_envelope_ignores_client_supplied_replay_seed() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Trigger",
            "idempotency_key": "forged-1",
            "workflow_id": Uuid::new_v4().to_string(),
            // A malicious client tries to inject replay state.
            "replay": {
                "replay_instance_id": Uuid::new_v4().to_string(),
                "pre_grounded": [Uuid::new_v4().to_string()],
            },
            "seed": { "pre_grounded": [Uuid::new_v4().to_string()] },
            "source_instance_id": Uuid::new_v4().to_string(),
            "resume_from": [Uuid::new_v4().to_string()],
        }))
        .unwrap();
        let env = parse_envelope(&bytes).expect("a forged seed field is ignored, not fatal");
        match env {
            // The envelope's Trigger shape has no field able to carry the
            // forged seed — it was dropped at the decoder.
            Envelope::Trigger {
                idempotency_key,
                workflow_id: _,
                scheduled_at,
                inputs,
            } => {
                assert_eq!(idempotency_key, "forged-1");
                assert!(scheduled_at.is_none());
                assert!(inputs.is_none());
            }
            _ => panic!("expected Trigger"),
        }
    }

    #[test]
    fn parses_cancel_envelope_reserved_shape() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Cancel",
            "idempotency_key": "ext-3",
            "target": { "type": "Instance", "workflow_instance_id": Uuid::new_v4().to_string() },
            "note": "operator cancel",
        }))
        .unwrap();
        let env = parse_envelope(&bytes).expect("cancel parses");
        assert!(matches!(env, Envelope::Cancel { .. }));
    }

    #[test]
    fn parses_wakeup_envelope_reserved_shape() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Wakeup",
            "idempotency_key": "ext-4",
            "name": "order-paid",
            "target": { "type": "Instance", "workflow_instance_id": Uuid::new_v4().to_string() },
            "payload": { "order_id": "C-123" },
        }))
        .unwrap();
        let env = parse_envelope(&bytes).expect("wakeup parses");
        assert!(matches!(env, Envelope::Wakeup { .. }));
    }

    #[test]
    fn rejects_envelope_missing_version() {
        let bytes = serde_json::to_vec(&json!({
            "variant": "Trigger",
            "idempotency_key": "x",
            "workflow_id": Uuid::new_v4().to_string(),
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("missing version rejected");
        assert!(matches!(err, EnvelopeError::MissingVersion));
    }

    #[test]
    fn rejects_envelope_unknown_version() {
        let bytes = serde_json::to_vec(&json!({
            "version": 99,
            "variant": "Trigger",
            "idempotency_key": "x",
            "workflow_id": Uuid::new_v4().to_string(),
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("unknown version rejected");
        assert!(matches!(err, EnvelopeError::UnknownVersion(99)));
    }

    #[test]
    fn rejects_envelope_missing_idempotency_key() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Trigger",
            "workflow_id": Uuid::new_v4().to_string(),
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("missing key rejected");
        assert!(matches!(err, EnvelopeError::MissingIdempotencyKey));
    }

    #[test]
    fn rejects_envelope_missing_variant() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "idempotency_key": "x",
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("missing variant rejected");
        assert!(matches!(err, EnvelopeError::MissingVariant));
    }

    #[test]
    fn rejects_envelope_unknown_variant() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Frob",
            "idempotency_key": "x",
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("unknown variant rejected");
        assert!(matches!(err, EnvelopeError::UnknownVariant(_)));
    }

    #[test]
    fn rejects_malformed_json() {
        let err = parse_envelope(b"{not json").expect_err("malformed JSON rejected");
        assert!(matches!(err, EnvelopeError::MalformedJson(_)));
    }

    #[test]
    fn rejects_trigger_missing_workflow_id() {
        let bytes = serde_json::to_vec(&json!({
            "version": 1,
            "variant": "Trigger",
            "idempotency_key": "x",
        }))
        .unwrap();
        let err = parse_envelope(&bytes).expect_err("trigger needs workflow_id");
        assert!(matches!(
            err,
            EnvelopeError::VariantDeserialize { ref variant, .. } if variant == "Trigger"
        ));
    }
}
