//! NATS-side external-signals ingress translator.
//!
//! External systems (webhook gateways, message-bus bridges, scheduled
//! forwarders) publish v=1 JSON signal envelopes onto the per-tenant NATS
//! JetStream subject `tickr.external.signals`. The conductor runs a
//! durable pull consumer that decodes each envelope, mints `signal_id`,
//! applies the same idempotency cache the HTTP-trigger path uses, and
//! forwards a wire `Signal` over the existing relay outbound channel.
//!
//! Trigger and Wakeup reuse the SQL-backed Event-variable/audit repository
//! plus NATS ctx working state. External Cancel is deliberately NATS-only:
//! its idempotency bucket is its sole durable state, and a fresh envelope's
//! only downstream effect is one relay forward.
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
use prost::Message;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tickr_ctx::envelope::SignalSource;
use tickr_migrations::{backend::WriterRepositoryBundle, scope_repository::ScopeStore};
use tickr_proto::signal as sp;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::canonical_json;
use crate::ingress_idempotency::{
    observe_ingress_boundary, DeliveryOutcome, IngressBoundary, IngressCoordinator, IngressEffects,
    IngressOperation, IngressOutcomeProof, IngressReservation, IngressTerminalOutcome,
    NatsIngressIdempotencyStore, RelayIntent, ReservationOutcome,
};

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

    async fn try_send_wakeup_signal(&self, signal: &sp::Signal) -> RelaySendOutcome {
        match crate::relay::try_send_signal(signal).await {
            Ok(crate::relay::TrySendOutcome::Sent) => RelaySendOutcome::Sent,
            Ok(crate::relay::TrySendOutcome::Saturated) => RelaySendOutcome::Saturated,
            Err(error) => RelaySendOutcome::Error(error),
        }
    }

    async fn try_send_gate_outcome(&self, outcome: &sp::GateOutcome) -> RelaySendOutcome {
        match crate::relay::try_send_gate_outcome(outcome).await {
            Ok(crate::relay::TrySendOutcome::Sent) => RelaySendOutcome::Sent,
            Ok(crate::relay::TrySendOutcome::Saturated) => RelaySendOutcome::Saturated,
            Err(error) => RelaySendOutcome::Error(error),
        }
    }
}

/// One accepted transport delivery. Transport receipt state remains private to
/// the adapter; the consumer sees only its identity, producer key, and bytes.
#[async_trait]
pub trait EventIngressDelivery: Send {
    fn transport_identity(&self) -> &str;
    fn producer_key(&self) -> Option<&str>;
    fn payload(&self) -> &[u8];

    /// Complete from a matching durable producer outcome.
    async fn complete(
        self: Box<Self>,
        producer_key: &str,
        proof: &IngressOutcomeProof,
    ) -> Result<()>;

    /// Complete a payload that cannot enter producer coordination.
    async fn reject_malformed(self: Box<Self>, reason: String) -> Result<()>;

    /// Preserve the accepted delivery for retry or reclaim.
    async fn leave_pending(self: Box<Self>) -> Result<()>;
}

/// Formation-selected External Event transport. Redis and NATS receipt
/// mechanics end at this interface.
#[async_trait]
pub trait EventIngress: Send + Sync {
    async fn next_delivery(&self) -> Result<Option<Box<dyn EventIngressDelivery>>>;
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

/// Outcome of a non-blocking relay-intent dispatch. Saturation and transport
/// errors both preserve source redelivery.
#[derive(Debug)]
pub enum RelaySendOutcome {
    /// Message accepted into the relay outbound queue.
    Sent,
    /// Outbound buffer saturated; NAK to NATS so it redelivers.
    Saturated,
    /// Non-saturation forward failure (relay uninitialized or channel closed).
    /// The durable relay intent remains ready and the source is redelivered.
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
pub const SUBJECT: &str = tickr_proto::coord::all_nats::EVENT_INGRESS_SUBJECT;

/// JetStream stream backing the subject. NATS stream names cannot contain
/// dots, so the stream uses underscores while the subject keeps the dotted
/// form.
pub const STREAM_NAME: &str = tickr_proto::coord::all_nats::EVENT_INGRESS_STREAM;

/// Durable pull-consumer name. One consumer per conductor — serial
/// processing matches the conductor's other ingress shapes and is trivially
/// upgradable to a consumer group if throughput demands it.
pub const CONSUMER_NAME: &str = tickr_proto::coord::all_nats::EVENT_INGRESS_CONSUMER;

/// Hardened all-NATS transport adapter. The production consumer never sees
/// its JetStream consumer, message, or delivery-disposition store.
pub struct NatsEventIngress {
    consumer: PullConsumer,
    delivery_store: Arc<NatsIngressIdempotencyStore>,
}

impl NatsEventIngress {
    pub async fn connect(nats: &NatsClient) -> Result<Self> {
        Ok(Self {
            consumer: init_stream_and_consumer(nats).await?,
            delivery_store: Arc::new(
                NatsIngressIdempotencyStore::open(nats)
                    .await
                    .context("opening ingress idempotency store")?,
            ),
        })
    }

    pub fn ingress_coordinator(&self) -> IngressCoordinator {
        IngressCoordinator::new(self.delivery_store.clone())
    }
}

struct NatsEventIngressDelivery {
    transport_identity: String,
    delivery_sequence: u64,
    payload_hash: [u8; 32],
    message: async_nats::jetstream::Message,
    delivery_store: Arc<NatsIngressIdempotencyStore>,
}

#[async_trait]
impl EventIngressDelivery for NatsEventIngressDelivery {
    fn transport_identity(&self) -> &str {
        &self.transport_identity
    }

    fn producer_key(&self) -> Option<&str> {
        None
    }

    fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    async fn complete(
        self: Box<Self>,
        producer_key: &str,
        proof: &IngressOutcomeProof,
    ) -> Result<()> {
        let producer_digest = format!("{:x}", Sha256::digest(producer_key.as_bytes()));
        let payload_digest = hex::encode(self.payload_hash);
        if proof.producer_digest() != producer_digest || proof.payload_digest() != payload_digest {
            return Err(anyhow!("terminal producer proof does not match delivery"));
        }
        let outcome = match proof.outcome() {
            IngressTerminalOutcome::Accepted => DeliveryOutcome::Accepted,
            IngressTerminalOutcome::Rejected => DeliveryOutcome::Rejected {
                reason: "producer request was permanently rejected".to_string(),
            },
        };
        self.delivery_store
            .record_delivery(
                self.delivery_sequence,
                Some(producer_key),
                &self.payload_hash,
                outcome,
            )
            .await?;
        observe_ingress_boundary(IngressBoundary::BeforeDeliveryAck);
        self.message
            .ack()
            .await
            .map_err(|error| anyhow!("acknowledging terminal NATS ingress delivery: {error}"))
    }

    async fn reject_malformed(self: Box<Self>, reason: String) -> Result<()> {
        self.delivery_store
            .record_delivery(
                self.delivery_sequence,
                None,
                &self.payload_hash,
                DeliveryOutcome::Rejected { reason },
            )
            .await?;
        observe_ingress_boundary(IngressBoundary::AfterPermanentRejection);
        observe_ingress_boundary(IngressBoundary::BeforeDeliveryAck);
        self.message
            .ack()
            .await
            .map_err(|error| anyhow!("acknowledging malformed NATS ingress delivery: {error}"))
    }

    async fn leave_pending(self: Box<Self>) -> Result<()> {
        self.message
            .ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|error| anyhow!("returning NATS ingress delivery to pending: {error}"))
    }
}

#[async_trait]
impl EventIngress for NatsEventIngress {
    async fn next_delivery(&self) -> Result<Option<Box<dyn EventIngressDelivery>>> {
        let mut messages = self
            .consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_millis(100))
            .messages()
            .await
            .context("fetching NATS ingress delivery")?;
        let Some(message) = messages.next().await else {
            return Ok(None);
        };
        let message = message.map_err(|error| anyhow!("reading NATS ingress delivery: {error}"))?;
        let delivery_sequence = message
            .info()
            .map_err(|error| anyhow!("reading NATS ingress delivery identity: {error}"))?
            .stream_sequence;
        Ok(Some(Box::new(NatsEventIngressDelivery {
            transport_identity: delivery_sequence.to_string(),
            delivery_sequence,
            payload_hash: stable_payload_hash(&message.payload),
            message,
            delivery_store: self.delivery_store.clone(),
        })))
    }
}

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

    // Durable pull consumer with explicit acknowledgement. Permanent rejects
    // are recorded before ACK; every transient failure remains pending.
    let consumer_cfg = pull::Config {
        durable_name: Some(CONSUMER_NAME.to_string()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ack_wait: ingress_ack_wait(),
        ..Default::default()
    };
    let consumer = stream
        .get_or_create_consumer(CONSUMER_NAME, consumer_cfg)
        .await
        .map_err(|e| anyhow!("failed to get_or_create consumer {}: {}", CONSUMER_NAME, e))?;

    Ok(consumer)
}

fn ingress_ack_wait() -> Duration {
    #[cfg(debug_assertions)]
    if let Ok(value) = std::env::var("TICKR_TEST_INGRESS_ACK_WAIT_MS") {
        if let Ok(milliseconds) = value.parse::<u64>() {
            return Duration::from_millis(milliseconds);
        }
    }
    Duration::from_secs(30)
}

/// Run the translator-loop with the production `GlobalRelaySender`. Thin
/// wrapper that the conductor's startup code calls; tests use
/// `run_translator_with_sender` to inject a capturing sender.
pub async fn run_translator(
    nats: NatsClient,
    repositories: Arc<WriterRepositoryBundle>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    run_translator_with_sender(
        nats,
        repositories,
        Arc::new(GlobalRelaySender),
        shutdown_token,
    )
    .await
}

#[async_trait]
pub trait IngressWorkingSet: Send + Sync {
    async fn process_trigger(
        &self,
        repositories: &WriterRepositoryBundle,
        request: crate::trigger_pipeline::TriggerRequest,
        signal_id: Uuid,
    ) -> std::result::Result<
        crate::trigger_pipeline::ReservedTriggerEffects,
        crate::trigger_pipeline::TriggerError,
    >;

    async fn process_wakeup(
        &self,
        repositories: &WriterRepositoryBundle,
        sender: &dyn crate::wakeup_translator::WakeupRelaySender,
        request: crate::wakeup_translator::WakeupRequest,
        signal_id: Uuid,
    ) -> Result<crate::wakeup_translator::WakeupOutcome>;
}

pub(crate) struct NatsIngressWorkingSet {
    nats: NatsClient,
}

impl NatsIngressWorkingSet {
    pub(crate) fn new(nats: NatsClient) -> Self {
        Self { nats }
    }
}

#[async_trait]
impl IngressWorkingSet for NatsIngressWorkingSet {
    async fn process_trigger(
        &self,
        repositories: &WriterRepositoryBundle,
        request: crate::trigger_pipeline::TriggerRequest,
        signal_id: Uuid,
    ) -> std::result::Result<
        crate::trigger_pipeline::ReservedTriggerEffects,
        crate::trigger_pipeline::TriggerError,
    > {
        crate::trigger_pipeline::process_reserved_trigger(
            repositories,
            &self.nats,
            request,
            signal_id,
        )
        .await
    }

    async fn process_wakeup(
        &self,
        repositories: &WriterRepositoryBundle,
        sender: &dyn crate::wakeup_translator::WakeupRelaySender,
        request: crate::wakeup_translator::WakeupRequest,
        signal_id: Uuid,
    ) -> Result<crate::wakeup_translator::WakeupOutcome> {
        crate::wakeup_translator::process_reserved_wakeup(
            repositories,
            &self.nats,
            sender,
            &crate::gate_index_lifecycle::gate_index(),
            request,
            signal_id,
        )
        .await
    }
}

pub(crate) struct ScopeStoreIngressWorkingSet {
    scope_store: Arc<dyn ScopeStore>,
}

impl ScopeStoreIngressWorkingSet {
    pub(crate) fn new(scope_store: Arc<dyn ScopeStore>) -> Self {
        Self { scope_store }
    }
}

#[async_trait]
impl IngressWorkingSet for ScopeStoreIngressWorkingSet {
    async fn process_trigger(
        &self,
        repositories: &WriterRepositoryBundle,
        request: crate::trigger_pipeline::TriggerRequest,
        signal_id: Uuid,
    ) -> std::result::Result<
        crate::trigger_pipeline::ReservedTriggerEffects,
        crate::trigger_pipeline::TriggerError,
    > {
        crate::trigger_pipeline::process_reserved_trigger_with_scope_store(
            repositories,
            self.scope_store.as_ref(),
            request,
            signal_id,
        )
        .await
    }

    async fn process_wakeup(
        &self,
        repositories: &WriterRepositoryBundle,
        sender: &dyn crate::wakeup_translator::WakeupRelaySender,
        request: crate::wakeup_translator::WakeupRequest,
        signal_id: Uuid,
    ) -> Result<crate::wakeup_translator::WakeupOutcome> {
        crate::wakeup_translator::process_reserved_wakeup_with_scope_store(
            repositories,
            self.scope_store.as_ref(),
            sender,
            &crate::gate_index_lifecycle::gate_index(),
            request,
            signal_id,
        )
        .await
    }
}

/// Run the hardened all-NATS adapter through the substrate-neutral production
/// consumer. Tests keep this wrapper to inject their relay sender.
pub async fn run_translator_with_sender(
    nats: NatsClient,
    repositories: Arc<WriterRepositoryBundle>,
    relay_sender: Arc<dyn RelaySender>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    let event_ingress = Arc::new(NatsEventIngress::connect(&nats).await?);
    let ingress_coordinator = event_ingress.ingress_coordinator();
    run_event_consumer(
        event_ingress,
        ingress_coordinator,
        repositories,
        Arc::new(NatsIngressWorkingSet::new(nats)),
        relay_sender,
        shutdown_token,
    )
    .await
}

/// Consume the selected EventIngress role without inspecting its transport.
/// Every terminal action is delegated back to the delivery receipt.
pub async fn run_event_consumer(
    event_ingress: Arc<dyn EventIngress>,
    ingress_coordinator: IngressCoordinator,
    repositories: Arc<WriterRepositoryBundle>,
    working_set: Arc<dyn IngressWorkingSet>,
    relay_sender: Arc<dyn RelaySender>,
    shutdown_token: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => break,
            next = event_ingress.next_delivery() => {
                match next {
                    Ok(Some(delivery)) => {
                        process_delivery(
                            repositories.as_ref(),
                            &ingress_coordinator,
                            working_set.as_ref(),
                            relay_sender.as_ref(),
                            delivery,
                        )
                        .await;
                    }
                    Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => {
                        eprintln!("event_ingress: delivery receive failed: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn process_delivery(
    repositories: &WriterRepositoryBundle,
    ingress_coordinator: &IngressCoordinator,
    working_set: &dyn IngressWorkingSet,
    relay_sender: &dyn RelaySender,
    delivery: Box<dyn EventIngressDelivery>,
) {
    let transport_identity = delivery.transport_identity().to_owned();
    let payload_hash = stable_payload_hash(delivery.payload());
    let envelope = match parse_envelope(delivery.payload()) {
        Ok(envelope) => envelope,
        Err(error) => {
            record_envelope_error(&error);
            eprintln!(
                "event_ingress: envelope permanently rejected: delivery={}, bytes={}, reason={}",
                transport_identity,
                delivery.payload().len(),
                error
            );
            if let Err(error) = delivery.reject_malformed(error.to_string()).await {
                eprintln!("event_ingress: durable malformed-envelope rejection failed: {error}");
            }
            return;
        }
    };
    let producer_key = match &envelope {
        Envelope::Trigger {
            idempotency_key, ..
        }
        | Envelope::Cancel {
            idempotency_key, ..
        }
        | Envelope::Wakeup {
            idempotency_key, ..
        } => idempotency_key.clone(),
    };
    if delivery
        .producer_key()
        .is_some_and(|transport_key| transport_key != producer_key)
    {
        if let Err(error) = delivery
            .reject_malformed("transport producer key does not match envelope".to_string())
            .await
        {
            eprintln!("event_ingress: durable producer-key rejection failed: {error}");
        }
        return;
    }

    let reservation = match ingress_coordinator
        .reserve(&producer_key, &payload_hash)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            eprintln!("event_ingress: producer reservation failed: {error}");
            leave_pending(delivery).await;
            return;
        }
    };
    match reservation {
        ReservationOutcome::Pending => leave_pending(delivery).await,
        ReservationOutcome::Complete(proof) => {
            SIGNALS_DEDUPLICATED.fetch_add(1, Ordering::Relaxed);
            complete_delivery(delivery, &producer_key, &proof).await;
        }
        ReservationOutcome::Rejected(proof) => {
            complete_delivery(delivery, &producer_key, &proof).await;
        }
        ReservationOutcome::Conflict {
            original_signal_id,
            original_hash,
            proof,
        } => {
            SIGNALS_DROPPED_IDEMPOTENCY_COLLISION.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "event_ingress: producer idempotency conflict: signal_id={original_signal_id}, original_hash={original_hash}, your_hash={}",
                hex::encode(payload_hash)
            );
            complete_delivery(delivery, &producer_key, &proof).await;
        }
        ReservationOutcome::Ready(operation, effects) => {
            forward_ready_intents(
                delivery,
                &producer_key,
                operation,
                effects.relay_intents,
                relay_sender,
            )
            .await;
        }
        ReservationOutcome::Acquired(reservation) => {
            observe_ingress_boundary(IngressBoundary::AfterReservation);
            match process_reserved_envelope(
                repositories,
                working_set,
                reservation.as_ref(),
                envelope.clone(),
            )
            .await
            {
                Ok(effects) => {
                    observe_ingress_boundary(IngressBoundary::AfterEffects);
                    let effects = match reservation.persist_effects(effects).await {
                        Ok(effects) => effects,
                        Err(error) => {
                            eprintln!("event_ingress: persist ready relay intent failed: {error}");
                            leave_pending(delivery).await;
                            return;
                        }
                    };
                    observe_ingress_boundary(IngressBoundary::AfterRelayIntentPersistence);
                    forward_ready_intents(
                        delivery,
                        &producer_key,
                        reservation.operation(),
                        effects.relay_intents,
                        relay_sender,
                    )
                    .await;
                }
                Err(IngressProcessingFailure::Permanent(reason)) => {
                    increment_processing_rejection(&envelope);
                    match reservation.reject(reason).await {
                        Ok(proof) => complete_delivery(delivery, &producer_key, &proof).await,
                        Err(error) => {
                            eprintln!("event_ingress: persist producer rejection failed: {error}");
                            leave_pending(delivery).await;
                        }
                    }
                }
                Err(IngressProcessingFailure::Transient(error)) => {
                    eprintln!("event_ingress: transient processing failure: {error}");
                    if let Err(abandon_error) = reservation.abandon().await {
                        eprintln!(
                            "event_ingress: release transient producer reservation failed: {abandon_error}"
                        );
                    }
                    leave_pending(delivery).await;
                }
            }
        }
    }
}

#[derive(Debug)]
enum IngressProcessingFailure {
    Permanent(String),
    Transient(anyhow::Error),
}

struct CollectingWakeupIntentSender {
    intents: Mutex<Vec<RelayIntent>>,
}

impl CollectingWakeupIntentSender {
    fn new() -> Self {
        Self {
            intents: Mutex::new(Vec::new()),
        }
    }

    fn into_intents(self) -> Result<Vec<RelayIntent>> {
        self.intents
            .into_inner()
            .map_err(|_| anyhow!("wakeup relay-intent collector poisoned"))
    }
}

#[async_trait]
impl crate::wakeup_translator::WakeupRelaySender for CollectingWakeupIntentSender {
    async fn send(&self, signal: &sp::Signal) -> Result<()> {
        self.intents
            .lock()
            .map_err(|_| anyhow!("wakeup relay-intent collector poisoned"))?
            .push(RelayIntent::WakeupSignal(signal.encode_to_vec()));
        Ok(())
    }

    async fn send_gate_outcome(&self, outcome: &sp::GateOutcome) -> Result<()> {
        self.intents
            .lock()
            .map_err(|_| anyhow!("wakeup relay-intent collector poisoned"))?
            .push(RelayIntent::GateOutcome(outcome.encode_to_vec()));
        Ok(())
    }
}

async fn process_reserved_envelope(
    repositories: &WriterRepositoryBundle,
    working_set: &dyn IngressWorkingSet,
    reservation: &dyn IngressReservation,
    envelope: Envelope,
) -> std::result::Result<IngressEffects, IngressProcessingFailure> {
    match envelope {
        Envelope::Trigger {
            idempotency_key,
            workflow_id,
            scheduled_at,
            inputs,
        } => {
            let hash_payload = serde_json::json!({
                "workflow_id": workflow_id,
                "scheduled_at": scheduled_at,
                "inputs": &inputs.clone().unwrap_or(Value::Object(Default::default())),
            });
            let request = crate::trigger_pipeline::TriggerRequest {
                workflow_id,
                scheduled_at,
                inputs,
                idempotency_key: Some(idempotency_key),
                source: SignalSource::ExternalNats {
                    subject: SUBJECT.to_string(),
                },
                hash_payload,
                name: None,
            };
            let effects = working_set
                .process_trigger(repositories, request, reservation.signal_id())
                .await
                .map_err(|error| {
                    if error.is_permanent_ingress_rejection() {
                        IngressProcessingFailure::Permanent(error.to_string())
                    } else {
                        IngressProcessingFailure::Transient(anyhow!(error))
                    }
                })?;
            let signal_effect = effects.signal.encode_to_vec();
            Ok(IngressEffects {
                relay_intents: vec![RelayIntent::Signal(signal_effect.clone())],
                signal_effect,
                event_results: effects.event_results,
            })
        }
        Envelope::Cancel {
            idempotency_key,
            target,
            reason,
            note,
        } => {
            let target: ExtTarget = serde_json::from_value(target).map_err(|error| {
                IngressProcessingFailure::Permanent(format!(
                    "decode Cancel.target as Signal::Target: {error}"
                ))
            })?;
            let reason: ExtCancelReason = serde_json::from_value(reason).map_err(|error| {
                IngressProcessingFailure::Permanent(format!(
                    "decode Cancel.reason as Signal::CancelReason: {error}"
                ))
            })?;
            let signal = sp::Signal {
                signal_id: reservation.signal_id().to_string(),
                idempotency_key: Some(idempotency_key),
                variant: Some(sp::signal::Variant::Cancel(sp::Cancel {
                    target: Some(target.into_proto()),
                    reason: Some(reason.into_proto()),
                    note,
                })),
            };
            let signal_effect = signal.encode_to_vec();
            Ok(IngressEffects {
                relay_intents: vec![RelayIntent::Signal(signal_effect.clone())],
                signal_effect,
                event_results: b"[]".to_vec(),
            })
        }
        Envelope::Wakeup {
            idempotency_key: _,
            name,
            target,
            payload,
        } => {
            if !target.is_null() {
                serde_json::from_value::<ExtTarget>(target).map_err(|error| {
                    IngressProcessingFailure::Permanent(format!(
                        "decode Wakeup.target as Signal::Target: {error}"
                    ))
                })?;
            }
            let request = crate::wakeup_translator::WakeupRequest {
                name,
                payload,
                idempotency_key: None,
            };
            let sender = CollectingWakeupIntentSender::new();
            working_set
                .process_wakeup(repositories, &sender, request, reservation.signal_id())
                .await
                .map_err(IngressProcessingFailure::Transient)?;
            let relay_intents = sender
                .into_intents()
                .map_err(IngressProcessingFailure::Transient)?;
            let signal_effect = serde_json::to_vec(&relay_intents)
                .map_err(|error| IngressProcessingFailure::Transient(anyhow!(error)))?;
            Ok(IngressEffects {
                signal_effect,
                event_results: b"[]".to_vec(),
                relay_intents,
            })
        }
    }
}

async fn forward_ready_intents(
    delivery: Box<dyn EventIngressDelivery>,
    producer_key: &str,
    operation: Arc<dyn IngressOperation>,
    intents: Vec<RelayIntent>,
    relay_sender: &dyn RelaySender,
) {
    for intent in intents {
        let outcome = match intent {
            RelayIntent::Signal(bytes) => match sp::Signal::decode(bytes.as_slice()) {
                Ok(signal) => relay_sender.try_send(&signal).await,
                Err(error) => {
                    eprintln!("event_ingress: corrupt durable Signal intent: {error}");
                    leave_pending(delivery).await;
                    return;
                }
            },
            RelayIntent::WakeupSignal(bytes) => match sp::Signal::decode(bytes.as_slice()) {
                Ok(signal) => relay_sender.try_send_wakeup_signal(&signal).await,
                Err(error) => {
                    eprintln!("event_ingress: corrupt durable Wakeup Signal intent: {error}");
                    leave_pending(delivery).await;
                    return;
                }
            },
            RelayIntent::GateOutcome(bytes) => match sp::GateOutcome::decode(bytes.as_slice()) {
                Ok(outcome) => relay_sender.try_send_gate_outcome(&outcome).await,
                Err(error) => {
                    eprintln!("event_ingress: corrupt durable GateOutcome intent: {error}");
                    leave_pending(delivery).await;
                    return;
                }
            },
        };
        match outcome {
            RelaySendOutcome::Sent => {}
            RelaySendOutcome::Saturated => {
                SIGNALS_RELAY_OUTBOUND_SATURATION.fetch_add(1, Ordering::Relaxed);
                leave_pending(delivery).await;
                return;
            }
            RelaySendOutcome::Error(error) => {
                eprintln!("event_ingress: relay intent forward failed: {error}");
                leave_pending(delivery).await;
                return;
            }
        }
    }

    match operation.mark_relayed().await {
        Ok(proof) => complete_delivery(delivery, producer_key, &proof).await,
        Err(error) => {
            eprintln!("event_ingress: persist relayed ingress intent failed: {error}");
            leave_pending(delivery).await;
        }
    }
}

async fn complete_delivery(
    delivery: Box<dyn EventIngressDelivery>,
    producer_key: &str,
    proof: &IngressOutcomeProof,
) {
    if let Err(error) = delivery.complete(producer_key, proof).await {
        eprintln!("event_ingress: terminal delivery completion failed: {error}");
    }
}

async fn leave_pending(delivery: Box<dyn EventIngressDelivery>) {
    if let Err(error) = delivery.leave_pending().await {
        eprintln!("event_ingress: preserving pending delivery failed: {error}");
    }
}

fn stable_payload_hash(bytes: &[u8]) -> [u8; 32] {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => canonical_json::hash(Some(&value)),
        Err(_) => Sha256::digest(bytes).into(),
    }
}

fn increment_processing_rejection(envelope: &Envelope) {
    match envelope {
        Envelope::Trigger { .. } => {
            REJECTED_TRIGGER_PROCESSING.fetch_add(1, Ordering::Relaxed);
        }
        Envelope::Cancel { .. } => {
            REJECTED_CANCEL_PROCESSING.fetch_add(1, Ordering::Relaxed);
        }
        Envelope::Wakeup { .. } => {}
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
