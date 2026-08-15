//! Implementation of relay service for conductor

use crate::config::ControlPlaneRelayConfig;
use crate::proto::conductor_relay_service_client::ConductorRelayServiceClient;
use crate::proto::{ConductorRelayMessage, EntityType};
use crate::relay::dispatch_gates::{DispatchGatesClient, DispatchGatesError};
use crate::signal_applied_notifier::SignalAppliedNotifier;
use crate::system_tasks::build_ack;
use crate::system_tasks::compaction_drain::{
    observe_compaction_boundary, AllNatsCompactionStaging, CompactionBoundary,
};
use anyhow::{Context, Result};
use async_nats::jetstream;
use async_nats::HeaderMap;
use async_stream;
use futures::{Stream, StreamExt};
use prost::Message;
use std::error::Error as StdError;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tickr_proto::codec::compaction::decode_envelope;
use tickr_proto::coord::all_nats::{ElectionDecision, TaskPickupRecord, TASK_PICKUP_BUCKET};
use tickr_proto::coord::{
    CompactionStaging, TaskCancellationAckConsumer, TaskCancellationAckDelivery,
    TaskCancellationFuture, TaskCancellationPublisher, TaskDispatchFuture, TaskDispatchPublisher,
    TaskEventConsumer, TaskEventDelivery, TaskEventFuture, TaskEventWriter, LIVENESS_BUCKET,
    LIVENESS_MARKER_CONSUMER, LIVENESS_MARKER_TTL, TASK_CANCEL_ACK_CONSUMER,
    TASK_CANCEL_ACK_STREAM, TASK_CANCEL_ACK_SUBJECT, TASK_CANCEL_STREAM, TASK_CANCEL_SUBJECT,
    TASK_DISPATCH_STREAM, TASK_DISPATCH_SUBJECT, TASK_EVENT_CONSUMER, TASK_EVENT_STREAM,
    TASK_EVENT_SUBJECT,
};
use tickr_proto::patch as pp;
use tickr_proto::signal as sp;
use tickr_proto::task as tc;
use tickr_proto::workflow as wf;
use tickr_proto::TenantId;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status, Streaming};
use uuid::Uuid;

// Global channel for sending relay messages
static RELAY_TX: LazyLock<Arc<Mutex<Option<mpsc::Sender<ConductorRelayMessage>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(None)));

/// Prevent a durable definition-lookup fault from hot-looping its JetStream
/// message and exhausting local log storage before an Operator can repair it.
const LOOKUP_INTEGRITY_RETRY_DELAY: Duration = Duration::from_secs(5);
const OUTCOME_SWEEP_BATCH: usize = 64;
const OUTCOME_ELECTION_RETRIES: usize = 8;
const MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
enum RelayEstablishmentError {
    #[error("Control-plane relay authentication rejected")]
    Unauthenticated,
    #[error("Control-plane relay Tenant binding rejected")]
    Forbidden,
    #[error("Control-plane relay connection timed out")]
    Timeout,
    #[error("Control-plane relay TLS handshake failed")]
    TlsHandshake,
    #[error("Control-plane relay peer unavailable")]
    Unavailable,
    #[error("Control-plane relay protocol rejected the stream")]
    Protocol,
}

fn classify_transport_error(
    error: tonic::transport::Error,
    uses_tls: bool,
) -> RelayEstablishmentError {
    let mut source: Option<&(dyn StdError + 'static)> = Some(&error);
    while let Some(current) = source {
        if current.downcast_ref::<rustls::Error>().is_some() {
            return RelayEstablishmentError::TlsHandshake;
        }
        if current
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::TimedOut)
        {
            return RelayEstablishmentError::Timeout;
        }
        if uses_tls {
            let diagnostic = current.to_string().to_ascii_lowercase();
            if diagnostic.contains("certificate")
                || diagnostic.contains("tls")
                || diagnostic.contains("handshake")
            {
                return RelayEstablishmentError::TlsHandshake;
            }
        }
        source = current.source();
    }
    RelayEstablishmentError::Unavailable
}

fn classify_relay_status(status: Status) -> RelayEstablishmentError {
    match status.code() {
        Code::Unauthenticated => RelayEstablishmentError::Unauthenticated,
        Code::PermissionDenied => RelayEstablishmentError::Forbidden,
        Code::DeadlineExceeded => RelayEstablishmentError::Timeout,
        Code::Unavailable => RelayEstablishmentError::Unavailable,
        _ => RelayEstablishmentError::Protocol,
    }
}

async fn connect_relay_client(
    config: &ControlPlaneRelayConfig,
) -> Result<ConductorRelayServiceClient<Channel>, RelayEstablishmentError> {
    let mut endpoint = Channel::from_shared(config.endpoint().to_owned())
        .map_err(|_| RelayEstablishmentError::Unavailable)?;
    if config.uses_tls() {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|error| classify_transport_error(error, true))?;
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| classify_transport_error(error, config.uses_tls()))?;
    Ok(ConductorRelayServiceClient::new(channel))
}

#[cfg(test)]
async fn connect_relay_client_with_tls(
    config: &ControlPlaneRelayConfig,
    tls: ClientTlsConfig,
) -> Result<ConductorRelayServiceClient<Channel>, RelayEstablishmentError> {
    let endpoint = Channel::from_shared(config.endpoint().to_owned())
        .map_err(|_| RelayEstablishmentError::Unavailable)?
        .tls_config(tls)
        .map_err(|error| classify_transport_error(error, true))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|error| classify_transport_error(error, true))?;
    Ok(ConductorRelayServiceClient::new(channel))
}

async fn establish_relay_stream<S>(
    client: &mut ConductorRelayServiceClient<Channel>,
    outbound: S,
    config: &ControlPlaneRelayConfig,
) -> Result<Streaming<ConductorRelayMessage>, RelayEstablishmentError>
where
    S: Stream<Item = ConductorRelayMessage> + Send + 'static,
{
    let mut request = Request::new(outbound);
    request
        .metadata_mut()
        .insert("authorization", config.authorization());
    client
        .stream_conductor_relay(request)
        .await
        .map(tonic::Response::into_inner)
        .map_err(classify_relay_status)
}

/// Inject the relay-tx slot directly. Production sets it via `run_streaming`
/// once the gRPC stream is established; integration tests that don't want
/// to stand up a full streaming connection use this to point the global at
/// a test channel. Idempotent: a subsequent call replaces the slot.
pub async fn init_relay_tx(tx: mpsc::Sender<ConductorRelayMessage>) {
    let mut guard = RELAY_TX.lock().await;
    *guard = Some(tx);
}

#[cfg(test)]
pub(crate) async fn stage_compaction_and_send_ack(
    nats: &async_nats::Client,
    payload: Vec<u8>,
    ack_tx: &mpsc::Sender<ConductorRelayMessage>,
) -> Result<(String, String)> {
    stage_compaction_through_role_and_send_ack(
        &AllNatsCompactionStaging::new(nats.clone()),
        payload,
        ack_tx,
    )
    .await
}

async fn stage_compaction_through_role_and_send_ack(
    staging: &dyn CompactionStaging,
    payload: Vec<u8>,
    ack_tx: &mpsc::Sender<ConductorRelayMessage>,
) -> Result<(String, String)> {
    let envelope = decode_envelope(&payload)?;
    let projection = envelope
        .projection
        .as_ref()
        .expect("decode_envelope guarantees a projection");
    let workflow_instance_id = projection.id.clone();
    let state = projection.state.clone();
    staging
        .stage(&payload)
        .await
        .map_err(|error| anyhow::anyhow!("stage Compaction through selected role: {error}"))?;
    let acknowledgement = build_ack(&workflow_instance_id, &envelope.correlation);
    observe_compaction_boundary(CompactionBoundary::BeforeCrossPlaneAcknowledgement);
    ack_tx
        .send(acknowledgement)
        .await
        .map_err(|error| anyhow::anyhow!("send CompactionAck: {error}"))?;
    observe_compaction_boundary(CompactionBoundary::AfterCrossPlaneAcknowledgement);
    Ok((workflow_instance_id, state))
}

/// Register a protobuf workflow definition through the Conductor relay.
pub async fn send_workflow_registration(definition: wf::WorkflowDefinition) -> Result<()> {
    let tx_guard = RELAY_TX.lock().await;

    if let Some(tx) = tx_guard.as_ref() {
        let workflow_id = definition.id.clone();
        let workflow_bytes = definition.encode_to_vec();

        // Create the registration message
        let msg = ConductorRelayMessage {
            entity_type: EntityType::SubmitWorkflow as i32,
            payload: workflow_bytes,
            tenant_id: None,
        };

        // Send the message through the relay
        tx.send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send workflow registration message: {}", e))?;

        println!(
            "Sent workflow registration for workflow ID: {}",
            workflow_id
        );
        Ok(())
    } else {
        Err(anyhow::anyhow!("Relay channel not initialized"))
    }
}

/// Forward an already-bincode-serialized workflow definition over the relay
/// as a `SubmitWorkflow` envelope. Used by the submission consumer to
/// avoid round-tripping the workflow definition through `Workflow` on
/// every cross-plane hand-off.
pub async fn forward_workflow_registration_bytes(payload: Vec<u8>) -> Result<()> {
    let tx_guard = RELAY_TX.lock().await;
    if let Some(tx) = tx_guard.as_ref() {
        let msg = ConductorRelayMessage {
            entity_type: EntityType::SubmitWorkflow as i32,
            payload,
            tenant_id: None,
        };
        tx.send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to forward SubmitWorkflow payload: {}", e))?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("Relay channel not initialized"))
    }
}

/// Outcome of a saturation-aware `try_send_signal`. `Sent` means the
/// outbound queue accepted the message and the relay client will deliver
/// it asynchronously; `Saturated` means the queue is full and the caller
/// should apply its own backpressure (e.g. NAK to NATS).
#[derive(Debug)]
pub enum TrySendOutcome {
    Sent,
    Saturated,
}

/// Non-blocking relay forward. Mirrors `send_signal` but uses `try_send`
/// instead of awaiting buffer capacity. Used by the NATS ingress
/// translator to distinguish "outbound queue full, redeliver later" from
/// "actual send failure" so it can NAK the NATS message correctly. The
/// HTTP-trigger path continues to use `send_signal` because synchronous
/// HTTP callers expect a 5xx on saturation rather than NAK semantics.
pub async fn try_send_signal(signal: &sp::Signal) -> Result<TrySendOutcome> {
    let tx_guard = RELAY_TX.lock().await;
    let Some(tx) = tx_guard.as_ref() else {
        return Err(anyhow::anyhow!("Relay channel not initialized"));
    };

    let payload = signal.encode_to_vec();

    let msg = ConductorRelayMessage {
        entity_type: EntityType::Signal as i32,
        payload,
        tenant_id: None,
    };

    match tx.try_send(msg) {
        Ok(()) => Ok(TrySendOutcome::Sent),
        Err(mpsc::error::TrySendError::Full(_)) => Ok(TrySendOutcome::Saturated),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err(anyhow::anyhow!("Relay outbound channel closed"))
        }
    }
}

/// Saturation-aware GateOutcome forward used by durable Event-ingress relay
/// intents. A full or unavailable relay must preserve source redelivery.
pub async fn try_send_gate_outcome(outcome: &sp::GateOutcome) -> Result<TrySendOutcome> {
    let tx_guard = RELAY_TX.lock().await;
    let Some(tx) = tx_guard.as_ref() else {
        return Err(anyhow::anyhow!("Relay channel not initialized"));
    };
    let msg = ConductorRelayMessage {
        entity_type: EntityType::GateOutcome as i32,
        payload: outcome.encode_to_vec(),
        tenant_id: None,
    };
    match tx.try_send(msg) {
        Ok(()) => Ok(TrySendOutcome::Sent),
        Err(mpsc::error::TrySendError::Full(_)) => Ok(TrySendOutcome::Saturated),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            Err(anyhow::anyhow!("Relay outbound channel closed"))
        }
    }
}

/// Emit a wire `Signal` envelope onto the relay. Proto-encoded with the
/// `EntityType::Signal` discriminator; the server's signal dispatcher reads the
/// payload and routes by its variant. Producer-side errors here are reported
/// synchronously to the HTTP caller so a relay-disconnect window surfaces as a
/// 5xx rather than a silently-dropped trigger.
pub async fn send_signal(signal: &sp::Signal) -> Result<()> {
    let tx_guard = RELAY_TX.lock().await;

    if let Some(tx) = tx_guard.as_ref() {
        let payload = signal.encode_to_vec();

        let msg = ConductorRelayMessage {
            entity_type: EntityType::Signal as i32,
            payload,
            tenant_id: None,
        };

        tx.send(msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send Signal: {}", e))?;

        Ok(())
    } else {
        Err(anyhow::anyhow!("Relay channel not initialized"))
    }
}

/// Emit a `GateOutcome` envelope onto the relay. The server's
/// `GateOutcome` handler transitions the matching dispatched gate
/// to `Satisfied { signal_id }` and runs `next_tasks` so downstream
/// edges fire. Proto-encoded with the `EntityType::GateOutcome`
/// discriminator. Producer-side errors here surface synchronously
/// to the wakeup translator so a relay-disconnect window degrades
/// the fan-out gracefully (the surviving gate stays dispatched
/// until a later wakeup retries).
pub async fn send_gate_outcome(outcome: &sp::GateOutcome) -> Result<()> {
    let tx_guard = RELAY_TX.lock().await;
    let Some(tx) = tx_guard.as_ref() else {
        return Err(anyhow::anyhow!("Relay channel not initialized"));
    };
    let payload = outcome.encode_to_vec();
    let msg = ConductorRelayMessage {
        entity_type: EntityType::GateOutcome as i32,
        payload,
        tenant_id: None,
    };
    tx.send(msg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send GateOutcome: {}", e))?;
    Ok(())
}

/// Emit a `PatchWorkflowInstance` envelope onto the relay — the conductor→
/// server leg of the patch pipeline. Bincode-serialized with
/// `EntityType::PatchWorkflowInstance` discriminator; the server drives the
/// two-phase apply (Stall, re-validate, apply) and relays a `PatchOutcome`
/// back. Producer-side errors surface synchronously so the patch pipeline
/// can leave the lifecycle row `Validating` for its re-drive loop rather
/// than losing the send.
pub async fn send_patch_workflow_instance(envelope: &pp::PatchEnvelope) -> Result<()> {
    let tx_guard = RELAY_TX.lock().await;
    let Some(tx) = tx_guard.as_ref() else {
        return Err(anyhow::anyhow!("Relay channel not initialized"));
    };
    // Publish the `tickr.patch` envelope prost-encoded — the server translates
    // it into its internal patch aggregate at its relay boundary.
    let payload = envelope.encode_to_vec();
    let msg = ConductorRelayMessage {
        entity_type: EntityType::PatchWorkflowInstance as i32,
        payload,
        tenant_id: None,
    };
    tx.send(msg)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send PatchWorkflowInstance: {}", e))?;
    Ok(())
}

/// Get-or-create the durable task-event update stream and its shared pull
/// consumer. The stream is a JetStream **work queue** (a message is removed
/// once any conductor acks it) and the consumer binds a single durable name,
/// so multiple conductor instances binding it load-balance delivery — the
/// compaction-drain pattern. `get_or_create` is idempotent; the stream config
/// matches the executor's publish-side `ensure_task_event_stream` exactly.
pub async fn task_event_consumer(
    nats: &async_nats::Client,
) -> Result<jetstream::consumer::PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: TASK_EVENT_STREAM.to_string(),
            subjects: vec![TASK_EVENT_SUBJECT.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-event stream: {}", e))?;
    let consumer = stream
        .get_or_create_consumer(
            TASK_EVENT_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(TASK_EVENT_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-event consumer: {}", e))?;
    Ok(consumer)
}

#[derive(Clone)]
pub struct NatsTaskEventConsumer {
    consumer: jetstream::consumer::PullConsumer,
}

impl NatsTaskEventConsumer {
    pub fn new(consumer: jetstream::consumer::PullConsumer) -> Self {
        Self { consumer }
    }
}

struct NatsTaskEventDelivery {
    message: jetstream::Message,
}

impl TaskEventDelivery for NatsTaskEventDelivery {
    fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    fn complete(self: Box<Self>) -> TaskEventFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.message
                .ack()
                .await
                .map_err(|error| format!("task-event acknowledgement failed: {error}"))
        })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskEventFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.message
                .ack_with(jetstream::AckKind::Nak(delay))
                .await
                .map_err(|error| format!("task-event retry failed: {error}"))
        })
    }
}

impl TaskEventConsumer for NatsTaskEventConsumer {
    fn next(&self) -> TaskEventFuture<'_, Result<Option<Box<dyn TaskEventDelivery>>, String>> {
        Box::pin(async move {
            let mut batch = self
                .consumer
                .batch()
                .max_messages(1)
                .expires(Duration::from_secs(1))
                .messages()
                .await
                .map_err(|error| format!("open task-event delivery batch: {error}"))?;
            match batch.next().await {
                Some(Ok(message)) => Ok(Some(
                    Box::new(NatsTaskEventDelivery { message }) as Box<dyn TaskEventDelivery>
                )),
                Some(Err(error)) => Err(format!("task-event pull failed: {error}")),
                None => Ok(None),
            }
        })
    }
}

#[derive(Clone)]
pub struct NatsTaskEventWriter {
    js: jetstream::Context,
}

impl NatsTaskEventWriter {
    pub fn new(nats: &async_nats::Client) -> Self {
        Self {
            js: jetstream::new(nats.clone()),
        }
    }
}

impl TaskEventWriter for NatsTaskEventWriter {
    fn prepare(&self) -> TaskEventFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.js
                .get_or_create_stream(jetstream::stream::Config {
                    name: TASK_EVENT_STREAM.to_owned(),
                    subjects: vec![TASK_EVENT_SUBJECT.to_owned()],
                    retention: jetstream::stream::RetentionPolicy::WorkQueue,
                    ..Default::default()
                })
                .await
                .map(|_| ())
                .map_err(|error| format!("get_or_create task-event stream: {error}"))
        })
    }

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_task_event: &'a [u8],
    ) -> TaskEventFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mut headers = HeaderMap::new();
            headers.insert(MESSAGE_ID_HEADER, identity);
            self.js
                .publish_with_headers(
                    TASK_EVENT_SUBJECT,
                    headers,
                    encoded_task_event.to_vec().into(),
                )
                .await
                .map_err(|error| format!("stage TaskEvent: {error}"))?
                .await
                .map_err(|error| format!("prove staged TaskEvent: {error}"))?;
            Ok(())
        })
    }
}
#[derive(Clone)]
pub struct NatsTaskDispatchPublisher {
    js: jetstream::Context,
}

impl NatsTaskDispatchPublisher {
    pub fn new(nats: &async_nats::Client) -> Self {
        Self {
            js: jetstream::new(nats.clone()),
        }
    }
}

impl TaskDispatchPublisher for NatsTaskDispatchPublisher {
    fn prepare(&self) -> TaskDispatchFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.js
                .get_or_create_stream(jetstream::stream::Config {
                    name: TASK_DISPATCH_STREAM.to_owned(),
                    subjects: vec![TASK_DISPATCH_SUBJECT.to_owned()],
                    retention: jetstream::stream::RetentionPolicy::WorkQueue,
                    ..Default::default()
                })
                .await
                .map(|_| ())
                .map_err(|error| format!("get_or_create task-dispatch stream: {error}"))
        })
    }

    fn stage<'a>(
        &'a self,
        identity: &'a str,
        encoded_dispatch: &'a [u8],
    ) -> TaskDispatchFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mut headers = HeaderMap::new();
            headers.insert(MESSAGE_ID_HEADER, identity);
            self.js
                .publish_with_headers(
                    TASK_DISPATCH_SUBJECT,
                    headers,
                    encoded_dispatch.to_vec().into(),
                )
                .await
                .map_err(|error| format!("stage TaskDispatch: {error}"))?
                .await
                .map_err(|error| format!("prove staged TaskDispatch: {error}"))?;
            Ok(())
        })
    }
}

pub enum TaskEventProjection {
    Forward(Option<crate::patch_pipeline::ParsedPatch>),
    Retry(Duration),
}

pub trait TaskEventProjector: Send + Sync {
    fn project<'a>(
        &'a self,
        task_event: &'a mut tc::TaskEvent,
    ) -> TaskEventFuture<'a, TaskEventProjection>;

    fn after_forwarded(
        &self,
        workflow_instance_id: Uuid,
        task_id: Uuid,
        patch: crate::patch_pipeline::ParsedPatch,
    ) -> TaskEventFuture<'static, ()>;
}

#[derive(Clone)]
pub struct NatsTaskEventProjector {
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    nats: async_nats::Client,
}

impl NatsTaskEventProjector {
    pub fn new(
        definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
        nats: async_nats::Client,
    ) -> Self {
        Self {
            definition_repository,
            nats,
        }
    }
}

impl TaskEventProjector for NatsTaskEventProjector {
    fn project<'a>(
        &'a self,
        task_event: &'a mut tc::TaskEvent,
    ) -> TaskEventFuture<'a, TaskEventProjection> {
        Box::pin(async move {
            match crate::routing_enrichment::enrich_completed_task_event(
                self.definition_repository.as_ref(),
                &self.nats,
                task_event,
            )
            .await
            {
                Ok(()) => {}
                Err(e @ crate::routing_enrichment::EnrichmentError::LookupIntegrity { .. }) => {
                    eprintln!(
                        "routing-variable enrichment failed for task {:?}: {} (fail closed: not forwarding; delayed redelivery)",
                        task_event.task_instance_id, e
                    );
                    return TaskEventProjection::Retry(LOOKUP_INTEGRITY_RETRY_DELAY);
                }
                Err(e @ crate::routing_enrichment::EnrichmentError::SplitStageDrop { .. }) => {
                    tracing::error!(
                        "routing-variable enrichment failed for task {:?}: {} (fail closed: \
                         escalating completion to a terminal task failure)",
                        task_event.task_instance_id,
                        e
                    );
                    task_event.kind = Some(tc::task_event::Kind::Failed(tc::task_event::Failed {}));
                }
                Err(crate::routing_enrichment::EnrichmentError::Forwardable(e)) => {
                    eprintln!(
                        "routing-variable enrichment failed for task {:?}: {:#} (forwarding un-enriched)",
                        task_event.task_instance_id, e
                    );
                }
            }

            let mut pending_self_patch = None;
            if matches!(task_event.kind, Some(tc::task_event::Kind::Completed(_))) {
                let workflow_instance_id =
                    Uuid::parse_str(&task_event.workflow_instance_id).expect("validated TaskEvent");
                let task_instance_id =
                    Uuid::parse_str(&task_event.task_instance_id).expect("validated TaskEvent");
                let task_id = Uuid::parse_str(&task_event.task_id).expect("validated TaskEvent");
                if let Some(doc) = crate::routing_enrichment::read_self_patch_output(
                    &self.nats,
                    workflow_instance_id,
                    task_instance_id,
                )
                .await
                {
                    match crate::patch_pipeline::parse_self_patch_document(&doc).await {
                        Ok(parsed) => {
                            let key =
                                crate::patch_pipeline::patch_key(workflow_instance_id, task_id);
                            if let Some(tc::task_event::Kind::Completed(completed)) =
                                &mut task_event.kind
                            {
                                completed.self_patch = Some(key.to_string());
                            }
                            pending_self_patch = Some(parsed);
                        }
                        Err(e) => {
                            eprintln!(
                                "self-patch document from task {:?} failed to parse: {} \
                                 (reshape lost; completion forwards unmarked)",
                                task_event.task_instance_id, e
                            );
                        }
                    }
                }
            }
            TaskEventProjection::Forward(pending_self_patch)
        })
    }

    fn after_forwarded(
        &self,
        workflow_instance_id: Uuid,
        task_id: Uuid,
        patch: crate::patch_pipeline::ParsedPatch,
    ) -> TaskEventFuture<'static, ()> {
        let definition_repository = Arc::clone(&self.definition_repository);
        let nats = self.nats.clone();
        Box::pin(async move {
            fork_self_patch(
                definition_repository.as_ref(),
                &nats,
                workflow_instance_id,
                task_id,
                patch,
            )
            .await;
        })
    }
}

/// Get-or-create the durable task-dispatch **work queue** the conductor
/// publishes dispatched tasks into. A JetStream work queue: an unpicked or
/// relay-blipped dispatch waits durably here instead of being lost on
/// fire-and-forget core NATS. `get_or_create` is idempotent and the config
/// matches the executor's consumer-init side (`dispatch_consumer`) exactly.
pub async fn ensure_task_dispatch_stream(nats: &async_nats::Client) -> Result<()> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: TASK_DISPATCH_STREAM.to_string(),
        subjects: vec![TASK_DISPATCH_SUBJECT.to_string()],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_or_create task-dispatch stream: {}", e))?;
    Ok(())
}

/// Get-or-create the durable conductor→executor cancel-request **work queue**.
/// The conductor publishes cancel-requests here; the executor drains them. A
/// JetStream work queue so a cancel-request survives a relay blip rather than
/// being lost on fire-and-forget core NATS. `get_or_create` is idempotent and
/// the config matches the executor's consumer-init side exactly.
pub async fn ensure_task_cancel_stream(nats: &async_nats::Client) -> Result<()> {
    let js = jetstream::new(nats.clone());
    js.get_or_create_stream(jetstream::stream::Config {
        name: TASK_CANCEL_STREAM.to_string(),
        subjects: vec![TASK_CANCEL_SUBJECT.to_string()],
        retention: jetstream::stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("get_or_create task-cancel stream: {}", e))?;
    Ok(())
}

/// Publish a cancel-request onto the durable task-cancel work queue for the
/// executor to drain, awaiting the publish ack so the request is durably staged
/// before the conductor moves on. Best-effort at the semantic level — the
/// server already grounded the task `Cancelled`; this only drives the kill.
pub async fn publish_task_cancel(nats: &async_nats::Client, payload: Vec<u8>) -> Result<()> {
    let js = jetstream::new(nats.clone());
    js.publish(TASK_CANCEL_SUBJECT, payload.into())
        .await
        .map_err(|e| anyhow::anyhow!("publish task cancel: {}", e))?
        .await
        .map_err(|e| anyhow::anyhow!("await task-cancel publish ack: {}", e))?;
    Ok(())
}

/// Get-or-create the durable executor→conductor cancel-ack **work queue** and
/// its shared pull consumer. Mirrors `task_event_consumer`: the executor
/// publishes acks here, and any conductor instance binding the shared durable
/// name load-balances the drain.
pub async fn cancel_ack_consumer(
    nats: &async_nats::Client,
) -> Result<jetstream::consumer::PullConsumer> {
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: TASK_CANCEL_ACK_STREAM.to_string(),
            subjects: vec![TASK_CANCEL_ACK_SUBJECT.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-cancel-ack stream: {}", e))?;
    let consumer = stream
        .get_or_create_consumer(
            TASK_CANCEL_ACK_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(TASK_CANCEL_ACK_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create task-cancel-ack consumer: {}", e))?;
    Ok(consumer)
}

#[derive(Clone)]
pub struct NatsTaskCancellationPublisher {
    nats: async_nats::Client,
}

impl NatsTaskCancellationPublisher {
    pub fn new(nats: &async_nats::Client) -> Self {
        Self { nats: nats.clone() }
    }
}

impl TaskCancellationPublisher for NatsTaskCancellationPublisher {
    fn prepare(&self) -> TaskCancellationFuture<'_, Result<(), String>> {
        Box::pin(async move {
            ensure_task_cancel_stream(&self.nats)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn stage<'a>(
        &'a self,
        encoded_cancellation: &'a [u8],
    ) -> TaskCancellationFuture<'a, Result<(), String>> {
        Box::pin(async move {
            publish_task_cancel(&self.nats, encoded_cancellation.to_vec())
                .await
                .map_err(|error| error.to_string())
        })
    }
}

#[derive(Clone)]
pub struct NatsTaskCancellationAckConsumer {
    consumer: jetstream::consumer::PullConsumer,
}

impl NatsTaskCancellationAckConsumer {
    pub fn new(consumer: jetstream::consumer::PullConsumer) -> Self {
        Self { consumer }
    }
}

struct NatsTaskCancellationAckDelivery {
    message: jetstream::Message,
}

impl TaskCancellationAckDelivery for NatsTaskCancellationAckDelivery {
    fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    fn complete(self: Box<Self>) -> TaskCancellationFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.message
                .ack()
                .await
                .map_err(|error| format!("cancellation acknowledgement failed: {error}"))
        })
    }

    fn retry(
        self: Box<Self>,
        delay: Option<Duration>,
    ) -> TaskCancellationFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.message
                .ack_with(jetstream::AckKind::Nak(delay))
                .await
                .map_err(|error| format!("cancellation acknowledgement retry failed: {error}"))
        })
    }
}

impl TaskCancellationAckConsumer for NatsTaskCancellationAckConsumer {
    fn next(
        &self,
    ) -> TaskCancellationFuture<'_, Result<Option<Box<dyn TaskCancellationAckDelivery>>, String>>
    {
        Box::pin(async move {
            let mut batch = self
                .consumer
                .batch()
                .max_messages(1)
                .expires(Duration::from_secs(1))
                .messages()
                .await
                .map_err(|error| format!("open cancellation acknowledgement batch: {error}"))?;
            match batch.next().await {
                Some(Ok(message)) => {
                    Ok(Some(Box::new(NatsTaskCancellationAckDelivery { message })
                        as Box<dyn TaskCancellationAckDelivery>))
                }
                Some(Err(error)) => {
                    Err(format!("cancellation acknowledgement pull failed: {error}"))
                }
                None => Ok(None),
            }
        })
    }
}

/// Drain executor cancel-acks off the durable queue and forward each onto the
/// relay as a `CANCEL_TASK_ACK` envelope, **acking on forward** (the durability
/// boundary is the conductor, same as `drain_task_events`). A relay blip leaves
/// the ack un-acked so the work queue redelivers it; the server's confirmation
/// flip is idempotent, so a duplicate forward is harmless.
pub async fn drain_cancel_acks(
    consumer: jetstream::consumer::PullConsumer,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    token: CancellationToken,
) {
    let mut messages = match consumer
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
    {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Failed to open cancel-ack consumer stream: {}", e);
            return;
        }
    };
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => break,
            next = messages.next() => match next {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    eprintln!("cancel-ack pull error: {}", e);
                    continue;
                }
                None => break,
            },
        };
        // Validate it decodes (as the published contract) before forwarding; a
        // poison message is acked-and-dropped rather than redelivered forever.
        if tc::CancelTaskAck::decode(&msg.payload[..]).is_err() {
            eprintln!("dropping undeserializable cancel-ack");
            let _ = msg.ack().await;
            continue;
        }
        let conductor_msg = ConductorRelayMessage {
            entity_type: EntityType::CancelTaskAck as i32,
            payload: msg.payload.to_vec(),
            tenant_id: None,
        };
        match relay_tx.send(conductor_msg).await {
            Ok(()) => {
                if let Err(e) = msg.ack().await {
                    eprintln!("cancel-ack ack failed: {} (redelivery converges)", e);
                }
            }
            Err(e) => {
                // Relay outbound closed (cycle ending). NAK so the ack stays in
                // the durable queue and redelivers on the next cycle.
                eprintln!("Error forwarding cancel-ack: {} (no ack; redelivers)", e);
                let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                break;
            }
        }
    }
}

pub async fn drain_cancellation_ack_source(
    consumer: Arc<dyn TaskCancellationAckConsumer>,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    token: CancellationToken,
) {
    loop {
        let delivery = tokio::select! {
            _ = token.cancelled() => break,
            delivery = consumer.next() => match delivery {
                Ok(Some(delivery)) => delivery,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(error) => {
                    eprintln!("cancellation acknowledgement pull failed: {error}");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            },
        };
        if tc::CancelTaskAck::decode(delivery.payload()).is_err() {
            eprintln!("dropping undeserializable cancellation acknowledgement");
            if let Err(error) = delivery.complete().await {
                eprintln!("failed to complete poison cancellation acknowledgement: {error}");
            }
            continue;
        }
        let conductor_msg = ConductorRelayMessage {
            entity_type: EntityType::CancelTaskAck as i32,
            payload: delivery.payload().to_vec(),
            tenant_id: None,
        };
        if let Err(error) = relay_tx.send(conductor_msg).await {
            eprintln!(
                "Error forwarding cancellation acknowledgement: {error} (pending; redelivers)"
            );
            let _ = delivery.retry(None).await;
            break;
        }
        if let Err(error) = delivery.complete().await {
            eprintln!(
                "cancellation acknowledgement completion failed: {error} (redelivery converges)"
            );
        }
    }
}

/// Publish a dispatched task into the durable task-dispatch work queue and,
/// **only after the publish ack**, emit the conductor's `Delivered` task event
/// onto the relay. Awaiting the ack is what sharpens `Delivered` from
/// "published to fire-and-forget core NATS" to "**durably staged** for an
/// executor": the `TaskDelivered` milestone the server publishes from this
/// event now means the work won't be lost even if no executor is up yet.
/// `delivered` carries no `executor_id` — no executor has picked it up.
pub async fn publish_dispatch_and_deliver(
    task_dispatch: &dyn TaskDispatchPublisher,
    relay_tx: &mpsc::Sender<ConductorRelayMessage>,
    payload: Vec<u8>,
    task_instance_id: Uuid,
    task_id: Uuid,
    workflow_instance_id: Uuid,
    workflow_id: Uuid,
) -> Result<()> {
    let identity = format!("dispatch:{task_instance_id}");
    task_dispatch
        .stage(&identity, &payload)
        .await
        .map_err(anyhow::Error::msg)?;

    // Post-ack: emit the `delivered` hand-off so the server publishes the
    // `TaskDelivered` milestone (durably-staged semantics). Authored directly on
    // the published task-coordination contract — no executor has picked the task
    // up yet, so `executor_id` is absent.
    let delivered = tc::TaskEvent {
        task_instance_id: task_instance_id.to_string(),
        task_id: task_id.to_string(),
        workflow_instance_id: workflow_instance_id.to_string(),
        workflow_id: workflow_id.to_string(),
        executor_id: None,
        kind: Some(tc::task_event::Kind::Delivered(
            tc::task_event::Delivered {},
        )),
    };
    let update_payload = delivered.encode_to_vec();
    let update_msg = ConductorRelayMessage {
        entity_type: EntityType::TaskEvent as i32,
        payload: update_payload,
        tenant_id: None,
    };
    relay_tx
        .send(update_msg)
        .await
        .map_err(|e| anyhow::anyhow!("send delivered task event: {}", e))?;
    Ok(())
}

/// Drain typed `TaskEvent`s off the durable update queue and forward each onto
/// the relay, **acking on forward** — the durability boundary is the conductor.
/// A relay/conductor blip (the relay send fails, or the conductor dies before
/// ack) leaves the event un-acked, so the work queue redelivers it on recovery
/// instead of dropping it — a completion can't wedge a run `InProgress`. The
/// completing task's declared routing variables are enriched onto the event
/// before forwarding; a lookup-integrity fault NAKs the completion back to
/// the queue (fail closed), an emitted-but-dropped declared variable
/// escalates the completion to a conductor-minted terminal task failure
/// (fail closed), and any other routing-var error forwards the un-enriched
/// event loudly rather than dropping the completion. The residual
/// conductor→server relay-hop gap (forwarded ≠ applied) is documented, not
/// closed.
pub async fn drain_task_event_source(
    consumer: Arc<dyn TaskEventConsumer>,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    projector: Arc<dyn TaskEventProjector>,
    token: CancellationToken,
) {
    loop {
        let delivery = tokio::select! {
            _ = token.cancelled() => break,
            next = consumer.next() => match next {
                Ok(Some(delivery)) => delivery,
                Ok(None) => continue,
                Err(error) => {
                    eprintln!("task-event pull error: {error}");
                    continue;
                }
            },
        };

        let mut task_event = match tc::TaskEvent::decode(delivery.payload()) {
            Ok(ev) => ev,
            Err(e) => {
                // Poison delivery is completed so it cannot redeliver forever.
                eprintln!("dropping undeserializable task event: {}", e);
                let _ = delivery.complete().await;
                continue;
            }
        };
        // Identity rides the wire as UUID strings. Parse the ids the drain acts
        // on once; a malformed id on an otherwise-decodable event is poison
        // (ack-drop) rather than an endless redelivery.
        let (event_wfi, event_tid) = match (
            Uuid::parse_str(&task_event.workflow_instance_id),
            Uuid::parse_str(&task_event.task_instance_id),
            Uuid::parse_str(&task_event.task_id),
        ) {
            (Ok(wfi), Ok(_), Ok(tid)) => (wfi, tid),
            _ => {
                eprintln!(
                    "dropping task event with malformed id(s): task_instance_id={:?}",
                    task_event.task_instance_id
                );
                let _ = delivery.complete().await;
                continue;
            }
        };
        println!(
            "Received TaskEvent from durable source: task_id={:?}, kind={:?}",
            task_event.task_instance_id, task_event.kind
        );

        let mut pending_self_patch = match projector.project(&mut task_event).await {
            TaskEventProjection::Forward(pending_self_patch) => pending_self_patch,
            TaskEventProjection::Retry(delay) => {
                let _ = delivery.retry(Some(delay)).await;
                continue;
            }
        };

        // Re-encode on the published contract (the event may now carry routing
        // variables) and forward to the conductor relay stream, then ack on
        // forward.
        {
            let payload = task_event.encode_to_vec();
            {
                let conductor_msg = ConductorRelayMessage {
                    entity_type: EntityType::TaskEvent as i32,
                    payload,
                    tenant_id: None,
                };
                match relay_tx.send(conductor_msg).await {
                    Ok(()) => {
                        // Complete only after relay-channel forwarding.
                        if let Err(e) = delivery.complete().await {
                            eprintln!("task-event completion failed: {e} (redelivery converges)");
                        }
                        // Fork the detected self-patch into the patch
                        // pipeline, AFTER the completion forwarded (FIFO: the
                        // server's Stall arms before any apply envelope).
                        // `patch_id = node_id` makes the row key
                        // `UUIDv5(instance, node_id)` — attempt-invariant, so
                        // a redelivered or retried completion replays the
                        // same row instead of re-applying. A persist failure
                        // here leaves the Stall to the server's TTL backstop:
                        // loud, and the reshape is lost-but-logged.
                        if let Some(parsed) = pending_self_patch.take() {
                            projector
                                .after_forwarded(event_wfi, event_tid, parsed)
                                .await;
                        }
                    }
                    Err(e) => {
                        // Relay outbound closed (cycle ending). Do NOT ack — NAK
                        // so the event stays in the durable queue and redelivers
                        // on the next cycle; this is the outage-survival path
                        // that keeps a completion from being lost to a relay blip.
                        eprintln!(
                            "Error forwarding task event to conductor: {} (no ack; redelivers)",
                            e
                        );
                        let _ = delivery.retry(None).await;
                        break;
                    }
                }
            }
        }
    }
}

/// Fresh all-NATS compatibility entry point used by existing focused laws.
pub async fn drain_task_events(
    consumer: jetstream::consumer::PullConsumer,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    nats: async_nats::Client,
    token: CancellationToken,
) {
    let projector = Arc::new(NatsTaskEventProjector::new(definition_repository, nats));
    drain_task_event_source(
        Arc::new(NatsTaskEventConsumer::new(consumer)),
        relay_tx,
        projector,
        token,
    )
    .await;
}

/// Ingress a self-patch parsed off a completion drain into the patch
/// pipeline: one row keyed `UUIDv5(instance, node_id)`, then relay (no new
/// tasks) or build (task-bearing `AddNode`s — jobs published onto the patch
/// build queue). Every outcome is logged; nothing here fails the completion,
/// which already forwarded and acked.
async fn fork_self_patch(
    repositories: &tickr_migrations::backend::WriterRepositoryBundle,
    nats: &async_nats::Client,
    workflow_instance_id: Uuid,
    task_id: Uuid,
    parsed: crate::patch_pipeline::ParsedPatch,
) {
    use crate::patch_pipeline::{process_patch, DefaultPatchRelaySender, PatchIngress};
    match process_patch(
        repositories,
        &DefaultPatchRelaySender,
        workflow_instance_id,
        // The emitting node's definition id is the author key — retried
        // completions re-run the same node, so they land on the same row.
        task_id,
        parsed,
        // Emitted from a task's reserved ctx patch output — a self-patch.
        crate::patch_pipeline::PatchProvenance::SelfEmitted,
    )
    .await
    {
        Ok(PatchIngress::Accepted {
            patch_key,
            build_jobs,
            ..
        }) => {
            println!(
                "self-patch {} ingressed for instance {} ({} build job(s))",
                patch_key,
                workflow_instance_id,
                build_jobs.len()
            );
            if !build_jobs.is_empty() {
                if let Err(e) =
                    crate::patch_pipeline::publish_patch_build_jobs(nats, &build_jobs).await
                {
                    eprintln!(
                        "self-patch {} build-job publish failed: {} \
                         (row stays Building; stall-TTL backstop releases the instance)",
                        patch_key, e
                    );
                }
            }
        }
        Ok(PatchIngress::RejectedInProgress {
            patch_key, reason, ..
        }) => {
            eprintln!(
                "self-patch {} rejected for instance {}: {}",
                patch_key, workflow_instance_id, reason
            );
        }
        Ok(PatchIngress::Replayed { row }) => {
            println!(
                "self-patch {} replayed (redelivered completion): status {}",
                row.patch_key, row.status
            );
        }
        Err(e) => {
            eprintln!(
                "self-patch ingress failed for instance {} node {}: {} \
                 (reshape lost; stall-TTL backstop releases the instance)",
                workflow_instance_id, task_id, e
            );
        }
    }
}

/// Get-or-create the dedicated liveness KV bucket with the spike-pinned config,
/// mirroring the executor's `ensure_liveness_bucket` exactly (the conductor may
/// boot before any executor has armed a key, so the consumer side ensures the
/// bucket — and thus its backing stream — exists before binding). `history: 1`
/// (`MaxMsgsPerSubject = 1`) and `limit_markers = Some(TTL)` flip on per-key TTL
/// and the subject-delete-marker emission; the marker TTL is the verdict-
/// durability window. Idempotent.
pub async fn ensure_liveness_bucket(nats: &async_nats::Client) -> Result<jetstream::kv::Store> {
    let js = jetstream::new(nats.clone());
    if let Ok(store) = js.get_key_value(LIVENESS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(jetstream::kv::Config {
        bucket: LIVENESS_BUCKET.to_string(),
        history: 1,
        storage: jetstream::stream::StorageType::File,
        limit_markers: Some(LIVENESS_MARKER_TTL),
        ..Default::default()
    })
    .await
    .map_err(|e| anyhow::anyhow!("create liveness KV bucket: {}", e))
}

/// Bind the **single shared durable pull consumer** on the liveness bucket's
/// backing-stream wildcard (`$KV.<bucket>.>`). Bound once at startup, it holds
/// no per-task state — any conductor instance binding the same durable name
/// load-balances delivery, so the conductor stays stateless (the
/// `task_event_consumer` pattern). `get_or_create` is idempotent.
pub async fn liveness_marker_consumer(
    nats: &async_nats::Client,
) -> Result<jetstream::consumer::PullConsumer> {
    ensure_liveness_bucket(nats).await?;
    let js = jetstream::new(nats.clone());
    let stream = js
        .get_stream(format!("KV_{}", LIVENESS_BUCKET))
        .await
        .map_err(|e| anyhow::anyhow!("get liveness KV backing stream: {}", e))?;
    let consumer = stream
        .get_or_create_consumer(
            LIVENESS_MARKER_CONSUMER,
            jetstream::consumer::pull::Config {
                durable_name: Some(LIVENESS_MARKER_CONSUMER.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                filter_subject: format!("$KV.{}.>", LIVENESS_BUCKET),
                // Bound redelivery so a marker whose handling was interrupted
                // (the conductor died mid-forward, or its NAK was lost as the
                // relay cycle tore the consumer down) is re-offered within
                // seconds rather than after the 30s JetStream default — a
                // liveness verdict should not wait half a minute to retry.
                // Safe to make short: forwarding is an in-memory relay send
                // acked immediately, so a successfully-handled marker is acked
                // well inside this window, and the server's idempotency guard
                // absorbs any rare duplicate forward.
                ack_wait: std::time::Duration::from_secs(5),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("get_or_create liveness marker consumer: {}", e))?;
    Ok(consumer)
}

async fn pickup_server_time(pickup: &jetstream::kv::Store) -> Result<i64> {
    let key = format!("watchdog.clock.{}", Uuid::new_v4().simple());
    pickup
        .put(&key, Vec::new().into())
        .await
        .map_err(|error| anyhow::anyhow!("write NATS server-time probe: {error}"))?;
    let entry = pickup
        .entry(&key)
        .await
        .map_err(|error| anyhow::anyhow!("read NATS server-time probe: {error}"))?
        .ok_or_else(|| anyhow::anyhow!("NATS server-time probe disappeared"))?;
    let _ = pickup.delete(&key).await;
    i64::try_from(entry.created.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| anyhow::anyhow!("NATS server time does not fit milliseconds"))
}

async fn load_pickup_record(
    pickup: &jetstream::kv::Store,
    key: &str,
) -> Result<Option<(TaskPickupRecord, u64)>> {
    let Some(entry) = pickup
        .entry(key)
        .await
        .map_err(|error| anyhow::anyhow!("read pickup outcome `{key}`: {error}"))?
    else {
        return Ok(None);
    };
    let record = serde_json::from_slice(&entry.value)
        .map_err(|error| anyhow::anyhow!("decode pickup outcome `{key}`: {error}"))?;
    Ok(Some((record, entry.revision)))
}

async fn update_pickup_record(
    pickup: &jetstream::kv::Store,
    key: &str,
    record: &TaskPickupRecord,
    revision: u64,
) -> Result<()> {
    let bytes = serde_json::to_vec(record)
        .map_err(|error| anyhow::anyhow!("encode pickup outcome `{key}`: {error}"))?;
    pickup
        .update(key, bytes.into(), revision)
        .await
        .map(|_| ())
        .map_err(|error| anyhow::anyhow!("update pickup outcome `{key}`: {error}"))
}

fn unhealthy_event(record: &TaskPickupRecord) -> Result<Vec<u8>> {
    let task = tc::TaskDispatch::decode(&record.payload[..])
        .map_err(|error| anyhow::anyhow!("decode due TaskDispatch: {error}"))?;
    Ok(tc::TaskEvent {
        task_instance_id: task.task_instance_id,
        task_id: task.task_id,
        workflow_instance_id: task.workflow_instance_id,
        workflow_id: task.workflow_id,
        executor_id: None,
        kind: Some(tc::task_event::Kind::Unhealthy(
            tc::task_event::Unhealthy {},
        )),
    }
    .encode_to_vec())
}

async fn reconcile_pickup_outcome(
    task_events: &dyn TaskEventWriter,
    pickup: &jetstream::kv::Store,
    key: &str,
    server_time_ms: i64,
) -> Result<()> {
    let Some((mut record, revision)) = load_pickup_record(pickup, key).await? else {
        return Ok(());
    };
    if record.liveness_is_due(server_time_ms) {
        let event = unhealthy_event(&record)?;
        match record.elect_due_liveness(server_time_ms, &event) {
            ElectionDecision::Won => {
                if update_pickup_record(pickup, key, &record, revision)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            ElectionDecision::Settled(_) | ElectionDecision::Rejected => {}
        }
    }

    for _ in 0..OUTCOME_ELECTION_RETRIES {
        let Some((mut record, revision)) = load_pickup_record(pickup, key).await? else {
            return Ok(());
        };
        let Some(elected) = record.terminal.as_mut() else {
            return Ok(());
        };
        if elected.event_enqueued {
            return Ok(());
        }
        let event = elected.event.clone();
        let identity = format!("{}.terminal", record.dispatch_key);
        task_events
            .stage(&identity, &event)
            .await
            .map_err(|error| anyhow::anyhow!("stage elected terminal TaskEvent: {error}"))?;
        record
            .terminal
            .as_mut()
            .expect("elected outcome was checked above")
            .event_enqueued = true;
        if update_pickup_record(pickup, key, &record, revision)
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
    Err(anyhow::anyhow!(
        "terminal TaskEvent enqueue kept losing conditional updates for `{key}`"
    ))
}

async fn sweep_attempt_outcomes_once(
    task_events: &dyn TaskEventWriter,
    pickup: &jetstream::kv::Store,
    keys: &mut jetstream::kv::Keys,
) -> Result<bool> {
    let server_time_ms = pickup_server_time(pickup).await?;
    let mut scanned = 0;
    while scanned < OUTCOME_SWEEP_BATCH {
        let Some(key) = keys.next().await else {
            return Ok(true);
        };
        let key = key.map_err(|error| anyhow::anyhow!("scan all-NATS pickup deadline: {error}"))?;
        if !key.starts_with("dispatch.") {
            continue;
        }
        scanned += 1;
        if let Err(error) =
            reconcile_pickup_outcome(task_events, pickup, &key, server_time_ms).await
        {
            eprintln!("all-NATS outcome reconciliation failed for `{key}`: {error}");
        }
    }
    Ok(false)
}

/// Periodically reconcile durable pickup deadlines and enqueue elected terminal
/// TaskEvents. Per-key TTL markers only accelerate this scan; their identity and
/// timing never author a verdict. The TaskEvent work queue remains authoritative
/// until `drain_task_events` acknowledges relay-channel forwarding.
pub async fn drain_attempt_outcomes(
    nats: async_nats::Client,
    consumer: Option<jetstream::consumer::PullConsumer>,
    token: CancellationToken,
) {
    let task_events = Arc::new(NatsTaskEventWriter::new(&nats));
    if let Err(error) = task_events.prepare().await {
        eprintln!("prepare all-NATS TaskEvents writer: {error}");
        return;
    }
    drain_attempt_outcomes_with_writer(nats, consumer, task_events, token).await;
}

pub async fn drain_attempt_outcomes_with_writer(
    nats: async_nats::Client,
    consumer: Option<jetstream::consumer::PullConsumer>,
    task_events: Arc<dyn TaskEventWriter>,
    token: CancellationToken,
) {
    let js = jetstream::new(nats);
    let pickup = match js.get_key_value(TASK_PICKUP_BUCKET).await {
        Ok(pickup) => pickup,
        Err(error) => {
            eprintln!("open all-NATS pickup outcome store: {error}");
            return;
        }
    };
    let mut markers = match consumer {
        Some(consumer) => match consumer
            .stream()
            .max_messages_per_batch(OUTCOME_SWEEP_BATCH)
            .messages()
            .await
        {
            Ok(markers) => Some(markers),
            Err(error) => {
                eprintln!("optional liveness wakeup stream unavailable: {error}");
                None
            }
        },
        None => None,
    };
    let mut cadence = tokio::time::interval(Duration::from_secs(1));
    let mut outcome_keys = None;
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = cadence.tick() => {}
            marker = async {
                markers
                    .as_mut()
                    .expect("marker branch is disabled without a stream")
                    .next()
                    .await
            }, if markers.is_some() => {
                match marker {
                    Some(Ok(marker)) => {
                        let _ = marker.ack().await;
                    }
                    Some(Err(error)) => {
                        eprintln!("optional liveness wakeup pull failed: {error}");
                    }
                    None => markers = None,
                }
            }
        }
        if outcome_keys.is_none() {
            outcome_keys = match pickup.keys().await {
                Ok(keys) => Some(keys),
                Err(error) => {
                    eprintln!("list all-NATS pickup deadlines: {error}");
                    None
                }
            };
        }
        if let Some(keys) = outcome_keys.as_mut() {
            match sweep_attempt_outcomes_once(task_events.as_ref(), &pickup, keys).await {
                Ok(true) => outcome_keys = None,
                Ok(false) => {}
                Err(error) => {
                    eprintln!("all-NATS outcome sweep failed: {error}");
                    outcome_keys = None;
                }
            }
        }
    }
}

/// Sends a workflow build update through the relay system

/// Self-healing wrapper around the relay stream. Retries the connect+stream
/// loop on any error so that startup ordering against the Control-plane Conductor relay
/// (and transient mid-flight drops) doesn't leave the conductor wedged.
#[derive(Clone)]
enum TaskEventRoles {
    AllNats,
    Selected {
        consumer: Arc<dyn TaskEventConsumer>,
        writer: Arc<dyn TaskEventWriter>,
    },
}
#[derive(Clone)]
enum TaskDispatchRole {
    AllNats,
    Selected(Arc<dyn TaskDispatchPublisher>),
}
#[derive(Clone)]
enum TaskCancellationRoles {
    AllNats,
    Selected {
        publisher: Arc<dyn TaskCancellationPublisher>,
        acknowledgements: Arc<dyn TaskCancellationAckConsumer>,
    },
}
#[derive(Clone)]
enum CompactionStagingRole {
    AllNats,
    Selected(Arc<dyn CompactionStaging>),
}

async fn rebuild_gate_index_for_reconnect(client: &DispatchGatesClient, tenant: TenantId) {
    match crate::gate_index_lifecycle::rebuild_from_server(client, tenant).await {
        Ok(0) => {}
        Ok(count) => {
            println!("gate_index rebuild: repopulated {count} dispatched gate(s) from server");
        }
        Err(DispatchGatesError::Unauthenticated) => {
            eprintln!("gate_index rebuild: Control-plane authentication rejected; index degraded to empty");
        }
        Err(DispatchGatesError::Forbidden) => {
            eprintln!("gate_index rebuild: Control-plane Tenant binding rejected; index degraded to empty");
        }
        Err(DispatchGatesError::Timeout) => {
            eprintln!(
                "gate_index rebuild: Control-plane snapshot timed out; index degraded to empty"
            );
        }
        Err(DispatchGatesError::Unavailable) => {
            eprintln!("gate_index rebuild: Control-plane snapshot peer unavailable; index degraded to empty");
        }
        Err(error) => {
            eprintln!("gate_index rebuild: {error}; index degraded to empty");
        }
    }
}

pub async fn run_streaming(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    signal_applied_notifier: Arc<dyn SignalAppliedNotifier>,
    gate_snapshot_client: DispatchGatesClient,
    tenant: TenantId,
) -> Result<()> {
    run_streaming_with_role_selection(
        shutdown_token,
        relay_config,
        definition_repository,
        signal_applied_notifier,
        gate_snapshot_client,
        tenant,
        TaskEventRoles::AllNats,
        TaskDispatchRole::AllNats,
        TaskCancellationRoles::AllNats,
        CompactionStagingRole::AllNats,
    )
    .await
}

pub async fn run_streaming_with_task_events(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    task_event_consumer: Arc<dyn TaskEventConsumer>,
    task_event_writer: Arc<dyn TaskEventWriter>,
    signal_applied_notifier: Arc<dyn SignalAppliedNotifier>,
    gate_snapshot_client: DispatchGatesClient,
    tenant: TenantId,
) -> Result<()> {
    run_streaming_with_role_selection(
        shutdown_token,
        relay_config,
        definition_repository,
        signal_applied_notifier,
        gate_snapshot_client,
        tenant,
        TaskEventRoles::Selected {
            consumer: task_event_consumer,
            writer: task_event_writer,
        },
        TaskDispatchRole::AllNats,
        TaskCancellationRoles::AllNats,
        CompactionStagingRole::AllNats,
    )
    .await
}
pub async fn run_streaming_with_roles(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    task_event_consumer: Arc<dyn TaskEventConsumer>,
    task_event_writer: Arc<dyn TaskEventWriter>,
    task_dispatch: Arc<dyn TaskDispatchPublisher>,
    task_cancellation: Arc<dyn TaskCancellationPublisher>,
    cancellation_acknowledgements: Arc<dyn TaskCancellationAckConsumer>,
    compaction_staging: Arc<dyn CompactionStaging>,
    signal_applied_notifier: Arc<dyn SignalAppliedNotifier>,
    gate_snapshot_client: DispatchGatesClient,
    tenant: TenantId,
) -> Result<()> {
    run_streaming_with_role_selection(
        shutdown_token,
        relay_config,
        definition_repository,
        signal_applied_notifier,
        gate_snapshot_client,
        tenant,
        TaskEventRoles::Selected {
            consumer: task_event_consumer,
            writer: task_event_writer,
        },
        TaskDispatchRole::Selected(task_dispatch),
        TaskCancellationRoles::Selected {
            publisher: task_cancellation,
            acknowledgements: cancellation_acknowledgements,
        },
        CompactionStagingRole::Selected(compaction_staging),
    )
    .await
}

async fn run_streaming_with_role_selection(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    signal_applied_notifier: Arc<dyn SignalAppliedNotifier>,
    gate_snapshot_client: DispatchGatesClient,
    tenant: TenantId,
    task_events: TaskEventRoles,
    task_dispatch: TaskDispatchRole,
    task_cancellation: TaskCancellationRoles,
    compaction_staging: CompactionStagingRole,
) -> Result<()> {
    use std::time::{Duration, Instant};

    const INITIAL_BACKOFF_MS: u64 = 250;
    const MAX_BACKOFF_MS: u64 = 5_000;
    const STABLE_THRESHOLD: Duration = Duration::from_secs(5);

    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        if shutdown_token.is_cancelled() {
            return Ok(());
        }

        rebuild_gate_index_for_reconnect(&gate_snapshot_client, tenant).await;

        let started = Instant::now();
        match try_run_streaming(
            shutdown_token.clone(),
            &relay_config,
            Arc::clone(&definition_repository),
            Arc::clone(&signal_applied_notifier),
            task_events.clone(),
            task_dispatch.clone(),
            task_cancellation.clone(),
            compaction_staging.clone(),
        )
        .await
        {
            Ok(()) => {
                if shutdown_token.is_cancelled() {
                    return Ok(());
                }
                eprintln!("Conductor relay stream ended; reconnecting...");
            }
            Err(e) => {
                eprintln!(
                    "Conductor relay error: {}; reconnecting in {}ms...",
                    e, backoff_ms
                );
            }
        }

        // Reset backoff if the previous attempt held the connection long enough
        // to count as "stable" — keeps recovery snappy after a one-off blip.
        if started.elapsed() >= STABLE_THRESHOLD {
            backoff_ms = INITIAL_BACKOFF_MS;
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            _ = shutdown_token.cancelled() => return Ok(()),
        }
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

/// One attempt at the bidirectional relay stream. Returns when the stream ends
/// or a transport error occurs. Caller (`run_streaming`) decides whether to retry.
async fn try_run_streaming(
    shutdown_token: CancellationToken,
    relay_config: &ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    signal_applied_notifier: Arc<dyn SignalAppliedNotifier>,
    task_events: TaskEventRoles,
    task_dispatch: TaskDispatchRole,
    task_cancellation: TaskCancellationRoles,
    compaction_staging: CompactionStagingRole,
) -> Result<()> {
    // Connect to NATS
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;
    let (task_event_consumer, task_event_writer): (
        Arc<dyn TaskEventConsumer>,
        Arc<dyn TaskEventWriter>,
    ) = match task_events {
        TaskEventRoles::AllNats => (
            Arc::new(NatsTaskEventConsumer::new(
                task_event_consumer(&nats).await?,
            )),
            Arc::new(NatsTaskEventWriter::new(&nats)),
        ),
        TaskEventRoles::Selected { consumer, writer } => (consumer, writer),
    };
    let task_dispatch: Arc<dyn TaskDispatchPublisher> = match task_dispatch {
        TaskDispatchRole::AllNats => Arc::new(NatsTaskDispatchPublisher::new(&nats)),
        TaskDispatchRole::Selected(publisher) => publisher,
    };
    let (task_cancellation, cancellation_acknowledgements): (
        Arc<dyn TaskCancellationPublisher>,
        Arc<dyn TaskCancellationAckConsumer>,
    ) = match task_cancellation {
        TaskCancellationRoles::AllNats => (
            Arc::new(NatsTaskCancellationPublisher::new(&nats)),
            Arc::new(NatsTaskCancellationAckConsumer::new(
                cancel_ack_consumer(&nats).await?,
            )),
        ),
        TaskCancellationRoles::Selected {
            publisher,
            acknowledgements,
        } => (publisher, acknowledgements),
    };
    let compaction_staging: Arc<dyn CompactionStaging> = match compaction_staging {
        CompactionStagingRole::AllNats => Arc::new(AllNatsCompactionStaging::new(nats.clone())),
        CompactionStagingRole::Selected(staging) => staging,
    };
    compaction_staging
        .prepare()
        .await
        .map_err(anyhow::Error::msg)?;
    task_dispatch.prepare().await.map_err(anyhow::Error::msg)?;
    task_cancellation
        .prepare()
        .await
        .map_err(anyhow::Error::msg)?;
    task_event_writer
        .prepare()
        .await
        .map_err(anyhow::Error::msg)?;

    // The endpoint policy was admitted before the runtime started. HTTPS
    // activates standard Web-PKI roots and verifies the URI hostname.
    let mut client = connect_relay_client(relay_config).await?;

    // Create channel for bidirectional communication
    let (tx, rx) = mpsc::channel::<ConductorRelayMessage>(32);
    let tx_for_updates = tx.clone();

    // Store the sender in the global RELAY_TX for access from other components
    {
        let mut tx_guard = RELAY_TX.lock().await;
        *tx_guard = Some(tx.clone());
    }

    let tx_for_nats = tx_for_updates.clone();
    let projector = Arc::new(NatsTaskEventProjector::new(
        Arc::clone(&definition_repository),
        nats.clone(),
    ));

    // Per-cycle token: cancelled when this attempt ends so the forwarder task
    // doesn't outlive its mpsc receiver across reconnects.
    let cycle_token = CancellationToken::new();
    let forwarder_token = cycle_token.clone();
    let _drop_guard = cycle_token.clone().drop_guard();

    let _forwarder_handle = tokio::spawn(drain_task_event_source(
        task_event_consumer,
        tx_for_nats,
        projector,
        forwarder_token,
    ));

    // Durable pickup deadlines are authoritative. Per-key TTL markers are only
    // optional wakeups for the bounded competing sweep; marker loss or consumer
    // setup failure cannot erase or author a verdict.
    let liveness_consumer = match liveness_marker_consumer(&nats).await {
        Ok(consumer) => Some(consumer),
        Err(error) => {
            eprintln!("optional liveness wakeup consumer unavailable: {error}");
            None
        }
    };
    let outcome_nats = nats.clone();
    let outcome_token = cycle_token.clone();
    let _outcome_handle = tokio::spawn(drain_attempt_outcomes_with_writer(
        outcome_nats,
        liveness_consumer,
        task_event_writer,
        outcome_token,
    ));

    // The selected acknowledgement source remains pending until the exact
    // retained bytes cross the Conductor relay forwarding boundary.
    let cancel_ack_tx = tx_for_updates.clone();
    let cancel_ack_token = cycle_token.clone();
    let _cancel_ack_handle = tokio::spawn(drain_cancellation_ack_source(
        cancellation_acknowledgements,
        cancel_ack_tx,
        cancel_ack_token,
    ));

    // Create outbound stream that takes messages from the mpsc channel
    let outbound = async_stream::stream! {
        let mut rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Initial message to establish the connection. It self-asserts this
        // conductor's tenant on the Control-plane relay channel at handshake — the Frontend
        // captures it into connection state and stamps every forwarded envelope
        // from that state, so the tenant rides addressing metadata, never a
        // decoded payload id. Derived from the operator-set slug: every
        // conductor of a tenant asserts the same id.
        let initial_message = ConductorRelayMessage {
            entity_type: 0, // Default value
            payload: Vec::new(),
            tenant_id: Some(TenantId::from_env().to_string()),
        };
        yield initial_message;

        while let Some(msg) = rx_stream.next().await {
            yield msg;
        }
    };

    let mut inbound = establish_relay_stream(&mut client, outbound, relay_config).await?;
    let nats_clone = nats.clone();
    let task_dispatch_clone = task_dispatch.clone();
    let compaction_staging_clone = compaction_staging.clone();
    let task_cancellation_clone = task_cancellation.clone();

    tokio::select! {
        res = async {
            while let Some(message) = inbound.next().await {
                match message {
                    Ok(msg) => {
                        match EntityType::try_from(msg.entity_type) {
                            Ok(EntityType::TaskQueueItem) => {
                                // 1. Decode the published dispatch contract and print it
                                if let Ok(task_queue_item) = tc::TaskDispatch::decode(&msg.payload[..]) {
                                    println!("Received TaskQueueItem: task_id={}", task_queue_item.task_instance_id);

                                    // 1a. For trigger-originated tasks, reconcile the (signal_id
                                    //     → run_id) linkage and rehydrate any missing NATS
                                    //     capture keys from the SQL repository BEFORE forwarding
                                    //     to the task queue. This guarantees the executor sees the
                                    //     captures by the time it pulls the task — a conductor
                                    //     crash after ingress can leave the NATS cache empty even
                                    //     though the durable Event variables remain available.
                                    if let Some(signal_id) = task_queue_item.originating_signal_id.as_deref()
                                        .and_then(|id| Uuid::parse_str(id).ok()) {
                                        if let Ok(wi_id) = Uuid::parse_str(&task_queue_item.workflow_instance_id) {
                                        if let Err(e) = crate::instance_creation_linkage::link_and_rehydrate(
                                            definition_repository.as_ref(),
                                            &nats_clone,
                                            signal_id,
                                            wi_id,
                                        )
                                        .await
                                        {
                                            eprintln!(
                                                "instance-creation linkage failed for signal {} / run {}: {} (forwarding the task anyway; rehydration will retry on subsequent TaskQueueItems for this run)",
                                                signal_id, wi_id, e
                                            );
                                        }
                                        }
                                    }

                                    // 1b. Mirror the run's live task graph into
                                    //     the run-scoped ctx KV under
                                    //     `tickr_graph` so a self-patching task
                                    //     reads the graph — identity code on
                                    //     every structure — straight from NATS,
                                    //     never the server. Written once at
                                    //     materialization (present-key guarded);
                                    //     covers cron- and trigger-fired runs
                                    //     alike. Best-effort: a failure only
                                    //     leaves the advisory mirror absent, it
                                    //     never blocks dispatch.
                                    {
                                        let run_id = Uuid::parse_str(&task_queue_item.workflow_instance_id);
                                        let workflow_id = Uuid::parse_str(&task_queue_item.workflow_id);
                                        if let (Ok(run_id), Ok(workflow_id)) = (run_id, workflow_id) {
                                        if let Err(e) = crate::ctx_graph_mirror::mirror_ctx_graph(
                                            definition_repository.as_ref(),
                                            &nats_clone,
                                            run_id,
                                            workflow_id,
                                        )
                                        .await
                                        {
                                            eprintln!(
                                                "ctx graph mirror failed for run {} (workflow {}): {} (dispatch continues; the mirror is advisory)",
                                                run_id, workflow_id, e
                                            );
                                        }
                                        }
                                    }

                                    // 2. Publish the dispatch into the durable
                                    //    work queue and, only after the publish
                                    //    ack, emit the conductor's `delivered`
                                    //    task event — the dispatch hand-off.
                                    //    `Delivered` now means "durably staged":
                                    //    the task waits in the substrate for an
                                    //    executor to pull, surviving a relay blip.
                                    if let Err(e) = publish_dispatch_and_deliver(
                                        task_dispatch_clone.as_ref(),
                                        &tx_for_updates,
                                        msg.payload.to_vec(),
                                        Uuid::parse_str(&task_queue_item.task_instance_id)?,
                                        Uuid::parse_str(&task_queue_item.task_id)?,
                                        Uuid::parse_str(&task_queue_item.workflow_instance_id)?,
                                        Uuid::parse_str(&task_queue_item.workflow_id)?,
                                    ).await {
                                        eprintln!("Failed to dispatch task into work queue: {}", e);
                                        continue;
                                    }
                                } else {
                                    eprintln!("Failed to deserialize TaskQueueRepoItem");
                                }
                            },
                            Ok(EntityType::CancelTask) => {
                                // The selected TaskCancellation adapter fsync-proves
                                // the exact request/generation/owner fence before
                                // owner notification. Failure leaves the request
                                // recoverable and crosses no source acknowledgement.
                                if let Err(error) = task_cancellation_clone
                                    .stage(&msg.payload)
                                    .await
                                {
                                    eprintln!(
                                        "Failed to stage cancellation through selected role: {error}"
                                    );
                                }
                            },
                            Ok(EntityType::Compaction) => {
                                // Server is shipping a terminal workflow + its task_instances
                                // for archival. Stage-then-drain: the payload is staged
                                // durably in the per-tenant NATS work queue and ACK'd
                                // immediately — ACK means "durably staged", not "archived",
                                // so live-state retirement is not gated on object-storage or
                                // SQL repository latency. The Compaction drain worker performs
                                // the archival from the staged queue.
                                let ack_tx = tx_for_updates.clone();
                                let staging = compaction_staging_clone.clone();
                                tokio::spawn(async move {
                                    match stage_compaction_through_role_and_send_ack(
                                        staging.as_ref(),
                                        msg.payload.to_vec(),
                                        &ack_tx,
                                    )
                                    .await
                                    {
                                        Ok((wfi_id, state)) => {
                                            println!(
                                                "Staged compaction job for workflow {} (state={})",
                                                wfi_id, state
                                            );
                                        }
                                        Err(error) => {
                                            // No ACK means durable staging was not confirmed.
                                            eprintln!(
                                                "compaction staging failed: {error:#} (no ACK; server will re-ship)"
                                            );
                                        }
                                    }
                                });
                            },
                            Ok(EntityType::CancelPrecondition) => {
                                // Server has emitted a precondition cleanup
                                // envelope for a gate that reached a terminal
                                // non-`Satisfied` state (timer fired or
                                // sibling cascade invalidated the edge).
                                // Drop the matching entry from the
                                // per-instance index. Idempotent on a
                                // missing entry — duplicate envelopes from
                                // racing timer-fire / cascade producers are
                                // absorbed silently.
                                match tc::CancelPrecondition::decode(&msg.payload[..]) {
                                    Ok(cancel) => match (
                                        Uuid::parse_str(&cancel.workflow_instance_id),
                                        Uuid::parse_str(&cancel.edge_id),
                                    ) {
                                        (Ok(workflow_instance_id), Ok(edge_id)) => {
                                            crate::gate_index_lifecycle::gate_index()
                                                .unregister(workflow_instance_id, edge_id);
                                        }
                                        _ => eprintln!("CancelPrecondition carries invalid UUIDs"),
                                    },
                                    Err(e) => {
                                        eprintln!("Failed to deserialize CancelPrecondition: {}", e);
                                    }
                                }
                            },
                            Ok(EntityType::DispatchPrecondition) => {
                                // Server has emitted a precondition envelope
                                // for a hyperedge gate whose source set just
                                // grounded. Register the gate in the
                                // per-instance index so the wakeup translator
                                // can match a payload-bearing signal against
                                // it. A bad predicate string is logged and
                                // dropped — the server's parser validates at
                                // registration so a parse error here would
                                // only be reachable through corrupted state.
                                match tc::DispatchPrecondition::decode(&msg.payload[..]) {
                                    Ok(precondition) => match (
                                        Uuid::parse_str(&precondition.workflow_instance_id),
                                        Uuid::parse_str(&precondition.edge_id),
                                    ) {
                                        (Ok(workflow_instance_id), Ok(edge_id)) => {
                                            let idx = crate::gate_index_lifecycle::gate_index();
                                            if let Err(e) = idx.register(
                                                workflow_instance_id,
                                                edge_id,
                                                &precondition.signal_name,
                                                precondition.predicate.as_deref(),
                                                precondition.captures_spec,
                                            ) {
                                                eprintln!(
                                                    "DispatchPrecondition register failed for ({workflow_instance_id}, {edge_id}): {e}"
                                                );
                                            }
                                        }
                                        _ => eprintln!("DispatchPrecondition carries invalid UUIDs"),
                                    },
                                    Err(e) => {
                                        eprintln!("Failed to deserialize DispatchPrecondition: {}", e);
                                    }
                                }
                            },
                            Ok(EntityType::SignalApplied) => {
                                // Persist materialization before emitting the
                                // optional latency hint. A hint failure cannot
                                // erase or acknowledge durable Signal state.
                                match sp::SignalApplied::decode(&msg.payload[..]) {
                                    Ok(applied) => match Uuid::parse_str(&applied.signal_id) {
                                        Ok(signal_id) => {
                                            crate::signal_cancels::materialize(
                                                definition_repository.as_ref(),
                                                signal_id,
                                                applied.matched_count,
                                            )
                                            .await
                                            .with_context(|| {
                                                format!(
                                                    "persist SignalApplied materialization for {signal_id}"
                                                )
                                            })?;

                                            signal_applied_notifier
                                                .notify_bytag_cancel_materialized(signal_id);
                                        }
                                        Err(error) => {
                                            eprintln!(
                                                "SignalApplied carries invalid signal_id: {error}"
                                            );
                                        }
                                    },
                                    Err(error) => {
                                        eprintln!("Failed to deserialize SignalApplied: {error}");
                                    }
                                }
                            },
                            Ok(EntityType::PatchOutcome) => {
                                // Server-emitted terminal outcome of a relayed
                                // Patch. Correlate it onto the lifecycle row
                                // keyed by patch_key; a row that already
                                // settled absorbs the duplicate silently (the
                                // re-drive loop can echo outcomes).
                                match pp::PatchOutcome::decode(&msg.payload[..]) {
                                    Ok(outcome) => {
                                        match crate::patch_pipeline::correlate_outcome(&definition_repository, &outcome).await {
                                            Ok(crate::patch_pipeline::OutcomeCorrelation::Settled) => {
                                                // Only the winning terminal correlation may
                                                // mirror the reshaped graph. Duplicate or late
                                                // outcomes are storage and ctx side-effect-free.
                                                let applied = matches!(
                                                    outcome.outcome.as_ref().and_then(|o| o.kind.as_ref()),
                                                    Some(pp::patch_outcome_kind::Kind::Applied(_))
                                                );
                                                if let (true, Some(graph_json), Ok(run_id)) = (
                                                    applied,
                                                    &outcome.reshaped_graph_json,
                                                    uuid::Uuid::parse_str(&outcome.workflow_instance_id),
                                                ) {
                                                    if let Err(e) = crate::ctx_graph_mirror::mirror_reshaped_ctx_graph(
                                                        &nats_clone,
                                                        run_id,
                                                        graph_json,
                                                    )
                                                    .await
                                                    {
                                                        eprintln!(
                                                            "ctx graph re-mirror on patch apply failed for run {}: {} (mirror stays at prior value)",
                                                            outcome.workflow_instance_id, e
                                                        );
                                                    }
                                                }
                                                println!(
                                                    "PatchOutcome settled patch {} on instance {}",
                                                    outcome.patch_key, outcome.workflow_instance_id
                                                );
                                            }
                                            Ok(crate::patch_pipeline::OutcomeCorrelation::Absorbed) => {
                                                println!(
                                                    "PatchOutcome for patch {} absorbed (row already terminal or unknown)",
                                                    outcome.patch_key
                                                );
                                            }
                                            Err(e) => {
                                                // Leave the row unsettled — the
                                                // re-drive loop re-sends and the
                                                // server replays the outcome.
                                                eprintln!(
                                                    "PatchOutcome correlation failed for {}: {}",
                                                    outcome.patch_key, e
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to deserialize PatchOutcome: {}", e);
                                    }
                                }
                            },
                            Ok(other) => {
                                println!("Received message with unexpected entity type: {:?}", other);
                            },
                            Err(e) => {
                                println!("Received message with invalid entity type: {}, error: {:?}", msg.entity_type, e);
                            }
                        }
                    },
                    Err(e) => {
                        eprintln!("Error receiving message: {}", e);
                        break;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        } => {
            if let Err(e) = res {
                eprintln!("Stream error: {}", e);
            }
        }
        _ = shutdown_token.cancelled() => {
            println!("Stream shutting down...");
        }
    }

    println!("Streaming session completed.");
    Ok(())
}

/// Tickr Lite's local coordination boundary for messages arriving from the
/// Control plane. The relay keeps the published protobuf contracts while the
/// supervisor supplies durable local role implementations.
#[async_trait::async_trait]
pub trait LiteRelayRoles: Send + Sync {
    /// Bind this relay cycle's outbound channel and resume durable outbound
    /// drains. A reconnect replaces only the transport; staged work remains.
    async fn relay_connected(
        &self,
        relay_tx: mpsc::Sender<ConductorRelayMessage>,
        cycle: CancellationToken,
    );

    /// Durably stage one TaskDispatch before the relay reports Delivered.
    async fn stage_task_dispatch(&self, payload: &[u8]) -> Result<()>;

    /// Fence and forward one server-authored task cancellation locally.
    async fn stage_task_cancellation(&self, payload: &[u8]) -> Result<()>;

    /// Durably stage one Compaction and return its published acknowledgement.
    async fn stage_compaction(&self, payload: &[u8]) -> Result<ConductorRelayMessage>;

    /// Emit an advisory wake after durable ByTag materialization is recorded.
    fn signal_applied(&self, signal_id: Uuid);
}

/// Run the existing cross-plane relay with Tickr Lite's local Data-plane
/// coordination roles. Connection failures retain the distributed relay's
/// bounded retry behavior and never reinterpret local durable state.
pub async fn run_streaming_lite(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    roles: Arc<dyn LiteRelayRoles>,
) -> Result<()> {
    run_streaming_lite_at(
        shutdown_token,
        relay_config,
        definition_repository,
        roles,
        TenantId::from_env().to_string(),
    )
    .await
}

async fn run_streaming_lite_at(
    shutdown_token: CancellationToken,
    relay_config: ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    roles: Arc<dyn LiteRelayRoles>,
    tenant_id: String,
) -> Result<()> {
    use std::time::{Duration, Instant};

    const INITIAL_BACKOFF_MS: u64 = 250;
    const MAX_BACKOFF_MS: u64 = 5_000;
    const STABLE_THRESHOLD: Duration = Duration::from_secs(5);

    let mut backoff_ms = INITIAL_BACKOFF_MS;
    loop {
        if shutdown_token.is_cancelled() {
            return Ok(());
        }
        let started = Instant::now();
        match try_run_streaming_lite(
            shutdown_token.clone(),
            &relay_config,
            Arc::clone(&definition_repository),
            &tenant_id,
            Arc::clone(&roles),
        )
        .await
        {
            Ok(()) if shutdown_token.is_cancelled() => return Ok(()),
            Ok(()) => eprintln!("Tickr Lite relay stream ended; reconnecting..."),
            Err(error) => {
                eprintln!("Tickr Lite relay error: {error}; reconnecting in {backoff_ms}ms...")
            }
        }
        if started.elapsed() >= STABLE_THRESHOLD {
            backoff_ms = INITIAL_BACKOFF_MS;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            _ = shutdown_token.cancelled() => return Ok(()),
        }
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
    }
}

async fn try_run_streaming_lite(
    shutdown_token: CancellationToken,
    relay_config: &ControlPlaneRelayConfig,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    tenant_id: &str,
    roles: Arc<dyn LiteRelayRoles>,
) -> Result<()> {
    let mut client = connect_relay_client(relay_config).await?;
    let (tx, rx) = mpsc::channel::<ConductorRelayMessage>(32);
    init_relay_tx(tx.clone()).await;

    let cycle = CancellationToken::new();
    let _cycle_guard = cycle.clone().drop_guard();
    roles.relay_connected(tx.clone(), cycle.clone()).await;

    let outbound = tokio_stream::once(ConductorRelayMessage {
        entity_type: 0,
        payload: Vec::new(),
        tenant_id: Some(tenant_id.to_owned()),
    })
    .chain(tokio_stream::wrappers::ReceiverStream::new(rx));
    let mut inbound = establish_relay_stream(&mut client, outbound, relay_config).await?;

    tokio::select! {
        result = async {
            while let Some(message) = inbound.next().await {
                let message = message?;
                match EntityType::try_from(message.entity_type) {
                    Ok(EntityType::TaskQueueItem) => {
                        let dispatch = tc::TaskDispatch::decode(&message.payload[..])?;
                        if let Some(signal_id) = dispatch
                            .originating_signal_id
                            .as_deref()
                            .and_then(|id| Uuid::parse_str(id).ok())
                        {
                            if let Ok(workflow_instance_id) =
                                Uuid::parse_str(&dispatch.workflow_instance_id)
                            {
                                if let Err(error) = crate::signal_captures::mark_materialized(
                                    definition_repository.as_ref(),
                                    signal_id,
                                    workflow_instance_id,
                                )
                                .await
                                {
                                    eprintln!(
                                        "Lite instance-creation linkage failed for signal {signal_id} / run {workflow_instance_id}: {error} (forwarding the task anyway)"
                                    );
                                }
                            }
                        }
                        roles.stage_task_dispatch(&message.payload).await?;
                        let delivered = tc::TaskEvent {
                            task_instance_id: dispatch.task_instance_id,
                            task_id: dispatch.task_id,
                            workflow_instance_id: dispatch.workflow_instance_id,
                            workflow_id: dispatch.workflow_id,
                            executor_id: None,
                            kind: Some(tc::task_event::Kind::Delivered(
                                tc::task_event::Delivered {},
                            )),
                        };
                        tx.send(ConductorRelayMessage {
                            entity_type: EntityType::TaskEvent as i32,
                            payload: delivered.encode_to_vec(),
                            tenant_id: None,
                        }).await.map_err(|error| anyhow::anyhow!(
                            "send local Delivered event: {error}"
                        ))?;
                    }
                    Ok(EntityType::CancelTask) => {
                        roles.stage_task_cancellation(&message.payload).await?;
                    }
                    Ok(EntityType::Compaction) => {
                        let acknowledgement = roles.stage_compaction(&message.payload).await?;
                        tx.send(acknowledgement).await.map_err(|error| anyhow::anyhow!(
                            "send local Compaction acknowledgement: {error}"
                        ))?;
                    }
                    Ok(EntityType::CancelPrecondition) => {
                        if let Ok(cancel) = tc::CancelPrecondition::decode(&message.payload[..]) {
                            if let (Ok(workflow_instance_id), Ok(edge_id)) = (
                                Uuid::parse_str(&cancel.workflow_instance_id),
                                Uuid::parse_str(&cancel.edge_id),
                            ) {
                                crate::gate_index_lifecycle::gate_index()
                                    .unregister(workflow_instance_id, edge_id);
                            }
                        }
                    }
                    Ok(EntityType::DispatchPrecondition) => {
                        if let Ok(precondition) =
                            tc::DispatchPrecondition::decode(&message.payload[..])
                        {
                            if let (Ok(workflow_instance_id), Ok(edge_id)) = (
                                Uuid::parse_str(&precondition.workflow_instance_id),
                                Uuid::parse_str(&precondition.edge_id),
                            ) {
                                crate::gate_index_lifecycle::gate_index().register(
                                    workflow_instance_id,
                                    edge_id,
                                    &precondition.signal_name,
                                    precondition.predicate.as_deref(),
                                    precondition.captures_spec,
                                )?;
                            }
                        }
                    }
                    Ok(EntityType::SignalApplied) => {
                        let applied = sp::SignalApplied::decode(&message.payload[..])?;
                        let signal_id = Uuid::parse_str(&applied.signal_id)?;
                        crate::signal_cancels::materialize(
                            definition_repository.as_ref(),
                            signal_id,
                            applied.matched_count,
                        )
                        .await
                        .with_context(|| {
                            format!("persist SignalApplied materialization for {signal_id}")
                        })?;
                        roles.signal_applied(signal_id);
                    }
                    Ok(EntityType::PatchOutcome) => {
                        let outcome = pp::PatchOutcome::decode(&message.payload[..])?;
                        crate::patch_pipeline::correlate_outcome(
                            definition_repository.as_ref(),
                            &outcome,
                        )
                        .await?;
                    }
                    Ok(other) => {
                        println!("Tickr Lite relay received unexpected entity type: {other:?}");
                    }
                    Err(error) => {
                        eprintln!(
                            "Tickr Lite relay received invalid entity type {}: {error:?}",
                            message.entity_type
                        );
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        } => result?,
        _ = shutdown_token.cancelled() => {}
    }
    Ok(())
}

#[cfg(all(test, not(madsim)))]
mod relay_security_tests {
    use super::*;
    use crate::gate_index_lifecycle::{gate_index, rebuild_from_server};
    use crate::proto::conductor_relay_service_server::{
        ConductorRelayService, ConductorRelayServiceServer,
    };
    use crate::relay::dispatch_gates::{
        list_dispatched_gates, DispatchGatesClient, DispatchGatesError, DispatchedGate,
    };
    use crate::system_tasks::events_pull::{
        pull_once, EventsPullClient, EventsPullError, PullOutcome,
    };
    use axum::extract::{Path, Query, State};
    use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response as AxumResponse};
    use axum::{routing::get, Json, Router};
    use chrono::{DateTime, Utc};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ServerConfig, ServerConnection, StreamOwned};
    use serde::Deserialize;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread::JoinHandle;
    use tempfile::TempDir;
    use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::config::DataPlaneSql;
    use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
    use tonic::{Response, Streaming};

    const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_CA_PEM: &str = include_str!("../system_tasks/testdata/control-plane-test-ca.pem");
    const TEST_SERVER_CERT_PEM: &str =
        include_str!("../system_tasks/testdata/control-plane-test-server.pem");
    const TEST_SERVER_KEY_PEM: &str =
        include_str!("../system_tasks/testdata/control-plane-test-server-key.pem");

    type RelayStream =
        Pin<Box<dyn Stream<Item = Result<ConductorRelayMessage, Status>> + Send + 'static>>;

    #[derive(Default)]
    struct RelayObservation {
        authorization_count: AtomicUsize,
        authorization_matches: AtomicBool,
        message_count: AtomicUsize,
        message_contains_token: AtomicBool,
        certificate_metadata_seen: AtomicBool,
    }

    #[derive(Clone)]
    struct OriginRelay {
        observation: Arc<RelayObservation>,
        expected_tenant: Option<TenantId>,
    }

    #[tonic::async_trait]
    impl ConductorRelayService for OriginRelay {
        type StreamConductorRelayStream = RelayStream;

        async fn stream_conductor_relay(
            &self,
            request: Request<Streaming<ConductorRelayMessage>>,
        ) -> Result<Response<Self::StreamConductorRelayStream>, Status> {
            let authorizations: Vec<_> =
                request.metadata().get_all("authorization").iter().collect();
            self.observation
                .authorization_count
                .store(authorizations.len(), Ordering::Release);
            let authorization_matches = authorizations.len() == 1
                && authorizations[0].as_encoded_bytes() == TEST_AUTHORIZATION.as_bytes();
            self.observation
                .authorization_matches
                .store(authorization_matches, Ordering::Release);
            self.observation.certificate_metadata_seen.store(
                [
                    "x-forwarded-client-cert",
                    "x-tls-client-cert",
                    "ssl-client-cert",
                ]
                .into_iter()
                .any(|name| request.metadata().contains_key(name)),
                Ordering::Release,
            );
            if !authorization_matches {
                return Err(Status::unauthenticated("relay credential was not admitted"));
            }

            let observation = Arc::clone(&self.observation);
            let expected_tenant = self.expected_tenant;
            let mut inbound = request.into_inner();
            let first = inbound
                .message()
                .await
                .map_err(|_| Status::invalid_argument("invalid relay handshake"))?;
            if let Some(message) = first {
                if expected_tenant.is_some_and(|tenant| {
                    message.tenant_id.as_deref() != Some(tenant.to_string().as_str())
                }) {
                    return Err(Status::permission_denied(
                        "relay Tenant binding was not admitted",
                    ));
                }
                observe_relay_message(&observation, &message);
            }

            let (response_tx, response_rx) = mpsc::channel(1);
            tokio::spawn(async move {
                while let Some(message) = inbound.next().await {
                    let Ok(message) = message else {
                        break;
                    };
                    observe_relay_message(&observation, &message);
                }
                drop(response_tx);
            });
            Ok(Response::new(Box::pin(
                tokio_stream::wrappers::ReceiverStream::new(response_rx),
            )))
        }
    }

    fn observe_relay_message(observation: &RelayObservation, message: &ConductorRelayMessage) {
        observation.message_count.fetch_add(1, Ordering::AcqRel);
        let token = TEST_BEARER_TOKEN.as_bytes();
        if message
            .payload
            .windows(token.len())
            .any(|window| window == token)
            || message.tenant_id.as_deref().is_some_and(|tenant| {
                tenant
                    .as_bytes()
                    .windows(token.len())
                    .any(|window| window == token)
            })
        {
            observation
                .message_contains_token
                .store(true, Ordering::Release);
        }
    }

    #[derive(Clone)]
    struct TlsRelayProxy {
        upstream: String,
    }

    #[tonic::async_trait]
    impl ConductorRelayService for TlsRelayProxy {
        type StreamConductorRelayStream = RelayStream;

        async fn stream_conductor_relay(
            &self,
            request: Request<Streaming<ConductorRelayMessage>>,
        ) -> Result<Response<Self::StreamConductorRelayStream>, Status> {
            let metadata = request.metadata().clone();
            let inbound = request
                .into_inner()
                .filter_map(|message| futures::future::ready(message.ok()));
            let mut upstream_request = Request::new(inbound);
            *upstream_request.metadata_mut() = metadata;
            let mut client = ConductorRelayServiceClient::connect(self.upstream.clone())
                .await
                .map_err(|_| Status::unavailable("relay origin unavailable"))?;
            let response = client.stream_conductor_relay(upstream_request).await?;
            Ok(Response::new(Box::pin(response.into_inner())))
        }
    }

    async fn start_origin(
        observation: Arc<RelayObservation>,
        expected_tenant: Option<TenantId>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = async_stream::stream! {
            loop {
                yield listener.accept().await.map(|(stream, _)| stream);
            }
        };
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(ConductorRelayServiceServer::new(OriginRelay {
                    observation,
                    expected_tenant,
                }))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (address, server)
    }

    async fn start_tls_proxy(
        upstream: String,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = async_stream::stream! {
            loop {
                yield listener.accept().await.map(|(stream, _)| stream);
            }
        };
        let identity = Identity::from_pem(TEST_SERVER_CERT_PEM, TEST_SERVER_KEY_PEM);
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(ConductorRelayServiceServer::new(TlsRelayProxy { upstream }))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (address, server)
    }

    fn test_tls() -> ClientTlsConfig {
        ClientTlsConfig::new().ca_certificate(Certificate::from_pem(TEST_CA_PEM))
    }

    #[derive(Default)]
    struct HttpOriginObservation {
        requests: AtomicUsize,
        api_live_state_queries: AtomicUsize,
        gate_snapshots: AtomicUsize,
        pull_cycles: AtomicUsize,
        pull_cursor_seen: AtomicBool,
        certificate_header_seen: AtomicBool,
    }

    #[derive(Clone, serde::Serialize)]
    struct StubEvent {
        id: Uuid,
        ts: DateTime<Utc>,
        event_type: String,
        payload: serde_json::Value,
        archived_at: DateTime<Utc>,
    }

    #[derive(Clone)]
    struct HttpOriginState {
        tenant: TenantId,
        visible_instance_id: Uuid,
        gate: DispatchedGate,
        event: StubEvent,
        observation: Arc<HttpOriginObservation>,
    }

    #[derive(Deserialize)]
    struct TenantQuery {
        tenant: Uuid,
    }

    #[derive(Deserialize)]
    struct PullQuery {
        tenant: Uuid,
        after_archived_at: Option<DateTime<Utc>>,
        after_id: Option<Uuid>,
        #[allow(dead_code)]
        limit: Option<u32>,
    }

    fn observe_plaintext_http(headers: &HeaderMap, observation: &HttpOriginObservation) {
        observation.requests.fetch_add(1, Ordering::AcqRel);
        let certificate_header_seen = headers.keys().any(|name| {
            matches!(
                name.as_str(),
                "x-forwarded-client-cert" | "x-tls-client-cert" | "ssl-client-cert"
            )
        });
        observation
            .certificate_header_seen
            .fetch_or(certificate_header_seen, Ordering::AcqRel);
    }

    fn admitted_http(headers: &HeaderMap) -> bool {
        headers
            .get(AUTHORIZATION)
            .is_some_and(|value| value.as_bytes() == TEST_AUTHORIZATION.as_bytes())
    }

    async fn api_live_state(
        Path(instance_id): Path<Uuid>,
        State(state): State<HttpOriginState>,
        headers: HeaderMap,
    ) -> AxumResponse {
        observe_plaintext_http(&headers, &state.observation);
        if !admitted_http(&headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        state
            .observation
            .api_live_state_queries
            .fetch_add(1, Ordering::AcqRel);
        if instance_id != state.visible_instance_id {
            return StatusCode::NOT_FOUND.into_response();
        }
        Json(serde_json::json!({
            "id": instance_id,
            "state": "Running",
        }))
        .into_response()
    }

    async fn dispatched_gates(
        Query(query): Query<TenantQuery>,
        State(state): State<HttpOriginState>,
        headers: HeaderMap,
    ) -> AxumResponse {
        observe_plaintext_http(&headers, &state.observation);
        if !admitted_http(&headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if query.tenant != state.tenant.as_uuid() {
            return StatusCode::FORBIDDEN.into_response();
        }
        state
            .observation
            .gate_snapshots
            .fetch_add(1, Ordering::AcqRel);
        Json(vec![state.gate]).into_response()
    }

    async fn events_pull(
        Query(query): Query<PullQuery>,
        State(state): State<HttpOriginState>,
        headers: HeaderMap,
    ) -> AxumResponse {
        observe_plaintext_http(&headers, &state.observation);
        if !admitted_http(&headers) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if query.tenant != state.tenant.as_uuid() {
            return StatusCode::FORBIDDEN.into_response();
        }
        state.observation.pull_cycles.fetch_add(1, Ordering::AcqRel);
        let cursor = query.after_archived_at.zip(query.after_id);
        if cursor.is_some() {
            state
                .observation
                .pull_cursor_seen
                .store(true, Ordering::Release);
        }
        let rows = if cursor.is_none_or(|cursor| (state.event.archived_at, state.event.id) > cursor)
        {
            vec![state.event]
        } else {
            Vec::new()
        };
        Json(rows).into_response()
    }

    async fn start_http_origin(
        state: HttpOriginState,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/api/workflows/instances/{id}", get(api_live_state))
            .route("/api/internal/dispatched-gates", get(dispatched_gates))
            .route("/api/internal/events", get(events_pull))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (address, server)
    }

    struct HttpTlsTerminator {
        endpoint: String,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl HttpTlsTerminator {
        fn start(upstream: SocketAddr) -> Self {
            let certificates = vec![CertificateDer::from(pem_der(TEST_SERVER_CERT_PEM))];
            let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pem_der(TEST_SERVER_KEY_PEM)));
            let mut config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, key)
                .unwrap();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let config = Arc::new(config);
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let stop = Arc::new(AtomicBool::new(false));
            let worker_stop = Arc::clone(&stop);
            let worker = std::thread::spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => forward_decrypted_http(stream, upstream, &config),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });
            Self {
                endpoint: format!("https://localhost:{port}"),
                stop,
                worker: Some(worker),
            }
        }
    }

    impl Drop for HttpTlsTerminator {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            if let Some(worker) = self.worker.take() {
                worker.join().unwrap();
            }
        }
    }

    fn forward_decrypted_http(stream: TcpStream, upstream: SocketAddr, config: &Arc<ServerConfig>) {
        let _ = stream.set_nonblocking(false);
        let timeout = Some(Duration::from_secs(2));
        let _ = stream.set_read_timeout(timeout);
        let _ = stream.set_write_timeout(timeout);
        let Ok(connection) = ServerConnection::new(Arc::clone(config)) else {
            return;
        };
        let mut tls = StreamOwned::new(connection, stream);
        let request = read_http_request(&mut tls);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            return;
        };
        let Ok(mut origin) = TcpStream::connect(upstream) else {
            return;
        };
        let _ = origin.set_read_timeout(timeout);
        let _ = origin.set_write_timeout(timeout);
        let mut forwarded = request[..header_end].to_vec();
        forwarded.extend_from_slice(b"\r\nconnection: close\r\n\r\n");
        if origin.write_all(&forwarded).is_err() {
            return;
        }
        let mut response = Vec::new();
        if origin.read_to_end(&mut response).is_err() {
            return;
        }
        let _ = tls.write_all(&response);
        let _ = tls.flush();
    }

    fn read_http_request(stream: &mut impl Read) -> Vec<u8> {
        let mut request = Vec::with_capacity(2048);
        let mut buffer = [0_u8; 1024];
        loop {
            let Ok(read) = stream.read(&mut buffer) else {
                return request;
            };
            if read == 0 {
                return request;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return request;
            }
        }
    }

    fn pem_der(pem: &str) -> Vec<u8> {
        let mut output = Vec::new();
        let mut accumulator = 0_u32;
        let mut bits = 0_u8;
        for byte in pem
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .flat_map(str::bytes)
        {
            if byte == b'=' {
                break;
            }
            let value = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => continue,
            };
            accumulator = (accumulator << 6) | u32::from(value);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                output.push((accumulator >> bits) as u8);
                accumulator &= (1_u32 << bits) - 1;
            }
        }
        output
    }

    struct LocalTlsProxy {
        http: HttpTlsTerminator,
        relay_address: SocketAddr,
        relay: tokio::task::JoinHandle<()>,
    }

    impl LocalTlsProxy {
        async fn start(http_upstream: SocketAddr, relay_upstream: String) -> Self {
            let http = HttpTlsTerminator::start(http_upstream);
            let (relay_address, relay) = start_tls_proxy(relay_upstream).await;
            Self {
                http,
                relay_address,
                relay,
            }
        }

        fn http_endpoint(&self) -> &str {
            &self.http.endpoint
        }

        fn relay_endpoint(&self) -> String {
            format!("https://localhost:{}", self.relay_address.port())
        }
    }

    impl Drop for LocalTlsProxy {
        fn drop(&mut self) {
            self.relay.abort();
        }
    }

    fn trusted_http_client() -> reqwest::Client {
        reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap()
    }

    async fn event_repositories() -> (TempDir, Arc<WriterRepositoryBundle>) {
        let directory = TempDir::new().unwrap();
        let url = format!(
            "sqlite://{}",
            directory.path().join("secure-connection.db").display()
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &pool)
            .await
            .unwrap();
        pool.close().await;
        let writer = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap();
        (directory, Arc::new(writer))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn secure_control_plane_connection_scenario_uses_one_tls_proxy_and_plaintext_origins() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tenant = TenantId::from_slug("secure-connection-tenant");
        let foreign_tenant = TenantId::from_slug("foreign-secure-connection-tenant");
        let visible_instance_id = Uuid::new_v4();
        let event = StubEvent {
            id: Uuid::new_v4(),
            ts: "2026-08-14T00:00:00Z".parse().unwrap(),
            event_type: "WorkflowInstanceCreated".to_owned(),
            payload: serde_json::json!({"WorkflowInstanceCreated": {}}),
            archived_at: "2026-08-14T00:00:03Z".parse().unwrap(),
        };
        let observation = Arc::new(HttpOriginObservation::default());
        let state = HttpOriginState {
            tenant,
            visible_instance_id,
            gate: DispatchedGate {
                workflow_instance_id: visible_instance_id,
                edge_id: Uuid::new_v4(),
                signal_name: "release".to_owned(),
                predicate: None,
                captures_spec: Vec::new(),
            },
            event,
            observation: Arc::clone(&observation),
        };
        let (http_origin_address, http_origin) = start_http_origin(state).await;
        let relay_observation = Arc::new(RelayObservation::default());
        let (relay_origin_address, relay_origin) =
            start_origin(Arc::clone(&relay_observation), Some(tenant)).await;
        let proxy = LocalTlsProxy::start(
            http_origin_address,
            format!("http://{relay_origin_address}"),
        )
        .await;

        let api_response = trusted_http_client()
            .get(format!(
                "{}/api/workflows/instances/{visible_instance_id}",
                proxy.http_endpoint()
            ))
            .header(AUTHORIZATION, TEST_AUTHORIZATION)
            .send()
            .await
            .unwrap();
        assert_eq!(api_response.status(), StatusCode::OK);
        let foreign_instance = trusted_http_client()
            .get(format!(
                "{}/api/workflows/instances/{}",
                proxy.http_endpoint(),
                Uuid::new_v4()
            ))
            .header(AUTHORIZATION, TEST_AUTHORIZATION)
            .send()
            .await
            .unwrap();
        assert_eq!(foreign_instance.status(), StatusCode::NOT_FOUND);
        let missing_credential = trusted_http_client()
            .get(format!(
                "{}/api/workflows/instances/{visible_instance_id}",
                proxy.http_endpoint()
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(missing_credential.status(), StatusCode::UNAUTHORIZED);

        let gate_client = DispatchGatesClient::with_client(
            trusted_http_client(),
            proxy.http_endpoint(),
            TEST_BEARER_TOKEN,
        )
        .unwrap();
        assert_eq!(rebuild_from_server(&gate_client, tenant).await.unwrap(), 1);
        assert_eq!(gate_index().len(), 1);
        assert_eq!(
            list_dispatched_gates(&gate_client, foreign_tenant)
                .await
                .unwrap_err(),
            DispatchGatesError::Forbidden
        );

        let (_event_directory, repositories) = event_repositories().await;
        let events_client = EventsPullClient::with_client(
            trusted_http_client(),
            proxy.http_endpoint(),
            TEST_BEARER_TOKEN,
        )
        .unwrap();
        assert_eq!(
            pull_once(&repositories, &events_client, tenant.as_uuid())
                .await
                .unwrap(),
            PullOutcome {
                fetched: 1,
                inserted: 1,
            }
        );
        assert_eq!(
            pull_once(&repositories, &events_client, tenant.as_uuid())
                .await
                .unwrap(),
            PullOutcome {
                fetched: 0,
                inserted: 0,
            }
        );
        assert!(matches!(
            pull_once(&repositories, &events_client, foreign_tenant.as_uuid()).await,
            Err(EventsPullError::Forbidden)
        ));

        let relay_config =
            ControlPlaneRelayConfig::new(&proxy.relay_endpoint(), TEST_BEARER_TOKEN, false)
                .unwrap();
        for payload in [b"initial-relay".as_slice(), b"reconnect".as_slice()] {
            let mut client = connect_relay_client_with_tls(&relay_config, test_tls())
                .await
                .unwrap();
            let outbound = tokio_stream::iter([ConductorRelayMessage {
                entity_type: EntityType::TaskEvent as i32,
                payload: payload.to_vec(),
                tenant_id: Some(tenant.to_string()),
            }]);
            establish_relay_stream(&mut client, outbound, &relay_config)
                .await
                .unwrap();
        }
        let mut mismatched_relay = connect_relay_client_with_tls(&relay_config, test_tls())
            .await
            .unwrap();
        let mismatch = establish_relay_stream(
            &mut mismatched_relay,
            tokio_stream::iter([ConductorRelayMessage {
                entity_type: EntityType::TaskEvent as i32,
                payload: b"foreign-relay".to_vec(),
                tenant_id: Some(foreign_tenant.to_string()),
            }]),
            &relay_config,
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch, RelayEstablishmentError::Forbidden);

        let untrusted_gate =
            DispatchGatesClient::new(proxy.http_endpoint(), TEST_BEARER_TOKEN, false).unwrap();
        assert_eq!(
            list_dispatched_gates(&untrusted_gate, tenant)
                .await
                .unwrap_err(),
            DispatchGatesError::Unavailable
        );
        let mismatched_http_endpoint = proxy.http_endpoint().replace("localhost", "127.0.0.1");
        let mismatched_gate = DispatchGatesClient::with_client(
            trusted_http_client(),
            &mismatched_http_endpoint,
            TEST_BEARER_TOKEN,
        )
        .unwrap();
        assert_eq!(
            list_dispatched_gates(&mismatched_gate, tenant)
                .await
                .unwrap_err(),
            DispatchGatesError::Unavailable
        );

        tokio::time::timeout(Duration::from_secs(3), async {
            while relay_observation.message_count.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(observation.requests.load(Ordering::Acquire) >= 8);
        assert_eq!(
            observation.api_live_state_queries.load(Ordering::Acquire),
            2
        );
        assert_eq!(observation.gate_snapshots.load(Ordering::Acquire), 1);
        assert_eq!(observation.pull_cycles.load(Ordering::Acquire), 2);
        assert!(observation.pull_cursor_seen.load(Ordering::Acquire));
        assert!(!observation.certificate_header_seen.load(Ordering::Acquire));
        assert!(!relay_observation
            .certificate_metadata_seen
            .load(Ordering::Acquire));
        assert!(!relay_observation
            .message_contains_token
            .load(Ordering::Acquire));

        http_origin.abort();
        relay_origin.abort();
    }

    #[test]
    fn relay_configuration_is_secret_safe_and_rejects_plaintext_except_loopback() {
        let secure =
            ControlPlaneRelayConfig::new("https://control-plane.example", TEST_BEARER_TOKEN, false)
                .unwrap();
        let rendered = format!("{secure:?}");
        assert!(!rendered.contains(TEST_BEARER_TOKEN));
        assert!(rendered.contains("[REDACTED]"));
        assert!(secure.authorization_is_sensitive());

        assert_eq!(
            ControlPlaneRelayConfig::new("http://127.0.0.1:50052", TEST_BEARER_TOKEN, false,)
                .unwrap_err(),
            crate::config::ControlPlaneConfigError::InsecureEndpoint
        );
        assert_eq!(
            ControlPlaneRelayConfig::new(
                "http://control-plane.example:50052",
                TEST_BEARER_TOKEN,
                true,
            )
            .unwrap_err(),
            crate::config::ControlPlaneConfigError::InsecureEndpoint
        );
        ControlPlaneRelayConfig::new("http://127.0.0.1:50052", TEST_BEARER_TOKEN, true).unwrap();
        assert_eq!(
            ControlPlaneRelayConfig::new(
                "https://embedded@control-plane.example",
                TEST_BEARER_TOKEN,
                false,
            )
            .unwrap_err(),
            crate::config::ControlPlaneConfigError::InvalidEndpoint
        );
    }

    #[tokio::test]
    async fn verified_tls_proxy_receives_one_authorization_and_rejects_bad_trust_or_hostname() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let observation = Arc::new(RelayObservation::default());
        let (origin_address, origin) = start_origin(Arc::clone(&observation), None).await;
        let (proxy_address, proxy) = start_tls_proxy(format!("http://{origin_address}")).await;

        let config = ControlPlaneRelayConfig::new(
            &format!("https://localhost:{}", proxy_address.port()),
            TEST_BEARER_TOKEN,
            false,
        )
        .unwrap();
        let mut client = connect_relay_client_with_tls(&config, test_tls())
            .await
            .unwrap();
        let outbound = tokio_stream::iter([ConductorRelayMessage {
            entity_type: 0,
            payload: b"relay-handshake".to_vec(),
            tenant_id: Some(Uuid::new_v4().to_string()),
        }]);
        establish_relay_stream(&mut client, outbound, &config)
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            while observation.message_count.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.authorization_count.load(Ordering::Acquire), 1);
        assert!(observation.authorization_matches.load(Ordering::Acquire));
        assert!(!observation.message_contains_token.load(Ordering::Acquire));
        assert!(!observation
            .certificate_metadata_seen
            .load(Ordering::Acquire));

        assert!(matches!(
            connect_relay_client(&config).await,
            Err(RelayEstablishmentError::TlsHandshake)
        ));
        let mismatched = ControlPlaneRelayConfig::new(
            &format!("https://127.0.0.1:{}", proxy_address.port()),
            TEST_BEARER_TOKEN,
            false,
        )
        .unwrap();
        assert!(matches!(
            connect_relay_client_with_tls(&mismatched, test_tls()).await,
            Err(RelayEstablishmentError::TlsHandshake)
        ));

        let unused = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let unused_port = unused.local_addr().unwrap().port();
        drop(unused);
        let unavailable = ControlPlaneRelayConfig::new(
            &format!("https://localhost:{unused_port}"),
            TEST_BEARER_TOKEN,
            false,
        )
        .unwrap();
        assert!(matches!(
            connect_relay_client_with_tls(&unavailable, test_tls()).await,
            Err(RelayEstablishmentError::Unavailable)
        ));

        origin.abort();
        proxy.abort();
    }

    #[test]
    fn relay_establishment_statuses_have_distinct_secret_safe_classifications() {
        let cases = [
            (
                Status::unauthenticated("sentinel credential"),
                RelayEstablishmentError::Unauthenticated,
            ),
            (
                Status::permission_denied("sentinel credential"),
                RelayEstablishmentError::Forbidden,
            ),
            (
                Status::deadline_exceeded("sentinel credential"),
                RelayEstablishmentError::Timeout,
            ),
            (
                Status::unavailable("sentinel credential"),
                RelayEstablishmentError::Unavailable,
            ),
        ];
        for (status, expected) in cases {
            let classified = classify_relay_status(status);
            assert_eq!(classified, expected);
            assert!(!classified.to_string().contains("sentinel"));
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod lite_relay_tests {
    use super::*;
    use crate::proto::conductor_relay_service_server::{
        ConductorRelayService, ConductorRelayServiceServer,
    };
    use futures::Stream;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::ffi::OsString;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tempfile::TempDir;
    use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::config::DataPlaneSql;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status, Streaming};

    const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

    struct RelayEnvironment {
        previous: [(&'static str, Option<OsString>); 4],
    }

    impl RelayEnvironment {
        fn set(relay_url: &str, tenant_slug: &str) -> Self {
            let environment = Self {
                previous: [
                    (
                        "TICKR_CTRL_RELAY_URL",
                        std::env::var_os("TICKR_CTRL_RELAY_URL"),
                    ),
                    ("TICKR_TENANT_SLUG", std::env::var_os("TICKR_TENANT_SLUG")),
                    (
                        "TICKR_CONTROL_PLANE_BEARER_TOKEN",
                        std::env::var_os("TICKR_CONTROL_PLANE_BEARER_TOKEN"),
                    ),
                    (
                        "TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK",
                        std::env::var_os("TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK"),
                    ),
                ],
            };
            std::env::set_var("TICKR_CTRL_RELAY_URL", relay_url);
            std::env::set_var("TICKR_TENANT_SLUG", tenant_slug);
            std::env::set_var("TICKR_CONTROL_PLANE_BEARER_TOKEN", TEST_BEARER_TOKEN);
            std::env::set_var("TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK", "true");
            environment
        }
    }

    impl Drop for RelayEnvironment {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
    type RelayStream =
        Pin<Box<dyn Stream<Item = Result<ConductorRelayMessage, Status>> + Send + 'static>>;

    #[derive(Clone)]
    struct TestRelay {
        connections: Arc<AtomicUsize>,
        outage: CancellationToken,
        message: Option<ConductorRelayMessage>,
    }

    #[tonic::async_trait]
    impl ConductorRelayService for TestRelay {
        type StreamConductorRelayStream = RelayStream;

        async fn stream_conductor_relay(
            &self,
            _request: Request<Streaming<ConductorRelayMessage>>,
        ) -> Result<Response<Self::StreamConductorRelayStream>, Status> {
            self.connections.fetch_add(1, Ordering::Release);
            let (tx, rx) = mpsc::channel(1);
            let outage = self.outage.clone();
            let message = self.message.clone();
            tokio::spawn(async move {
                if let Some(message) = message {
                    if tx.send(Ok(message)).await.is_err() {
                        return;
                    }
                }
                outage.cancelled().await;
                drop(tx);
            });
            Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
        }
    }

    struct NoopRoles;

    #[async_trait::async_trait]
    impl LiteRelayRoles for NoopRoles {
        async fn relay_connected(
            &self,
            _relay_tx: mpsc::Sender<ConductorRelayMessage>,
            _cycle: CancellationToken,
        ) {
        }

        async fn stage_task_dispatch(&self, _payload: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn stage_task_cancellation(&self, _payload: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn stage_compaction(&self, _payload: &[u8]) -> Result<ConductorRelayMessage> {
            Ok(ConductorRelayMessage::default())
        }

        fn signal_applied(&self, _signal_id: Uuid) {}
    }

    async fn definition_repository() -> (TempDir, Arc<WriterRepositoryBundle>) {
        let directory = TempDir::new().unwrap();
        let url = format!("sqlite://{}", directory.path().join("relay.db").display());
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &pool)
            .await
            .unwrap();
        pool.close().await;
        let writer = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_writer()
            .await
            .unwrap();
        (directory, Arc::new(writer))
    }

    async fn start_relay(
        address: std::net::SocketAddr,
        connections: Arc<AtomicUsize>,
        outage: CancellationToken,
        message: Option<ConductorRelayMessage>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ConductorRelayServiceServer::new(TestRelay {
                    connections,
                    outage,
                    message,
                }))
                .serve(address)
                .await
                .unwrap();
        })
    }

    async fn await_connections(connections: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while connections.load(Ordering::Acquire) < expected {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lite_relay_uses_configured_control_plane_relay_url() {
        let _lock = ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let connections = Arc::new(AtomicUsize::new(0));
        let server = start_relay(
            address,
            Arc::clone(&connections),
            CancellationToken::new(),
            None,
        )
        .await;
        let _environment =
            RelayEnvironment::set(&format!("http://{address}"), "configured-relay-tenant");
        let relay_config = ControlPlaneRelayConfig::from_env().unwrap();
        let (_directory, repository) = definition_repository().await;
        let shutdown = CancellationToken::new();
        let relay = tokio::spawn(run_streaming_lite(
            shutdown.clone(),
            relay_config,
            repository,
            Arc::new(NoopRoles),
        ));

        await_connections(&connections, 1).await;
        shutdown.cancel();
        relay.await.unwrap().unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn lite_relay_survives_outage_and_reconnects() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let relay_url = format!("http://{address}");
        let connections = Arc::new(AtomicUsize::new(0));
        let first_outage = CancellationToken::new();
        let first_server = start_relay(
            address,
            Arc::clone(&connections),
            first_outage.clone(),
            None,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!first_server.is_finished(), "test relay failed to start");
        let (_directory, repository) = definition_repository().await;
        let shutdown = CancellationToken::new();
        let relay_config =
            ControlPlaneRelayConfig::new(&relay_url, TEST_BEARER_TOKEN, true).unwrap();
        let relay = tokio::spawn(run_streaming_lite_at(
            shutdown.clone(),
            relay_config,
            repository,
            Arc::new(NoopRoles),
            "test-tenant".to_owned(),
        ));

        await_connections(&connections, 1).await;
        first_outage.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        first_server.abort();
        first_server.await.unwrap_err();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!relay.is_finished(), "a relay outage must not fail Lite");

        let second_server = start_relay(
            address,
            Arc::clone(&connections),
            CancellationToken::new(),
            None,
        )
        .await;
        await_connections(&connections, 2).await;
        shutdown.cancel();
        relay.await.unwrap().unwrap();
        second_server.abort();
    }

    #[tokio::test]
    async fn lite_relay_links_trigger_signal_from_first_task_dispatch() {
        let signal_id = Uuid::new_v4();
        let workflow_id = Uuid::new_v4();
        let workflow_instance_id = Uuid::new_v4();
        let dispatch = tc::TaskDispatch {
            task_instance_id: Uuid::new_v4().to_string(),
            task_id: Uuid::new_v4().to_string(),
            workflow_instance_id: workflow_instance_id.to_string(),
            workflow_id: workflow_id.to_string(),
            name: "dispatch-task".to_owned(),
            task_type: 0,
            nix_expression_path: "/p".to_owned(),
            nix_args: vec![],
            outputs: vec![],
            inputs: vec![],
            secrets: vec![],
            tenant_id: "test-tenant".to_owned(),
            originating_signal_id: Some(signal_id.to_string()),
            gate_signal_ids: Default::default(),
            gate_signal_ids_ambient: vec![],
        };
        let message = ConductorRelayMessage {
            entity_type: EntityType::TaskQueueItem as i32,
            payload: dispatch.encode_to_vec(),
            tenant_id: None,
        };

        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let connections = Arc::new(AtomicUsize::new(0));
        let outage = CancellationToken::new();
        let server = start_relay(
            address,
            Arc::clone(&connections),
            outage.clone(),
            Some(message),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!server.is_finished(), "test relay failed to start");

        let (_directory, repository) = definition_repository().await;
        crate::signal_captures::insert(&repository, signal_id, workflow_id, Some(1), &[])
            .await
            .unwrap();
        let shutdown = CancellationToken::new();
        let relay_config =
            ControlPlaneRelayConfig::new(&format!("http://{address}"), TEST_BEARER_TOKEN, true)
                .unwrap();
        let relay = tokio::spawn(run_streaming_lite_at(
            shutdown.clone(),
            relay_config,
            Arc::clone(&repository),
            Arc::new(NoopRoles),
            "test-tenant".to_owned(),
        ));

        await_connections(&connections, 1).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = crate::signal_captures::read(&repository, signal_id)
                    .await
                    .unwrap()
                    .unwrap();
                if row.materialized_run_id == Some(workflow_instance_id) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("trigger signal was not linked from the first Lite task dispatch");

        shutdown.cancel();
        relay.await.unwrap().unwrap();
        outage.cancel();
        server.abort();
    }
}
