//! Conductor side of the API Command bus.
//!
//! All-NATS, all-Redis, and Tickr Lite request/reply carry an encoded
//! `ApiCommandRequest` into the shared dispatcher and receive an encoded
//! `ApiCommandResponse` with the HTTP-equivalent `status_code` and exactly one
//! typed payload. Each distributed adapter binds its own queue/group consumer;
//! the local adapter is called by the sole Conductor-owned writer.
//!
//! The adapters process Commands serially. An in-flight long Command can
//! therefore head-of-line-block later requests, but every mutation passes
//! through one ordered writer boundary.

use anyhow::Result;
use async_nats::Client as NatsClient;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use prost::Message as _;
use serde_json::Value;
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tickr_ctx::envelope::SignalSource;
use tickr_proto::coord::command_bus::{
    CommandRequestMetadata, CORRELATION_HEADER, DEADLINE_HEADER, DEFAULT_MAX_IN_FLIGHT,
    DEFAULT_MAX_PAYLOAD_BYTES,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cancel_pipeline::CancelTargetBody;
use crate::gate_index::GateIndex;
use crate::wakeup_translator::WakeupRelaySender;
use tickr_proto::tickr_api as api;

/// The single distributed subject all Command kinds travel on.
pub const COMMAND_SUBJECT: &str = tickr_proto::coord::all_nats::COMMAND_SUBJECT;

/// Queue group the conductor binds. One conductor per tenant today; the group
/// makes adding replicas a no-op rather than a fan-out-to-all.
pub const QUEUE_GROUP: &str = tickr_proto::coord::all_nats::COMMAND_QUEUE_GROUP;

/// Encoded Command dispatcher shared by the selected Command-bus consumer.
///
/// The transport receives only this role handler; repository and choreography
/// dependencies remain owned by the Conductor.
#[async_trait]
pub trait CommandBusHandler: Send + Sync {
    async fn handle(&self, payload: Vec<u8>) -> Vec<u8>;
}

/// Formation-selected Conductor side of the Command bus.
///
/// Implementations own their substrate client and protocol resources. The
/// Conductor component receives only this role-specific serving interface.
#[async_trait]
pub trait CommandBusConsumer: Send + Sync {
    async fn serve(
        &self,
        handler: Arc<dyn CommandBusHandler>,
        cancel: CancellationToken,
    ) -> Result<()>;
}

/// Fresh all-NATS Command-bus consumer.
pub struct NatsCommandBusConsumer {
    nats: NatsClient,
}

impl NatsCommandBusConsumer {
    pub fn new(nats: NatsClient) -> Self {
        Self { nats }
    }

    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::new(async_nats::connect(url).await?))
    }
}

#[async_trait]
impl CommandBusConsumer for NatsCommandBusConsumer {
    async fn serve(
        &self,
        handler: Arc<dyn CommandBusHandler>,
        cancel: CancellationToken,
    ) -> Result<()> {
        start_with_handler(self.nats.clone(), cancel, move |payload| {
            let handler = Arc::clone(&handler);
            async move { handler.handle(payload).await }
        })
        .await
    }
}

/// Everything the dispatch arms need that isn't a process-wide singleton. The
/// relay sender and the gate index are carried so the trigger / cancel /
/// wakeup arms reach the same machinery the HTTP handlers use. ByTag cancel
/// materialization is reconciled from the shared SQL repository; the selected
/// Signal-applied notifier remains an optional latency hint.
#[derive(Clone)]
pub struct ApiCommandsState {
    pub definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    pub nats: NatsClient,
    pub signal_applied_notifications:
        crate::signal_applied_notifier::SharedSignalAppliedReconciliationStream,
    pub relay_sender: Arc<dyn WakeupRelaySender>,
    /// Outbound seam for `PatchWorkflowInstance` envelopes — trait-carried so
    /// the patch dispatch arm is testable without the relay client, matching
    /// `relay_sender`.
    pub patch_relay_sender: Arc<dyn crate::patch_pipeline::PatchRelaySender>,
    pub gate_index: GateIndex,
}

/// Tickr Lite's command dependencies. The API still exchanges the production
/// Command envelopes; only the selected role implementations differ.
#[derive(Clone)]
pub struct LiteApiCommandsState {
    pub definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    pub relay_sender: Arc<dyn WakeupRelaySender>,
    pub patch_relay_sender: Arc<dyn crate::patch_pipeline::PatchRelaySender>,
    pub replay_relay_sender: Arc<dyn crate::replay_pipeline::ReplayRelaySender>,
    pub signal_applied_notifications:
        crate::signal_applied_notifier::SharedSignalAppliedReconciliationStream,
    pub gate_index: GateIndex,
}

/// Shared dependency view for the transport-independent Command dispatcher.
pub trait ApiCommandDispatchState {
    fn repositories(&self) -> &Arc<tickr_migrations::backend::WriterRepositoryBundle>;
    fn nats(&self) -> Option<&NatsClient>;
    fn relay_sender(&self) -> &Arc<dyn WakeupRelaySender>;
    fn patch_relay_sender(&self) -> &Arc<dyn crate::patch_pipeline::PatchRelaySender>;
    fn replay_relay_sender(&self) -> Option<&Arc<dyn crate::replay_pipeline::ReplayRelaySender>>;
    fn signal_applied_notifications(
        &self,
    ) -> &crate::signal_applied_notifier::SharedSignalAppliedReconciliationStream;
    fn gate_index(&self) -> &GateIndex;
}

impl ApiCommandDispatchState for ApiCommandsState {
    fn repositories(&self) -> &Arc<tickr_migrations::backend::WriterRepositoryBundle> {
        &self.definition_repository
    }

    fn nats(&self) -> Option<&NatsClient> {
        Some(&self.nats)
    }

    fn relay_sender(&self) -> &Arc<dyn WakeupRelaySender> {
        &self.relay_sender
    }

    fn patch_relay_sender(&self) -> &Arc<dyn crate::patch_pipeline::PatchRelaySender> {
        &self.patch_relay_sender
    }

    fn replay_relay_sender(&self) -> Option<&Arc<dyn crate::replay_pipeline::ReplayRelaySender>> {
        None
    }

    fn signal_applied_notifications(
        &self,
    ) -> &crate::signal_applied_notifier::SharedSignalAppliedReconciliationStream {
        &self.signal_applied_notifications
    }

    fn gate_index(&self) -> &GateIndex {
        &self.gate_index
    }
}

impl ApiCommandDispatchState for LiteApiCommandsState {
    fn repositories(&self) -> &Arc<tickr_migrations::backend::WriterRepositoryBundle> {
        &self.definition_repository
    }

    fn nats(&self) -> Option<&NatsClient> {
        None
    }

    fn relay_sender(&self) -> &Arc<dyn WakeupRelaySender> {
        &self.relay_sender
    }

    fn patch_relay_sender(&self) -> &Arc<dyn crate::patch_pipeline::PatchRelaySender> {
        &self.patch_relay_sender
    }

    fn replay_relay_sender(&self) -> Option<&Arc<dyn crate::replay_pipeline::ReplayRelaySender>> {
        Some(&self.replay_relay_sender)
    }

    fn signal_applied_notifications(
        &self,
    ) -> &crate::signal_applied_notifier::SharedSignalAppliedReconciliationStream {
        &self.signal_applied_notifications
    }

    fn gate_index(&self) -> &GateIndex {
        &self.gate_index
    }
}

#[async_trait]
impl CommandBusHandler for ApiCommandsState {
    async fn handle(&self, payload: Vec<u8>) -> Vec<u8> {
        handle_local_request(self, &payload).await
    }
}

/// Bind the queue-group subscriber and process commands serially until the
/// cancellation token fires.
pub async fn start(state: ApiCommandsState, cancel: CancellationToken) -> Result<()> {
    NatsCommandBusConsumer::new(state.nats.clone())
        .serve(Arc::new(state), cancel)
        .await
}

struct PendingCommand {
    metadata: CommandRequestMetadata,
    payload: Vec<u8>,
    reply: async_nats::Subject,
}

/// Serve the all-NATS Command bus through one bounded, serial mutation path.
///
/// Public only so the backend-law suite can exercise the real transport with
/// a deterministic handler; production uses [`start`].
pub async fn start_with_handler<F, Fut>(
    nats: NatsClient,
    cancel: CancellationToken,
    handler: F,
) -> Result<()>
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Vec<u8>> + Send + 'static,
{
    let mut sub = nats
        .queue_subscribe(COMMAND_SUBJECT, QUEUE_GROUP.into())
        .await?;
    nats.flush().await?;
    println!(
        "api_commands_consumer: subscribed, subject={}, queue_group={}",
        COMMAND_SUBJECT, QUEUE_GROUP
    );

    let correlations = Arc::new(Mutex::new(HashSet::with_capacity(DEFAULT_MAX_IN_FLIGHT)));
    let (sender, mut receiver) = mpsc::channel::<PendingCommand>(DEFAULT_MAX_IN_FLIGHT);
    let worker_nats = nats.clone();
    let worker_correlations = Arc::clone(&correlations);
    let worker_cancel = cancel.child_token();
    let worker_cancelled = worker_cancel.clone();
    let worker = tokio::spawn(async move {
        loop {
            let pending = tokio::select! {
                _ = worker_cancelled.cancelled() => break,
                pending = receiver.recv() => {
                    let Some(pending) = pending else { break };
                    pending
                }
            };
            let correlation_id = pending.metadata.correlation_id;
            let response = if pending.metadata.is_expired() {
                admission_error(
                    408,
                    api::CommandErrorCode::BadRequest,
                    "command deadline expired before dispatch",
                )
            } else {
                tokio::select! {
                    _ = worker_cancelled.cancelled() => break,
                    response = handler(pending.payload) => response,
                }
            };
            publish_reply(&worker_nats, pending.reply, response).await;
            remove_correlation(&worker_correlations, correlation_id);
        }
        match worker_correlations.lock() {
            Ok(mut active) => active.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    });

    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => {
                println!("api_commands_consumer: shutdown signal received");
                break;
            }
            maybe_msg = sub.next() => {
                let Some(msg) = maybe_msg else {
                    println!("api_commands_consumer: subscription ended");
                    break;
                };
                msg
            }
        };
        let Some(reply) = msg.reply.clone() else {
            eprintln!("api_commands_consumer: message without reply subject, dropping");
            continue;
        };
        if msg.payload.len() > DEFAULT_MAX_PAYLOAD_BYTES {
            publish_reply(
                &nats,
                reply,
                admission_error(
                    413,
                    api::CommandErrorCode::BadRequest,
                    "command payload too large",
                ),
            )
            .await;
            continue;
        }
        let metadata = match request_metadata(&msg) {
            Ok(metadata) => metadata,
            Err(message) => {
                publish_reply(
                    &nats,
                    reply,
                    admission_error(400, api::CommandErrorCode::BadRequest, message),
                )
                .await;
                continue;
            }
        };
        if metadata.is_expired() {
            publish_reply(
                &nats,
                reply,
                admission_error(
                    408,
                    api::CommandErrorCode::BadRequest,
                    "command deadline expired before admission",
                ),
            )
            .await;
            continue;
        }

        let admission_error_message =
            match reserve_correlation(&correlations, metadata.correlation_id) {
                CorrelationAdmission::Accepted => None,
                CorrelationAdmission::Duplicate => Some((
                    409,
                    api::CommandErrorCode::BadRequest,
                    "duplicate command correlation",
                )),
                CorrelationAdmission::Saturated => Some((
                    503,
                    api::CommandErrorCode::Unavailable,
                    "command consumer saturated",
                )),
                CorrelationAdmission::Unavailable => Some((
                    503,
                    api::CommandErrorCode::Unavailable,
                    "command consumer unavailable",
                )),
            };
        if let Some((status, code, message)) = admission_error_message {
            publish_reply(&nats, reply, admission_error(status, code, message)).await;
            continue;
        }

        let correlation_id = metadata.correlation_id;
        if sender
            .try_send(PendingCommand {
                metadata,
                payload: msg.payload.to_vec(),
                reply: reply.clone(),
            })
            .is_err()
        {
            remove_correlation(&correlations, correlation_id);
            publish_reply(
                &nats,
                reply,
                admission_error(
                    503,
                    api::CommandErrorCode::Unavailable,
                    "command consumer saturated",
                ),
            )
            .await;
        }
    }

    sub.unsubscribe().await?;
    nats.flush().await?;
    worker_cancel.cancel();
    drop(sender);
    let _ = worker.await;
    Ok(())
}

fn request_metadata(msg: &async_nats::Message) -> Result<CommandRequestMetadata, &'static str> {
    let headers = msg.headers.as_ref().ok_or("missing command metadata")?;
    let correlation_id = headers
        .get(CORRELATION_HEADER)
        .ok_or("missing command correlation")?
        .as_str()
        .parse()
        .map_err(|_| "invalid command correlation")?;
    let deadline_unix_ms = headers
        .get(DEADLINE_HEADER)
        .ok_or("missing command deadline")?
        .as_str()
        .parse()
        .map_err(|_| "invalid command deadline")?;
    Ok(CommandRequestMetadata {
        correlation_id,
        deadline_unix_ms,
    })
}

fn admission_error(status: u32, code: api::CommandErrorCode, message: &str) -> Vec<u8> {
    error_response(status, code, message.to_string()).encode_to_vec()
}

async fn publish_reply(nats: &NatsClient, reply: async_nats::Subject, response: Vec<u8>) {
    if let Err(e) = nats.publish(reply, response.into()).await {
        eprintln!("api_commands_consumer: failed to publish reply: {}", e);
        return;
    }
    if let Err(e) = nats.flush().await {
        eprintln!("api_commands_consumer: flush after reply failed: {}", e);
    }
}

enum CorrelationAdmission {
    Accepted,
    Duplicate,
    Saturated,
    Unavailable,
}

fn reserve_correlation(
    correlations: &Mutex<HashSet<Uuid>>,
    correlation_id: Uuid,
) -> CorrelationAdmission {
    let Ok(mut active) = correlations.lock() else {
        return CorrelationAdmission::Unavailable;
    };
    if active.contains(&correlation_id) {
        CorrelationAdmission::Duplicate
    } else if active.len() >= DEFAULT_MAX_IN_FLIGHT {
        CorrelationAdmission::Saturated
    } else {
        active.insert(correlation_id);
        CorrelationAdmission::Accepted
    }
}

fn remove_correlation(correlations: &Mutex<HashSet<Uuid>>, correlation_id: Uuid) {
    match correlations.lock() {
        Ok(mut active) => {
            active.remove(&correlation_id);
        }
        Err(poisoned) => {
            poisoned.into_inner().remove(&correlation_id);
        }
    }
}

/// Decode the envelope and dispatch by command kind. A malformed envelope is a
/// 400 with a `BAD_REQUEST` error payload — the API renders it as a 400.
async fn handle(state: &impl ApiCommandDispatchState, payload: &[u8]) -> api::ApiCommandResponse {
    let request = match api::ApiCommandRequest::decode(payload) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                format!("malformed ApiCommandRequest: {}", e),
            );
        }
    };

    match request.body {
        Some(api::api_command_request::Body::Register(req)) => dispatch_register(state, req).await,
        Some(api::api_command_request::Body::Trigger(req)) => dispatch_trigger(state, req).await,
        Some(api::api_command_request::Body::Wakeup(req)) => dispatch_wakeup(state, req).await,
        Some(api::api_command_request::Body::Cancel(req)) => dispatch_cancel(state, req).await,
        Some(api::api_command_request::Body::Patch(req)) => dispatch_patch(state, req).await,
        Some(api::api_command_request::Body::Replay(req)) => dispatch_replay(state, req).await,
        Some(api::api_command_request::Body::Ping(_)) => dispatch_ping(),
        None => unsupported_response(),
    }
}

/// Dispatch one encoded production Command envelope without selecting a
/// transport. Tickr Lite's sole local writer calls this entry point; the
/// distributed subscriber above calls the same path before publishing to NATS.
pub async fn handle_local_request(state: &impl ApiCommandDispatchState, payload: &[u8]) -> Vec<u8> {
    handle(state, payload).await.encode_to_vec()
}

/// Reply to a Ping with a side-effect-free 200 ack. The Ping is an explicit
/// "does the command consumer answer" probe — a dedicated variant rather than a
/// reuse of a read command, so it touches no state. It powers the health
/// surface's Conductor row, which is honestly a *command-plane-responsive* check
/// (the command consumer answered), not a claim that the relay loop is live.
fn dispatch_ping() -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code: 200,
        payload: Some(api::api_command_response::Payload::Ping(
            api::PingPayload {},
        )),
    }
}

/// Run the patch pipeline and project its ingress verdict onto the wire
/// envelope. Document validity (Nickel eval + well-formed kernel ops) is
/// checked here synchronously — a bad document is a 400 and never opens a
/// row. Everything after ingress is asynchronous: the reply only carries the
/// minted `patch_id` the submitter polls the lifecycle row by.
async fn dispatch_patch(
    state: &impl ApiCommandDispatchState,
    req: api::PatchRequest,
) -> api::ApiCommandResponse {
    use crate::patch_pipeline::{parse_patch_document, process_patch, PatchError, PatchIngress};

    let workflow_instance_id = match Uuid::parse_str(&req.workflow_instance_id) {
        Ok(id) => id,
        Err(e) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                format!("invalid workflow_instance_id: {}", e),
            );
        }
    };

    let parsed = match parse_patch_document(&req.nickel_source).await {
        Ok(parsed) => parsed,
        Err(e) => {
            return error_response(400, api::CommandErrorCode::BadRequest, e.to_string());
        }
    };

    // Mint the patch identity at ingress. `patch_key` derives from it
    // (UUIDv5 over the target instance), so every re-drive of this ingressed
    // request lands on the same lifecycle row.
    let patch_id = Uuid::new_v4();

    match process_patch(
        state.repositories().as_ref(),
        state.patch_relay_sender().as_ref(),
        workflow_instance_id,
        patch_id,
        parsed,
        // Arrived over the command bus — an externally-authored patch.
        crate::patch_pipeline::PatchProvenance::External,
    )
    .await
    {
        Ok(PatchIngress::Accepted {
            patch_id,
            patch_key,
            build_jobs,
        }) => {
            // Build-at-patch: publish the per-task jobs after the ingress
            // transaction committed (publish-after-commit ordering). A
            // publish failure leaves the row at `Building` — loud and
            // pollable — while the server's stall-TTL backstop bounds the
            // instance's wait.
            if let (Some(nats), false) = (state.nats(), build_jobs.is_empty()) {
                if let Err(e) =
                    crate::patch_pipeline::publish_patch_build_jobs(nats, &build_jobs).await
                {
                    eprintln!(
                        "patch {} build-job publish failed: {} (row stays Building)",
                        patch_key, e
                    );
                }
            }
            patch_response(
                202,
                api::patch_payload::Outcome::Accepted(api::patch_payload::Accepted {
                    patch_id: patch_id.to_string(),
                }),
            )
        }
        Ok(PatchIngress::RejectedInProgress {
            patch_id, reason, ..
        }) => patch_response(
            409,
            api::patch_payload::Outcome::Rejected(api::patch_payload::Rejected {
                patch_id: patch_id.to_string(),
                reason,
            }),
        ),
        // Unreachable from the bus (a fresh submit mints a fresh patch_id) —
        // covered for completeness: a replayed row acknowledges ingress.
        Ok(PatchIngress::Replayed { row }) => patch_response(
            202,
            api::patch_payload::Outcome::Accepted(api::patch_payload::Accepted {
                patch_id: row.patch_id.to_string(),
            }),
        ),
        Err(e @ PatchError::Parse(_)) => {
            error_response(400, api::CommandErrorCode::BadRequest, e.to_string())
        }
        Err(e @ PatchError::Persist(_)) => {
            error_response(500, api::CommandErrorCode::Internal, e.to_string())
        }
    }
}

/// Run the replay pipeline and project its ingress verdict onto the wire
/// envelope. The replay's carried state is minted conductor-side from the
/// archive inside `process_replay` — this arm only decodes the constrained
/// request shape (which has no field able to carry a seed) and maps outcomes.
async fn dispatch_replay(
    state: &impl ApiCommandDispatchState,
    req: api::ReplayRequest,
) -> api::ApiCommandResponse {
    use crate::replay_pipeline::{
        process_replay, DefaultReplayRelaySender, ReplayError, ReplayIngress, ReplayRelaySender,
        ReplayRequest as PReq,
    };

    let source_instance_id = match Uuid::parse_str(&req.source_instance_id) {
        Ok(id) => id,
        Err(e) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                format!("invalid source_instance_id: {}", e),
            );
        }
    };

    // `resume_from` is a set of HyperNode ids; a malformed entry is a 400.
    let mut resume_from = Vec::with_capacity(req.resume_from.len());
    for raw in &req.resume_from {
        match Uuid::parse_str(raw) {
            Ok(id) => resume_from.push(id),
            Err(e) => {
                return error_response(
                    400,
                    api::CommandErrorCode::BadRequest,
                    format!("invalid resume_from id `{}`: {}", raw, e),
                );
            }
        }
    }
    let resume_from = if resume_from.is_empty() {
        None
    } else {
        Some(resume_from)
    };

    // The inputs shadow arrives as `map<string, string>` with JSON-encoded
    // values (the Wakeup-captures wire convention); decode each back to its
    // JSON value. A malformed encoding is a 400.
    let mut inputs = std::collections::HashMap::with_capacity(req.inputs.len());
    for (key, raw) in req.inputs {
        match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(value) => {
                inputs.insert(key, value);
            }
            Err(e) => {
                return error_response(
                    400,
                    api::CommandErrorCode::BadRequest,
                    format!("invalid inputs value for `{}`: {}", key, e),
                );
            }
        }
    }

    let pipeline_req = PReq {
        source_instance_id,
        resume_from,
        name: req.name,
        idempotency_key: req.idempotency_key,
        inputs,
    };

    let distributed_sender;
    let sender: &dyn ReplayRelaySender = if let Some(sender) = state.replay_relay_sender() {
        sender.as_ref()
    } else {
        distributed_sender = DefaultReplayRelaySender {
            nats: state
                .nats()
                .expect("distributed Command state carries NATS")
                .clone(),
        };
        &distributed_sender
    };

    match process_replay(state.repositories().as_ref(), sender, pipeline_req).await {
        Ok(ReplayIngress::Accepted {
            replay_instance_id,
            doomed,
        }) => replay_response(
            202,
            api::replay_payload::Outcome::Accepted(api::replay_payload::Accepted {
                replay_instance_id: replay_instance_id.to_string(),
                doomed: doomed.iter().map(|d| d.to_string()).collect(),
            }),
        ),
        Ok(ReplayIngress::Deduplicated { replay_instance_id }) => replay_response(
            200,
            api::replay_payload::Outcome::Deduplicated(api::replay_payload::Deduplicated {
                replay_instance_id: replay_instance_id.to_string(),
            }),
        ),
        Ok(ReplayIngress::VersionUnresolvable { replay_instance_id }) => replay_response(
            404,
            api::replay_payload::Outcome::VersionUnresolvable(
                api::replay_payload::VersionUnresolvable {
                    replay_instance_id: replay_instance_id.to_string(),
                },
            ),
        ),
        Err(e @ ReplayError::RootUnfireable { .. })
        | Err(e @ ReplayError::NoFailedNodes)
        // A shadow key that is not a declared trigger capture of the pinned
        // version (undeclared, or a task-produced value that is never
        // shadowable) is a caller error — a bad request.
        | Err(e @ ReplayError::ShadowUndeclared { .. })
        | Err(e @ ReplayError::ShadowTaskProduced { .. }) => {
            error_response(400, api::CommandErrorCode::BadRequest, e.to_string())
        }
        // The replayed run's own re-hydration never completed — a precondition
        // conflict, not a bad request.
        Err(e @ ReplayError::ParentNeverHydrated { .. })
        // The pinned version's declared-capture schema is not resolvable from
        // the definition mirror, so the shadow cannot be validated — a
        // precondition conflict.
        | Err(e @ ReplayError::ShadowSchemaUnresolvable { .. }) => {
            error_response(409, api::CommandErrorCode::BadRequest, e.to_string())
        }
        Err(e @ ReplayError::Persist(_)) | Err(e @ ReplayError::Archive(_)) => {
            error_response(500, api::CommandErrorCode::Internal, e.to_string())
        }
    }
}

/// Run the register pipeline and project its outcome onto the wire envelope.
/// The error `Display` strings are the exact HTTP messages today's handler
/// returns; the API renders them into register's historical body shape.
async fn dispatch_register(
    state: &impl ApiCommandDispatchState,
    req: api::RegisterRequest,
) -> api::ApiCommandResponse {
    use crate::register_pipeline::{
        process_register, process_register_local, RegisterError, RegisterOutcome, RegisterRequest,
    };

    let pipeline_req = RegisterRequest {
        nickel_source: req.nickel_source,
        namespace: req.namespace,
    };
    let result = if let Some(nats) = state.nats() {
        process_register(state.repositories().as_ref(), nats, pipeline_req).await
    } else {
        process_register_local(state.repositories().as_ref(), pipeline_req).await
    };
    match result {
        Ok(RegisterOutcome::Inserted {
            workflow_id,
            workflow_version,
            task_count,
            message,
        }) => register_response(
            202,
            api::register_payload::Outcome::Inserted(api::register_payload::Inserted {
                workflow_id: workflow_id.to_string(),
                workflow_version,
                task_count: task_count as u32,
                message,
            }),
        ),
        Ok(RegisterOutcome::NoOp {
            workflow_id,
            workflow_version,
            message,
        }) => register_response(
            200,
            api::register_payload::Outcome::NoOp(api::register_payload::NoOp {
                workflow_id: workflow_id.to_string(),
                workflow_version,
                message,
            }),
        ),
        Ok(RegisterOutcome::Refreshed {
            workflow_id,
            workflow_version,
            message,
        }) => register_response(
            200,
            api::register_payload::Outcome::Refreshed(api::register_payload::Refreshed {
                workflow_id: workflow_id.to_string(),
                workflow_version,
                message,
            }),
        ),
        Ok(RegisterOutcome::BuildRequeued {
            workflow_id,
            workflow_version,
            task_count,
            message,
        }) => register_response(
            202,
            api::register_payload::Outcome::BuildRequeued(api::register_payload::BuildRequeued {
                workflow_id: workflow_id.to_string(),
                workflow_version,
                task_count: task_count as u32,
                message,
            }),
        ),
        Err(e @ RegisterError::Parse(_)) => {
            error_response(400, api::CommandErrorCode::BadRequest, e.to_string())
        }
        Err(e @ RegisterError::Timeout) => {
            error_response(408, api::CommandErrorCode::ParseTimeout, e.to_string())
        }
        Err(e @ RegisterError::Persist(_)) => {
            error_response(500, api::CommandErrorCode::Internal, e.to_string())
        }
    }
}

/// Run the trigger pipeline and project its outcome onto the wire envelope.
/// The pipeline (`trigger_pipeline::process_trigger`) is already
/// transport-agnostic — this arm is the bus adapter: it decodes the wire
/// shape, hardcodes `SignalSource::Manual` (preserving today's HTTP behavior;
/// auth-driven source attribution is deferred), forwards the minted `Signal`
/// over the relay on `Fresh`, and maps outcomes / errors to the HTTP-equivalent
/// status the API forwards verbatim.
async fn dispatch_trigger(
    state: &impl ApiCommandDispatchState,
    req: api::TriggerRequest,
) -> api::ApiCommandResponse {
    use crate::trigger_pipeline::{
        process_trigger, TriggerError, TriggerOutcome, TriggerRequest as PReq,
    };

    // workflow_id is validated API-side too; parse defensively here so a
    // malformed id surfaces as today's 400 rather than a panic.
    let workflow_id = match Uuid::parse_str(&req.workflow_id) {
        Ok(id) => id,
        Err(_) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                "invalid workflow id".to_string(),
            )
        }
    };
    let scheduled_at = match req.scheduled_at.as_deref() {
        Some(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(dt) => Some(dt.with_timezone(&Utc)),
            Err(_) => {
                return error_response(
                    400,
                    api::CommandErrorCode::BadRequest,
                    "invalid scheduled_at".to_string(),
                )
            }
        },
        None => None,
    };
    let inputs = match req.inputs.as_deref() {
        Some(bytes) => match serde_json::from_slice::<Value>(bytes) {
            Ok(v) => Some(v),
            Err(_) => {
                return error_response(
                    400,
                    api::CommandErrorCode::BadRequest,
                    "invalid inputs JSON".to_string(),
                )
            }
        },
        None => None,
    };

    // HTTP-shaped hashing: the workflow_id rides in the URL path on the API
    // side, so the idempotency hash keys only `inputs` (matching the HTTP
    // trigger handler, not the wider NATS-ingress tuple).
    let hash_payload = inputs.clone().unwrap_or(Value::Object(Default::default()));
    let scheduled_at_str = scheduled_at.map(|dt| dt.to_rfc3339());

    let pipeline_req = PReq {
        workflow_id,
        scheduled_at,
        inputs,
        idempotency_key: req.idempotency_key,
        source: SignalSource::Manual,
        hash_payload,
        name: req.name,
    };

    let result = if let Some(nats) = state.nats() {
        process_trigger(state.repositories().as_ref(), nats, pipeline_req).await
    } else {
        crate::trigger_pipeline::process_trigger_local(state.repositories().as_ref(), pipeline_req)
            .await
    };
    let outcome = match result {
        Ok(o) => o,
        Err(TriggerError::WorkflowNotFound { .. }) => {
            return error_response(
                404,
                api::CommandErrorCode::NotFound,
                "workflow not found".to_string(),
            )
        }
        Err(TriggerError::InputsProvidedButNoCaptures) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                "workflow declares no captures; remove the `inputs` field or extend the workflow definition with captures".to_string(),
            )
        }
        Err(TriggerError::CapturesExtractionFailed {
            name,
            jsonpath,
            message,
        }) => {
            return error_response(
                400,
                api::CommandErrorCode::BadRequest,
                format!(
                    "capture `{}` JSONPath `{}` failed to apply: {}",
                    name, jsonpath, message
                ),
            )
        }
        Err(e @ TriggerError::WorkflowLookup(_))
        | Err(e @ TriggerError::Idempotency(_))
        | Err(e @ TriggerError::RepositoryWrite(_))
        | Err(e @ TriggerError::NatsWrite(_))
        | Err(e @ TriggerError::ScopeWrite(_))
        | Err(e @ TriggerError::EffectsEncoding(_)) => {
            return error_response(500, api::CommandErrorCode::Internal, e.to_string())
        }
    };

    match outcome {
        TriggerOutcome::Fresh {
            signal_id, signal, ..
        } => {
            // The HTTP trigger path blocks on the relay send and surfaces a
            // failure as 503; mirror that here.
            if let Err(e) = state.relay_sender().send(&signal).await {
                return error_response(
                    503,
                    api::CommandErrorCode::Unavailable,
                    format!("relay unreachable: {}", e),
                );
            }
            trigger_response(
                200,
                api::trigger_payload::Outcome::Fresh(api::trigger_payload::Fresh {
                    signal_id: signal_id.to_string(),
                    scheduled_at: scheduled_at_str,
                }),
            )
        }
        TriggerOutcome::Deduplicated { original_signal_id } => trigger_response(
            200,
            api::trigger_payload::Outcome::Deduplicated(api::trigger_payload::Deduplicated {
                original_signal_id: original_signal_id.to_string(),
                scheduled_at: scheduled_at_str,
            }),
        ),
        TriggerOutcome::Conflict {
            original_signal_id,
            original_hash,
            your_hash,
        } => trigger_response(
            409,
            api::trigger_payload::Outcome::Conflict(api::trigger_payload::Conflict {
                original_signal_id: original_signal_id.to_string(),
                original_input_hash: original_hash,
                your_input_hash: your_hash,
            }),
        ),
    }
}

/// Run the wakeup translator and project its outcome onto the wire envelope.
/// `wakeup_translator::process_wakeup` is already transport-agnostic; this arm
/// passes the subscriber state's `gate_index` and `relay_sender` through, the
/// same handles the HTTP wakeup handler uses.
async fn dispatch_wakeup(
    state: &impl ApiCommandDispatchState,
    req: api::WakeupRequest,
) -> api::ApiCommandResponse {
    use crate::wakeup_translator::{process_wakeup, WakeupOutcome, WakeupRequest as PReq};

    let payload = match req.payload.as_deref() {
        Some(bytes) => match serde_json::from_slice::<Value>(bytes) {
            Ok(v) => Some(v),
            Err(_) => {
                return error_response(
                    400,
                    api::CommandErrorCode::BadRequest,
                    "invalid payload JSON".to_string(),
                )
            }
        },
        None => None,
    };

    let pipeline_req = PReq {
        name: req.name,
        payload,
        idempotency_key: req.idempotency_key,
    };

    let result = if let Some(nats) = state.nats() {
        process_wakeup(
            state.repositories().as_ref(),
            nats,
            state.relay_sender().as_ref(),
            state.gate_index(),
            pipeline_req,
        )
        .await
    } else {
        crate::wakeup_translator::process_wakeup_local(
            state.repositories().as_ref(),
            state.relay_sender().as_ref(),
            state.gate_index(),
            pipeline_req,
        )
        .await
    };
    match result {
        Ok(WakeupOutcome::Fresh {
            signal_id,
            matched_workflows,
            gates_matched,
        }) => wakeup_response(
            200,
            api::wakeup_payload::Outcome::Fresh(api::wakeup_payload::Fresh {
                signal_id: signal_id.to_string(),
                matched_workflows,
                gates_matched,
            }),
        ),
        Ok(WakeupOutcome::Deduplicated { original_signal_id }) => wakeup_response(
            200,
            api::wakeup_payload::Outcome::Deduplicated(api::wakeup_payload::Deduplicated {
                original_signal_id: original_signal_id.to_string(),
            }),
        ),
        Ok(WakeupOutcome::Conflict {
            original_signal_id,
            original_hash,
            your_hash,
        }) => wakeup_response(
            409,
            api::wakeup_payload::Outcome::Conflict(api::wakeup_payload::Conflict {
                original_signal_id: original_signal_id.to_string(),
                original_input_hash: original_hash,
                your_input_hash: your_hash,
            }),
        ),
        Err(e) => error_response(500, api::CommandErrorCode::Internal, format!("{}", e)),
    }
}

/// Run the cancel pipeline and project its outcome onto the wire envelope.
/// The pipeline owns idempotency, durable ByTag Signal staging, relay
/// forwarding, bounded materialization reconciliation, and audit projection.
async fn dispatch_cancel(
    state: &impl ApiCommandDispatchState,
    req: api::CancelRequest,
) -> api::ApiCommandResponse {
    use crate::cancel_pipeline::{
        process_cancel, CancelError, CancelOutcome, CancelRequest as PReq,
    };

    let Some(proto_target) = req.target.and_then(|t| t.target) else {
        return error_response(
            400,
            api::CommandErrorCode::BadRequest,
            "missing `target`".to_string(),
        );
    };
    let target = match proto_target {
        api::cancel_target::Target::Instance(i) => {
            let workflow_instance_id = match Uuid::parse_str(&i.workflow_instance_id) {
                Ok(id) => id,
                Err(_) => {
                    return error_response(
                        400,
                        api::CommandErrorCode::BadRequest,
                        "invalid workflow_instance_id".to_string(),
                    )
                }
            };
            let node_id = match i.node_id.as_deref() {
                Some(s) => match Uuid::parse_str(s) {
                    Ok(id) => Some(id),
                    Err(_) => {
                        return error_response(
                            400,
                            api::CommandErrorCode::BadRequest,
                            "invalid node_id".to_string(),
                        )
                    }
                },
                None => None,
            };
            CancelTargetBody::Instance {
                workflow_instance_id,
                node_id,
            }
        }
        api::cancel_target::Target::ByTag(b) => CancelTargetBody::ByTag { filter: b.filter },
    };

    let pipeline_req = PReq {
        target,
        note: req.note,
        idempotency_key: req.idempotency_key,
    };

    let result = if let Some(nats) = state.nats() {
        process_cancel(
            state.repositories().as_ref(),
            nats,
            state.signal_applied_notifications().as_ref(),
            pipeline_req,
        )
        .await
    } else {
        crate::cancel_pipeline::process_cancel_local(
            state.repositories().as_ref(),
            state.signal_applied_notifications().as_ref(),
            pipeline_req,
        )
        .await
    };
    match result {
        Ok(CancelOutcome::Instance { signal_id }) => cancel_response(
            200,
            api::cancel_payload::Outcome::Instance(api::cancel_payload::Instance {
                signal_id: signal_id.to_string(),
            }),
        ),
        Ok(CancelOutcome::ByTag {
            signal_id,
            instances_matched,
        }) => cancel_response(
            200,
            api::cancel_payload::Outcome::ByTag(api::cancel_payload::ByTag {
                signal_id: signal_id.to_string(),
                instances_matched,
            }),
        ),
        Ok(CancelOutcome::Deduplicated { original_signal_id }) => cancel_response(
            200,
            api::cancel_payload::Outcome::Deduplicated(api::cancel_payload::Deduplicated {
                original_signal_id: original_signal_id.to_string(),
            }),
        ),
        Ok(CancelOutcome::Conflict {
            original_signal_id,
            original_hash,
            your_hash,
        }) => cancel_response(
            409,
            api::cancel_payload::Outcome::Conflict(api::cancel_payload::Conflict {
                original_signal_id: original_signal_id.to_string(),
                original_input_hash: original_hash,
                your_input_hash: your_hash,
            }),
        ),
        Err(CancelError::ByTagTimeout { signal_id }) => error_response(
            503,
            api::CommandErrorCode::Unavailable,
            format!("timed out waiting for durable Signal materialization; signal_id={signal_id}"),
        ),
        Err(e @ CancelError::RelayUnreachable(_)) => {
            error_response(503, api::CommandErrorCode::Unavailable, e.to_string())
        }
        Err(
            e @ CancelError::SerializeTarget(_)
            | e @ CancelError::IdempotencyBucket(_)
            | e @ CancelError::IdempotencyCheck(_)
            | e @ CancelError::DurableSignalState(_),
        ) => error_response(500, api::CommandErrorCode::Internal, e.to_string()),
    }
}

/// Wrap a replay ingress outcome in the response envelope.
fn replay_response(
    status_code: u32,
    outcome: api::replay_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Replay(
            api::ReplayPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Wrap a patch ingress outcome in the response envelope.
fn patch_response(
    status_code: u32,
    outcome: api::patch_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Patch(
            api::PatchPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Wrap a cancel outcome in the response envelope.
fn cancel_response(
    status_code: u32,
    outcome: api::cancel_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Cancel(
            api::CancelPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Wrap a wakeup outcome in the response envelope.
fn wakeup_response(
    status_code: u32,
    outcome: api::wakeup_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Wakeup(
            api::WakeupPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Wrap a trigger outcome in the response envelope.
fn trigger_response(
    status_code: u32,
    outcome: api::trigger_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Trigger(
            api::TriggerPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Wrap a register outcome in the response envelope.
fn register_response(
    status_code: u32,
    outcome: api::register_payload::Outcome,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Register(
            api::RegisterPayload {
                outcome: Some(outcome),
            },
        )),
    }
}

/// Build an error-payload response carrying the HTTP-equivalent status and a
/// typed error code.
fn error_response(
    status_code: u32,
    code: api::CommandErrorCode,
    message: String,
) -> api::ApiCommandResponse {
    api::ApiCommandResponse {
        status_code,
        payload: Some(api::api_command_response::Payload::Error(
            api::ErrorPayload {
                code: code as i32,
                message,
            },
        )),
    }
}

/// Reply for a command kind this conductor doesn't handle (a not-yet-wired
/// variant, or an empty envelope). The API forwards this as 501 Not
/// Implemented.
fn unsupported_response() -> api::ApiCommandResponse {
    error_response(
        501,
        api::CommandErrorCode::UnsupportedCommand,
        "command not supported by this conductor".to_string(),
    )
}
