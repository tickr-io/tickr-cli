//! Conductor side of the API command bus.
//!
//! One queue-group subscriber on the single subject `tickr.api.commands` —
//! the subject names the API<->conductor relationship rather than each verb.
//! Each inbound message is an encoded `ApiCommandRequest`; the subscriber
//! decodes it, dispatches by `oneof` variant to the matching pipeline
//! function, and replies on the message's reply inbox with an encoded
//! `ApiCommandResponse` carrying the HTTP-equivalent `status_code` plus
//! exactly one `payload` variant.
//!
//! Processing is serial — each command is `.await`ed inline in the receive
//! loop, matching the precedent in `build_pipeline::worker` and
//! `nats_ingress`. The known cost (an in-flight register head-of-line-blocks a
//! queued trigger) is accepted for the single-tenant MVP.

use anyhow::Result;
use async_nats::Client as NatsClient;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use prost::Message as _;
use serde_json::Value;
use sqlx::PgPool;
use std::sync::Arc;
use tickr_ctx::envelope::SignalSource;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cancel_pipeline::CancelTargetBody;
use crate::gate_index::GateIndex;
use crate::wakeup_translator::WakeupRelaySender;
use tickr_proto::tickr_api as api;

/// The single subject all four command kinds travel on.
pub const COMMAND_SUBJECT: &str = "tickr.api.commands";

/// Queue group the conductor binds. One conductor per tenant today; the group
/// makes adding replicas a no-op rather than a fan-out-to-all.
pub const QUEUE_GROUP: &str = "tickr-conductor-api-commands";

/// Everything the dispatch arms need that isn't a process-wide singleton. The
/// relay sender and the gate index are carried so the trigger / cancel /
/// wakeup arms reach the same machinery the HTTP handlers use. (The
/// idempotency cache is a process-wide singleton, and the ByTag cancel
/// relay-back correlates over the `signal_applied.<signal_id>` tenant-NATS
/// subject reached via `nats` inside the pipeline, not threaded here.)
#[derive(Clone)]
pub struct ApiCommandsState {
    pub pg_pool: Arc<PgPool>,
    pub nats: NatsClient,
    pub relay_sender: Arc<dyn WakeupRelaySender>,
    /// Outbound seam for `PatchWorkflowInstance` envelopes — trait-carried so
    /// the patch dispatch arm is testable without the relay client, matching
    /// `relay_sender`.
    pub patch_relay_sender: Arc<dyn crate::patch_pipeline::PatchRelaySender>,
    pub gate_index: GateIndex,
}

/// Bind the queue-group subscriber and process commands serially until the
/// cancellation token fires.
pub async fn start(state: ApiCommandsState, cancel: CancellationToken) -> Result<()> {
    let mut sub = state
        .nats
        .queue_subscribe(COMMAND_SUBJECT, QUEUE_GROUP.into())
        .await?;
    println!(
        "api_commands_consumer: subscribed, subject={}, queue_group={}",
        COMMAND_SUBJECT, QUEUE_GROUP
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                println!("api_commands_consumer: shutdown signal received");
                break;
            }
            maybe_msg = sub.next() => {
                match maybe_msg {
                    Some(msg) => process_one(&state, msg).await,
                    None => {
                        println!("api_commands_consumer: subscription ended");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Decode one inbound request, dispatch it, and publish the reply on the
/// message's reply inbox. A request with no reply subject can't be a NATS
/// request/reply call; log and drop it.
async fn process_one(state: &ApiCommandsState, msg: async_nats::Message) {
    let Some(reply) = msg.reply.clone() else {
        eprintln!("api_commands_consumer: message without reply subject, dropping");
        return;
    };

    let response = handle(state, &msg.payload).await;
    let bytes = response.encode_to_vec();
    if let Err(e) = state.nats.publish(reply, bytes.into()).await {
        eprintln!("api_commands_consumer: failed to publish reply: {}", e);
        return;
    }
    // Flush so the synchronous HTTP caller's request resolves promptly rather
    // than waiting on the client's periodic writer flush.
    if let Err(e) = state.nats.flush().await {
        eprintln!("api_commands_consumer: flush after reply failed: {}", e);
    }
}

/// Decode the envelope and dispatch by command kind. A malformed envelope is a
/// 400 with a `BAD_REQUEST` error payload — the API renders it as a 400.
async fn handle(state: &ApiCommandsState, payload: &[u8]) -> api::ApiCommandResponse {
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
    state: &ApiCommandsState,
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
        &state.pg_pool,
        state.patch_relay_sender.as_ref(),
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
            if !build_jobs.is_empty() {
                if let Err(e) =
                    crate::patch_pipeline::publish_patch_build_jobs(&state.nats, &build_jobs).await
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
    state: &ApiCommandsState,
    req: api::ReplayRequest,
) -> api::ApiCommandResponse {
    use crate::replay_pipeline::{
        process_replay, DefaultReplayRelaySender, ReplayError, ReplayIngress, ReplayRequest as PReq,
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

    // The seed's ctx re-hydration writes NATS KV; the default sender carries a
    // NATS client for it and relays Signals over the global channel.
    let sender = DefaultReplayRelaySender {
        nats: state.nats.clone(),
    };

    match process_replay(state.pg_pool.as_ref(), &sender, pipeline_req).await {
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
    state: &ApiCommandsState,
    req: api::RegisterRequest,
) -> api::ApiCommandResponse {
    use crate::register_pipeline::{
        process_register, RegisterError, RegisterOutcome, RegisterRequest,
    };

    let pipeline_req = RegisterRequest {
        nickel_source: req.nickel_source,
        namespace: req.namespace,
    };
    match process_register(state.pg_pool.as_ref(), &state.nats, pipeline_req).await {
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
    state: &ApiCommandsState,
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

    let outcome = match process_trigger(state.pg_pool.as_ref(), &state.nats, pipeline_req).await {
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
        | Err(e @ TriggerError::PostgresWrite(_))
        | Err(e @ TriggerError::NatsWrite(_)) => {
            return error_response(500, api::CommandErrorCode::Internal, e.to_string())
        }
    };

    match outcome {
        TriggerOutcome::Fresh { signal_id, signal } => {
            // The HTTP trigger path blocks on the relay send and surfaces a
            // failure as 503; mirror that here.
            if let Err(e) = state.relay_sender.send(&signal).await {
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
    state: &ApiCommandsState,
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

    match process_wakeup(
        state.pg_pool.as_ref(),
        &state.nats,
        state.relay_sender.as_ref(),
        &state.gate_index,
        pipeline_req,
    )
    .await
    {
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
/// The pipeline (`cancel_pipeline::process_cancel`) owns the idempotency
/// check, the ByTag register-before-forward ordering, the `SignalApplied`
/// await, and the audit-row write.
async fn dispatch_cancel(
    state: &ApiCommandsState,
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

    match process_cancel(state.pg_pool.as_ref(), &state.nats, pipeline_req).await {
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
        // ByTag timeout: the signal_id rides in the error message (the API has
        // no structured field for it on the error path).
        Err(CancelError::ByTagTimeout { signal_id }) => error_response(
            503,
            api::CommandErrorCode::Unavailable,
            format!(
                "timed out waiting for server-side SignalApplied; signal_id={}",
                signal_id
            ),
        ),
        Err(e @ CancelError::RelayUnreachable(_)) => {
            error_response(503, api::CommandErrorCode::Unavailable, e.to_string())
        }
        Err(
            e @ CancelError::SerializeTarget(_)
            | e @ CancelError::IdempotencyBucket(_)
            | e @ CancelError::IdempotencyCheck(_),
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
