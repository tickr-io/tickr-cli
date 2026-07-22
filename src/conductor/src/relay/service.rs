//! Implementation of relay service for conductor

use crate::proto::conductor_relay_service_client::ConductorRelayServiceClient;
use crate::proto::{ConductorRelayMessage, EntityType};
use crate::system_tasks::{build_ack, stage_compaction_payload};
use anyhow::Result;
use async_nats::jetstream;
use async_stream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tickr_proto::codec::compaction::decode_envelope;
use tickr_proto::coord::{
    parse_liveness_key, LIVENESS_BUCKET, LIVENESS_MARKER_CONSUMER, LIVENESS_MARKER_TTL,
    MARKER_REASON_EXPIRY, TASK_CANCEL_ACK_CONSUMER, TASK_CANCEL_ACK_STREAM,
    TASK_CANCEL_ACK_SUBJECT, TASK_CANCEL_STREAM, TASK_CANCEL_SUBJECT, TASK_DISPATCH_STREAM,
    TASK_DISPATCH_SUBJECT, TASK_EVENT_CONSUMER, TASK_EVENT_STREAM, TASK_EVENT_SUBJECT,
};
use tickr_proto::patch as pp;
use tickr_proto::signal as sp;
use tickr_proto::task as tc;
use tickr_proto::workflow as wf;
use tickr_proto::TenantId;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use uuid::Uuid;

// Global channel for sending relay messages
static RELAY_TX: Lazy<Arc<Mutex<Option<mpsc::Sender<ConductorRelayMessage>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Prevent a durable definition-lookup fault from hot-looping its JetStream
/// message and exhausting local log storage before an Operator can repair it.
const LOOKUP_INTEGRITY_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Inject the relay-tx slot directly. Production sets it via `run_streaming`
/// once the gRPC stream is established; integration tests that don't want
/// to stand up a full streaming connection use this to point the global at
/// a test channel. Idempotent: a subsequent call replaces the slot.
pub async fn init_relay_tx(tx: mpsc::Sender<ConductorRelayMessage>) {
    let mut guard = RELAY_TX.lock().await;
    *guard = Some(tx);
}

/// Register a protobuf workflow definition through the coordinator relay.
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

/// Publish a dispatched task into the durable task-dispatch work queue and,
/// **only after the publish ack**, emit the conductor's `Delivered` task event
/// onto the relay. Awaiting the ack is what sharpens `Delivered` from
/// "published to fire-and-forget core NATS" to "**durably staged** for an
/// executor": the `TaskDelivered` milestone the server publishes from this
/// event now means the work won't be lost even if no executor is up yet.
/// `delivered` carries no `executor_id` — no executor has picked it up.
pub async fn publish_dispatch_and_deliver(
    nats: &async_nats::Client,
    relay_tx: &mpsc::Sender<ConductorRelayMessage>,
    payload: Vec<u8>,
    task_instance_id: Uuid,
    task_id: Uuid,
    workflow_instance_id: Uuid,
    workflow_id: Uuid,
) -> Result<()> {
    let js = jetstream::new(nats.clone());
    // Publish into the work queue and await the publish ack — the dispatch is
    // durably staged in the substrate before we declare it Delivered.
    js.publish(TASK_DISPATCH_SUBJECT, payload.into())
        .await
        .map_err(|e| anyhow::anyhow!("publish task dispatch: {}", e))?
        .await
        .map_err(|e| anyhow::anyhow!("await task-dispatch publish ack: {}", e))?;

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
pub async fn drain_task_events(
    consumer: jetstream::consumer::PullConsumer,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    nats: async_nats::Client,
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
            eprintln!("Failed to open task-event consumer stream: {}", e);
            return;
        }
    };
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => break,
            next = messages.next() => match next {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    eprintln!("task-event pull error: {}", e);
                    continue;
                }
                None => break,
            },
        };

        let mut task_event = match tc::TaskEvent::decode(&msg.payload[..]) {
            Ok(ev) => ev,
            Err(e) => {
                // Poison message: ack to drop it rather than redeliver an
                // undeserializable event forever.
                eprintln!("dropping undeserializable task event: {}", e);
                let _ = msg.ack().await;
                continue;
            }
        };
        // Identity rides the wire as UUID strings. Parse the ids the drain acts
        // on once; a malformed id on an otherwise-decodable event is poison
        // (ack-drop) rather than an endless redelivery.
        let (event_wfi, event_tii, event_tid) = match (
            Uuid::parse_str(&task_event.workflow_instance_id),
            Uuid::parse_str(&task_event.task_instance_id),
            Uuid::parse_str(&task_event.task_id),
        ) {
            (Ok(wfi), Ok(tii), Ok(tid)) => (wfi, tii, tid),
            _ => {
                eprintln!(
                    "dropping task event with malformed id(s): task_instance_id={:?}",
                    task_event.task_instance_id
                );
                let _ = msg.ack().await;
                continue;
            }
        };
        println!(
            "Received TaskEvent from JetStream: task_id={:?}, kind={:?}",
            task_event.task_instance_id, task_event.kind
        );

        // Enrich the completing task's declared routing variables onto the
        // event before relaying (the executor stays dumb and publishes a bare
        // event). Forwarding a completion without a routing variable the task
        // actually produced would silently strand any loop gated on it, so
        // both genuine faults fail closed: a lookup-integrity fault is NAK'd
        // back to the durable queue after a bounded delay, recoverable once
        // the fault is fixed without creating a hot redelivery loop; and an
        // emitted-but-dropped declared variable — deterministic, so redelivery
        // can never enrich it — is escalated to a conductor-minted terminal
        // task failure. A bag-read failure is surfaced loudly and the
        // un-enriched event forwarded, so task completion is never dropped on
        // a routing-var bug.
        match crate::routing_enrichment::enrich_completed_task_event(
            definition_repository.as_ref(),
            &nats,
            &mut task_event,
        )
        .await
        {
            Ok(()) => {}
            Err(e @ crate::routing_enrichment::EnrichmentError::LookupIntegrity { .. }) => {
                eprintln!(
                    "routing-variable enrichment failed for task {:?}: {} (fail closed: not forwarding; delayed redelivery)",
                    task_event.task_instance_id, e
                );
                let _ = msg
                    .ack_with(jetstream::AckKind::Nak(Some(LOOKUP_INTEGRITY_RETRY_DELAY)))
                    .await;
                continue;
            }
            Err(e @ crate::routing_enrichment::EnrichmentError::SplitStageDrop { .. }) => {
                // Rewrite the completion into the conductor-minted failure
                // verdict — the same relay channel the liveness verdict rides,
                // no new envelope — so the server grounds a terminal task
                // failure that cascades through the existing loop machinery
                // instead of the stuck loop reading as a slow run.
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

        // Self-patch detection (emit-and-exit): a completing task may carry a
        // raw Patch document on its reserved `tickr_patch` output. The
        // conductor detects it HERE, on the completion drain — it is upstream
        // of the server and owns the parser — parses it, and stamps the
        // attempt-invariant `patch_key` onto the forwarded completion so the
        // server arms the Stall on presence alone, atomically with the
        // completion and before its cascade walk. The pipeline fork itself
        // runs AFTER the completion forwards (below): the relay channel is
        // FIFO, so the Stall is always armed before any patch envelope can
        // ask to apply. A document that fails to parse stamps nothing — the
        // reshape is lost-but-logged and the instance never stalls for it.
        let mut pending_self_patch: Option<crate::patch_pipeline::ParsedPatch> = None;
        if matches!(task_event.kind, Some(tc::task_event::Kind::Completed(_))) {
            if let Some(doc) =
                crate::routing_enrichment::read_self_patch_output(&nats, event_wfi, event_tii).await
            {
                match crate::patch_pipeline::parse_self_patch_document(&doc).await {
                    Ok(parsed) => {
                        let key = crate::patch_pipeline::patch_key(event_wfi, event_tid);
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
                        // Ack-on-forward: the durability boundary is here.
                        if let Err(e) = msg.ack().await {
                            eprintln!("task-event ack failed: {} (redelivery converges)", e);
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
                            fork_self_patch(
                                definition_repository.as_ref(),
                                &nats,
                                event_wfi,
                                event_tid,
                                parsed,
                            )
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
                        let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                        break;
                    }
                }
            }
        }
    }
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

/// Drain liveness delete markers off the bucket wildcard and forward each true
/// **expiry** as a conductor-origin `Unhealthy` `TaskEvent` onto the relay,
/// **acking on forward** — the durability boundary, identical to
/// `drain_task_events`. A forward failure (relay outbound closed, conductor
/// dies before ack) leaves the marker un-acked, so the durable consumer
/// redelivers it on recovery: a verdict can't be lost to a relay/conductor
/// blip, and a marker that fired while every conductor was down is still
/// pending when one returns.
///
/// Classification keys on the `Nats-Marker-Reason` header: forward **only**
/// `MaxAge` (true expiry — the executor went dark). A re-arm is a plain PUT (no
/// marker at all) and the executor's terminal delete is a `KV-Operation: DEL`
/// tombstone; both are skipped-and-acked as noise. Correctness does not rest on
/// the filter — even a delete that slipped through as `Unhealthy` is a server-
/// side no-op on the already-terminal task (idempotency).
pub async fn drain_liveness_markers(
    consumer: jetstream::consumer::PullConsumer,
    relay_tx: mpsc::Sender<ConductorRelayMessage>,
    token: CancellationToken,
) {
    let prefix = format!("$KV.{}.", LIVENESS_BUCKET);
    let mut messages = match consumer
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
    {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Failed to open liveness marker consumer stream: {}", e);
            return;
        }
    };
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => break,
            next = messages.next() => match next {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    eprintln!("liveness marker pull error: {}", e);
                    continue;
                }
                None => break,
            },
        };

        // Forward only true expiry markers; skip-and-ack the noise (re-arm
        // PUTs, terminal DEL tombstones).
        let is_expiry = msg
            .headers
            .as_ref()
            .and_then(|h| h.get(async_nats::header::NATS_MARKER_REASON))
            .map(|v| v.to_string() == MARKER_REASON_EXPIRY)
            .unwrap_or(false);
        if !is_expiry {
            let _ = msg.ack().await;
            continue;
        }

        // The identity rides the marker subject (`$KV.<bucket>.<wf>.<wi>.<ti>`)
        // for free — no per-task state on the conductor.
        let Some(identity) = msg
            .subject
            .strip_prefix(&prefix)
            .and_then(parse_liveness_key)
        else {
            eprintln!(
                "liveness marker with unparseable subject {:?}; acking",
                msg.subject.as_str()
            );
            let _ = msg.ack().await;
            continue;
        };

        // Conductor-origin `Unhealthy` — identity-only. `executor_id` is unknown
        // (the executor is gone); `task_id` is not part of the liveness key, and
        // the server reads the real one off the instance it fetches by
        // `task_instance_id`, so a nil placeholder here is never read.
        let event = tc::TaskEvent {
            task_instance_id: identity.task_instance_id.to_string(),
            task_id: Uuid::nil().to_string(),
            workflow_instance_id: identity.workflow_instance_id.to_string(),
            workflow_id: identity.workflow_id.to_string(),
            executor_id: None,
            kind: Some(tc::task_event::Kind::Unhealthy(
                tc::task_event::Unhealthy {},
            )),
        };
        // Author the conductor-origin `Unhealthy` on the published contract.
        let payload = event.encode_to_vec();
        let conductor_msg = ConductorRelayMessage {
            entity_type: EntityType::TaskEvent as i32,
            payload,
            tenant_id: None,
        };
        match relay_tx.send(conductor_msg).await {
            Ok(()) => {
                // Ack-on-forward: the durability boundary.
                if let Err(e) = msg.ack().await {
                    eprintln!("liveness marker ack failed: {} (redelivery converges)", e);
                }
            }
            Err(e) => {
                // Relay outbound closed (cycle ending). Do NOT ack — NAK so the
                // marker stays pending and redelivers on the next cycle.
                eprintln!(
                    "Error forwarding Unhealthy task event: {} (no ack; redelivers)",
                    e
                );
                let _ = msg.ack_with(jetstream::AckKind::Nak(None)).await;
                break;
            }
        }
    }
}

/// Sends a workflow build update through the relay system

/// Self-healing wrapper around the relay stream. Retries the connect+stream
/// loop on any error so that startup ordering against the coordinator gRPC server
/// (and transient mid-flight drops) doesn't leave the conductor wedged.
pub async fn run_streaming(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
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
        match try_run_streaming(shutdown_token.clone(), Arc::clone(&definition_repository)).await {
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
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
) -> Result<()> {
    // Connect to NATS
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;

    // Connect to the configured coordinator's public relay endpoint.
    let channel = Channel::from_shared(tickr_proto::config::coordinator_relay_url())?
        .connect()
        .await?;
    let mut client = ConductorRelayServiceClient::new(channel);

    // Create channel for bidirectional communication
    let (tx, rx) = mpsc::channel::<ConductorRelayMessage>(32);
    let tx_for_updates = tx.clone();

    // Store the sender in the global RELAY_TX for access from other components
    {
        let mut tx_guard = RELAY_TX.lock().await;
        *tx_guard = Some(tx.clone());
    }

    // Durable task-dispatch work queue. The conductor publishes dispatched
    // tasks into it (awaiting the publish ack before emitting `Delivered`); the
    // executor drains it pull-to-capacity. Ensure it exists before the first
    // dispatch — a publish to a subject with no stream would otherwise fail.
    ensure_task_dispatch_stream(&nats).await?;

    // Durable conductor→executor cancel-request work queue. The conductor
    // publishes cancel-requests into it (on inbound `CANCEL_TASK` envelopes);
    // the executor drains it. Ensure it exists before the first publish.
    ensure_task_cancel_stream(&nats).await?;

    // Durable task-event update leg. The executor publishes typed `TaskEvent`s
    // into this JetStream work queue; the conductor drains via a shared durable
    // pull consumer and acks on forward (see `task_event_consumer` /
    // `drain_task_events`).
    let consumer = task_event_consumer(&nats).await?;

    let tx_for_nats = tx_for_updates.clone();
    let definitions_for_forwarder = Arc::clone(&definition_repository);
    let nats_for_forwarder = nats.clone();

    // Per-cycle token: cancelled when this attempt ends so the forwarder task
    // doesn't outlive its mpsc receiver across reconnects.
    let cycle_token = CancellationToken::new();
    let forwarder_token = cycle_token.clone();
    let _drop_guard = cycle_token.clone().drop_guard();

    let _forwarder_handle = tokio::spawn(drain_task_events(
        consumer,
        tx_for_nats,
        definitions_for_forwarder,
        nats_for_forwarder,
        forwarder_token,
    ));

    // Liveness marker-consumer: a third `TaskEvent` producer alongside the
    // executor's updates and the `Delivered` producer. The shared durable
    // consumer drains the bucket-wildcard delete markers and forwards each
    // expiry as an `Unhealthy` verdict, ack-on-forward — bound once here, on the
    // same per-cycle relay channel as `drain_task_events`.
    let liveness_consumer = liveness_marker_consumer(&nats).await?;
    let liveness_tx = tx_for_updates.clone();
    let liveness_token = cycle_token.clone();
    let _liveness_handle = tokio::spawn(drain_liveness_markers(
        liveness_consumer,
        liveness_tx,
        liveness_token,
    ));

    // Cancel-ack drain: forwards each executor `CancelTaskAck` onto the relay
    // as a `CANCEL_TASK_ACK` envelope (ack-on-forward), the mirror of
    // `drain_task_events`. Bound once here on the same per-cycle relay channel.
    let cancel_ack_consumer = cancel_ack_consumer(&nats).await?;
    let cancel_ack_tx = tx_for_updates.clone();
    let cancel_ack_token = cycle_token.clone();
    let _cancel_ack_handle = tokio::spawn(drain_cancel_acks(
        cancel_ack_consumer,
        cancel_ack_tx,
        cancel_ack_token,
    ));

    // Create outbound stream that takes messages from the mpsc channel
    let outbound = async_stream::stream! {
        let mut rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Initial message to establish the connection. It self-asserts this
        // conductor's tenant on the coordinator channel at handshake — the coordinator
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

    let response = client.stream_conductor_relay(outbound).await?;
    let mut inbound = response.into_inner();
    let nats_clone = nats.clone();

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
                                        &nats_clone,
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
                                // Server→conductor cancel-request: republish it
                                // onto the durable task-cancel work queue the
                                // executor drains (the dispatch-leg mirror).
                                // Best-effort — the task's state is already
                                // grounded; a publish failure only leaves the
                                // kill unconfirmed, never stalls the cancel.
                                if let Err(e) = publish_task_cancel(
                                    &nats_clone,
                                    msg.payload.to_vec(),
                                ).await {
                                    eprintln!("Failed to publish cancel-request into work queue: {}", e);
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
                                let nats_for_stage = nats_clone.clone();
                                tokio::spawn(async move {
                                    // Decode the proto envelope only for the ack correlation +
                                    // the instance id; the raw bytes are staged verbatim so the
                                    // drain decodes the same encoding the server shipped.
                                    match decode_envelope(&msg.payload) {
                                        Ok(envelope) => {
                                            let projection = envelope
                                                .projection
                                                .as_ref()
                                                .expect("decode_envelope guarantees a projection");
                                            let wfi_id = projection.id.clone();
                                            let state = projection.state.clone();
                                            match stage_compaction_payload(
                                                &nats_for_stage,
                                                msg.payload.to_vec(),
                                            )
                                            .await
                                            {
                                                Ok(()) => {
                                                    let ack_msg =
                                                        build_ack(&wfi_id, &envelope.correlation);
                                                    if let Err(e) = ack_tx.send(ack_msg).await {
                                                        eprintln!(
                                                            "Failed to send COMPACTION_ACK for {}: {}",
                                                            wfi_id, e
                                                        );
                                                    } else {
                                                        println!(
                                                            "Staged compaction job for workflow {} (state={})",
                                                            wfi_id, state
                                                        );
                                                    }
                                                }
                                                Err(e) => {
                                                    // No ACK means durable staging was not confirmed.
                                                    eprintln!(
                                                        "compaction staging failed for {}: {} (no ACK; server will re-ship)",
                                                        wfi_id, e
                                                    );
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to decode compaction envelope: {}",
                                                e
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
                                // Server-emitted relay-back stamping the
                                // materialized impact count of a signal
                                // (today: ByTag-cancel fan-out). Relay
                                // routing is uniform `Any`, so the conductor
                                // that receives this relay-back need not be
                                // the one holding the open HTTP wait —
                                // re-publish it onto tenant NATS keyed by
                                // signal_id so whichever conductor is waiting
                                // picks it up. Correlation lives off the
                                // relay stream, so it survives a reconnect.
                                match sp::SignalApplied::decode(&msg.payload[..])
                                    .map_err(anyhow::Error::from)
                                    .and_then(|a| Ok(uuid::Uuid::parse_str(&a.signal_id)?))
                                {
                                    Ok(signal_id) => {
                                        let subject =
                                            crate::cancel_pipeline::signal_applied_subject(signal_id);
                                        if let Err(e) = nats_clone
                                            .publish(subject, msg.payload.clone().into())
                                            .await
                                        {
                                            eprintln!(
                                                "Failed to publish SignalApplied for signal_id={} onto tenant NATS: {}",
                                                signal_id, e
                                            );
                                        } else if let Err(e) = nats_clone.flush().await {
                                            eprintln!(
                                                "Failed to flush SignalApplied for signal_id={} onto tenant NATS: {}",
                                                signal_id, e
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to deserialize SignalApplied: {}", e);
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

    /// Wake an in-process waiter for one materialized ByTag cancellation.
    fn signal_applied(&self, signal_id: Uuid);
}

/// Run the existing cross-plane relay with Tickr Lite's local Data-plane
/// coordination roles. Connection failures retain the distributed relay's
/// bounded retry behavior and never reinterpret local durable state.
pub async fn run_streaming_lite(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    roles: Arc<dyn LiteRelayRoles>,
) -> Result<()> {
    run_streaming_lite_at(
        shutdown_token,
        definition_repository,
        roles,
        tickr_proto::config::coordinator_relay_url(),
        TenantId::from_env().to_string(),
    )
    .await
}

async fn run_streaming_lite_at(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    roles: Arc<dyn LiteRelayRoles>,
    relay_url: String,
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
            Arc::clone(&definition_repository),
            &relay_url,
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
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    relay_url: &str,
    tenant_id: &str,
    roles: Arc<dyn LiteRelayRoles>,
) -> Result<()> {
    let channel = Channel::from_shared(relay_url.to_owned())?
        .connect()
        .await?;
    let mut client = ConductorRelayServiceClient::new(channel);
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
    let response = client.stream_conductor_relay(outbound).await?;
    let mut inbound = response.into_inner();

    tokio::select! {
        result = async {
            while let Some(message) = inbound.next().await {
                let message = message?;
                match EntityType::try_from(message.entity_type) {
                    Ok(EntityType::TaskQueueItem) => {
                        let dispatch = tc::TaskDispatch::decode(&message.payload[..])?;
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
                        roles.signal_applied(Uuid::parse_str(&applied.signal_id)?);
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
mod lite_relay_tests {
    use super::*;
    use crate::proto::conductor_relay_service_server::{
        ConductorRelayService, ConductorRelayServiceServer,
    };
    use futures::Stream;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;
    use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::config::DataPlaneSql;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status, Streaming};

    type RelayStream =
        Pin<Box<dyn Stream<Item = Result<ConductorRelayMessage, Status>> + Send + 'static>>;

    #[derive(Clone)]
    struct TestRelay {
        connections: Arc<AtomicUsize>,
        outage: CancellationToken,
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
            tokio::spawn(async move {
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
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ConductorRelayServiceServer::new(TestRelay {
                    connections,
                    outage,
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
    async fn lite_relay_survives_outage_and_reconnects() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let relay_url = format!("http://{address}");
        let connections = Arc::new(AtomicUsize::new(0));
        let first_outage = CancellationToken::new();
        let first_server =
            start_relay(address, Arc::clone(&connections), first_outage.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!first_server.is_finished(), "test relay failed to start");
        let (_directory, repository) = definition_repository().await;
        let shutdown = CancellationToken::new();
        let relay = tokio::spawn(run_streaming_lite_at(
            shutdown.clone(),
            repository,
            Arc::new(NoopRoles),
            relay_url,
            "test-tenant".to_owned(),
        ));

        await_connections(&connections, 1).await;
        first_outage.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        first_server.abort();
        first_server.await.unwrap_err();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!relay.is_finished(), "a relay outage must not fail Lite");

        let second_server =
            start_relay(address, Arc::clone(&connections), CancellationToken::new()).await;
        await_connections(&connections, 2).await;
        shutdown.cancel();
        relay.await.unwrap().unwrap();
        second_server.abort();
    }
}
