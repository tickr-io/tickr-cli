//! Shared cancel-ingress pipeline.
//!
//! Cancel has two transports: the HTTP cancel routes (`POST
//! /api/signals/cancel` plus the two path-encoded sugar routes) and the API
//! component's command bus. Both translate a cancel intent into a wire
//! `Signal::Cancel`, run the applicable idempotency check, and persist durable
//! Signal state before forwarding ByTag work. Signal-applied notifications only
//! reduce reconciliation latency; bounded SQL reads decide materialization.

use std::collections::HashMap;
use std::time::Duration;

use async_nats::Client as NatsClient;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::signal_repository::PENDING_SIGNAL_CANCEL_APPLIED_COUNT;
use tickr_proto::signal as sp;
use uuid::Uuid;

use crate::signal_cancels::SignalCancelRow;

/// Maximum time an API request waits for durable ByTag materialization.
pub const SIGNAL_APPLIED_DEADLINE: Duration = Duration::from_secs(15);

/// Durable Signal state is read at this bounded cadence even when every
/// notification is lost or unavailable.
const SIGNAL_APPLIED_RECONCILIATION_INTERVAL: Duration = Duration::from_millis(50);

/// Cancel target the transport-specific caller assembles. Mirrors the wire
/// `Signal::Target`: `Instance` names one live run (optionally one node within
/// it); `ByTag` fans out across every live instance whose merged tags match.
/// Serde shape is the HTTP request body's `{ "kind": ..., ... }`.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CancelTargetBody {
    Instance {
        workflow_instance_id: Uuid,
        #[serde(default)]
        node_id: Option<Uuid>,
    },
    ByTag {
        filter: HashMap<String, String>,
    },
}

/// Producer intent the transport-specific caller assembles.
pub struct CancelRequest {
    pub target: CancelTargetBody,
    pub note: Option<String>,
    pub idempotency_key: Option<String>,
}

/// Outcome of `process_cancel`. Parallels `TriggerOutcome` / `WakeupOutcome`.
pub enum CancelOutcome {
    /// Instance target accepted. No relay-back path today; the audit row is
    /// written with `applied_count = 1`.
    Instance { signal_id: Uuid },
    /// ByTag target resolved with the server's materialized impact count.
    ByTag {
        signal_id: Uuid,
        instances_matched: u32,
    },
    /// Idempotent retry — same key, byte-identical body.
    Deduplicated { original_signal_id: Uuid },
    /// Same key, different body — a client bug.
    Conflict {
        original_signal_id: Uuid,
        original_hash: String,
        your_hash: String,
    },
}

/// Failure modes the pipeline distinguishes. The `Display` strings on the
/// non-timeout variants are the exact HTTP messages today's handler returns,
/// so callers reproduce them via `err.to_string()`. `ByTagTimeout` carries the
/// `signal_id` separately so each transport can render it its own way.
#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("serialize target: {0}")]
    SerializeTarget(#[source] anyhow::Error),
    #[error("idempotency bucket: {0}")]
    IdempotencyBucket(#[source] anyhow::Error),
    #[error("idempotency check: {0}")]
    IdempotencyCheck(#[source] anyhow::Error),
    #[error("durable Signal state: {0}")]
    DurableSignalState(#[source] anyhow::Error),
    #[error("relay unreachable: {0}")]
    RelayUnreachable(#[source] anyhow::Error),
    #[error("timed out waiting for durable Signal materialization")]
    ByTagTimeout { signal_id: Uuid },
}

/// Run the shared cancel pipeline. Mints a `signal_id`, consults the
/// idempotency cache, stages ByTag Signal state, forwards the wire Signal, and
/// reconciles its materialization from SQL. Side effects run only past the
/// dedup/conflict short-circuit.
pub async fn process_cancel<S>(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
    notifications: &tokio::sync::Mutex<S>,
    req: CancelRequest,
) -> Result<CancelOutcome, CancelError>
where
    S: crate::signal_applied_notifier::SignalAppliedReconciliationStream,
{
    let signal_id = Uuid::new_v4();
    let target_json =
        serde_json::to_value(&req.target).map_err(|e| CancelError::SerializeTarget(e.into()))?;

    let hash_payload = json!({
        "target": &target_json,
        "note": &req.note,
    });
    let input_hash = crate::canonical_json::hash(Some(&hash_payload));

    // Idempotency remains transport-specific and precedes the role-neutral
    // Signal-applied observation path.
    if let Some(key) = req.idempotency_key.as_deref() {
        let bucket = crate::idempotency::open_bucket(nats)
            .await
            .map_err(CancelError::IdempotencyBucket)?;
        let outcome = crate::idempotency::check_or_insert(&bucket, key, signal_id, &input_hash)
            .await
            .map_err(CancelError::IdempotencyCheck)?;
        match outcome {
            crate::idempotency::CacheOutcome::Fresh => {}
            crate::idempotency::CacheOutcome::DeduplicatedSameHash { original_signal_id } => {
                return Ok(CancelOutcome::Deduplicated { original_signal_id });
            }
            crate::idempotency::CacheOutcome::ConflictDifferentHash {
                original_signal_id,
                original_hash,
            } => {
                return Ok(CancelOutcome::Conflict {
                    original_signal_id,
                    original_hash,
                    your_hash: hex::encode(input_hash),
                });
            }
        }
    }

    process_prepared_cancel(repositories, notifications, req, signal_id, target_json).await
}

/// Tickr Lite cancellation uses the same durable Signal reconciliation as the
/// distributed path. Its bounded in-process channel is only a latency hint.
pub async fn process_cancel_local<S>(
    repositories: &WriterRepositoryBundle,
    notifications: &tokio::sync::Mutex<S>,
    req: CancelRequest,
) -> Result<CancelOutcome, CancelError>
where
    S: crate::signal_applied_notifier::SignalAppliedReconciliationStream,
{
    process_cancel_with_notifications(repositories, notifications, req).await
}

/// Cancellation path shared by transient notifier adapters.
///
/// Notification delivery can only shorten the next durable Signal-state read;
/// it cannot settle the request or change the relay acknowledgement boundary.
pub async fn process_cancel_with_notifications<S>(
    repositories: &WriterRepositoryBundle,
    notifications: &tokio::sync::Mutex<S>,
    req: CancelRequest,
) -> Result<CancelOutcome, CancelError>
where
    S: crate::signal_applied_notifier::SignalAppliedReconciliationStream,
{
    let signal_id = Uuid::new_v4();
    let target_json = serde_json::to_value(&req.target)
        .map_err(|error| CancelError::SerializeTarget(error.into()))?;
    process_prepared_cancel(repositories, notifications, req, signal_id, target_json).await
}

async fn process_prepared_cancel<S>(
    repositories: &WriterRepositoryBundle,
    notifications: &tokio::sync::Mutex<S>,
    req: CancelRequest,
    signal_id: Uuid,
    target_json: serde_json::Value,
) -> Result<CancelOutcome, CancelError>
where
    S: crate::signal_applied_notifier::SignalAppliedReconciliationStream,
{
    let wire_target = match &req.target {
        CancelTargetBody::Instance {
            workflow_instance_id,
            node_id,
        } => sp::target::Addressing::Instance(sp::target::Instance {
            workflow_instance_id: workflow_instance_id.to_string(),
            node_id: node_id.map(|node| node.to_string()),
        }),
        CancelTargetBody::ByTag { filter } => sp::target::Addressing::ByTag(sp::target::ByTag {
            filter: filter.clone(),
        }),
    };
    let signal = sp::Signal {
        signal_id: signal_id.to_string(),
        idempotency_key: req.idempotency_key.clone(),
        variant: Some(sp::signal::Variant::Cancel(sp::Cancel {
            target: Some(sp::Target {
                addressing: Some(wire_target),
            }),
            reason: Some(sp::CancelReason {
                reason: Some(sp::cancel_reason::Reason::UserRequested(
                    sp::cancel_reason::UserRequested { actor: None },
                )),
            }),
            note: req.note.clone(),
        })),
    };

    let is_bytag = matches!(req.target, CancelTargetBody::ByTag { .. });
    if is_bytag {
        crate::signal_cancels::stage_pending(
            repositories,
            &SignalCancelRow {
                signal_id,
                applied_count: PENDING_SIGNAL_CANCEL_APPLIED_COUNT,
                target: target_json.clone(),
                note: req.note.clone(),
            },
        )
        .await
        .map_err(CancelError::DurableSignalState)?;
    }

    let mut notification_stream = if is_bytag {
        Some(notifications.lock().await)
    } else {
        None
    };
    crate::relay::send_signal(&signal)
        .await
        .map_err(CancelError::RelayUnreachable)?;

    if let Some(stream) = notification_stream.as_mut() {
        let instances_matched =
            await_bytag_materialization(repositories, signal_id, &mut **stream).await?;
        Ok(CancelOutcome::ByTag {
            signal_id,
            instances_matched,
        })
    } else {
        let row = SignalCancelRow {
            signal_id,
            applied_count: 1,
            target: target_json,
            note: req.note,
        };
        if let Err(error) = crate::signal_cancels::insert(repositories, &row).await {
            eprintln!("cancel pipeline: signal_cancels persist failed: {error}");
        }
        Ok(CancelOutcome::Instance { signal_id })
    }
}

async fn durable_materialization_count(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
) -> Result<Option<u32>, CancelError> {
    crate::signal_cancels::materialized_count(repositories, signal_id)
        .await
        .map_err(CancelError::DurableSignalState)
}

async fn await_bytag_materialization<S>(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
    notifications: &mut S,
) -> Result<u32, CancelError>
where
    S: crate::signal_applied_notifier::SignalAppliedReconciliationStream,
{
    let deadline = tokio::time::Instant::now() + SIGNAL_APPLIED_DEADLINE;
    loop {
        if let Some(count) = durable_materialization_count(repositories, signal_id).await? {
            return Ok(count);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(CancelError::ByTagTimeout { signal_id });
        }
        let bounded_wait = remaining.min(SIGNAL_APPLIED_RECONCILIATION_INTERVAL);
        let _ = notifications.next_reconciliation(bounded_wait).await;
    }
}
