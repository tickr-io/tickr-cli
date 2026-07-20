//! Shared cancel-ingress pipeline.
//!
//! Cancel has two transports: the HTTP cancel routes (`POST
//! /api/signals/cancel` plus the two path-encoded sugar routes) and the API
//! component's command bus. Both translate a cancel intent into a wire
//! `Signal::Cancel`, run the idempotency cache, subscribe-before-forward for
//! ByTag targets, await the server's `SignalApplied` relay-back, and persist
//! the audit row. This module is the shared middle layer, mirroring
//! `trigger_pipeline` / `wakeup_translator`.
//!
//! **Ordering invariant (correctness-critical):** for ByTag targets the
//! `signal_applied.<signal_id>` tenant-NATS subscription is established (and
//! flushed) BEFORE the signal is forwarded, so a fast-relaying server can't
//! deliver the relay-back before the subscription exists. The correlation
//! rides tenant NATS rather than an in-process registry, so it survives a
//! relay reconnect and any conductor can relay the apply in — the conductor
//! holding the open HTTP wait picks it up off the subject.

use std::collections::HashMap;
use std::time::Duration;

use async_nats::Client as NatsClient;
use futures::StreamExt;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use tickr_proto::signal as sp;
use tokio::time::timeout;
use uuid::Uuid;

use crate::signal_cancels::SignalCancelRow;

/// How long to wait for the server's `SignalApplied` relay-back before giving
/// up on a ByTag fan-out. Tuned generously so a slow scan over a large fleet
/// still resolves; a partitioned server surfaces as a 503.
pub const SIGNAL_APPLIED_DEADLINE: Duration = Duration::from_secs(15);

/// Tenant-NATS subject prefix on which the server's `SignalApplied` relay-back
/// is re-published, keyed by `signal_id`. The conductor forwarding the signal
/// subscribes to `signal_applied.<signal_id>`; whichever conductor receives
/// the relay-back (routing is uniform `Any`) publishes to it. Keeping the
/// correlation on tenant NATS — not an in-process registry — is what lets it
/// survive a relay reconnect and cross conductor instances.
pub const SIGNAL_APPLIED_SUBJECT_PREFIX: &str = "signal_applied";

/// Build the `signal_applied.<signal_id>` subject. A hyphenated UUID is
/// dot-free, so it is a single valid subject token.
pub fn signal_applied_subject(signal_id: Uuid) -> String {
    format!("{SIGNAL_APPLIED_SUBJECT_PREFIX}.{signal_id}")
}

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
    #[error("relay unreachable: {0}")]
    RelayUnreachable(#[source] anyhow::Error),
    #[error("timed out waiting for server-side SignalApplied")]
    ByTagTimeout { signal_id: Uuid },
}

/// Run the shared cancel pipeline. Mints a `signal_id`, consults the
/// idempotency cache, forwards the wire `Signal::Cancel`, awaits
/// `SignalApplied` for ByTag targets, and persists the `signal_cancels` audit
/// row. Side effects run only past the dedup/conflict short-circuit.
pub async fn process_cancel(
    pool: &PgPool,
    nats: &NatsClient,
    req: CancelRequest,
) -> Result<CancelOutcome, CancelError> {
    let signal_id = Uuid::new_v4();
    let target_json =
        serde_json::to_value(&req.target).map_err(|e| CancelError::SerializeTarget(e.into()))?;

    let hash_payload = json!({
        "target": &target_json,
        "note": &req.note,
    });
    let input_hash = crate::canonical_json::hash(Some(&hash_payload));

    // Idempotency: same key + same body returns the cached signal_id; a body
    // change against the same key is a 409 collision.
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

    // HTTP-driven cancels carry no actor attribution surface; the audit thread
    // is the signal_id itself.
    let wire_target = match &req.target {
        CancelTargetBody::Instance {
            workflow_instance_id,
            node_id,
        } => sp::target::Addressing::Instance(sp::target::Instance {
            workflow_instance_id: workflow_instance_id.to_string(),
            node_id: node_id.map(|n| n.to_string()),
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

    // ORDERING INVARIANT: subscribe to the relay-back subject BEFORE
    // forwarding (and flush so the interest reaches NATS) so a fast-relaying
    // server can't publish the SignalApplied before our subscription exists.
    // The subscription rides tenant NATS keyed by signal_id, independent of
    // the relay stream, so the correlation survives a relay reconnect and any
    // conductor can relay the apply in. Instance targets have no relay-back
    // path today.
    let mut applied_sub = if is_bytag {
        let subject = signal_applied_subject(signal_id);
        let sub = nats.subscribe(subject).await.map_err(|e| {
            CancelError::RelayUnreachable(anyhow::anyhow!("subscribe signal_applied: {e}"))
        })?;
        nats.flush().await.map_err(|e| {
            CancelError::RelayUnreachable(anyhow::anyhow!("flush signal_applied subscription: {e}"))
        })?;
        Some(sub)
    } else {
        None
    };

    if let Err(e) = crate::relay::send_signal(&signal).await {
        // Dropping `applied_sub` here auto-unsubscribes; nothing to clean up.
        return Err(CancelError::RelayUnreachable(e));
    }

    if let Some(sub) = applied_sub.as_mut() {
        let applied = match timeout(SIGNAL_APPLIED_DEADLINE, sub.next()).await {
            Ok(Some(msg)) => {
                // Decode the published `tickr.signal.SignalApplied` re-published
                // onto tenant NATS by the relay-consuming conductor.
                match sp::SignalApplied::decode(&msg.payload[..]) {
                    Ok(applied) => applied,
                    Err(e) => {
                        eprintln!("cancel pipeline: malformed SignalApplied relay-back: {}", e);
                        return Err(CancelError::ByTagTimeout { signal_id });
                    }
                }
            }
            // A closed subscription (conductor/NATS death) or an elapsed
            // deadline both leave the correlation unresolved: fail so the
            // caller retries idempotently with a fresh signal_id.
            Ok(None) | Err(_) => {
                return Err(CancelError::ByTagTimeout { signal_id });
            }
        };
        let row = SignalCancelRow {
            signal_id,
            applied_count: applied.matched_count as i32,
            target: target_json,
            note: req.note,
        };
        if let Err(e) = crate::signal_cancels::insert(pool, &row).await {
            eprintln!("cancel pipeline: signal_cancels persist failed: {}", e);
        }
        Ok(CancelOutcome::ByTag {
            signal_id,
            instances_matched: applied.matched_count,
        })
    } else {
        // Instance target: no relay-back today; record the audit row with
        // applied_count=1 so the read endpoint has a stable shape across both
        // targets. The server applies the cancel asynchronously.
        let row = SignalCancelRow {
            signal_id,
            applied_count: 1,
            target: target_json,
            note: req.note,
        };
        if let Err(e) = crate::signal_cancels::insert(pool, &row).await {
            eprintln!("cancel pipeline: signal_cancels persist failed: {}", e);
        }
        Ok(CancelOutcome::Instance { signal_id })
    }
}
