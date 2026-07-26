//! Shared substrate constants and key helpers for the **task-instance liveness
//! watchdog** — the data-plane dead-man's-switch that detects a task whose
//! executor has gone dark.
//!
//! These items are the contract between the two data-plane sides that never
//! reference each other: the **executor** (producer — arms/re-arms/deletes a
//! per-task *liveness key* in a dedicated NATS KV bucket) and the **conductor**
//! (consumer — drains the bucket's delete markers and reports `Unhealthy`).
//! They live on the published contract so both planes import one definition and
//! cannot drift — only the shared *names, the key scheme, and the marker
//! classification* live here; the actual KV setup (`ensure_liveness_bucket`) is
//! duplicated on each data-plane side, because a bucket ensure needs NATS and
//! the published contract crate must not depend on it.

use std::time::Duration;
use uuid::Uuid;

/// The dedicated KV bucket holding one *liveness key* per running task
/// instance. Shared name so the executor's producer and the conductor's
/// marker-consumer address the same bucket.
pub const LIVENESS_BUCKET: &str = super::all_nats::LIVENESS_BUCKET;

/// Durable pull-consumer name the conductor binds on the bucket's backing
/// stream. Shared across conductor instances — NATS load-balances delivery
/// across whoever binds the same durable name, so the consumer holds no
/// per-task state (the `task_event_consumer` pattern).
pub const LIVENESS_MARKER_CONSUMER: &str = super::all_nats::LIVENESS_MARKER_CONSUMER;

/// `SubjectDeleteMarkerTTL` for the bucket — the **verdict-durability window**.
/// A delete marker that fires while *every* conductor is down is still pending
/// in the backing stream when one returns (the durable consumer drains it),
/// so a control-plane bounce can't silently swallow a liveness verdict. Sized
/// generously (24h) relative to the liveness timeout default (2 min); the happy
/// path acks-on-forward immediately, so a long window costs nothing but
/// insurance.
pub const LIVENESS_MARKER_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Env var setting the **liveness timeout = the per-key TTL**, in whole
/// seconds. System-internal (a platform-reliability knob), deliberately **not**
/// a workflow-DSL field — a workflow author has no business tuning how fast the
/// platform notices a dead worker. The refresh cadence is *derived* as TTL/4,
/// never independently configurable.
pub const LIVENESS_TIMEOUT_ENV: &str = "TICKR_LIVENESS_TIMEOUT_SECS";

/// Default liveness timeout: 2 minutes. A crashed executor's task is detected
/// within ~this window rather than holding the run hostage for the full
/// (minutes-to-hours) execution budget.
pub const DEFAULT_LIVENESS_TIMEOUT_SECS: u64 = 120;

/// The `Nats-Marker-Reason` value the NATS server stamps on a **per-key-TTL
/// expiry** delete marker. The marker-consumer forwards *only* messages
/// carrying this reason: a re-arm is a plain PUT (no marker at all — the
/// subject never empties on a supersede) and the executor's terminal delete is
/// a `KV-Operation: DEL` tombstone, so an allowlist on this one value skips all
/// the noise. Correctness rests on the server's idempotency guard, not on this
/// filter.
pub const MARKER_REASON_EXPIRY: &str = "MaxAge";

/// The full-identity *liveness key* for a task instance:
/// `<workflow_id>.<workflow_instance_id>.<task_instance_id>`. The key is the
/// KV subject, so the identity rides the delete marker for free — the
/// marker-consumer reconstructs the task instance from the key alone, carrying
/// no side state. UUIDs contain no `.`, so the segments are unambiguous.
pub fn liveness_key(
    workflow_id: Uuid,
    workflow_instance_id: Uuid,
    task_instance_id: Uuid,
) -> String {
    format!("{workflow_id}.{workflow_instance_id}.{task_instance_id}")
}

/// The task identity carried by a liveness key — the only thing the
/// marker-consumer needs to mint a conductor-origin `Unhealthy` `TaskEvent`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct LivenessIdentity {
    pub workflow_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub task_instance_id: Uuid,
}

/// Reconstruct the task identity from a liveness key
/// (`<wf>.<wi>.<ti>`). `None` if the key is not exactly three UUID segments.
pub fn parse_liveness_key(key: &str) -> Option<LivenessIdentity> {
    let mut parts = key.split('.');
    let workflow_id = Uuid::parse_str(parts.next()?).ok()?;
    let workflow_instance_id = Uuid::parse_str(parts.next()?).ok()?;
    let task_instance_id = Uuid::parse_str(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(LivenessIdentity {
        workflow_id,
        workflow_instance_id,
        task_instance_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips_through_parse() {
        let wf = Uuid::new_v4();
        let wi = Uuid::new_v4();
        let ti = Uuid::new_v4();
        let key = liveness_key(wf, wi, ti);
        let parsed = parse_liveness_key(&key).expect("parse");
        assert_eq!(parsed.workflow_id, wf);
        assert_eq!(parsed.workflow_instance_id, wi);
        assert_eq!(parsed.task_instance_id, ti);
    }

    #[test]
    fn parse_rejects_malformed_keys() {
        assert!(parse_liveness_key("not-a-uuid").is_none());
        // Two segments — missing the task instance id.
        let two = format!("{}.{}", Uuid::new_v4(), Uuid::new_v4());
        assert!(parse_liveness_key(&two).is_none());
        // Four segments — an extra trailing token.
        let four = format!(
            "{}.{}.{}.{}",
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4()
        );
        assert!(parse_liveness_key(&four).is_none());
    }
}
