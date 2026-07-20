//! Shared substrate for the executor **component-liveness key** — the process-
//! lifetime self-reaping KV key that lets the health endpoint count the executor
//! fleet and read its saturation without a durable registry and without scanning
//! per-task liveness keys.
//!
//! This bucket is **deliberately separate** from the task-liveness bucket: a
//! component-key expiry must never enter the conductor's task-death verdict path,
//! so nothing binds this bucket's wildcard (no marker consumer, no reaper). Only
//! the shared bucket name, the key scheme, and the `{cap, in_flight}` value
//! schema live here — the executor is the sole writer and the health endpoint the
//! sole reader; the two never reference each other, so both import this one
//! definition off the published contract.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The dedicated KV bucket holding one component-liveness key per live executor
/// process. Separate from [`LIVENESS_BUCKET`](super::LIVENESS_BUCKET) on purpose:
/// the task-liveness bucket's wildcard is bound by the conductor's marker
/// consumer, so a key expiry there becomes an `Unhealthy` task verdict. This
/// bucket has no such consumer — a component-key expiry just means the executor
/// is gone, never a task-death marker.
pub const COMPONENT_LIVENESS_BUCKET: &str = "tickr_component_liveness";

/// The component-liveness key for one executor process: `executor.<boot-uuid>`.
/// The `executor.` prefix namespaces the key so a future second component kind
/// can share the bucket without colliding.
pub fn component_liveness_key(executor_id: Uuid) -> String {
    format!("executor.{executor_id}")
}

/// The value schema of a component-liveness key: the executor's dispatch
/// concurrency `cap` and its current `in_flight` count. `in_flight` is derived at
/// write time as `cap − available_permits` off the shared dispatch semaphore, so
/// the health endpoint reads fleet saturation straight from the bucket without a
/// side counter. Written cadence-only (up to ~one TTL stale) — a coarse
/// saturation gauge, deliberately not coupled to dispatch/completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentLivenessValue {
    /// Dispatch concurrency cap of the executor.
    pub cap: usize,
    /// Tasks in flight = `cap − available_permits`.
    pub in_flight: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_executor_prefixed() {
        let id = Uuid::new_v4();
        assert_eq!(component_liveness_key(id), format!("executor.{id}"));
    }

    #[test]
    fn value_round_trips_through_json() {
        let v = ComponentLivenessValue {
            cap: 8,
            in_flight: 3,
        };
        let bytes = serde_json::to_vec(&v).expect("serialize");
        let back: ComponentLivenessValue = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(v, back);
    }
}
