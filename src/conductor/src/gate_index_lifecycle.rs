//! Lifecycle wiring around the per-instance `GateIndex`. The pure
//! index lives in `gate_index`; this module owns the process-wide
//! singleton plus the integration points with the inbound relay
//! handler and the wakeup translator.
//!
//! The index is in-memory only; the authoritative source is the
//! published live-state snapshot. On every relay reconnect the
//! conductor calls `GET_DISPATCHED_GATES` against the server (via
//! the Frontend's HTTP subquery channel), then `replace_all`s
//! this index so concurrent readers don't observe a partial state.

use once_cell::sync::Lazy;
use tickr_proto::TenantId;

use crate::gate_index::GateIndex;
use crate::relay::dispatch_gates::{
    list_dispatched_gates, DispatchGatesClient, DispatchGatesError,
};

static GATE_INDEX: Lazy<GateIndex> = Lazy::new(GateIndex::new);

/// Shared handle to the process-wide gate index. The wakeup
/// translator and the inbound relay handler both reach for this when
/// resolving / mutating the index.
pub fn gate_index() -> GateIndex {
    GATE_INDEX.clone()
}

/// Repopulate the index from the cluster's authoritative state.
/// Called at every relay reconnect: clears the in-memory state then
/// re-runs `register(...)` for every gate the server still
/// considers `Dispatched`. The two operations are bundled into a
/// single `replace_all` call so concurrent readers don't observe a
/// partial index.
///
/// Any Control-plane HTTP-channel failure clears the stale index and is returned
/// as a typed degraded outcome. Authentication and Tenant-binding rejection
/// therefore cannot be mistaken for a healthy empty snapshot; the next inbound
/// `DispatchPrecondition` can still restock the index.
pub async fn rebuild_from_server(
    client: &DispatchGatesClient,
    tenant: TenantId,
) -> Result<usize, DispatchGatesError> {
    let dispatched = match list_dispatched_gates(client, tenant).await {
        Ok(dispatched) => dispatched,
        Err(error) => {
            GATE_INDEX.replace_all(Vec::new());
            return Err(error);
        }
    };
    let count = dispatched.len();
    let entries = dispatched
        .into_iter()
        .map(|d| {
            (
                d.workflow_instance_id,
                d.edge_id,
                d.signal_name,
                d.predicate,
                d.captures_spec,
            )
        })
        .collect();
    GATE_INDEX.replace_all(entries);
    Ok(count)
}
