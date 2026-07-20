//! Boot-time cluster-state snapshot for the gate index.
//!
//! On every relay reconnect the conductor reconciles its in-memory
//! gate index from the published authoritative snapshot by
//! asking for every gate the cluster currently considers dispatched.
//! That snapshot is the boot-time twin of the steady-state
//! `DispatchPrecondition` / `CancelPrecondition` envelopes the relay
//! carries — both keep this node's gate index consistent with the published snapshot —
//! so it lives alongside the relay subsystem. HTTP is an incidental
//! transport here: the call reaches the server's `GET_DISPATCHED_GATES`
//! cluster query through the coordinator's passthrough, the same channel
//! the read surface uses for live cluster state, kept off the relay so
//! a slow query can't back-pressure system events.
//!
//! A coordinator that times out, is unreachable, or answers non-2xx is
//! surfaced as a typed `DispatchGatesError`; the caller
//! (`gate_index_lifecycle::rebuild_from_server`) degrades to an empty
//! rebuild, and the next inbound `DispatchPrecondition` restocks the
//! index.

use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tickr_proto::workflow::CaptureDeclaration;
use tickr_proto::TenantId;
use uuid::Uuid;

/// One entry in the coordinator's dispatched-gates snapshot. This shape is the
/// JSON contract consumed by the conductor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchedGate {
    pub workflow_instance_id: Uuid,
    pub edge_id: Uuid,
    pub signal_name: String,
    pub predicate: Option<String>,
    pub captures_spec: Vec<CaptureDeclaration>,
}

/// Budget for the snapshot call. Bounded short: the failure mode is a
/// degrade-to-empty rebuild, so a long stall buys nothing.
const TIMEOUT: Duration = Duration::from_millis(1_500);

/// Failure modes of the dispatched-gates snapshot. Each variant retains a
/// distinct operator-facing message.
#[derive(Debug, Error)]
pub enum DispatchGatesError {
    /// The coordinator did not respond within the configured timeout.
    #[error("coordinator call timed out")]
    Timeout,
    /// Connect-side failure: DNS, refused, socket error, TLS handshake.
    #[error("coordinator unreachable: {0}")]
    Unreachable(String),
    /// The coordinator responded with 404. Distinct from `Unreachable`
    /// because it's an answer (no such resource), not a failure.
    #[error("coordinator returned 404 for {0}")]
    NotFound(String),
    /// Non-2xx, non-404 response. The upstream body is discarded because it
    /// may contain control-plane diagnostics or secrets.
    #[error("coordinator returned status {status}")]
    Server { status: u16 },
    /// The coordinator responded successfully but its body did not parse
    /// into `Vec<DispatchedGate>` — a contract drift between conductor
    /// and coordinator.
    #[error("failed to decode coordinator response: {0}")]
    Decode(String),
}

/// `GET {coordinator_url}/api/internal/dispatched-gates?tenant=<uuid>` — every
/// hyperedge gate the cluster currently considers dispatched **for `tenant`**,
/// used to repopulate the conductor's in-memory gate index after a relay
/// reconnect. The tenant is named on the request so the snapshot is scoped to
/// this conductor's own slice and never carries another tenant's gates.
pub async fn list_dispatched_gates(
    coordinator_url: &str,
    tenant: TenantId,
) -> Result<Vec<DispatchedGate>, DispatchGatesError> {
    let url = format!(
        "{}/api/internal/dispatched-gates",
        coordinator_url.trim_end_matches('/'),
    );
    let response = reqwest::Client::new()
        .get(&url)
        .query(&[("tenant", tenant.to_string())])
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|e| classify_reqwest_error(e, &url))?;

    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Err(DispatchGatesError::NotFound(url));
    }
    if !status.is_success() {
        return Err(DispatchGatesError::Server {
            status: status.as_u16(),
        });
    }
    response
        .json::<Vec<DispatchedGate>>()
        .await
        .map_err(|e| DispatchGatesError::Decode(e.to_string()))
}

fn classify_reqwest_error(err: reqwest::Error, url: &str) -> DispatchGatesError {
    if err.is_timeout() {
        DispatchGatesError::Timeout
    } else {
        DispatchGatesError::Unreachable(format!("{}: {}", url, err))
    }
}
