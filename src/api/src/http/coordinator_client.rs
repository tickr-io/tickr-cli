//! HTTP client to the coordinator's UI-facing API. Used by the API component's UI
//! routes to subquery live cluster state from the coordinator (the coordinator
//! remains the gateway for live cluster queries).
//!
//! Distinct from the conductor↔coordinator scheduler relay: that channel carries
//! system events (task updates, build updates, compaction acks). Mixing UI
//! query load onto the relay would let a slow UI request back-pressure
//! compaction acks and vice versa. The two channels are operationally
//! independent here.
//!
//! Owns the per-request timeout policy. A coordinator that times out or is
//! unreachable surfaces as `CoordinatorClientError::Timeout` /
//! `CoordinatorClientError::Unreachable`; handlers map those to graceful
//! degradation (`live_data_available: false`) or a 503, depending on whether
//! the route can serve archive-only.

use crate::http::dto::{
    ClockInstance, TaskInstanceResponse, UpcomingInstanceResponse, WorkflowInstanceResponse,
};
use reqwest::StatusCode;
use std::time::Duration;
use thiserror::Error;
use tickr_proto::instance::InstanceSnapshot;
use tickr_proto::TenantId;
use uuid::Uuid;

/// Default budget for any single coordinator call. Bounded short because each
/// UI request can carry several of these, and the failure mode is graceful
/// degradation — long stalls would block the whole UI request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1_500);

#[derive(Debug, Error)]
pub enum CoordinatorClientError {
    /// The coordinator did not respond within the configured timeout.
    #[error("coordinator call timed out")]
    Timeout,
    /// Connect-side failure: DNS, refused, socket error, TLS handshake.
    #[error("coordinator unreachable: {0}")]
    Unreachable(String),
    /// The coordinator responded with 404. Distinct from `Unreachable` because
    /// it's an answer (no such resource), not a failure.
    #[error("coordinator returned 404 for {0}")]
    NotFound(String),
    /// Non-2xx, non-404 response from the coordinator. The upstream body is
    /// deliberately discarded: it may contain internal diagnostics or secrets.
    #[error("coordinator returned status {status}")]
    Server { status: u16 },
    /// The coordinator responded successfully but its body did not parse into the
    /// expected DTO. Indicates a contract drift between conductor and
    /// coordinator.
    #[error("failed to decode coordinator response: {0}")]
    Decode(String),
}

/// Wrapper around `reqwest::Client` exposing the narrow set of coordinator
/// operations the read surface needs. Each method handles one route on
/// coordinator's HTTP API.
#[derive(Clone, Debug)]
pub struct CoordinatorClient {
    base_url: String,
    http: reqwest::Client,
    timeout: Duration,
    /// The tenant this data-plane API component reads back as. The shared
    /// control-plane coordinator requires the tenant on every operational read, so
    /// the client stamps it on tenant-scoped requests. Resolved from the tenant
    /// slug env at construction — the same source the rest of the data plane
    /// derives its identity from.
    tenant: TenantId,
}

impl CoordinatorClient {
    /// Construct a client with the default timeout.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_timeout(base_url, DEFAULT_TIMEOUT)
    }

    /// Validate Control-plane query configuration without requiring startup
    /// reachability; availability remains an operator health concern.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, CoordinatorClientError> {
        let base_url = base_url.into();
        reqwest::Url::parse(&base_url).map_err(|error| {
            CoordinatorClientError::Unreachable(format!("invalid coordinator URL: {error}"))
        })?;
        Ok(Self::new(base_url))
    }

    pub fn with_timeout(base_url: impl Into<String>, timeout: Duration) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
            timeout,
            tenant: TenantId::from_env(),
        }
    }

    /// `GET /api/workflows/instances/{id}` against the coordinator. Returns the
    /// live **instance snapshot** — the same shape the archive path derives,
    /// `storage: live` — or a typed error. The coordinator answers 503 when its
    /// cluster query fails, which surfaces here as `Server { status: 503 }`;
    /// the handler maps that to its own 503 so "live store unreachable"
    /// reaches the UI distinct from 404.
    pub async fn get_workflow_instance(
        &self,
        instance_id: Uuid,
    ) -> Result<InstanceSnapshot, CoordinatorClientError> {
        // The read-back is tenant-scoped: name this component's tenant source so
        // the control plane never serves an unfiltered cross-tenant read.
        let url = format!(
            "{}/api/workflows/instances/{}?tenant={}",
            self.base_url.trim_end_matches('/'),
            instance_id,
            self.tenant
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/workflows/instances` against the coordinator. Returns **every**
    /// live cluster instance, unfiltered — the latest-run-state resolver folds
    /// these to the newest non-terminal instance per workflow id in one
    /// round-trip.
    pub async fn list_all_workflow_instances(
        &self,
    ) -> Result<Vec<WorkflowInstanceResponse>, CoordinatorClientError> {
        let url = format!(
            "{}/api/workflows/instances",
            self.base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/workflows/{workflow_id}/instances` against the coordinator.
    /// Returns the live cluster's current instances for that workflow.
    pub async fn list_workflow_instances(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowInstanceResponse>, CoordinatorClientError> {
        let url = format!(
            "{}/api/workflows/{}/instances",
            self.base_url.trim_end_matches('/'),
            workflow_id
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/workflows/instances/{instance_id}/tasks` against the
    /// coordinator. Returns the live task instances belonging to that workflow
    /// instance.
    pub async fn list_task_instances(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<TaskInstanceResponse>, CoordinatorClientError> {
        let url = format!(
            "{}/api/workflows/instances/{}/tasks",
            self.base_url.trim_end_matches('/'),
            workflow_instance_id
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/dashboard/clock?start=&end=` against the coordinator. Returns the
    /// live workflow instances whose `scheduled_at` falls in the window — the
    /// live half of the day-clock, merged with the PG archive half by the
    /// caller.
    pub async fn dashboard_clock(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<ClockInstance>, CoordinatorClientError> {
        let mut url = format!(
            "{}/api/dashboard/clock",
            self.base_url.trim_end_matches('/')
        );
        let mut params = Vec::new();
        if let Some(s) = start {
            params.push(format!("start={s}"));
        }
        if let Some(e) = end {
            params.push(format!("end={e}"));
        }
        if !params.is_empty() {
            url.push('?');
            url.push_str(&params.join("&"));
        }
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/dashboard/upcoming` against the coordinator. Returns the live
    /// `Scheduled` instances the wheel will fire next (unsorted/untrimmed); the
    /// caller sorts by `next_run_at` and trims to its limit.
    pub async fn dashboard_upcoming(
        &self,
    ) -> Result<Vec<UpcomingInstanceResponse>, CoordinatorClientError> {
        let url = format!(
            "{}/api/dashboard/upcoming",
            self.base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }

    /// `GET /api/internal/health` against the coordinator — the **control-plane
    /// rollup** (this coordinator plus the control plane it fronts). Reached over one
    /// HTTP hop because the UI touches control plane *only* through the coordinator,
    /// so the control plane is one rollup, never per-component. Not tenant-scoped
    /// (a substrate-reachability read, not an operational data read). An
    /// unreachable coordinator surfaces as `Timeout`/`Unreachable`, which the health
    /// row maps to `unhealthy` — mirroring the coordinator's own
    /// live-store-unreachable degrade path.
    pub async fn internal_health(&self) -> Result<ControlPlaneRollup, CoordinatorClientError> {
        let url = format!(
            "{}/api/internal/health",
            self.base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        map_status_then_decode(response, &url).await
    }
}

/// The body of the coordinator's `/api/internal/health` route — the single
/// control-plane status the operator health page's Control-plane row consumes.
/// One rollup, never per-component, because the UI reaches control plane only
/// through the coordinator.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ControlPlaneRollup {
    pub status: String,
}

// The connect/request arm is kept distinct from the catch-all so the intent of
// each failure class stays legible (and so they can diverge later); both map to
// `Unreachable` today.
#[allow(clippy::if_same_then_else)]
fn classify_reqwest_error(err: reqwest::Error, url: &str) -> CoordinatorClientError {
    if err.is_timeout() {
        CoordinatorClientError::Timeout
    } else if err.is_connect() || err.is_request() {
        CoordinatorClientError::Unreachable(format!("{}: {}", url, err))
    } else {
        CoordinatorClientError::Unreachable(format!("{}: {}", url, err))
    }
}

async fn map_status_then_decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    url: &str,
) -> Result<T, CoordinatorClientError> {
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Err(CoordinatorClientError::NotFound(url.to_string()));
    }
    if !status.is_success() {
        // Never retain or forward an upstream error body. It belongs to a
        // separate trust boundary and can contain datastore or cluster detail.
        return Err(CoordinatorClientError::Server {
            status: status.as_u16(),
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|e| CoordinatorClientError::Decode(e.to_string()))
}
