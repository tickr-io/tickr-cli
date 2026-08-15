//! HTTP client to the Control plane's HTTP subquery channel. Used by the API
//! component's UI routes to query live cluster state through the Frontend.
//!
//! Distinct from the Conductor relay: that channel carries system events (task
//! updates, build updates, compaction acknowledgements). Mixing UI query load
//! onto the relay would let a slow UI request back-pressure compaction
//! acknowledgements and vice versa. The two channels are operationally
//! independent here.
//!
//! Owns the per-request timeout policy. A Control plane request that times out
//! or is unreachable surfaces as `ControlPlaneClientError::Timeout` /
//! `ControlPlaneClientError::Unreachable`; handlers map those to graceful
//! degradation (`live_data_available: false`) or a 503, depending on whether
//! the route can serve archive-only.

use crate::http::dto::{
    ClockInstance, TaskInstanceResponse, UpcomingInstanceResponse, WorkflowInstanceResponse,
};
use reqwest::{
    header::{HeaderValue, AUTHORIZATION},
    StatusCode, Url,
};
use std::{
    fmt,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tickr_proto::instance::InstanceSnapshot;
use tickr_proto::TenantId;
use uuid::Uuid;

/// Default budget for any single Control plane call. Bounded short because each
/// UI request can carry several of these, and the failure mode is graceful
/// degradation — long stalls would block the whole UI request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(1_500);

pub const CONTROL_PLANE_BEARER_TOKEN_ENV: &str = "TICKR_CONTROL_PLANE_BEARER_TOKEN";
pub const ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK_ENV: &str =
    "TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ControlPlaneClientError {
    #[error(
        "TICKR_CONTROL_PLANE_BEARER_TOKEN is required when a Control-plane endpoint is configured"
    )]
    MissingBearerToken,
    #[error("TICKR_CONTROL_PLANE_BEARER_TOKEN must be a canonical 32-byte base64url token")]
    InvalidBearerToken,
    #[error("Control-plane HTTP endpoint is invalid")]
    InvalidEndpoint,
    #[error(
        "Control-plane HTTP endpoint must use verified HTTPS, except explicitly enabled loopback HTTP"
    )]
    InsecureEndpoint,
    /// The Control plane rejected the configured credential.
    #[error("Control plane authentication rejected")]
    Unauthenticated,
    /// The Control plane rejected an explicit Tenant assertion for the credential.
    #[error("Control plane tenant authorization rejected")]
    Forbidden,
    /// The Control plane did not respond within the configured timeout.
    #[error("Control plane call timed out")]
    Timeout,
    /// Connect-side failure: DNS, refused, socket error, TLS handshake.
    #[error("Control plane unavailable")]
    Unreachable,
    /// The Control plane responded with 404. Distinct from `Unreachable` because
    /// it is an answer (no such resource), not a failure.
    #[error("Control plane returned 404")]
    NotFound,
    /// Non-2xx, non-404 response from the Control plane. The upstream body is
    /// deliberately discarded: it may contain internal diagnostics or secrets.
    #[error("Control plane returned status {status}")]
    Server { status: u16 },
    /// The Control plane responded successfully but its body did not parse into
    /// the expected DTO. Indicates a contract drift between Data and Control planes.
    #[error("failed to decode Control plane response: {0}")]
    Decode(String),
}

/// Wrapper around `reqwest::Client` exposing the narrow set of Control-plane
/// operations the read surface needs. Each method handles one route on the
/// Frontend's HTTP subquery channel.
#[derive(Clone)]
pub struct ControlPlaneClient {
    base_url: String,
    http: reqwest::Client,
    timeout: Duration,
    authorization: HeaderValue,
    authentication_rejected: Arc<AtomicBool>,
    /// The tenant this Data-plane API component reads back as. The shared
    /// Control plane requires the tenant on every operational read, so the
    /// client stamps it on tenant-scoped requests. Resolved from the tenant slug
    /// environment variable at construction — the same source the rest of the
    /// Data plane derives its identity from.
    tenant: TenantId,
}

impl fmt::Debug for ControlPlaneClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneClient")
            .field("endpoint", &"[CONFIGURED]")
            .field("authorization", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ControlPlaneClient {
    /// Validate process configuration without requiring startup reachability;
    /// availability remains an operator health concern.
    pub fn try_new(base_url: impl Into<String>) -> Result<Self, ControlPlaneClientError> {
        let token = std::env::var(CONTROL_PLANE_BEARER_TOKEN_ENV).ok();
        let allow_insecure_loopback = std::env::var(ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK_ENV).ok();
        Self::from_values(
            base_url,
            token.as_deref(),
            allow_insecure_loopback.as_deref(),
        )
    }

    fn from_values(
        base_url: impl Into<String>,
        bearer_token: Option<&str>,
        allow_insecure_loopback: Option<&str>,
    ) -> Result<Self, ControlPlaneClientError> {
        let token = bearer_token.ok_or(ControlPlaneClientError::MissingBearerToken)?;
        Self::new(base_url, token, allow_insecure_loopback == Some("true"))
    }

    pub fn new(
        base_url: impl Into<String>,
        bearer_token: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, ControlPlaneClientError> {
        Self::with_timeout(
            base_url,
            bearer_token,
            allow_insecure_loopback,
            DEFAULT_TIMEOUT,
        )
    }

    pub fn with_timeout(
        base_url: impl Into<String>,
        bearer_token: &str,
        allow_insecure_loopback: bool,
        timeout: Duration,
    ) -> Result<Self, ControlPlaneClientError> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ControlPlaneClientError::InvalidEndpoint)?;
        Self::with_client(
            http,
            base_url,
            bearer_token,
            allow_insecure_loopback,
            timeout,
        )
    }

    fn with_client(
        http: reqwest::Client,
        base_url: impl Into<String>,
        bearer_token: &str,
        allow_insecure_loopback: bool,
        timeout: Duration,
    ) -> Result<Self, ControlPlaneClientError> {
        if !is_canonical_bearer_token(bearer_token.as_bytes()) {
            return Err(ControlPlaneClientError::InvalidBearerToken);
        }
        let base_url = base_url.into();
        let endpoint =
            Url::parse(&base_url).map_err(|_| ControlPlaneClientError::InvalidEndpoint)?;
        let host = endpoint
            .host_str()
            .ok_or(ControlPlaneClientError::InvalidEndpoint)?;
        match endpoint.scheme() {
            "https" => {}
            "http" if allow_insecure_loopback && is_loopback_host(host) => {}
            "http" => return Err(ControlPlaneClientError::InsecureEndpoint),
            _ => return Err(ControlPlaneClientError::InvalidEndpoint),
        }
        let mut authorization = format!("Bearer {bearer_token}")
            .parse::<HeaderValue>()
            .map_err(|_| ControlPlaneClientError::InvalidBearerToken)?;
        authorization.set_sensitive(true);
        Ok(Self {
            base_url,
            http,
            timeout,
            authorization,
            authentication_rejected: Arc::new(AtomicBool::new(false)),
            tenant: TenantId::from_env(),
        })
    }

    /// `GET /api/workflows/instances/{id}` against the Control plane. Returns the
    /// live **instance snapshot** — the same shape the archive path derives,
    /// `storage: live` — or a typed error. The Frontend answers 503 when its
    /// cluster query fails, which surfaces here as `Server { status: 503 }`;
    /// the handler maps that to its own 503 so "live store unreachable"
    /// reaches the UI distinct from 404.
    pub async fn get_workflow_instance(
        &self,
        instance_id: Uuid,
    ) -> Result<InstanceSnapshot, ControlPlaneClientError> {
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
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    /// `GET /api/workflows/instances` against the Control plane. Returns every
    /// live cluster instance in the bearer credential's Tenant scope; the
    /// latest-run-state resolver folds these to the newest non-terminal
    /// instance per workflow id in one round-trip.
    pub async fn list_all_workflow_instances(
        &self,
    ) -> Result<Vec<WorkflowInstanceResponse>, ControlPlaneClientError> {
        let url = format!(
            "{}/api/workflows/instances",
            self.base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    /// `GET /api/workflows/{workflow_id}/instances` against the Control plane.
    /// Returns that workflow's live instances in the credential's Tenant scope.
    pub async fn list_workflow_instances(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<WorkflowInstanceResponse>, ControlPlaneClientError> {
        let url = format!(
            "{}/api/workflows/{}/instances",
            self.base_url.trim_end_matches('/'),
            workflow_id
        );
        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    /// `GET /api/workflows/instances/{instance_id}/tasks` against the Control
    /// plane. Returns the live task instances belonging to that workflow
    /// instance.
    pub async fn list_task_instances(
        &self,
        workflow_instance_id: Uuid,
    ) -> Result<Vec<TaskInstanceResponse>, ControlPlaneClientError> {
        let url = format!(
            "{}/api/workflows/instances/{}/tasks",
            self.base_url.trim_end_matches('/'),
            workflow_instance_id
        );
        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    /// `GET /api/dashboard/clock?start=&end=` against the Control plane. Returns the
    /// live workflow instances whose `scheduled_at` falls in the window — the
    /// live half of the day-clock, merged with the PG archive half by the
    /// caller.
    pub async fn dashboard_clock(
        &self,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Vec<ClockInstance>, ControlPlaneClientError> {
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
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    /// `GET /api/dashboard/upcoming` against the Control plane. Returns the live
    /// `Scheduled` instances the wheel will fire next (unsorted/untrimmed); the
    /// caller sorts by `next_run_at` and trims to its limit.
    pub async fn dashboard_upcoming(
        &self,
    ) -> Result<Vec<UpcomingInstanceResponse>, ControlPlaneClientError> {
        let url = format!(
            "{}/api/dashboard/upcoming",
            self.base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, self.authorization.clone())
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| classify_reqwest_error(e, &url))?;

        self.decode_protected(response, &url).await
    }

    async fn decode_protected<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
        url: &str,
    ) -> Result<T, ControlPlaneClientError> {
        let result = map_status_then_decode(response, url).await;
        match &result {
            Ok(_) => self.authentication_rejected.store(false, Ordering::Release),
            Err(ControlPlaneClientError::Unauthenticated) => {
                self.authentication_rejected.store(true, Ordering::Release);
            }
            Err(_) => {}
        }
        result
    }

    /// `GET /api/internal/health` against the Frontend — the **Control-plane
    /// rollup** (the Frontend plus the Server it fronts). This tenant-neutral
    /// probe never carries the bearer. A prior typed authentication rejection
    /// dominates a healthy public rollup until a protected request succeeds, so
    /// Health cannot claim the dependency is usable from reachability alone.
    /// Timeout and unavailable-peer outcomes remain distinct.
    pub async fn internal_health(&self) -> Result<ControlPlaneRollup, ControlPlaneClientError> {
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

        let rollup = map_status_then_decode(response, &url).await?;
        if self.authentication_rejected.load(Ordering::Acquire) {
            Err(ControlPlaneClientError::Unauthenticated)
        } else {
            Ok(rollup)
        }
    }
}

/// The body of the Frontend's `/api/internal/health` route — the single
/// Control-plane status the operator Health page consumes. One rollup, never
/// per-component, because the UI reaches the Control plane only through the
/// Frontend.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ControlPlaneRollup {
    pub status: String,
}

// The connect/request arm is kept distinct from the catch-all so the intent of
// each failure class stays legible (and so they can diverge later); both map to
// `Unreachable` today.
#[allow(clippy::if_same_then_else)]
fn classify_reqwest_error(err: reqwest::Error, _url: &str) -> ControlPlaneClientError {
    if err.is_timeout() {
        ControlPlaneClientError::Timeout
    } else {
        ControlPlaneClientError::Unreachable
    }
}

async fn map_status_then_decode<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    _url: &str,
) -> Result<T, ControlPlaneClientError> {
    let status = response.status();
    if status == StatusCode::UNAUTHORIZED {
        return Err(ControlPlaneClientError::Unauthenticated);
    }
    if status == StatusCode::FORBIDDEN {
        return Err(ControlPlaneClientError::Forbidden);
    }
    if status == StatusCode::NOT_FOUND {
        return Err(ControlPlaneClientError::NotFound);
    }
    if !status.is_success() {
        // Never retain or forward an upstream error body. It belongs to a
        // separate trust boundary and can contain datastore or cluster detail.
        return Err(ControlPlaneClientError::Server {
            status: status.as_u16(),
        });
    }
    response
        .json::<T>()
        .await
        .map_err(|e| ControlPlaneClientError::Decode(e.to_string()))
}

fn is_canonical_bearer_token(token: &[u8]) -> bool {
    token.len() == 43
        && token
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_'))
        && matches!(token.last(), Some(b'A' | b'Q' | b'g' | b'w'))
}

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Path, http::HeaderMap, routing::get, Json, Router};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ServerConfig, ServerConnection, StreamOwned};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_CA_PEM: &str =
        include_str!("../../../conductor/src/system_tasks/testdata/control-plane-test-ca.pem");
    const TEST_SERVER_CERT_PEM: &str =
        include_str!("../../../conductor/src/system_tasks/testdata/control-plane-test-server.pem");
    const TEST_SERVER_KEY_PEM: &str = include_str!(
        "../../../conductor/src/system_tasks/testdata/control-plane-test-server-key.pem"
    );

    #[test]
    fn connection_configuration_requires_canonical_secret_and_verified_transport() {
        assert_eq!(
            ControlPlaneClient::from_values("https://control-plane.example", None, None,)
                .unwrap_err(),
            ControlPlaneClientError::MissingBearerToken
        );
        for malformed in [
            "",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            " AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA ",
        ] {
            assert_eq!(
                ControlPlaneClient::from_values(
                    "https://control-plane.example",
                    Some(malformed),
                    None,
                )
                .unwrap_err(),
                ControlPlaneClientError::InvalidBearerToken
            );
        }

        ControlPlaneClient::from_values(
            "https://control-plane.example",
            Some(TEST_BEARER_TOKEN),
            None,
        )
        .unwrap();
        ControlPlaneClient::from_values(
            "http://127.0.0.1:8000",
            Some(TEST_BEARER_TOKEN),
            Some("true"),
        )
        .unwrap();
        assert_eq!(
            ControlPlaneClient::from_values(
                "http://127.0.0.1:8000",
                Some(TEST_BEARER_TOKEN),
                None,
            )
            .unwrap_err(),
            ControlPlaneClientError::InsecureEndpoint
        );
        assert_eq!(
            ControlPlaneClient::from_values(
                "http://192.0.2.1:8000",
                Some(TEST_BEARER_TOKEN),
                Some("true"),
            )
            .unwrap_err(),
            ControlPlaneClientError::InsecureEndpoint
        );
    }

    #[test]
    fn diagnostics_redact_secret_and_endpoint_configuration() {
        let endpoint = "https://configuration-sentinel.invalid";
        let client = ControlPlaneClient::new(endpoint, TEST_BEARER_TOKEN, false).unwrap();
        let rendered = format!("{client:?}");
        assert!(!rendered.contains(TEST_BEARER_TOKEN));
        assert!(!rendered.contains(endpoint));
        assert!(rendered.contains("[REDACTED]"));
        assert!(client.authorization.is_sensitive());

        let invalid_token = "credential-sentinel";
        let error = ControlPlaneClient::new(endpoint, invalid_token, false).unwrap_err();
        let rendered_error = format!("{error:?} {error}");
        assert!(!rendered_error.contains(invalid_token));
        assert!(!rendered_error.contains(endpoint));
        let generated_docs = crate::http::routes::openapi_yaml().unwrap();
        assert!(!generated_docs.contains(TEST_BEARER_TOKEN));
        assert!(!generated_docs.contains(endpoint));
    }

    #[tokio::test]
    async fn protected_requests_send_authorization_and_classify_rejection() {
        let all_authorized = Arc::new(AtomicBool::new(false));
        let workflow_authorized = Arc::new(AtomicBool::new(false));
        let instance_authorized = Arc::new(AtomicBool::new(false));
        let tasks_authorized = Arc::new(AtomicBool::new(false));
        let clock_authorized = Arc::new(AtomicBool::new(false));
        let upcoming_authorized = Arc::new(AtomicBool::new(false));
        let health_authorized = Arc::new(AtomicBool::new(false));
        let all_observation = Arc::clone(&all_authorized);
        let workflow_observation = Arc::clone(&workflow_authorized);
        let instance_observation = Arc::clone(&instance_authorized);
        let tasks_observation = Arc::clone(&tasks_authorized);
        let clock_observation = Arc::clone(&clock_authorized);
        let upcoming_observation = Arc::clone(&upcoming_authorized);
        let health_observation = Arc::clone(&health_authorized);
        let app = Router::new()
            .route(
                "/api/workflows/instances",
                get(move |headers: HeaderMap| {
                    let observed = Arc::clone(&all_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(Vec::<WorkflowInstanceResponse>::new())
                    }
                }),
            )
            .route(
                "/api/workflows/{id}/instances",
                get(move |Path(_id): Path<String>, headers: HeaderMap| {
                    let observed = Arc::clone(&workflow_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(Vec::<WorkflowInstanceResponse>::new())
                    }
                }),
            )
            .route(
                "/api/workflows/instances/{id}",
                get(move |Path(_id): Path<String>, headers: HeaderMap| {
                    let observed = Arc::clone(&instance_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(InstanceSnapshot::default())
                    }
                }),
            )
            .route(
                "/api/workflows/instances/{id}/tasks",
                get(move |Path(_id): Path<String>, headers: HeaderMap| {
                    let observed = Arc::clone(&tasks_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(Vec::<TaskInstanceResponse>::new())
                    }
                }),
            )
            .route(
                "/api/dashboard/clock",
                get(move |headers: HeaderMap| {
                    let observed = Arc::clone(&clock_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(Vec::<ClockInstance>::new())
                    }
                }),
            )
            .route(
                "/api/dashboard/upcoming",
                get(move |headers: HeaderMap| {
                    let observed = Arc::clone(&upcoming_observation);
                    async move {
                        observed.store(has_authorization(&headers), Ordering::SeqCst);
                        Json(Vec::<UpcomingInstanceResponse>::new())
                    }
                }),
            )
            .route(
                "/api/internal/health",
                get(move |headers: HeaderMap| {
                    let observed = Arc::clone(&health_observation);
                    async move {
                        observed.store(headers.contains_key(AUTHORIZATION), Ordering::SeqCst);
                        Json(serde_json::json!({"status": "healthy"}))
                    }
                }),
            );
        let base_url = spawn_http_server(app).await;
        let client = ControlPlaneClient::new(base_url, TEST_BEARER_TOKEN, true).unwrap();
        assert!(client
            .list_all_workflow_instances()
            .await
            .unwrap()
            .is_empty());
        assert!(client
            .list_workflow_instances(Uuid::new_v4())
            .await
            .unwrap()
            .is_empty());
        client.get_workflow_instance(Uuid::new_v4()).await.unwrap();
        assert!(client
            .list_task_instances(Uuid::new_v4())
            .await
            .unwrap()
            .is_empty());
        assert!(client.dashboard_clock(None, None).await.unwrap().is_empty());
        assert!(client.dashboard_upcoming().await.unwrap().is_empty());
        assert_eq!(client.internal_health().await.unwrap().status, "healthy");
        assert!(all_authorized.load(Ordering::SeqCst));
        assert!(workflow_authorized.load(Ordering::SeqCst));
        assert!(instance_authorized.load(Ordering::SeqCst));
        assert!(tasks_authorized.load(Ordering::SeqCst));
        assert!(clock_authorized.load(Ordering::SeqCst));
        assert!(upcoming_authorized.load(Ordering::SeqCst));
        assert!(!health_authorized.load(Ordering::SeqCst));

        let rejecting = Router::new()
            .route(
                "/api/workflows/instances",
                get(|| async { Json(Vec::<WorkflowInstanceResponse>::new()) }),
            )
            .route(
                "/api/workflows/instances/{id}",
                get(|| async { StatusCode::UNAUTHORIZED }),
            )
            .route(
                "/api/workflows/instances/{id}/tasks",
                get(|| async { StatusCode::FORBIDDEN }),
            )
            .route(
                "/api/dashboard/clock",
                get(|| async { StatusCode::UNAUTHORIZED }),
            )
            .route(
                "/api/dashboard/upcoming",
                get(|| async { StatusCode::UNAUTHORIZED }),
            )
            .route(
                "/api/internal/health",
                get(|| async { Json(serde_json::json!({"status": "healthy"})) }),
            );
        let rejecting_url = spawn_http_server(rejecting).await;
        let client =
            ControlPlaneClient::new(rejecting_url.clone(), TEST_BEARER_TOKEN, true).unwrap();
        assert_eq!(
            client
                .get_workflow_instance(Uuid::new_v4())
                .await
                .unwrap_err(),
            ControlPlaneClientError::Unauthenticated
        );
        assert_eq!(
            client
                .list_task_instances(Uuid::new_v4())
                .await
                .unwrap_err(),
            ControlPlaneClientError::Forbidden
        );
        let clock_error = client.dashboard_clock(None, None).await.unwrap_err();
        assert_eq!(clock_error, ControlPlaneClientError::Unauthenticated);
        let rendered = format!("{clock_error:?} {clock_error}");
        assert!(!rendered.contains(TEST_BEARER_TOKEN));
        assert!(!rendered.contains(&rejecting_url));
        assert_eq!(
            client.internal_health().await.unwrap_err(),
            ControlPlaneClientError::Unauthenticated
        );

        assert!(client
            .list_all_workflow_instances()
            .await
            .unwrap()
            .is_empty());
        assert_eq!(client.internal_health().await.unwrap().status, "healthy");

        assert_eq!(
            client.dashboard_upcoming().await.unwrap_err(),
            ControlPlaneClientError::Unauthenticated
        );
        assert_eq!(
            client.internal_health().await.unwrap_err(),
            ControlPlaneClientError::Unauthenticated
        );
    }

    #[tokio::test]
    async fn verified_tls_proxy_authenticates_and_rejects_bad_trust_or_hostname() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (detail_url, detail_authorized) = spawn_tls_server();
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let detail_client = ControlPlaneClient::with_client(
            trusted_http,
            &detail_url,
            TEST_BEARER_TOKEN,
            false,
            DEFAULT_TIMEOUT,
        )
        .unwrap();
        assert!(matches!(
            detail_client
                .get_workflow_instance(Uuid::new_v4())
                .await
                .unwrap_err(),
            ControlPlaneClientError::Decode(_)
        ));
        assert!(detail_authorized.load(Ordering::SeqCst));

        let (tasks_url, tasks_authorized) = spawn_tls_server();
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let tasks_client = ControlPlaneClient::with_client(
            trusted_http,
            &tasks_url,
            TEST_BEARER_TOKEN,
            false,
            DEFAULT_TIMEOUT,
        )
        .unwrap();
        assert!(tasks_client
            .list_task_instances(Uuid::new_v4())
            .await
            .unwrap()
            .is_empty());
        assert!(tasks_authorized.load(Ordering::SeqCst));

        let (untrusted_url, _) = spawn_tls_server();
        let untrusted_client =
            ControlPlaneClient::new(untrusted_url, TEST_BEARER_TOKEN, false).unwrap();
        assert_eq!(
            untrusted_client
                .get_workflow_instance(Uuid::new_v4())
                .await
                .unwrap_err(),
            ControlPlaneClientError::Unreachable
        );

        let (matching_url, _) = spawn_tls_server();
        let mismatched_url = matching_url.replace("localhost", "127.0.0.1");
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let mismatched_client = ControlPlaneClient::with_client(
            trusted_http,
            mismatched_url,
            TEST_BEARER_TOKEN,
            false,
            DEFAULT_TIMEOUT,
        )
        .unwrap();
        assert_eq!(
            mismatched_client
                .list_task_instances(Uuid::new_v4())
                .await
                .unwrap_err(),
            ControlPlaneClientError::Unreachable
        );
    }

    async fn spawn_http_server(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    fn has_authorization(headers: &HeaderMap) -> bool {
        headers
            .get(AUTHORIZATION)
            .is_some_and(|value| value.as_bytes() == TEST_AUTHORIZATION.as_bytes())
    }

    fn spawn_tls_server() -> (String, Arc<AtomicBool>) {
        let certificates = vec![CertificateDer::from(pem_der(TEST_SERVER_CERT_PEM))];
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pem_der(TEST_SERVER_KEY_PEM)));
        let config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certificates, key)
                .unwrap(),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_authorization = Arc::new(AtomicBool::new(false));
        let server_observation = Arc::clone(&saw_authorization);
        std::thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            let Ok(connection) = ServerConnection::new(config) else {
                return;
            };
            let mut stream = StreamOwned::new(connection, stream);
            let mut request = Vec::with_capacity(2048);
            let mut buffer = [0_u8; 1024];
            loop {
                let Ok(read) = stream.read(&mut buffer) else {
                    return;
                };
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            server_observation.store(
                request.lines().any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.eq_ignore_ascii_case("authorization")
                            && value.trim() == TEST_AUTHORIZATION
                    })
                }),
                Ordering::SeqCst,
            );
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n[]",
            );
            let _ = stream.flush();
        });
        (format!("https://localhost:{port}"), saw_authorization)
    }

    fn pem_der(pem: &str) -> Vec<u8> {
        let mut output = Vec::new();
        let mut accumulator = 0_u32;
        let mut bits = 0_u8;
        for byte in pem
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .flat_map(str::bytes)
        {
            if byte == b'=' {
                break;
            }
            let value = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => continue,
            };
            accumulator = (accumulator << 6) | u32::from(value);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                output.push((accumulator >> bits) as u8);
                accumulator &= (1_u32 << bits) - 1;
            }
        }
        output
    }
}
