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
//! cluster query through the Frontend's HTTP subquery channel, the same channel
//! the read surface uses for live cluster state, kept off the relay so
//! a slow query can't back-pressure system events.
//!
//! A Control-plane HTTP channel that times out, is unreachable, or answers non-2xx is
//! surfaced as a typed `DispatchGatesError`; the caller
//! (`gate_index_lifecycle::rebuild_from_server`) degrades to an empty
//! rebuild, and the next inbound `DispatchPrecondition` restocks the
//! index.

use std::fmt;
use std::time::Duration;

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tickr_proto::workflow::CaptureDeclaration;
use tickr_proto::TenantId;
use uuid::Uuid;

use crate::config::{ControlPlaneConfigError, ControlPlaneHttpClient};

/// One entry in the Control plane's dispatched-gates snapshot. This shape is the
/// JSON contract consumed by the conductor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchedGate {
    pub workflow_instance_id: Uuid,
    pub edge_id: Uuid,
    pub signal_name: String,
    pub predicate: Option<String>,
    pub captures_spec: Vec<CaptureDeclaration>,
}

#[derive(Clone)]
pub struct DispatchGatesClient {
    http: ControlPlaneHttpClient,
}

impl fmt::Debug for DispatchGatesClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DispatchGatesClient")
            .field("http", &self.http)
            .finish()
    }
}

impl DispatchGatesClient {
    pub fn from_env(
        control_plane_http_url: Option<String>,
    ) -> Result<Option<Self>, ControlPlaneConfigError> {
        ControlPlaneHttpClient::from_env(control_plane_http_url)
            .map(|client| client.map(|http| Self { http }))
    }

    pub fn new(
        control_plane_http_url: &str,
        bearer_token: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, ControlPlaneConfigError> {
        ControlPlaneHttpClient::new(
            control_plane_http_url,
            bearer_token,
            allow_insecure_loopback,
        )
        .map(|http| Self { http })
    }

    #[cfg(test)]
    pub(crate) fn with_client(
        client: reqwest::Client,
        control_plane_http_url: &str,
        bearer_token: &str,
    ) -> Result<Self, ControlPlaneConfigError> {
        ControlPlaneHttpClient::with_client(client, control_plane_http_url, bearer_token)
            .map(|http| Self { http })
    }
}

/// Budget for the snapshot call. Bounded short: the failure mode is a
/// degrade-to-empty rebuild, so a long stall buys nothing.
const TIMEOUT: Duration = Duration::from_millis(1_500);

/// Failure modes of the dispatched-gates snapshot. Each variant retains a
/// distinct operator-facing message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DispatchGatesError {
    #[error("Control-plane rejected gate snapshot authentication")]
    Unauthenticated,
    #[error("Control-plane rejected gate snapshot Tenant binding")]
    Forbidden,
    #[error("Control-plane gate snapshot timed out")]
    Timeout,
    #[error("Control-plane gate snapshot peer is unavailable")]
    Unavailable,
    #[error("Control-plane gate snapshot route was not found")]
    NotFound,
    #[error("Control-plane gate snapshot returned HTTP status {status}")]
    Server { status: u16 },
    #[error("Control-plane gate snapshot response is invalid")]
    InvalidResponse,
}

/// `GET {control_plane_http_url}/api/internal/dispatched-gates?tenant=<uuid>` — every
/// hyperedge gate the cluster currently considers dispatched **for `tenant`**,
/// used to repopulate the conductor's in-memory gate index after a relay
/// reconnect. The tenant is named on the request so the snapshot is scoped to
/// this conductor's own slice and never carries another tenant's gates.
pub async fn list_dispatched_gates(
    client: &DispatchGatesClient,
    tenant: TenantId,
) -> Result<Vec<DispatchedGate>, DispatchGatesError> {
    let response = client
        .http
        .get("/api/internal/dispatched-gates")
        .query(&[("tenant", tenant.to_string())])
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(classify_reqwest_error)?;

    match response.status() {
        StatusCode::UNAUTHORIZED => return Err(DispatchGatesError::Unauthenticated),
        StatusCode::FORBIDDEN => return Err(DispatchGatesError::Forbidden),
        StatusCode::NOT_FOUND => return Err(DispatchGatesError::NotFound),
        status if !status.is_success() => {
            return Err(DispatchGatesError::Server {
                status: status.as_u16(),
            });
        }
        _ => {}
    }
    response
        .json::<Vec<DispatchedGate>>()
        .await
        .map_err(|_| DispatchGatesError::InvalidResponse)
}

fn classify_reqwest_error(error: reqwest::Error) -> DispatchGatesError {
    if error.is_timeout() {
        DispatchGatesError::Timeout
    } else {
        DispatchGatesError::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ServerConfig, ServerConnection, StreamOwned};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_CA_PEM: &str = include_str!("../system_tasks/testdata/control-plane-test-ca.pem");
    const TEST_SERVER_CERT_PEM: &str =
        include_str!("../system_tasks/testdata/control-plane-test-server.pem");
    const TEST_SERVER_KEY_PEM: &str =
        include_str!("../system_tasks/testdata/control-plane-test-server-key.pem");

    #[test]
    fn connection_policy_and_diagnostics_are_secret_safe() {
        DispatchGatesClient::new("https://control-plane.example", TEST_BEARER_TOKEN, false)
            .unwrap();
        DispatchGatesClient::new("http://127.0.0.1:8000", TEST_BEARER_TOKEN, true).unwrap();
        assert_eq!(
            DispatchGatesClient::new("http://127.0.0.1:8000", TEST_BEARER_TOKEN, false,)
                .unwrap_err(),
            ControlPlaneConfigError::InsecureEndpoint
        );
        assert_eq!(
            DispatchGatesClient::new("http://control-plane.example", TEST_BEARER_TOKEN, true,)
                .unwrap_err(),
            ControlPlaneConfigError::InsecureEndpoint
        );
        assert_eq!(
            DispatchGatesClient::new("https://control-plane.example", "not-a-token", false)
                .unwrap_err(),
            ControlPlaneConfigError::InvalidBearerToken
        );

        let client =
            DispatchGatesClient::new("http://127.0.0.1:8000", TEST_BEARER_TOKEN, true).unwrap();
        let rendered = format!("{client:?}");
        assert!(!rendered.contains(TEST_BEARER_TOKEN));
        assert!(rendered.contains("[REDACTED]"));
        assert!(client.http.authorization_is_sensitive());
    }

    #[tokio::test]
    async fn authentication_and_tenant_rejections_are_distinct_secret_safe_outcomes() {
        let tenant = TenantId::from_slug("snapshot-status-test");
        for (status, expected) in [
            ("401 Unauthorized", DispatchGatesError::Unauthenticated),
            ("403 Forbidden", DispatchGatesError::Forbidden),
            ("404 Not Found", DispatchGatesError::NotFound),
            (
                "503 Service Unavailable",
                DispatchGatesError::Server { status: 503 },
            ),
        ] {
            let (endpoint, saw_authorization) = spawn_http_server(status, TEST_BEARER_TOKEN);
            let client = DispatchGatesClient::new(&endpoint, TEST_BEARER_TOKEN, true).unwrap();
            let error = list_dispatched_gates(&client, tenant).await.unwrap_err();
            assert_eq!(error, expected);
            assert!(!error.to_string().contains(TEST_BEARER_TOKEN));
            assert!(saw_authorization.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn timeout_remains_distinct_from_an_unavailable_peer() {
        let endpoint = spawn_hanging_http_server();
        let client = DispatchGatesClient::new(&endpoint, TEST_BEARER_TOKEN, true).unwrap();
        assert_eq!(
            list_dispatched_gates(&client, TenantId::from_slug("snapshot-timeout-test"))
                .await
                .unwrap_err(),
            DispatchGatesError::Timeout
        );
    }

    #[tokio::test]
    async fn verified_tls_proxy_authenticates_and_rejects_bad_trust_or_hostname() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tenant = TenantId::from_slug("snapshot-tls-test");

        let (trusted_url, saw_authorization) = spawn_tls_server();
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let trusted_client =
            DispatchGatesClient::with_client(trusted_http, &trusted_url, TEST_BEARER_TOKEN)
                .unwrap();
        assert!(list_dispatched_gates(&trusted_client, tenant)
            .await
            .unwrap()
            .is_empty());
        assert!(saw_authorization.load(Ordering::SeqCst));

        let (untrusted_url, _) = spawn_tls_server();
        let untrusted_client =
            DispatchGatesClient::new(&untrusted_url, TEST_BEARER_TOKEN, false).unwrap();
        assert_eq!(
            list_dispatched_gates(&untrusted_client, tenant)
                .await
                .unwrap_err(),
            DispatchGatesError::Unavailable
        );

        let (matching_url, _) = spawn_tls_server();
        let mismatched_url = matching_url.replace("localhost", "127.0.0.1");
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let mismatched_client =
            DispatchGatesClient::with_client(trusted_http, &mismatched_url, TEST_BEARER_TOKEN)
                .unwrap();
        assert_eq!(
            list_dispatched_gates(&mismatched_client, tenant)
                .await
                .unwrap_err(),
            DispatchGatesError::Unavailable
        );
    }

    fn spawn_hanging_http_server() -> String {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let _ = read_request(&mut stream);
            std::thread::sleep(TIMEOUT + Duration::from_millis(100));
        });
        format!("http://127.0.0.1:{port}")
    }

    fn spawn_http_server(status: &'static str, body: &'static str) -> (String, Arc<AtomicBool>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_authorization = Arc::new(AtomicBool::new(false));
        let server_observation = Arc::clone(&saw_authorization);
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let request = read_request(&mut stream);
            server_observation.store(has_authorization(&request), Ordering::SeqCst);
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        });
        (format!("http://127.0.0.1:{port}"), saw_authorization)
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
            let request = read_request(&mut stream);
            server_observation.store(has_authorization(&request), Ordering::SeqCst);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n[]",
            );
            let _ = stream.flush();
        });
        (format!("https://localhost:{port}"), saw_authorization)
    }

    fn read_request(stream: &mut impl Read) -> Vec<u8> {
        let mut request = Vec::with_capacity(2048);
        let mut buffer = [0_u8; 1024];
        loop {
            let Ok(read) = stream.read(&mut buffer) else {
                return request;
            };
            if read == 0 {
                return request;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return request;
            }
        }
    }

    fn has_authorization(request: &[u8]) -> bool {
        String::from_utf8_lossy(request).lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("authorization") && value.trim() == TEST_AUTHORIZATION
            })
        })
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
