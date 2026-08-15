//! Events pull cycle: lands tenant-visible server events in the tenant
//! events projection (the conductor-side `events` table).
//!
//! Each tick derives the next upstream keyset cursor from committed projection
//! rows, fetches one page without holding a SQL transaction or writer lock, and
//! delegates atomic idempotent insertion to the selected repository.
//!
//! Concurrent cycles may fetch the same page. Correctness comes from the
//! repository's duplicate suppression and contiguous public `seq` assignment,
//! not from serializing network work. An empty projection means "pull from the
//! beginning" — the rebuild path is the boot path, with no stored high-water.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use tickr_migrations::backend::{RepositoryError, WriterRepositoryBundle};
use tickr_migrations::event_repository::EventProjectionInput;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub use crate::config::ControlPlaneConfigError as EventsPullConfigError;
use crate::config::ControlPlaneHttpClient;

/// Tick interval — one leg of the Event log page's staleness budget
/// (sweep 5s + watermark 2s + pull 5s + UI poll 5s ≈ 17s worst case).
pub const PULL_INTERVAL: Duration = Duration::from_secs(5);

/// Hard client budget for the Control-plane HTTP call.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(3);

/// Batch cap requested per pull. The serve side caps at 1000; its default keeps
/// one transactionally inserted page bounded.
const PULL_BATCH_LIMIT: u32 = 500;

#[derive(Debug, thiserror::Error)]
pub enum EventsPullError {
    #[error("Control-plane rejected Events Pull authentication")]
    Unauthenticated,
    #[error("Control-plane rejected Events Pull Tenant binding")]
    Forbidden,
    #[error("Events Pull timed out")]
    Timeout,
    #[error("Control-plane Events Pull peer is unavailable")]
    Unavailable,
    #[error("Control-plane Events Pull returned HTTP status {0}")]
    UnexpectedStatus(StatusCode),
    #[error("Control-plane Events Pull response is invalid")]
    InvalidResponse,
    #[error("Tenant events projection operation failed")]
    Projection(#[source] RepositoryError),
}

#[derive(Clone)]
pub struct EventsPullClient {
    http: ControlPlaneHttpClient,
}

impl fmt::Debug for EventsPullClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventsPullClient")
            .field("http", &self.http)
            .finish()
    }
}

impl EventsPullClient {
    pub fn from_env(
        control_plane_http_url: Option<String>,
    ) -> Result<Option<Self>, EventsPullConfigError> {
        ControlPlaneHttpClient::from_env(control_plane_http_url)
            .map(|client| client.map(|http| Self { http }))
    }

    pub fn new(
        control_plane_http_url: &str,
        bearer_token: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, EventsPullConfigError> {
        ControlPlaneHttpClient::new(
            control_plane_http_url,
            bearer_token,
            allow_insecure_loopback,
        )
        .map(|http| Self { http })
    }

    #[cfg(test)]
    fn from_values(
        control_plane_http_url: Option<&str>,
        bearer_token: Option<&str>,
        allow_insecure_loopback: Option<&str>,
    ) -> Result<Option<Self>, EventsPullConfigError> {
        ControlPlaneHttpClient::from_values(
            control_plane_http_url,
            bearer_token,
            allow_insecure_loopback,
        )
        .map(|client| client.map(|http| Self { http }))
    }

    #[cfg(test)]
    pub(crate) fn with_client(
        client: reqwest::Client,
        control_plane_http_url: &str,
        bearer_token: &str,
    ) -> Result<Self, EventsPullConfigError> {
        ControlPlaneHttpClient::with_client(client, control_plane_http_url, bearer_token)
            .map(|http| Self { http })
    }
}

/// One served event row as the Control plane's JSON encodes it. Field names
/// match the Control plane's `EventResponse` / the archive's column names.
#[derive(serde::Deserialize, Debug, Clone)]
pub struct PulledEvent {
    pub id: Uuid,
    pub ts: DateTime<Utc>,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub archived_at: DateTime<Utc>,
}

/// What one tick did — surfaced for logs and tests.
#[derive(Debug, PartialEq, Eq)]
pub struct PullOutcome {
    pub fetched: usize,
    pub inserted: u64,
}

/// Run the pull cycle until shutdown. Spawned once per conductor replica;
/// replica timers are unsynchronized, which may produce a denser-than-5s
/// effective cadence across the fleet — harmless (smaller batches).
pub async fn run_events_pull(
    repositories: Arc<WriterRepositoryBundle>,
    client: EventsPullClient,
    tenant: Uuid,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                println!("events pull: shutdown signal received, stopping.");
                return;
            }
            _ = tokio::time::sleep(PULL_INTERVAL) => {
                match pull_once(&repositories, &client, tenant).await {
                    Ok(PullOutcome { fetched, inserted }) => {
                        if fetched > 0 {
                            tracing::debug!(
                                "events pull: fetched {} row(s), {} new",
                                fetched,
                                inserted
                            );
                        }
                    }
                    // A failed fetch writes nothing; a failed insertion rolls
                    // back the complete page. The next tick re-derives its
                    // position from committed rows.
                    Err(error) => eprintln!("events pull cycle error: {error}"),
                }
            }
        }
    }
}

/// One Pull cycle. Cursor derivation and the control-plane fetch happen before
/// the repository opens its insertion transaction.
pub async fn pull_once(
    repositories: &WriterRepositoryBundle,
    client: &EventsPullClient,
    tenant: Uuid,
) -> Result<PullOutcome, EventsPullError> {
    let cursor = repositories
        .event_archive_cursor()
        .await
        .map_err(EventsPullError::Projection)?
        .map(|cursor| (cursor.archived_at, cursor.id));
    let batch = fetch_batch(client, tenant, cursor).await?;
    let fetched = batch.len();
    let page = batch
        .into_iter()
        .map(|event| EventProjectionInput {
            id: event.id,
            ts: event.ts,
            event_type: event.event_type,
            payload: event.payload,
            archived_at: event.archived_at,
        })
        .collect::<Vec<_>>();
    let inserted = repositories
        .insert_event_page(&page)
        .await
        .map_err(EventsPullError::Projection)?;
    Ok(PullOutcome { fetched, inserted })
}

/// `GET {control_plane_http_url}/api/internal/events` with the keyset cursor (absent
/// on first pull / after a rebuild). Non-2xx is an error, never an empty
/// batch — "no new events" and "serve path down" must stay distinguishable.
async fn fetch_batch(
    client: &EventsPullClient,
    tenant: Uuid,
    cursor: Option<(DateTime<Utc>, Uuid)>,
) -> Result<Vec<PulledEvent>, EventsPullError> {
    // Scope the pull to this conductor's own tenant — the archive is a shared
    // multi-tenant table, so the projection must receive only its tenant's slice.
    let mut request = client
        .http
        .get("/api/internal/events")
        .timeout(CONTROL_PLANE_TIMEOUT)
        .query(&[
            ("tenant", tenant.to_string()),
            ("limit", PULL_BATCH_LIMIT.to_string()),
        ]);
    if let Some((archived_at, id)) = cursor {
        request = request.query(&[
            ("after_archived_at", archived_at.to_rfc3339()),
            ("after_id", id.to_string()),
        ]);
    }
    let response = request.send().await.map_err(|error| {
        if error.is_timeout() {
            EventsPullError::Timeout
        } else {
            EventsPullError::Unavailable
        }
    })?;
    match response.status() {
        StatusCode::UNAUTHORIZED => return Err(EventsPullError::Unauthenticated),
        StatusCode::FORBIDDEN => return Err(EventsPullError::Forbidden),
        status if !status.is_success() => return Err(EventsPullError::UnexpectedStatus(status)),
        _ => {}
    }
    response
        .json::<Vec<PulledEvent>>()
        .await
        .map_err(|_| EventsPullError::InvalidResponse)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ServerConfig, ServerConnection, StreamOwned};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TEST_CA_PEM: &str = include_str!("testdata/control-plane-test-ca.pem");
    const TEST_SERVER_CERT_PEM: &str = include_str!("testdata/control-plane-test-server.pem");
    const TEST_SERVER_KEY_PEM: &str = include_str!("testdata/control-plane-test-server-key.pem");

    #[test]
    fn connection_configuration_requires_a_secret_only_when_an_endpoint_exists() {
        assert!(EventsPullClient::from_values(None, None, None)
            .unwrap()
            .is_none());
        assert_eq!(
            EventsPullClient::from_values(Some("https://control-plane.example"), None, None)
                .unwrap_err(),
            EventsPullConfigError::MissingBearerToken
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
                EventsPullClient::from_values(
                    Some("https://control-plane.example"),
                    Some(malformed),
                    None,
                )
                .unwrap_err(),
                EventsPullConfigError::InvalidBearerToken
            );
        }

        EventsPullClient::from_values(
            Some("https://control-plane.example"),
            Some(TEST_BEARER_TOKEN),
            None,
        )
        .unwrap();
        EventsPullClient::from_values(
            Some("http://127.0.0.1:8000"),
            Some(TEST_BEARER_TOKEN),
            Some("true"),
        )
        .unwrap();
        EventsPullClient::from_values(
            Some("http://[::1]:8000"),
            Some(TEST_BEARER_TOKEN),
            Some("true"),
        )
        .unwrap();
        assert_eq!(
            EventsPullClient::from_values(
                Some("http://127.0.0.1:8000"),
                Some(TEST_BEARER_TOKEN),
                None,
            )
            .unwrap_err(),
            EventsPullConfigError::InsecureEndpoint
        );
        assert_eq!(
            EventsPullClient::from_values(
                Some("http://control-plane.example"),
                Some(TEST_BEARER_TOKEN),
                Some("true"),
            )
            .unwrap_err(),
            EventsPullConfigError::InsecureEndpoint
        );
    }

    #[test]
    fn client_diagnostics_redact_the_bearer_token() {
        let client =
            EventsPullClient::new("http://127.0.0.1:8000", TEST_BEARER_TOKEN, true).unwrap();
        let rendered = format!("{client:?}");
        assert!(!rendered.contains(TEST_BEARER_TOKEN));
        assert!(rendered.contains("[REDACTED]"));
        assert!(client.http.authorization_is_sensitive());
    }

    #[tokio::test]
    async fn verified_tls_proxy_authenticates_and_rejects_bad_trust_or_hostname() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tenant = Uuid::new_v4();

        let (trusted_url, saw_authorization) = spawn_tls_server();
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let trusted_client = test_client(&trusted_url, trusted_http);
        assert!(fetch_batch(&trusted_client, tenant, None)
            .await
            .unwrap()
            .is_empty());
        assert!(saw_authorization.load(Ordering::SeqCst));

        let (untrusted_url, _) = spawn_tls_server();
        let untrusted_client =
            EventsPullClient::new(&untrusted_url, TEST_BEARER_TOKEN, false).unwrap();
        assert!(matches!(
            fetch_batch(&untrusted_client, tenant, None).await,
            Err(EventsPullError::Unavailable)
        ));

        let (matching_url, _) = spawn_tls_server();
        let mismatched_url = matching_url.replace("localhost", "127.0.0.1");
        let trusted_http = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(TEST_CA_PEM.as_bytes()).unwrap())
            .build()
            .unwrap();
        let mismatched_client = test_client(&mismatched_url, trusted_http);
        assert!(matches!(
            fetch_batch(&mismatched_client, tenant, None).await,
            Err(EventsPullError::Unavailable)
        ));
    }

    fn test_client(endpoint: &str, client: reqwest::Client) -> EventsPullClient {
        EventsPullClient::with_client(client, endpoint, TEST_BEARER_TOKEN).unwrap()
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
