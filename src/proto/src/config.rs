/// Returns the NATS URL to connect to.
///
/// Reads `TICKR_NATS_URL` from the environment, falling back to
/// `nats://localhost:4222` for the standard dev-loop stack
/// (`docker-compose-infra.yml`).
pub fn nats_url() -> String {
    std::env::var("TICKR_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

/// Postgres URL for the data-plane (conductor-side) archive of terminal workflow runs.
///
/// The repo-local launcher supplies this value. Production deployments must
/// inject an independently managed credential.
pub fn conductor_postgres_url() -> String {
    std::env::var("TICKR_CONDUCTOR_POSTGRES_URL").expect("TICKR_CONDUCTOR_POSTGRES_URL is required")
}

/// Base URL of the configured Tickr coordinator's HTTP API.
pub fn coordinator_http_url() -> String {
    std::env::var("TICKR_COORDINATOR_HTTP_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string())
}

/// URL of the coordinator's public conductor-relay gRPC endpoint.
pub fn coordinator_relay_url() -> String {
    std::env::var("TICKR_COORDINATOR_RELAY_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9095".to_string())
}
