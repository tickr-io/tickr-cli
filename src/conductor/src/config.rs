use anyhow::{bail, Result};
use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::{RequestBuilder, Url};
use std::fmt;
use std::net::IpAddr;
use tonic::metadata::{Ascii, MetadataValue};

pub const CONTROL_PLANE_BEARER_TOKEN_ENV: &str = "TICKR_CONTROL_PLANE_BEARER_TOKEN";
pub const ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK_ENV: &str =
    "TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK";

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneConfigError {
    #[error(
        "TICKR_CONTROL_PLANE_BEARER_TOKEN is required when a Control-plane endpoint is configured"
    )]
    MissingBearerToken,
    #[error("TICKR_CONTROL_PLANE_BEARER_TOKEN must be a canonical 32-byte base64url token")]
    InvalidBearerToken,
    #[error("Control-plane endpoint is invalid")]
    InvalidEndpoint,
    #[error(
        "Control-plane endpoint must use verified HTTPS, except explicitly enabled loopback HTTP"
    )]
    InsecureEndpoint,
}

/// Shared authenticated transport policy for Conductor HTTP subqueries.
///
/// The authorization value is marked sensitive and is attached only when a
/// consumer builds a request through [`Self::get`].
#[derive(Clone)]
pub(crate) struct ControlPlaneHttpClient {
    client: reqwest::Client,
    endpoint: Url,
    authorization: HeaderValue,
}

impl fmt::Debug for ControlPlaneHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneHttpClient")
            .field("endpoint", &self.endpoint)
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}

impl ControlPlaneHttpClient {
    pub(crate) fn from_env(
        control_plane_http_url: Option<String>,
    ) -> Result<Option<Self>, ControlPlaneConfigError> {
        let token = std::env::var(CONTROL_PLANE_BEARER_TOKEN_ENV).ok();
        let allow_insecure_loopback = std::env::var(ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK_ENV).ok();
        Self::from_values(
            control_plane_http_url.as_deref(),
            token.as_deref(),
            allow_insecure_loopback.as_deref(),
        )
    }

    pub(crate) fn new(
        control_plane_http_url: &str,
        bearer_token: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, ControlPlaneConfigError> {
        Self::from_values(
            Some(control_plane_http_url),
            Some(bearer_token),
            allow_insecure_loopback.then_some("true"),
        )?
        .ok_or(ControlPlaneConfigError::InvalidEndpoint)
    }

    pub(crate) fn from_values(
        control_plane_http_url: Option<&str>,
        bearer_token: Option<&str>,
        allow_insecure_loopback: Option<&str>,
    ) -> Result<Option<Self>, ControlPlaneConfigError> {
        let Some(endpoint) = control_plane_http_url else {
            return Ok(None);
        };
        let token = validated_bearer_token(bearer_token)?;
        let endpoint = validated_control_plane_endpoint(endpoint, allow_insecure_loopback)?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ControlPlaneConfigError::InvalidEndpoint)?;
        Ok(Some(Self {
            client,
            endpoint,
            authorization: authorization_header(token)?,
        }))
    }

    #[cfg(test)]
    pub(crate) fn with_client(
        client: reqwest::Client,
        control_plane_http_url: &str,
        bearer_token: &str,
    ) -> Result<Self, ControlPlaneConfigError> {
        let endpoint = Url::parse(control_plane_http_url)
            .map_err(|_| ControlPlaneConfigError::InvalidEndpoint)?;
        let token = validated_bearer_token(Some(bearer_token))?;
        Ok(Self {
            client,
            endpoint,
            authorization: authorization_header(token)?,
        })
    }

    pub(crate) fn get(&self, path: &str) -> RequestBuilder {
        let url = format!(
            "{}/{}",
            self.endpoint.as_str().trim_end_matches('/'),
            path.trim_start_matches('/'),
        );
        self.client
            .get(url)
            .header(AUTHORIZATION, self.authorization.clone())
    }

    #[cfg(test)]
    pub(crate) fn authorization_is_sensitive(&self) -> bool {
        self.authorization.is_sensitive()
    }
}

/// Authenticated transport policy for the Conductor's gRPC relay.
///
/// The endpoint is admitted once at startup. HTTPS uses tonic's standard
/// Web-PKI roots and hostname verification; plaintext is possible only for an
/// explicitly enabled loopback endpoint. The authorization metadata is marked
/// sensitive and is attached only to a newly created relay request.
#[derive(Clone)]
pub struct ControlPlaneRelayConfig {
    endpoint: Url,
    authorization: MetadataValue<Ascii>,
}

impl fmt::Debug for ControlPlaneRelayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRelayConfig")
            .field("endpoint", &self.endpoint)
            .field("authorization", &"[REDACTED]")
            .finish()
    }
}

impl ControlPlaneRelayConfig {
    pub fn from_env() -> Result<Self, ControlPlaneConfigError> {
        let endpoint = tickr_proto::config::ctrl_relay_url();
        let token = std::env::var(CONTROL_PLANE_BEARER_TOKEN_ENV).ok();
        let allow_insecure_loopback = std::env::var(ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK_ENV).ok();
        Self::from_values(
            &endpoint,
            token.as_deref(),
            allow_insecure_loopback.as_deref(),
        )
    }

    pub fn new(
        endpoint: &str,
        bearer_token: &str,
        allow_insecure_loopback: bool,
    ) -> Result<Self, ControlPlaneConfigError> {
        Self::from_values(
            endpoint,
            Some(bearer_token),
            allow_insecure_loopback.then_some("true"),
        )
    }

    fn from_values(
        endpoint: &str,
        bearer_token: Option<&str>,
        allow_insecure_loopback: Option<&str>,
    ) -> Result<Self, ControlPlaneConfigError> {
        let token = validated_bearer_token(bearer_token)?;
        let endpoint = validated_control_plane_endpoint(endpoint, allow_insecure_loopback)?;
        let mut authorization = format!("Bearer {token}")
            .parse::<MetadataValue<Ascii>>()
            .map_err(|_| ControlPlaneConfigError::InvalidBearerToken)?;
        authorization.set_sensitive(true);
        Ok(Self {
            endpoint,
            authorization,
        })
    }

    pub(crate) fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    pub(crate) fn authorization(&self) -> MetadataValue<Ascii> {
        self.authorization.clone()
    }

    pub(crate) fn uses_tls(&self) -> bool {
        self.endpoint.scheme() == "https"
    }

    #[cfg(test)]
    pub(crate) fn authorization_is_sensitive(&self) -> bool {
        self.authorization.is_sensitive()
    }
}

fn validated_bearer_token(bearer_token: Option<&str>) -> Result<&str, ControlPlaneConfigError> {
    let token = bearer_token.ok_or(ControlPlaneConfigError::MissingBearerToken)?;
    if !is_canonical_bearer_token(token.as_bytes()) {
        return Err(ControlPlaneConfigError::InvalidBearerToken);
    }
    Ok(token)
}

fn authorization_header(token: &str) -> Result<HeaderValue, ControlPlaneConfigError> {
    let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| ControlPlaneConfigError::InvalidBearerToken)?;
    authorization.set_sensitive(true);
    Ok(authorization)
}

fn validated_control_plane_endpoint(
    endpoint: &str,
    allow_insecure_loopback: Option<&str>,
) -> Result<Url, ControlPlaneConfigError> {
    let endpoint = Url::parse(endpoint).map_err(|_| ControlPlaneConfigError::InvalidEndpoint)?;
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(ControlPlaneConfigError::InvalidEndpoint);
    }
    let host = endpoint
        .host_str()
        .ok_or(ControlPlaneConfigError::InvalidEndpoint)?;
    match endpoint.scheme() {
        "https" => {}
        "http" if allow_insecure_loopback == Some("true") && is_loopback_host(host) => {}
        "http" => return Err(ControlPlaneConfigError::InsecureEndpoint),
        _ => return Err(ControlPlaneConfigError::InvalidEndpoint),
    }
    Ok(endpoint)
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

#[derive(Clone, PartialEq, Eq)]
pub struct LogStorageConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

impl std::fmt::Debug for LogStorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStorageConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .field("region", &self.region)
            .finish()
    }
}

impl LogStorageConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        fn required(value: Option<String>, name: &str) -> Result<String> {
            match value.map(|value| value.trim().to_owned()) {
                Some(value) if !value.is_empty() => Ok(value),
                _ => bail!("required environment variable {name} is missing or empty"),
            }
        }
        Ok(Self {
            endpoint: required(
                lookup("TICKR_LOG_STORAGE_ENDPOINT"),
                "TICKR_LOG_STORAGE_ENDPOINT",
            )?,
            bucket: required(
                lookup("TICKR_LOG_STORAGE_BUCKET"),
                "TICKR_LOG_STORAGE_BUCKET",
            )?,
            access_key_id: required(
                lookup("TICKR_LOG_STORAGE_ACCESS_KEY_ID"),
                "TICKR_LOG_STORAGE_ACCESS_KEY_ID",
            )?,
            secret_access_key: required(
                lookup("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY"),
                "TICKR_LOG_STORAGE_SECRET_ACCESS_KEY",
            )?,
            region: required(
                lookup("TICKR_LOG_STORAGE_REGION"),
                "TICKR_LOG_STORAGE_REGION",
            )?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn storage_config_is_complete_and_secret_safe() {
        let values = HashMap::from([
            ("TICKR_LOG_STORAGE_ENDPOINT", "http://127.0.0.1:9000"),
            ("TICKR_LOG_STORAGE_BUCKET", "tickr-logs"),
            ("TICKR_LOG_STORAGE_ACCESS_KEY_ID", "dev-access"),
            (
                "TICKR_LOG_STORAGE_SECRET_ACCESS_KEY",
                "secret-not-for-errors",
            ),
            ("TICKR_LOG_STORAGE_REGION", "local"),
        ]);
        let config =
            LogStorageConfig::from_lookup(|key| values.get(key).map(ToString::to_string)).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("dev-access"));
        assert!(!debug.contains("secret-not-for-errors"));
        let error = LogStorageConfig::from_lookup(|key| {
            (key != "TICKR_LOG_STORAGE_SECRET_ACCESS_KEY")
                .then(|| values.get(key).map(ToString::to_string))
                .flatten()
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY"));
        assert!(!error.contains("secret-not-for-errors"));
    }
}
