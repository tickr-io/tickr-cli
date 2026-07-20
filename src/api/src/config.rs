use anyhow::{bail, Context, Result};
use std::net::SocketAddr;

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

    pub fn operator(&self) -> Result<opendal::Operator> {
        let builder = opendal::services::S3::default()
            .bucket(&self.bucket)
            .endpoint(&self.endpoint)
            .access_key_id(&self.access_key_id)
            .secret_access_key(&self.secret_access_key)
            .region(&self.region);
        Ok(opendal::Operator::new(builder)
            .context("failed to configure log storage")?
            .finish())
    }
}

pub fn api_bind_addr() -> Result<SocketAddr> {
    api_bind_addr_from(std::env::var("TICKR_API_BIND_ADDR").ok())
}

fn api_bind_addr_from(value: Option<String>) -> Result<SocketAddr> {
    value
        .as_deref()
        .unwrap_or("127.0.0.1:6000")
        .parse()
        .context("TICKR_API_BIND_ADDR must be a socket address")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn bind_defaults_to_loopback_and_rejects_invalid_values() {
        assert_eq!(
            api_bind_addr_from(None).unwrap(),
            "127.0.0.1:6000".parse().unwrap()
        );
        assert!(api_bind_addr_from(Some("not-an-address".into())).is_err());
    }

    #[test]
    fn storage_config_requires_every_value_without_echoing_secrets() {
        let mut values = HashMap::from([
            ("TICKR_LOG_STORAGE_ENDPOINT", "http://127.0.0.1:9000"),
            ("TICKR_LOG_STORAGE_BUCKET", "tickr-logs"),
            ("TICKR_LOG_STORAGE_ACCESS_KEY_ID", "dev-access"),
            ("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY", "super-secret-value"),
            ("TICKR_LOG_STORAGE_REGION", "local"),
        ]);
        let config =
            LogStorageConfig::from_lookup(|key| values.get(key).map(ToString::to_string)).unwrap();
        assert_eq!(config.bucket, "tickr-logs");
        let debug = format!("{config:?}");
        assert!(!debug.contains("dev-access"));
        assert!(!debug.contains("super-secret-value"));
        values.remove("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY");
        let error = LogStorageConfig::from_lookup(|key| values.get(key).map(ToString::to_string))
            .unwrap_err()
            .to_string();
        assert!(error.contains("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY"));
        assert!(!error.contains("super-secret-value"));
    }
}
