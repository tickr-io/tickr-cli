use anyhow::{bail, Result};

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
