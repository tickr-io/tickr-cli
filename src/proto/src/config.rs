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

/// Complete SQL configuration selected once before a data-plane store is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataPlaneSql {
    Postgres { url: String },
    Sqlite { url: String },
}

impl DataPlaneSql {
    pub fn implementation(&self) -> &'static str {
        match self {
            Self::Postgres { .. } => "postgres",
            Self::Sqlite { .. } => "sqlite",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataPlaneSqlConfigError {
    UnsupportedBackend(String),
    MissingPostgresUrl,
    MissingSqliteUrl,
    UnsupportedSqliteTopology(Option<String>),
}

impl std::fmt::Display for DataPlaneSqlConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedBackend(value) => write!(
                f,
                "TICKR_SQL_BACKEND must be `postgres` or `sqlite`, got `{value}`"
            ),
            Self::MissingPostgresUrl => {
                f.write_str("TICKR_CONDUCTOR_POSTGRES_URL is required for postgres")
            }
            Self::MissingSqliteUrl => {
                f.write_str("TICKR_CONDUCTOR_SQLITE_URL is required for sqlite")
            }
            Self::UnsupportedSqliteTopology(None) => {
                f.write_str("TICKR_SQL_TOPOLOGY=single-node is required for sqlite")
            }
            Self::UnsupportedSqliteTopology(Some(value)) => write!(
                f,
                "TICKR_SQL_TOPOLOGY must be `single-node` for sqlite, got `{value}`"
            ),
        }
    }
}

impl std::error::Error for DataPlaneSqlConfigError {}

/// Resolves the shared data-plane SQL selection without opening a connection.
///
/// Postgres is the default and deliberately ignores the topology selector.
/// SQLite is admitted only for the explicitly declared single-node formation.
pub fn resolve_data_plane_sql(
    backend: Option<&str>,
    topology: Option<&str>,
    postgres_url: Option<&str>,
    sqlite_url: Option<&str>,
) -> Result<DataPlaneSql, DataPlaneSqlConfigError> {
    match backend {
        None | Some("postgres") => required_url(postgres_url)
            .map(|url| DataPlaneSql::Postgres { url })
            .ok_or(DataPlaneSqlConfigError::MissingPostgresUrl),
        Some("sqlite") => {
            if topology != Some("single-node") {
                return Err(DataPlaneSqlConfigError::UnsupportedSqliteTopology(
                    topology.map(str::to_owned),
                ));
            }
            required_url(sqlite_url)
                .map(|url| DataPlaneSql::Sqlite { url })
                .ok_or(DataPlaneSqlConfigError::MissingSqliteUrl)
        }
        Some(value) => Err(DataPlaneSqlConfigError::UnsupportedBackend(
            value.to_owned(),
        )),
    }
}

/// Reads and resolves the process-wide data-plane SQL selection.
pub fn data_plane_sql() -> Result<DataPlaneSql, DataPlaneSqlConfigError> {
    let backend = std::env::var("TICKR_SQL_BACKEND").ok();
    let topology = std::env::var("TICKR_SQL_TOPOLOGY").ok();
    let postgres_url = std::env::var("TICKR_CONDUCTOR_POSTGRES_URL").ok();
    let sqlite_url = std::env::var("TICKR_CONDUCTOR_SQLITE_URL").ok();
    resolve_data_plane_sql(
        backend.as_deref(),
        topology.as_deref(),
        postgres_url.as_deref(),
        sqlite_url.as_deref(),
    )
}

fn required_url(value: Option<&str>) -> Option<String> {
    value
        .filter(|url| !url.trim().is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod data_plane_sql_tests {
    use super::{resolve_data_plane_sql, DataPlaneSql, DataPlaneSqlConfigError};

    #[test]
    fn postgres_is_the_default_and_ignores_topology() {
        assert_eq!(
            resolve_data_plane_sql(None, Some("distributed"), Some("postgres://db/tickr"), None,),
            Ok(DataPlaneSql::Postgres {
                url: "postgres://db/tickr".to_owned(),
            })
        );
    }

    #[test]
    fn sqlite_requires_url_and_exact_single_node_topology() {
        assert_eq!(
            resolve_data_plane_sql(Some("sqlite"), None, None, Some("sqlite:///tmp/tickr.db"),),
            Err(DataPlaneSqlConfigError::UnsupportedSqliteTopology(None))
        );
        assert_eq!(
            resolve_data_plane_sql(
                Some("sqlite"),
                Some("single_node"),
                None,
                Some("sqlite:///tmp/tickr.db"),
            ),
            Err(DataPlaneSqlConfigError::UnsupportedSqliteTopology(Some(
                "single_node".to_owned(),
            )))
        );
        assert_eq!(
            resolve_data_plane_sql(Some("sqlite"), Some("single-node"), None, None),
            Err(DataPlaneSqlConfigError::MissingSqliteUrl)
        );
        assert_eq!(
            resolve_data_plane_sql(
                Some("sqlite"),
                Some("single-node"),
                None,
                Some("sqlite:///tmp/tickr.db"),
            ),
            Ok(DataPlaneSql::Sqlite {
                url: "sqlite:///tmp/tickr.db".to_owned(),
            })
        );
    }

    #[test]
    fn unknown_backend_is_rejected() {
        assert_eq!(
            resolve_data_plane_sql(
                Some("mysql"),
                Some("single-node"),
                Some("postgres://db/tickr"),
                Some("sqlite:///tmp/tickr.db"),
            ),
            Err(DataPlaneSqlConfigError::UnsupportedBackend(
                "mysql".to_owned(),
            ))
        );
    }
}

/// Base URL of the configured Control plane's HTTP subquery channel.
pub fn ctrl_http_url() -> String {
    std::env::var("TICKR_CTRL_HTTP_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string())
}

/// URL of the configured Control plane's Conductor relay endpoint.
pub fn ctrl_relay_url() -> String {
    std::env::var("TICKR_CTRL_RELAY_URL").unwrap_or_else(|_| "http://127.0.0.1:9095".to_string())
}

#[cfg(test)]
mod control_plane_url_tests {
    use std::sync::Mutex;

    use super::{ctrl_http_url, ctrl_relay_url};

    static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn reads_control_plane_urls_from_the_renamed_variables() {
        let _lock = ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let environment = ControlPlaneUrlEnvironment::set(
            Some("http://control-plane.test:8000"),
            Some("http://control-plane.test:9095"),
            None,
            None,
        );

        assert_eq!(ctrl_http_url(), "http://control-plane.test:8000");
        assert_eq!(ctrl_relay_url(), "http://control-plane.test:9095");

        drop(environment);
    }

    #[test]
    fn defaults_when_renamed_variables_are_absent_even_with_legacy_variables() {
        let _lock = ENVIRONMENT_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let environment = ControlPlaneUrlEnvironment::set(
            None,
            None,
            Some("http://legacy.test:8000"),
            Some("http://legacy.test:9095"),
        );

        assert_eq!(ctrl_http_url(), "http://127.0.0.1:8000");
        assert_eq!(ctrl_relay_url(), "http://127.0.0.1:9095");

        drop(environment);
    }

    struct ControlPlaneUrlEnvironment {
        previous: [(&'static str, Option<std::ffi::OsString>); 4],
    }

    impl ControlPlaneUrlEnvironment {
        fn set(
            http_url: Option<&str>,
            relay_url: Option<&str>,
            legacy_http_url: Option<&str>,
            legacy_relay_url: Option<&str>,
        ) -> Self {
            let mut environment = Self {
                previous: [
                    (
                        "TICKR_CTRL_HTTP_URL",
                        std::env::var_os("TICKR_CTRL_HTTP_URL"),
                    ),
                    (
                        "TICKR_CTRL_RELAY_URL",
                        std::env::var_os("TICKR_CTRL_RELAY_URL"),
                    ),
                    (
                        "TICKR_COORDINATOR_HTTP_URL",
                        std::env::var_os("TICKR_COORDINATOR_HTTP_URL"),
                    ),
                    (
                        "TICKR_COORDINATOR_RELAY_URL",
                        std::env::var_os("TICKR_COORDINATOR_RELAY_URL"),
                    ),
                ],
            };

            environment.set_value("TICKR_CTRL_HTTP_URL", http_url);
            environment.set_value("TICKR_CTRL_RELAY_URL", relay_url);
            environment.set_value("TICKR_COORDINATOR_HTTP_URL", legacy_http_url);
            environment.set_value("TICKR_COORDINATOR_RELAY_URL", legacy_relay_url);
            environment
        }

        fn set_value(&mut self, name: &str, value: Option<&str>) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }

    impl Drop for ControlPlaneUrlEnvironment {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }
}
