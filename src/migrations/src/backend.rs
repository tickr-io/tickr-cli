//! Selected data-plane SQL connection roles.
//!
//! Factories consume the already-resolved process selection. They open and
//! verify role-specific repository bundles without exposing backend pools.

use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgPool, SqlitePool};
use tickr_proto::config::DataPlaneSql;

use crate::{
    verify_current, verify_postgres_schema, verify_sqlite_current, verify_sqlite_schema,
    MigrationTarget, MigrationsDriftError, SchemaVerificationError,
};

const POSTGRES_CONNECTIONS: u32 = 10;
const SQLITE_READER_CONNECTIONS: u32 = 10;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    Configuration,
    Unavailable,
    ContentionTimeout,
    ConstraintConflict,
    IncompatibleSchema,
    CorruptStoredValue,
    Internal,
}

impl fmt::Display for RepositoryErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Configuration => "configuration",
            Self::Unavailable => "unavailable",
            Self::ContentionTimeout => "contention timeout",
            Self::ConstraintConflict => "constraint conflict",
            Self::IncompatibleSchema => "incompatible schema",
            Self::CorruptStoredValue => "corrupt stored value",
            Self::Internal => "internal",
        };
        f.write_str(value)
    }
}

#[derive(Debug)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    source: BoxError,
}

impl RepositoryError {
    pub(crate) fn new(
        kind: RepositoryErrorKind,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            source: Box::new(source),
        }
    }

    pub fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} data-plane repository failure: {}",
            self.kind, self.source
        )
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryRole {
    Writer,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositoryMetadata {
    pub implementation: &'static str,
    pub role: RepositoryRole,
}

#[derive(Debug, thiserror::Error)]
#[error("SQLite repository URL must identify a file-backed database, got `{url}`")]
struct InvalidSqliteUrl {
    url: String,
}

pub struct RepositoryFactory {
    selection: DataPlaneSql,
}

impl RepositoryFactory {
    pub fn new(selection: DataPlaneSql) -> Self {
        Self { selection }
    }

    pub fn implementation(&self) -> &'static str {
        self.selection.implementation()
    }

    pub async fn open_writer(&self) -> Result<WriterRepositoryBundle, RepositoryError> {
        match &self.selection {
            DataPlaneSql::Postgres { url } => {
                let pool = PgPoolOptions::new()
                    .max_connections(POSTGRES_CONNECTIONS)
                    .connect(url)
                    .await
                    .map_err(repository_sqlx_error)?;
                if let Err(error) = verify_postgres_role(&pool).await {
                    pool.close().await;
                    return Err(error);
                }
                Ok(WriterRepositoryBundle {
                    pool: BackendPool::Postgres(pool),
                })
            }
            DataPlaneSql::Sqlite { url } => {
                validate_sqlite_file_url(url)?;
                let options =
                    crate::sqlite_writer_options(url, false).map_err(repository_sqlx_error)?;
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(SQLITE_BUSY_TIMEOUT)
                    .connect_with(options)
                    .await
                    .map_err(repository_sqlx_error)?;
                if let Err(error) = verify_sqlite_role(&pool).await {
                    pool.close().await;
                    return Err(error);
                }
                Ok(WriterRepositoryBundle {
                    pool: BackendPool::Sqlite(pool),
                })
            }
        }
    }

    pub async fn open_read_only(&self) -> Result<ReadOnlyRepositoryBundle, RepositoryError> {
        match &self.selection {
            DataPlaneSql::Postgres { url } => {
                let pool = PgPoolOptions::new()
                    .max_connections(POSTGRES_CONNECTIONS)
                    .after_connect(|connection, _| {
                        Box::pin(async move {
                            sqlx::query("SET default_transaction_read_only = on")
                                .execute(connection)
                                .await?;
                            Ok(())
                        })
                    })
                    .connect(url)
                    .await
                    .map_err(repository_sqlx_error)?;
                if let Err(error) = verify_postgres_role(&pool).await {
                    pool.close().await;
                    return Err(error);
                }
                Ok(ReadOnlyRepositoryBundle {
                    pool: BackendPool::Postgres(pool),
                })
            }
            DataPlaneSql::Sqlite { url } => {
                validate_sqlite_file_url(url)?;
                let options = sqlite_read_only_options(url).map_err(repository_sqlx_error)?;
                let pool = SqlitePoolOptions::new()
                    .max_connections(SQLITE_READER_CONNECTIONS)
                    .acquire_timeout(SQLITE_BUSY_TIMEOUT)
                    .connect_with(options)
                    .await
                    .map_err(repository_sqlx_error)?;
                if let Err(error) = verify_sqlite_role(&pool).await {
                    pool.close().await;
                    return Err(error);
                }
                Ok(ReadOnlyRepositoryBundle {
                    pool: BackendPool::Sqlite(pool),
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BackendPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

#[derive(Debug, Clone)]
pub struct WriterRepositoryBundle {
    pub(crate) pool: BackendPool,
}

impl WriterRepositoryBundle {
    /// Wrap an already-open Postgres writer pool during incremental process
    /// composition. Workflow callers receive this operation surface, not the
    /// pool retained by other repository slices.
    pub fn from_postgres_pool(pool: PgPool) -> Self {
        Self {
            pool: BackendPool::Postgres(pool),
        }
    }

    pub fn metadata(&self) -> RepositoryMetadata {
        RepositoryMetadata {
            implementation: self.implementation(),
            role: RepositoryRole::Writer,
        }
    }

    pub fn implementation(&self) -> &'static str {
        self.pool.implementation()
    }

    pub async fn verify_schema(&self) -> Result<(), RepositoryError> {
        self.pool.verify_schema().await
    }

    pub async fn probe(&self) -> Result<(), RepositoryError> {
        self.pool.probe().await
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

#[derive(Debug, Clone)]
pub struct ReadOnlyRepositoryBundle {
    pub(crate) pool: BackendPool,
}

impl ReadOnlyRepositoryBundle {
    /// Wrap an already-open Postgres pool in the API's read-only operation
    /// surface while unrelated archive reads complete their repository cutover.
    pub fn from_postgres_pool(pool: PgPool) -> Self {
        Self {
            pool: BackendPool::Postgres(pool),
        }
    }

    pub fn metadata(&self) -> RepositoryMetadata {
        RepositoryMetadata {
            implementation: self.implementation(),
            role: RepositoryRole::ReadOnly,
        }
    }

    pub fn implementation(&self) -> &'static str {
        self.pool.implementation()
    }

    pub async fn verify_schema(&self) -> Result<(), RepositoryError> {
        self.pool.verify_schema().await
    }

    pub async fn probe(&self) -> Result<(), RepositoryError> {
        self.pool.probe().await
    }

    /// Repository-owned Health law: a trivial read proves reachability, then the
    /// selected implementation's migration identity and logical schema are verified.
    pub async fn health_check(&self) -> Result<(), RepositoryError> {
        self.pool.probe().await?;
        self.pool.verify_schema().await
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

impl BackendPool {
    fn implementation(&self) -> &'static str {
        match self {
            Self::Postgres(_) => "postgres",
            Self::Sqlite(_) => "sqlite",
        }
    }

    async fn verify_schema(&self) -> Result<(), RepositoryError> {
        match self {
            Self::Postgres(pool) => verify_postgres_role(pool).await,
            Self::Sqlite(pool) => verify_sqlite_role(pool).await,
        }
    }

    async fn probe(&self) -> Result<(), RepositoryError> {
        match self {
            Self::Postgres(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(repository_sqlx_error)?;
            }
            Self::Sqlite(pool) => {
                sqlx::query("SELECT 1")
                    .execute(pool)
                    .await
                    .map_err(repository_sqlx_error)?;
            }
        }
        Ok(())
    }

    async fn close(&self) {
        match self {
            Self::Postgres(pool) => pool.close().await,
            Self::Sqlite(pool) => pool.close().await,
        }
    }
}

fn validate_sqlite_file_url(url: &str) -> Result<(), RepositoryError> {
    let lower = url.to_ascii_lowercase();
    if !lower.starts_with("sqlite:")
        || lower == "sqlite::memory:"
        || lower.contains("mode=memory")
        || lower.ends_with(":memory:")
    {
        return Err(RepositoryError::new(
            RepositoryErrorKind::Configuration,
            InvalidSqliteUrl {
                url: url.to_owned(),
            },
        ));
    }
    Ok(())
}

fn sqlite_read_only_options(url: &str) -> Result<SqliteConnectOptions, sqlx::Error> {
    Ok(SqliteConnectOptions::from_str(url)?
        .create_if_missing(false)
        .read_only(true)
        .foreign_keys(true)
        .busy_timeout(SQLITE_BUSY_TIMEOUT))
}

async fn verify_postgres_role(pool: &PgPool) -> Result<(), RepositoryError> {
    verify_current(MigrationTarget::Conductor, pool)
        .await
        .map_err(repository_migration_error)?;
    verify_postgres_schema(pool)
        .await
        .map_err(repository_schema_error)
}

async fn verify_sqlite_role(pool: &SqlitePool) -> Result<(), RepositoryError> {
    verify_sqlite_current(MigrationTarget::Conductor, pool)
        .await
        .map_err(repository_migration_error)?;
    verify_sqlite_schema(pool)
        .await
        .map_err(repository_schema_error)
}

fn repository_migration_error(source: MigrationsDriftError) -> RepositoryError {
    let kind = match &source {
        MigrationsDriftError::Query { source, .. } => classify_sqlx_error(source),
        _ => RepositoryErrorKind::IncompatibleSchema,
    };
    RepositoryError::new(kind, source)
}

fn repository_schema_error(source: SchemaVerificationError) -> RepositoryError {
    let kind = match &source {
        SchemaVerificationError::Query { source, .. } => classify_sqlx_error(source),
        SchemaVerificationError::Incompatible { .. } => RepositoryErrorKind::IncompatibleSchema,
    };
    RepositoryError::new(kind, source)
}

pub(crate) fn repository_sqlx_error(source: sqlx::Error) -> RepositoryError {
    let kind = classify_sqlx_error(&source);
    RepositoryError::new(kind, source)
}

fn classify_sqlx_error(source: &sqlx::Error) -> RepositoryErrorKind {
    match source {
        sqlx::Error::Configuration(_) => RepositoryErrorKind::Configuration,
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => RepositoryErrorKind::Unavailable,
        sqlx::Error::PoolTimedOut => RepositoryErrorKind::ContentionTimeout,
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => {
            RepositoryErrorKind::CorruptStoredValue
        }
        sqlx::Error::Database(database) => {
            let code = database.code();
            let code = code.as_deref().unwrap_or_default();
            let message = database.message().to_ascii_lowercase();
            if database.is_unique_violation()
                || database.is_foreign_key_violation()
                || database.is_check_violation()
                || code.starts_with("23")
                || code == "19"
            {
                RepositoryErrorKind::ConstraintConflict
            } else if code == "5"
                || code == "6"
                || code == "55P03"
                || code.starts_with("40")
                || message.contains("database is locked")
                || message.contains("database is busy")
            {
                RepositoryErrorKind::ContentionTimeout
            } else if code.starts_with("08")
                || code == "14"
                || message.contains("unable to open database")
                || message.contains("connection")
            {
                RepositoryErrorKind::Unavailable
            } else {
                RepositoryErrorKind::Internal
            }
        }
        _ => RepositoryErrorKind::Internal,
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ImageExt};
    use tickr_proto::config::{resolve_data_plane_sql, DataPlaneSqlConfigError};

    use super::*;
    use crate::{apply_sqlite, apply_target, sqlite_writer_options};

    fn sqlite_url(path: &Path) -> String {
        format!("sqlite://{}", path.display())
    }

    async fn migrated_sqlite() -> (TempDir, String) {
        let directory = tempfile::tempdir().unwrap();
        let url = sqlite_url(&directory.path().join("tickr.db"));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &pool)
            .await
            .unwrap();
        pool.close().await;
        (directory, url)
    }

    async fn migrated_postgres() -> Option<(
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
        String,
    )> {
        let container = Postgres::default()
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
            .ok()?;
        let port = container.get_host_port_ipv4(5432).await.ok()?;
        let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
        let pool = PgPoolOptions::new().connect(&url).await.ok()?;
        apply_target(MigrationTarget::Conductor, &pool).await.ok()?;
        pool.close().await;
        Some((container, url))
    }

    #[test]
    fn shared_selector_defaults_and_refuses_unsupported_sqlite_topologies() {
        assert_eq!(
            resolve_data_plane_sql(None, Some("ignored"), Some("postgres://db/tickr"), None),
            Ok(DataPlaneSql::Postgres {
                url: "postgres://db/tickr".to_owned(),
            })
        );
        assert_eq!(
            resolve_data_plane_sql(Some("sqlite"), None, None, Some("sqlite:///tmp/tickr.db"),),
            Err(DataPlaneSqlConfigError::UnsupportedSqliteTopology(None))
        );
        assert_eq!(
            resolve_data_plane_sql(
                Some("sqlite"),
                Some("distributed"),
                None,
                Some("sqlite:///tmp/tickr.db"),
            ),
            Err(DataPlaneSqlConfigError::UnsupportedSqliteTopology(Some(
                "distributed".to_owned(),
            )))
        );
    }

    #[tokio::test]
    async fn sqlite_roles_are_file_backed_bounded_and_read_only_after_reopen() {
        let (_directory, url) = migrated_sqlite().await;
        let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url });
        let writer = factory.open_writer().await.unwrap();
        assert_eq!(
            writer.metadata(),
            RepositoryMetadata {
                implementation: "sqlite",
                role: RepositoryRole::Writer,
            }
        );
        let BackendPool::Sqlite(writer_pool) = &writer.pool else {
            unreachable!();
        };
        assert_eq!(writer_pool.options().get_max_connections(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(writer_pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(writer_pool)
                .await
                .unwrap(),
            "wal"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA busy_timeout")
                .fetch_one(writer_pool)
                .await
                .unwrap(),
            5_000
        );
        sqlx::query("INSERT INTO signal_wakeups (signal_id, name, matched_workflows, created_at) VALUES (?, ?, ?, ?)")
            .bind("00000000-0000-0000-0000-000000000001")
            .bind("role-law")
            .bind(1_i64)
            .bind(1_i64)
            .execute(writer_pool)
            .await
            .unwrap();
        let duplicate = sqlx::query("INSERT INTO signal_wakeups (signal_id, name, matched_workflows, created_at) VALUES (?, ?, ?, ?)")
            .bind("00000000-0000-0000-0000-000000000001")
            .bind("duplicate")
            .bind(1_i64)
            .bind(2_i64)
            .execute(writer_pool)
            .await
            .unwrap_err();
        let duplicate = repository_sqlx_error(duplicate);
        assert_eq!(duplicate.kind(), RepositoryErrorKind::ConstraintConflict);
        assert!(duplicate.source().is_some());
        writer.close().await;

        let reader = factory.open_read_only().await.unwrap();
        assert_eq!(reader.metadata().role, RepositoryRole::ReadOnly);
        let BackendPool::Sqlite(reader_pool) = &reader.pool else {
            unreachable!();
        };
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(reader_pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(reader_pool)
                .await
                .unwrap(),
            "wal"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA busy_timeout")
                .fetch_one(reader_pool)
                .await
                .unwrap(),
            5_000
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT name FROM signal_wakeups WHERE signal_id = ?",)
                .bind("00000000-0000-0000-0000-000000000001")
                .fetch_one(reader_pool)
                .await
                .unwrap(),
            "role-law"
        );
        assert!(sqlx::query("INSERT INTO signal_wakeups (signal_id, name, matched_workflows, created_at) VALUES (?, ?, ?, ?)")
            .bind("00000000-0000-0000-0000-000000000002")
            .bind("forbidden")
            .bind(0_i64)
            .bind(2_i64)
            .execute(reader_pool)
            .await
            .is_err());
        reader.verify_schema().await.unwrap();
        reader.probe().await.unwrap();
        reader.close().await;
    }

    #[tokio::test]
    async fn sqlite_factory_refuses_missing_non_file_and_incompatible_databases() {
        let directory = tempfile::tempdir().unwrap();
        let missing_url = sqlite_url(&directory.path().join("missing.db"));
        let missing = RepositoryFactory::new(DataPlaneSql::Sqlite { url: missing_url })
            .open_writer()
            .await
            .unwrap_err();
        assert_eq!(missing.kind(), RepositoryErrorKind::Unavailable);

        let in_memory = RepositoryFactory::new(DataPlaneSql::Sqlite {
            url: "sqlite::memory:".to_owned(),
        })
        .open_writer()
        .await
        .unwrap_err();
        assert_eq!(in_memory.kind(), RepositoryErrorKind::Configuration);
        let contradictory = RepositoryFactory::new(DataPlaneSql::Sqlite {
            url: "postgres://db/tickr".to_owned(),
        })
        .open_writer()
        .await
        .unwrap_err();
        assert_eq!(contradictory.kind(), RepositoryErrorKind::Configuration);

        let (_directory, url) = migrated_sqlite().await;
        let tamper_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, false).unwrap())
            .await
            .unwrap();
        sqlx::query("ALTER TABLE signal_wakeups ADD COLUMN incompatible TEXT")
            .execute(&tamper_pool)
            .await
            .unwrap();
        tamper_pool.close().await;
        let incompatible = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
            .open_read_only()
            .await
            .unwrap_err();
        assert_eq!(incompatible.kind(), RepositoryErrorKind::IncompatibleSchema);
    }

    #[tokio::test]
    async fn postgres_roles_verify_schema_and_enforce_the_reader_role() {
        let Some((_container, url)) = migrated_postgres().await else {
            return;
        };
        let factory = RepositoryFactory::new(DataPlaneSql::Postgres { url });
        let writer = factory.open_writer().await.unwrap();
        writer.verify_schema().await.unwrap();
        writer.probe().await.unwrap();
        writer.close().await;

        let reader = factory.open_read_only().await.unwrap();
        let BackendPool::Postgres(reader_pool) = &reader.pool else {
            unreachable!();
        };
        let read_only: String = sqlx::query_scalar("SHOW default_transaction_read_only")
            .fetch_one(reader_pool)
            .await
            .unwrap();
        assert_eq!(read_only, "on");
        assert!(sqlx::query("INSERT INTO signal_wakeups (signal_id, name, matched_workflows, created_at) VALUES ($1, $2, $3, now())")
            .bind(uuid::Uuid::nil())
            .bind("forbidden")
            .bind(0_i64)
            .execute(reader_pool)
            .await
            .is_err());
        reader.close().await;
    }

    #[tokio::test]
    async fn postgres_factory_classifies_schema_refusal() {
        let Some((_container, url)) = migrated_postgres().await else {
            return;
        };
        let pool = PgPoolOptions::new().connect(&url).await.unwrap();
        sqlx::query("ALTER TABLE signal_wakeups ALTER COLUMN name DROP NOT NULL")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let error = RepositoryFactory::new(DataPlaneSql::Postgres { url })
            .open_writer()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), RepositoryErrorKind::IncompatibleSchema);
    }
}
