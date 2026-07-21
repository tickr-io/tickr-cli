//! API-side data-plane repository role preparation.
//!
//! Process composition resolves SQL selection once and passes it here. Opening
//! the reader verifies schema compatibility, never migrates, and makes SQLite
//! connections read-only.

use tickr_migrations::backend::{ReadOnlyRepositoryBundle, RepositoryError, RepositoryFactory};
use tickr_proto::config::DataPlaneSql;

pub async fn configure_read_only(
    selection: DataPlaneSql,
) -> Result<ReadOnlyRepositoryBundle, RepositoryError> {
    RepositoryFactory::new(selection).open_read_only().await
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use tickr_migrations::backend::{RepositoryMetadata, RepositoryRole};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};

    use super::*;

    #[tokio::test]
    async fn configures_the_resolved_sqlite_url_as_the_read_only_role() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", directory.path().join("tickr.db").display());
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;

        let repositories = configure_read_only(DataPlaneSql::Sqlite { url })
            .await
            .unwrap();
        assert_eq!(
            repositories.metadata(),
            RepositoryMetadata {
                implementation: "sqlite",
                role: RepositoryRole::ReadOnly,
            }
        );
        repositories.close().await;
    }
}
