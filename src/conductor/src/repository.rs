//! Conductor-side data-plane repository role preparation.
//!
//! Process composition resolves SQL selection once and passes it here. Opening
//! the writer verifies schema compatibility but never applies migrations.

use tickr_migrations::backend::{RepositoryError, RepositoryFactory, WriterRepositoryBundle};
use tickr_proto::config::DataPlaneSql;

pub async fn configure_writer(
    selection: DataPlaneSql,
) -> Result<WriterRepositoryBundle, RepositoryError> {
    RepositoryFactory::new(selection).open_writer().await
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;
    use tickr_migrations::backend::{RepositoryMetadata, RepositoryRole};
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};

    use super::*;

    #[tokio::test]
    async fn configures_the_resolved_sqlite_url_as_the_writer_role() {
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

        let repositories = configure_writer(DataPlaneSql::Sqlite { url })
            .await
            .unwrap();
        assert_eq!(
            repositories.metadata(),
            RepositoryMetadata {
                implementation: "sqlite",
                role: RepositoryRole::Writer,
            }
        );
        repositories.close().await;
    }
}
