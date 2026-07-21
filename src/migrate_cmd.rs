use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tickr_migrations::{
    apply_sqlite, apply_target, sqlite_writer_options, verify_postgres_schema,
    verify_sqlite_current, verify_sqlite_schema, MigrationTarget,
};
use tickr_proto::config::DataPlaneSql;

pub async fn run() -> Result<()> {
    let selection =
        tickr_proto::config::data_plane_sql().context("resolving data-plane SQL configuration")?;
    match selection {
        DataPlaneSql::Postgres { url } => {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&url)
                .await
                .context("connecting to configured conductor Postgres")?;
            apply_target(MigrationTarget::Conductor, &pool).await?;
            tickr_migrations::verify_current(MigrationTarget::Conductor, &pool).await?;
            verify_postgres_schema(&pool).await?;
            pool.close().await;
            println!("conductor postgres migrations applied and verified.");
        }
        DataPlaneSql::Sqlite { url } => {
            let options = sqlite_writer_options(&url, true)
                .context("parsing configured conductor SQLite URL")?;
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .context("connecting to configured conductor SQLite")?;
            apply_sqlite(MigrationTarget::Conductor, &pool).await?;
            verify_sqlite_current(MigrationTarget::Conductor, &pool).await?;
            verify_sqlite_schema(&pool).await?;
            pool.close().await;
            println!("conductor sqlite migrations applied and verified.");
        }
    }
    Ok(())
}
