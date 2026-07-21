//! SQL-backed Wakeup audit adapter.

use anyhow::{Context, Result};
use tickr_migrations::backend::WriterRepositoryBundle;
pub use tickr_migrations::signal_repository::SignalWakeupInput as SignalWakeupRow;

pub async fn insert(repositories: &WriterRepositoryBundle, row: &SignalWakeupRow) -> Result<()> {
    repositories
        .insert_signal_wakeup(row)
        .await
        .context("insert Signal Wakeup audit")?;
    Ok(())
}
