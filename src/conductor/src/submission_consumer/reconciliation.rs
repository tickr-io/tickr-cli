//! Boot-time reconciliation: republish a submission message for every
//! workflow row currently at `Ready`. Runs exactly once at conductor
//! startup, before the submission consumer subscribes. No periodic
//! re-scan in steady state.

use crate::submission_consumer::consumer::publish_submission;
use crate::submission_consumer::message::SubmissionMessage;
use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use sqlx::PgPool;
use uuid::Uuid;

/// Republish a `SubmissionMessage` per workflow row at `status = Ready`.
/// Returns the number of messages published. Logs and continues on
/// per-row publish failures — startup shouldn't wedge on a transient
/// NATS hiccup.
pub async fn reconcile_orphan_ready_rows(pg_pool: &PgPool, nats: &NatsClient) -> Result<usize> {
    let rows: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT id, version FROM workflows WHERE status = 'Ready'")
            .fetch_all(pg_pool)
            .await
            .context("scan workflows for orphan Ready rows")?;

    let total = rows.len();
    let mut published = 0usize;
    for (workflow_id, workflow_version) in rows {
        let msg = SubmissionMessage {
            workflow_id,
            workflow_version: workflow_version.clone(),
        };
        match publish_submission(nats, &msg).await {
            Ok(()) => published += 1,
            Err(e) => {
                eprintln!(
                    "boot reconciliation: failed to republish ({}, {}): {}",
                    workflow_id, workflow_version, e
                );
            }
        }
    }
    if total > 0 {
        println!(
            "Boot-time reconciliation: republished {}/{} orphan Ready rows onto submission queue",
            published, total
        );
    }
    Ok(published)
}
