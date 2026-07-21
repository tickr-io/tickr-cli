//! Trigger-derived Event-variable adapter.
//!
//! SQL transaction and encoding details live in the selected data-plane
//! repository. This module only translates the conductor's typed ctx envelopes.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tickr_ctx::envelope::Envelope;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::signal_repository::{
    SignalCapturesInput, SignalCapturesRecord, SignalLinkageOutcome,
};
use uuid::Uuid;

#[derive(Debug)]
pub struct SignalCapturesRow {
    pub signal_id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_version: Option<i64>,
    pub captures: Vec<NamedEnvelope>,
    pub created_at: DateTime<Utc>,
    pub materialized_run_id: Option<Uuid>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NamedEnvelope {
    pub name: String,
    pub envelope: Envelope,
}

pub async fn insert(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
    workflow_id: Uuid,
    workflow_version: Option<i64>,
    captures: &[NamedEnvelope],
) -> Result<()> {
    let captures =
        serde_json::to_value(captures).context("serialize Trigger-derived Event variables")?;
    repositories
        .insert_signal_captures(&SignalCapturesInput {
            signal_id,
            workflow_id,
            workflow_version,
            captures,
        })
        .await
        .context("insert Trigger-derived Event variables")?;
    Ok(())
}

pub async fn read(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
) -> Result<Option<SignalCapturesRow>> {
    repositories
        .signal_captures(signal_id)
        .await
        .context("read Trigger-derived Event variables")?
        .map(decode_record)
        .transpose()
}

pub async fn mark_materialized(
    repositories: &WriterRepositoryBundle,
    signal_id: Uuid,
    workflow_instance_id: Uuid,
) -> Result<SignalLinkageOutcome> {
    repositories
        .link_signal_captures(signal_id, workflow_instance_id)
        .await
        .context("link Signal to materialized Workflow instance")
}

fn decode_record(record: SignalCapturesRecord) -> Result<SignalCapturesRow> {
    let captures = serde_json::from_value(record.captures)
        .context("decode Trigger-derived Event-variable envelopes")?;
    Ok(SignalCapturesRow {
        signal_id: record.signal_id,
        workflow_id: record.workflow_id,
        workflow_version: record.workflow_version,
        captures,
        created_at: record.created_at,
        materialized_run_id: record.materialized_run_id,
        terminal_at: record.terminal_at,
    })
}
