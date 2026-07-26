//! Distributed definition-submission reconciliation.
//!
//! The NATS pointer queue only requests an earlier authoritative SQL scan.
//! Startup and timer-led leases keep `Ready -> Submitted` progress independent
//! of notification delivery and Conductor lifetime.

use crate::submission_consumer::message::SubmissionMessage;
use crate::submission_consumer::SUBMISSION_QUEUE_SUBJECT;
use anyhow::{Context, Result};
use async_nats::Client as NatsClient;

/// Publish an advisory submission pointer.
pub async fn publish_submission(nats: &NatsClient, msg: &SubmissionMessage) -> Result<()> {
    let payload = bincode::serialize(msg).context("serialize submission message")?;
    nats.publish(SUBMISSION_QUEUE_SUBJECT, payload.into())
        .await
        .context("publish to submission queue")?;
    Ok(())
}
