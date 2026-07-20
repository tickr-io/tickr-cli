//! Compaction envelope and acknowledgement codecs.

use anyhow::{Context, Result};
use prost::Message;

use crate::archive::{CompactionAck, CompactionEnvelope};

/// Decode a compaction egress payload. Returns the envelope only when it carries
/// a projection — a byte string that prost happens to accept but that has no
/// projection is not a valid compaction payload.
pub fn decode_envelope(bytes: &[u8]) -> Result<CompactionEnvelope> {
    let envelope = CompactionEnvelope::decode(bytes).context("decode compaction envelope proto")?;
    if envelope.projection.is_none() {
        anyhow::bail!("compaction envelope proto carries no projection");
    }
    Ok(envelope)
}

/// Proto-encode the acknowledgement, echoing the envelope's opaque correlation.
pub fn encode_ack(workflow_instance_id: String, correlation: String) -> Vec<u8> {
    CompactionAck {
        workflow_instance_id,
        correlation,
    }
    .encode_to_vec()
}

/// Decode the acknowledgement proto.
pub fn decode_ack(bytes: &[u8]) -> Result<CompactionAck> {
    CompactionAck::decode(bytes).context("decode compaction ack proto")
}
