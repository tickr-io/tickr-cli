//! Reading the stored workflow definition back into usable shapes.
//!
//! Registration persists the published `tickr.workflow` proto contract as the
//! `definition` JSONB. Every conductor read consumes that contract directly.

use anyhow::{Context, Result};
use tickr_proto::codec::definition::definition_proto_from_json;
use tickr_proto::workflow as wf;

/// Decode a stored proto-JSON definition into the published proto contract.
pub fn proto_from_stored_definition(
    definition: serde_json::Value,
) -> Result<wf::WorkflowDefinition> {
    definition_proto_from_json(definition).context("decode stored proto workflow definition")
}
