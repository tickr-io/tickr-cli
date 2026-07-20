//! Pure workflow-definition read codec.

use anyhow::{Context, Result};

use crate::workflow as wf;

/// Encode a workflow definition for the conductor's `definition` JSONB column.
pub fn definition_proto_to_json(definition: &wf::WorkflowDefinition) -> Result<serde_json::Value> {
    serde_json::to_value(definition).context("encode proto workflow definition")
}

/// Rehydrate a stored `definition` JSONB straight to the published proto
/// message. Registration and reads use the same protobuf JSON shape.
pub fn definition_proto_from_json(definition: serde_json::Value) -> Result<wf::WorkflowDefinition> {
    serde_json::from_value(definition).context("decode stored proto workflow definition")
}
