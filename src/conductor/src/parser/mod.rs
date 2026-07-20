pub mod builder;
pub mod duration;
pub mod nickel;
pub mod types;

use anyhow::Result;
use tickr_proto::workflow as wf;
use tickr_proto::TenantId;

/// The main `Parser` struct that exposes the parsing functionality for the conductor
pub struct Parser;
use builder::parse_workflow_from_json;

impl Parser {
    /// Evaluates a Nickel source string and parses the resulting JSON into a
    /// protobuf `WorkflowDefinition`, the runtime's canonical authored model.
    /// `tenant` is the runtime identity threaded in from the caller (the
    /// conductor reads it from its environment); it becomes the leading identity
    /// segment so the same `namespace.slug` under two tenants stays distinct.
    /// `namespace` is supplied at registration (not in the source); it qualifies
    /// the author-written slug. An empty `namespace` normalises to `default`
    /// inside the builder.
    pub async fn parse_workflow(
        nickel_source: &str,
        tenant: TenantId,
        namespace: &str,
    ) -> Result<wf::WorkflowDefinition> {
        // Evaluate the Nickel source to get JSON
        let json_str = nickel::nickel_eval(nickel_source).await?;

        // Parse the JSON into the proto workflow-definition contract.
        builder::parse_workflow_from_json_for_tenant(&json_str, tenant, namespace).await
    }

    pub async fn parse_workflow_from_json(
        json_str: &str,
        namespace: &str,
    ) -> Result<wf::WorkflowDefinition> {
        parse_workflow_from_json(json_str, namespace).await
    }
}
