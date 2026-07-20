use std::collections::BTreeSet;

const EXPECTED_OPERATIONS: &[(&str, &str)] = &[
    ("/", "get"),
    ("/health", "get"),
    ("/api/health", "get"),
    ("/api/workflows", "get"),
    ("/api/workflows/register", "post"),
    ("/api/workflows/{workflow_id}", "get"),
    ("/api/workflows/{workflow_id}/trigger", "post"),
    ("/api/workflows/instances/{id}/patch", "post"),
    ("/api/workflows/instances/{id}/replay", "post"),
    ("/api/workflows/instances/{id}/replays", "get"),
    ("/api/patches/{patch_id}", "get"),
    ("/api/patches/{patch_id}/source", "get"),
    ("/api/signals/cancel", "post"),
    ("/api/workflows/instances/{id}/cancel", "post"),
    ("/api/workflows/instances/{id}/tasks/{task_id}/cancel", "post"),
    ("/api/signals/wakeup", "post"),
    ("/api/signals/{signal_id}", "get"),
    ("/api/workflows/{id}/instances", "get"),
    ("/api/workflows/{id}/calendar", "get"),
    ("/api/workflows/instances/{id}", "get"),
    ("/api/workflows/instances/{id}/tasks", "get"),
    ("/api/workflows/instances/{id}/context", "get"),
    ("/api/workflows/instances/{id}/events", "get"),
    ("/api/workflows/instances/{id}/tasks/{task_id}/events", "get"),
    ("/api/tenant", "get"),
    ("/api/dashboard/clock", "get"),
    ("/api/dashboard/upcoming", "get"),
    ("/api/events", "get"),
    ("/api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_instance_id}/logs", "get"),
];

#[test]
fn generated_openapi_is_deterministic_and_committed() {
    let first = tickr_api::http::routes::openapi_yaml().expect("generate OpenAPI");
    let second = tickr_api::http::routes::openapi_yaml().expect("regenerate OpenAPI");
    assert_eq!(first, second, "generation must be byte deterministic");

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../console/openapi.yaml");
    let committed = std::fs::read_to_string(path).expect("read committed OpenAPI");
    assert_eq!(committed, first, "run `just generate-openapi`");
}

#[test]
fn generated_openapi_covers_every_runtime_operation() {
    let document = tickr_api::http::routes::openapi_document();
    let actual: BTreeSet<_> = document
        .paths
        .paths
        .iter()
        .flat_map(|(path, item)| {
            let mut methods = Vec::new();
            if item.get.is_some() {
                methods.push((path.as_str(), "get"));
            }
            if item.post.is_some() {
                methods.push((path.as_str(), "post"));
            }
            methods
        })
        .collect();
    let expected: BTreeSet<_> = EXPECTED_OPERATIONS.iter().copied().collect();
    assert_eq!(actual, expected);
}

#[test]
fn register_and_patch_success_schemas_are_closed_and_status_specific() {
    let document = serde_json::to_value(tickr_api::http::routes::openapi_document())
        .expect("OpenAPI serializes");

    let response_ref = |path: &str, method: &str, status: &str| {
        document
            .pointer(&format!(
                "/paths/{}/{method}/responses/{status}/content/application~1json/schema/$ref",
                path.replace('~', "~0").replace('/', "~1")
            ))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };

    assert_eq!(
        response_ref("/api/workflows/register", "post", "200").as_deref(),
        Some("#/components/schemas/RegisterSettledResponse")
    );
    assert_eq!(
        response_ref("/api/workflows/register", "post", "202").as_deref(),
        Some("#/components/schemas/RegisterQueuedResponse")
    );
    assert_eq!(
        response_ref("/api/workflows/instances/{id}/patch", "post", "202").as_deref(),
        Some("#/components/schemas/PatchAcceptedResponse")
    );
    assert_eq!(
        response_ref("/api/workflows/instances/{id}/patch", "post", "409").as_deref(),
        Some("#/components/schemas/PatchRejectedResponse")
    );
    assert_eq!(
        response_ref("/api/patches/{patch_id}", "get", "200").as_deref(),
        Some("#/components/schemas/PatchStatusResponse")
    );

    let schema = |name: &str| {
        document
            .pointer(&format!("/components/schemas/{name}"))
            .unwrap_or_else(|| panic!("missing schema {name}"))
    };
    assert_eq!(
        schema("RegisterSettledStatus")["enum"],
        serde_json::json!(["NoOp", "Refreshed"])
    );
    assert_eq!(
        schema("RegisterQueuedStatus")["enum"],
        serde_json::json!(["Building", "BuildRequeued"])
    );
    assert_eq!(
        schema("PatchAcceptedStatus")["enum"],
        serde_json::json!(["accepted"])
    );
    assert_eq!(
        schema("PatchRejectedStatus")["enum"],
        serde_json::json!(["rejected"])
    );
    assert_eq!(
        schema("PatchLifecycleStatus")["enum"],
        serde_json::json!([
            "Validating",
            "Building",
            "Submitted",
            "Applied",
            "Rejected",
            "BuildFailed"
        ])
    );

    let required: BTreeSet<_> = schema("PatchStatusResponse")["required"]
        .as_array()
        .expect("PatchStatusResponse.required")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    assert_eq!(
        required,
        BTreeSet::from([
            "applied_version",
            "outcome",
            "patch_id",
            "reason",
            "status",
            "updated_at",
            "workflow_instance_id",
        ]),
        "nullable Patch status fields are present, not optional"
    );
}
