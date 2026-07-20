//! Shared fixtures for archive-read tests.
//!
//! The union instance blob is captured from the compaction drain's serialized
//! output. Task-row fixtures derive from its embedded task-instance projection.

#![allow(dead_code)]

use serde_json::{json, Value};
use tickr_proto::instance as ip;
use uuid::Uuid;

const TERMINAL_INSTANCE_FIXTURE: &str =
    include_str!("../../../conductor/tests/fixtures/terminal_union_instance.json");

/// A captured terminal-instance blob with its top-level identity/lifecycle
/// fields overridden, so one real fixture can seed several distinct archived
/// runs. The nested definition/graph/history is the drain's real output.
pub fn instance_blob(
    id: Uuid,
    workflow_id: Uuid,
    state: &str,
    scheduled_at: Option<&str>,
) -> Value {
    let mut v: Value = serde_json::from_str(TERMINAL_INSTANCE_FIXTURE).expect("fixture parses");
    let obj = v.as_object_mut().unwrap();
    obj.insert("id".into(), json!(id.to_string()));
    obj.insert("workflow_id".into(), json!(workflow_id.to_string()));
    obj.insert("state".into(), json!(state));
    obj.insert(
        "scheduled_at".into(),
        scheduled_at.map(|s| json!(s)).unwrap_or(Value::Null),
    );
    v
}

/// The captured terminal task-instance blob, rebound to a given owning instance
/// (its `id` and `task_id` stay the fixture's, so the owning instance's minted
/// set / graph node still reference it). Used by the instance-detail path.
pub fn task_blob_rebound(workflow_instance_id: Uuid, workflow_id: Uuid) -> Value {
    let fixture: Value = serde_json::from_str(TERMINAL_INSTANCE_FIXTURE).expect("fixture parses");
    let mut v = fixture["task_instances"]
        .as_array()
        .and_then(|tasks| tasks.first())
        .cloned()
        .expect("terminal union carries a task instance");
    let obj = v.as_object_mut().unwrap();
    obj.insert(
        "workflow_instance_id".into(),
        json!(workflow_instance_id.to_string()),
    );
    obj.insert("workflow_id".into(), json!(workflow_id.to_string()));
    v
}

/// A captured task-instance blob with its identity/name/state overridden, so a
/// test can insert several distinct archived task rows under one instance.
pub fn task_blob_new(
    id: Uuid,
    task_id: Uuid,
    workflow_instance_id: Uuid,
    workflow_id: Uuid,
    name: &str,
    state: &str,
) -> Value {
    let mut v = task_blob_rebound(workflow_instance_id, workflow_id);
    let obj = v.as_object_mut().unwrap();
    obj.insert("id".into(), json!(id.to_string()));
    obj.insert("task_id".into(), json!(task_id.to_string()));
    obj.insert("name".into(), json!(name));
    obj.insert("state".into(), json!(state));
    v
}

/// A `SnapshotTaskInstance` JSON entry as the union projection embeds it,
/// serialized from the real proto so the union blob deserializes cleanly. The
/// per-instance task list reads these embedded records off the one stored shape.
pub fn embedded_task(id: Uuid, task_id: Uuid, name: &str, state: &str) -> Value {
    serde_json::to_value(ip::SnapshotTaskInstance {
        id: id.to_string(),
        task_id: task_id.to_string(),
        name: name.to_string(),
        task_type: "RegularTask".to_string(),
        state: state.to_string(),
        executor_id: None,
        attempt: 0,
        started_at: None,
        completed_at: None,
        cancel_reason: None,
        kill_confirmation: None,
        transitions: Vec::new(),
    })
    .expect("serialize embedded task-instance record")
}

/// The captured union instance blob with a specific set of embedded
/// task-instance records — the union carries its task list, so the per-instance
/// task read sources from this one stored shape.
pub fn instance_blob_with_tasks(
    id: Uuid,
    workflow_id: Uuid,
    state: &str,
    tasks: Vec<Value>,
) -> Value {
    let mut v = instance_blob(id, workflow_id, state, None);
    v.as_object_mut()
        .unwrap()
        .insert("task_instances".into(), Value::Array(tasks));
    v
}

/// Insert one archived instance blob into `workflow_instances`. `state` and
/// `scheduled_at` are read from the blob so the top-level columns agree with it.
pub async fn insert_instance(pool: &sqlx::PgPool, blob: &Value) {
    let id = Uuid::parse_str(blob["id"].as_str().unwrap()).unwrap();
    let workflow_id = Uuid::parse_str(blob["workflow_id"].as_str().unwrap()).unwrap();
    let state = blob["state"].as_str().unwrap().to_string();
    let scheduled_at: Option<chrono::DateTime<chrono::Utc>> = blob["scheduled_at"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));
    sqlx::query(
        r#"
        INSERT INTO workflow_instances
            (id, workflow_id, name, state, scheduled_at, instance)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(id)
    .bind(workflow_id)
    .bind(blob["name"].as_str().unwrap_or(""))
    .bind(&state)
    .bind(scheduled_at)
    .bind(blob)
    .execute(pool)
    .await
    .expect("insert archived workflow instance");
}

/// Insert one archived task-instance blob into `task_instances`.
pub async fn insert_task(pool: &sqlx::PgPool, blob: &Value) {
    let id = Uuid::parse_str(blob["id"].as_str().unwrap()).unwrap();
    let wi_id = Uuid::parse_str(blob["workflow_instance_id"].as_str().unwrap()).unwrap();
    let workflow_id = Uuid::parse_str(blob["workflow_id"].as_str().unwrap()).unwrap();
    let task_id = Uuid::parse_str(blob["task_id"].as_str().unwrap()).unwrap();
    let state = blob["state"].as_str().unwrap().to_string();
    let attempt = blob["attempt"].as_i64().unwrap_or(0);
    sqlx::query(
        r#"
        INSERT INTO task_instances
            (id, workflow_instance_id, workflow_id, task_id,
             name, state, task_instance, attempt)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(id)
    .bind(wi_id)
    .bind(workflow_id)
    .bind(task_id)
    .bind(blob["name"].as_str().unwrap_or(""))
    .bind(&state)
    .bind(blob)
    .bind(attempt)
    .execute(pool)
    .await
    .expect("insert archived task instance");
}
