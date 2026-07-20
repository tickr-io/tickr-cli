//! Integration coverage for compaction projection persistence.
//!
//! The conductor consumes serialized values from the published archive
//! projection and persists them to PostgreSQL.

#![cfg(not(madsim))]

mod common;

use chrono::Utc;
use prost::Message;
use sqlx::Row;
use std::path::PathBuf;
use std::sync::Arc;
use tickr_conductor::system_tasks::persist_compaction_projection;
use tickr_proto::archive as ap;
use tickr_proto::instance::{AppliedPatchView, PatchOpView};
use uuid::Uuid;

fn fixture_projection(name: &str) -> ap::ArchiveProjection {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    serde_json::from_str(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read fixture {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("decode fixture {}: {error}", path.display()))
}

/// Receive a published Compaction envelope exactly as the relay does, then use
/// only its projection at the archive boundary.
fn received_projection(name: &str) -> ap::ArchiveProjection {
    let bytes = ap::CompactionEnvelope {
        projection: Some(fixture_projection(name)),
        correlation: "fixture-correlation".to_owned(),
        shipped_at: Some(Utc::now().to_rfc3339()),
    }
    .encode_to_vec();
    tickr_proto::codec::compaction::decode_envelope(&bytes)
        .expect("fixture is a valid published Compaction envelope")
        .projection
        .expect("validated envelope includes its projection")
}

fn projection_with_tasks(state: &str, task_count: usize) -> ap::ArchiveProjection {
    assert!(task_count > 0, "persistence scenarios require a task row");
    let mut projection = received_projection("terminal_union_instance.json");
    projection.id = Uuid::new_v4().to_string();
    projection.workflow_id = Uuid::new_v4().to_string();
    projection.name = format!("compaction-receiver-test-{}", projection.id);
    projection.state = state.to_owned();
    projection.scheduled_at = Some(Utc::now().to_rfc3339());
    projection.task_instances.truncate(1);
    projection.task_instances[0].id = Uuid::new_v4().to_string();
    projection.task_instances[0].task_id = Uuid::new_v4().to_string();
    while projection.task_instances.len() < task_count {
        let mut task = projection.task_instances[0].clone();
        task.id = Uuid::new_v4().to_string();
        task.task_id = Uuid::new_v4().to_string();
        projection.task_instances.push(task);
    }
    projection
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persists_workflow_instance_and_task_instances_in_one_txn(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_db, pool)) = common::test_db().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let projection = projection_with_tasks("Completed", 2);
    let wfi_id = Uuid::parse_str(&projection.id)?;

    persist_compaction_projection(&pool, &projection, Some(Utc::now()), None).await?;

    let wfi_count: i64 = sqlx::query("SELECT count(*) FROM workflow_instances WHERE id = $1")
        .bind(wfi_id)
        .fetch_one(pool.as_ref())
        .await?
        .get(0);
    assert_eq!(wfi_count, 1);

    let ti_count: i64 =
        sqlx::query("SELECT count(*) FROM task_instances WHERE workflow_instance_id = $1")
            .bind(wfi_id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    assert_eq!(ti_count, 2);

    let run_info: (serde_json::Value, serde_json::Value, serde_json::Value) = sqlx::query_as(
        "SELECT ctx_envelope, runtime_params, log_uris FROM workflow_run_info WHERE workflow_instance_id = $1",
    )
    .bind(wfi_id)
    .fetch_one(pool.as_ref())
    .await?;
    assert_eq!(run_info.0, serde_json::Value::Array(Vec::new()));
    assert_eq!(
        run_info.1.get("workflow_id"),
        Some(&serde_json::Value::String(projection.workflow_id.clone()))
    );
    let log_uris = run_info.2.as_object().expect("log_uris is an object");
    assert_eq!(log_uris.len(), 2);
    let task_id = &projection.task_instances[0].id;
    let expected_uri = format!(
        "s3://tickr-logs/task_logs/{}/{}/{}.gz",
        projection.workflow_id, projection.id, task_id
    );
    assert_eq!(
        log_uris.get(task_id).and_then(serde_json::Value::as_str),
        Some(expected_uri.as_str())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_projection_decodes_through_the_published_archive_codec(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_db, pool)) = common::test_db().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let projection = received_projection("terminal_union_instance.json");
    let id = Uuid::parse_str(&projection.id)?;

    persist_compaction_projection(&pool, &projection, Some(Utc::now()), None).await?;
    let stored: serde_json::Value =
        sqlx::query("SELECT instance FROM workflow_instances WHERE id = $1")
            .bind(id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    let decoded = tickr_proto::codec::archive::archive_projection_from_json(stored)?;

    assert_eq!(decoded, projection);
    assert!(
        decoded.runnable.is_some(),
        "fixture retains the runnable union arm"
    );
    assert_eq!(decoded.task_instances.len(), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_delivery_is_idempotent_via_on_conflict() -> Result<(), Box<dyn std::error::Error>>
{
    let Some((_db, pool)) = common::test_db().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let projection = projection_with_tasks("Failed", 1);
    let wfi_id = Uuid::parse_str(&projection.id)?;
    let ti_id = Uuid::parse_str(&projection.task_instances[0].id)?;

    persist_compaction_projection(&pool, &projection, Some(Utc::now()), None).await?;
    persist_compaction_projection(&pool, &projection, Some(Utc::now()), None).await?;

    let wfi_count: i64 = sqlx::query("SELECT count(*) FROM workflow_instances WHERE id = $1")
        .bind(wfi_id)
        .fetch_one(pool.as_ref())
        .await?
        .get(0);
    assert_eq!(wfi_count, 1);
    let ti_count: i64 = sqlx::query("SELECT count(*) FROM task_instances WHERE id = $1")
        .bind(ti_id)
        .fetch_one(pool.as_ref())
        .await?
        .get(0);
    assert_eq!(ti_count, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_archive_excludes_cluster_provenance_without_removing_graph_slot_ids(
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((_db, pool)) = common::test_db().await else {
        return Ok(());
    };
    let pool = Arc::new(pool);
    let mut projection = received_projection("terminal_union_instance.json");
    let graph_slot_id = Uuid::new_v4().to_string();
    projection.applied_patches.push(AppliedPatchView {
        patch_key: Uuid::new_v4().to_string(),
        prior_version: 0,
        version: 1,
        reason: Some("fixture graph-slot reference".to_owned()),
        provenance: "external".to_owned(),
        applied_at: Utc::now().to_rfc3339(),
        ops: vec![PatchOpView {
            op: "AddNode".to_owned(),
            node_id: Some(graph_slot_id.clone()),
            edge_id: None,
            sources: Vec::new(),
            targets: Vec::new(),
        }],
        minted_map: Default::default(),
    });
    let id = Uuid::parse_str(&projection.id)?;
    persist_compaction_projection(&pool, &projection, Some(Utc::now()), None).await?;

    const FORBIDDEN: [&str; 6] = [
        "owned",
        "tombstoned",
        "timer_id",
        "task_mapping",
        "stalled",
        "stalled_by",
    ];
    let instance_json: serde_json::Value =
        sqlx::query("SELECT instance FROM workflow_instances WHERE id = $1")
            .bind(id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    let instance_raw = serde_json::to_string(&instance_json)?;
    for forbidden in FORBIDDEN {
        assert!(
            !instance_raw.contains(forbidden),
            "stored instance contains `{forbidden}`"
        );
    }
    assert!(
        instance_raw.contains(&graph_slot_id),
        "a graph-slot node_id remains published archive data"
    );

    for table in ["workflow_instances", "task_instances"] {
        let has_cluster_node_column: Option<(String,)> = sqlx::query_as(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND column_name = 'node_id'",
        )
        .bind(table)
        .fetch_optional(pool.as_ref())
        .await?;
        assert!(
            has_cluster_node_column.is_none(),
            "{table} stores no cluster provenance"
        );
    }
    let runtime_params: serde_json::Value =
        sqlx::query("SELECT runtime_params FROM workflow_run_info WHERE workflow_instance_id = $1")
            .bind(id)
            .fetch_one(pool.as_ref())
            .await?
            .get(0);
    assert!(runtime_params.get("shipped_from_node").is_none());
    Ok(())
}
