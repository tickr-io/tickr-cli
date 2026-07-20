//! Integration tests for the conductor replay pipeline against ephemeral PG.
//!
//! Fixtures write published union archive projections directly so replay is
//! exercised against multiple valid archive shapes.

#![cfg(not(madsim))]

mod common;

use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use tickr_conductor::replay_pipeline::{
    fetch_row, process_replay, reconcile_orphan_replay_rows, replay_instance_id, ReplayError,
    ReplayIngress, ReplayRelaySender, ReplayRequest, STATUS_MATERIALIZING, STATUS_RELEASED,
    STATUS_VERSION_UNRESOLVABLE,
};
use tickr_conductor::replay_rehydration::RehydrationPlan;
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
use tickr_proto::archive as ap;
use tickr_proto::codec::definition::definition_proto_to_json;
use tickr_proto::instance as ip;
use tickr_proto::runnable as rp;
use tickr_proto::signal as sp;
use tickr_proto::workflow as wf;

async fn start_pg() -> Option<(common::DbGuard, PgPool)> {
    common::test_db().await
}

/// Recording sender: buffers every relayed Signal and re-hydration call so a
/// test can assert the drive happened without standing up the relay or NATS.
struct CapturingReplaySender {
    signals: Mutex<Vec<sp::Signal>>,
    rehydrations: Mutex<Vec<(Uuid, RehydrationPlan)>>,
}

impl CapturingReplaySender {
    fn new() -> Self {
        Self {
            signals: Mutex::new(Vec::new()),
            rehydrations: Mutex::new(Vec::new()),
        }
    }

    async fn signals(&self) -> Vec<sp::Signal> {
        self.signals.lock().await.clone()
    }

    async fn rehydration_count(&self) -> usize {
        self.rehydrations.lock().await.len()
    }

    async fn rehydrations(&self) -> Vec<(Uuid, RehydrationPlan)> {
        self.rehydrations.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl ReplayRelaySender for CapturingReplaySender {
    async fn send(&self, signal: &sp::Signal) -> Result<()> {
        self.signals.lock().await.push(signal.clone());
        Ok(())
    }

    async fn rehydrate(&self, replay_run_id: Uuid, plan: &RehydrationPlan) -> Result<()> {
        self.rehydrations
            .lock()
            .await
            .push((replay_run_id, plan.clone()));
        Ok(())
    }
}

/// Sender whose relay is down — the drive-failure window that leaves a durable
/// `Materializing` row for the re-drive / boot-reconcile.
struct FailingReplaySender;

#[async_trait::async_trait]
impl ReplayRelaySender for FailingReplaySender {
    async fn send(&self, _signal: &sp::Signal) -> Result<()> {
        Err(anyhow::anyhow!("relay channel down"))
    }

    async fn rehydrate(&self, _replay_run_id: Uuid, _plan: &RehydrationPlan) -> Result<()> {
        Err(anyhow::anyhow!("nats down"))
    }
}

struct ArchivedFixture {
    id: Uuid,
    workflow_id: Uuid,
    a: Uuid,
    b: Uuid,
    projection: ap::ArchiveProjection,
}

fn task_definition(
    id: Uuid,
    workflow_id: Uuid,
    name: &str,
    outputs: Vec<String>,
) -> wf::TaskDefinition {
    wf::TaskDefinition {
        id: id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: name.to_string(),
        task_type: wf::TaskType::Regular as i32,
        nix_expression_path: "x".to_string(),
        outputs,
        ..Default::default()
    }
}

/// Construct the archived `start → a → b → end` runnable section directly on
/// the published proto. `a` is carried forward and `b` is the failed replay
/// frontier, so the replay is born-Stalled and must be re-hydrated then
/// released.
fn terminal_fixture(name: &str, workflow_version: i64) -> ArchivedFixture {
    let id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let start = Uuid::new_v4();
    let end = Uuid::new_v4();
    let edge = |source: Uuid, target: Uuid| rp::RunnableEdge {
        id: Uuid::new_v4().to_string(),
        sources: vec![source.to_string()],
        targets: vec![target.to_string()],
        kind: wf::EdgeKind::Control as i32,
        gates: Vec::new(),
    };
    let graph = rp::RunnableGraph {
        nodes: vec![
            rp::RunnableNode {
                id: start.to_string(),
                node_type: wf::NodeType::Start as i32,
                ground: rp::GroundState::Success as i32,
                grounded_at: None,
            },
            rp::RunnableNode {
                id: a.to_string(),
                node_type: wf::NodeType::Task as i32,
                ground: rp::GroundState::Success as i32,
                grounded_at: Some(chrono::Utc::now().to_rfc3339()),
            },
            rp::RunnableNode {
                id: b.to_string(),
                node_type: wf::NodeType::Task as i32,
                ground: rp::GroundState::Failed as i32,
                grounded_at: Some(chrono::Utc::now().to_rfc3339()),
            },
            rp::RunnableNode {
                id: end.to_string(),
                node_type: wf::NodeType::End as i32,
                ground: rp::GroundState::Pending as i32,
                grounded_at: None,
            },
        ],
        edges: vec![edge(start, a), edge(a, b), edge(b, end)],
        start: start.to_string(),
        end: end.to_string(),
        head: Vec::new(),
        tail: String::new(),
    };
    let projection = ap::ArchiveProjection {
        runnable: Some(rp::RunnableProjection {
            tasks: vec![
                task_definition(a, workflow_id, "a", Vec::new()),
                task_definition(b, workflow_id, "b", Vec::new()),
            ],
            graph: Some(graph),
            workflow_version,
        }),
        id: id.to_string(),
        workflow_id: workflow_id.to_string(),
        name: name.to_string(),
        workflow_name: "replay-fixture".to_string(),
        workflow_version,
        state: "Failed".to_string(),
        ..Default::default()
    };
    ArchivedFixture {
        id,
        workflow_id,
        a,
        b,
        projection,
    }
}

/// A second valid replayable projection used to broaden archive corpus coverage.
fn secondary_archive_fixture() -> ArchivedFixture {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/secondary_union_instance.json");
    let projection: ap::ArchiveProjection = serde_json::from_str(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("decode {}: {error}", path.display()));
    let runnable = projection.runnable.as_ref().expect("runnable section");
    let task = runnable.tasks.first().expect("fixture task");
    ArchivedFixture {
        id: Uuid::parse_str(&projection.id).expect("fixture instance id"),
        workflow_id: Uuid::parse_str(&projection.workflow_id).expect("fixture workflow id"),
        a: Uuid::parse_str(&task.id).expect("fixture task id"),
        b: Uuid::parse_str(&task.id).expect("fixture task id"),
        projection,
    }
}

async fn insert_archived_run(
    pool: &PgPool,
    fixture: &ArchivedFixture,
    ctx_envelope: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO workflow_instances (id, workflow_id, name, state, instance)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(fixture.id)
    .bind(fixture.workflow_id)
    .bind(&fixture.projection.name)
    .bind(&fixture.projection.state)
    .bind(serde_json::to_value(&fixture.projection)?)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO workflow_run_info (workflow_instance_id, ctx_envelope)
         VALUES ($1, $2)",
    )
    .bind(fixture.id)
    .bind(ctx_envelope)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_task_instance(
    pool: &PgPool,
    fixture: &ArchivedFixture,
    task_instance_id: Uuid,
    task_id: Uuid,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO task_instances
            (id, workflow_instance_id, workflow_id, task_id, name, state, task_instance, attempt)
         VALUES ($1, $2, $3, $4, 'a', 'Completed', '{}'::jsonb, 0)",
    )
    .bind(task_instance_id)
    .bind(fixture.id)
    .bind(fixture.workflow_id)
    .bind(task_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn request(source_instance_id: Uuid, key: Option<&str>) -> ReplayRequest {
    ReplayRequest {
        source_instance_id,
        resume_from: None,
        name: None,
        idempotency_key: key.map(str::to_string),
        inputs: HashMap::new(),
    }
}

/// Happy path: ingress mints the seed from the union archive projection, opens
/// the row under the deterministic id, then drives born-Stall re-hydration and
/// release.
#[tokio::test]
async fn ingress_persists_row_with_seed_sha256_and_drives_release() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("replay-pipeline-fixture", 0);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .expect("insert union archive");

    let sender = CapturingReplaySender::new();
    let outcome = process_replay(&pool, &sender, request(fixture.id, Some("key-1")))
        .await
        .expect("ingress");
    let replay_id = match outcome {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected Accepted, got {other:?}"),
    };

    let row = fetch_row(&pool, replay_id)
        .await
        .expect("read row")
        .expect("row present");
    assert_eq!(row.status, STATUS_RELEASED, "drive settled the row");
    assert_eq!(row.source_instance_id, fixture.id);
    let sha = row.seed_sha256.expect("seed_sha256 witness present");
    assert_eq!(sha.len(), 64, "sha256 is 64 hex chars");
    assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(
        sender.signals().await.len(),
        2,
        "Trigger then Resume relayed"
    );
    assert_eq!(sender.rehydration_count().await, 1, "ctx re-hydrated once");
}

#[tokio::test]
async fn secondary_union_projection_replays_through_the_archive_read() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = secondary_archive_fixture();
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .expect("insert secondary union projection");

    assert!(matches!(
        process_replay(
            &pool,
            &CapturingReplaySender::new(),
            request(fixture.id, Some("secondary"))
        )
        .await,
        Ok(ReplayIngress::Accepted { .. })
    ));
}

#[tokio::test]
async fn blob_absent_source_parks_version_unresolvable() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let sender = CapturingReplaySender::new();
    let outcome = process_replay(&pool, &sender, request(Uuid::new_v4(), None))
        .await
        .expect("ingress");
    let replay_id = match outcome {
        ReplayIngress::VersionUnresolvable { replay_instance_id } => replay_instance_id,
        other => panic!("expected VersionUnresolvable, got {other:?}"),
    };
    let row = fetch_row(&pool, replay_id).await.unwrap().unwrap();
    assert_eq!(row.status, STATUS_VERSION_UNRESOLVABLE);
    assert!(row.seed_sha256.is_none());
    assert!(sender.signals().await.is_empty());
}

#[tokio::test]
async fn idempotency_collision_returns_existing_replay_id() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("dedup", 0);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    let sender = CapturingReplaySender::new();
    let first = process_replay(&pool, &sender, request(fixture.id, Some("dup")))
        .await
        .unwrap();
    let first_id = match first {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected Accepted, got {other:?}"),
    };
    let second = process_replay(&pool, &sender, request(fixture.id, Some("dup")))
        .await
        .unwrap();
    assert_eq!(
        second,
        ReplayIngress::Deduplicated {
            replay_instance_id: first_id
        }
    );
}

#[tokio::test]
async fn omitted_key_mints_fresh_replay_every_post() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("unkeyed", 0);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    let sender = CapturingReplaySender::new();
    let ids = [
        process_replay(&pool, &sender, request(fixture.id, None))
            .await
            .unwrap(),
        process_replay(&pool, &sender, request(fixture.id, None))
            .await
            .unwrap(),
    ]
    .map(|outcome| match outcome {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected Accepted, got {other:?}"),
    });
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn boot_reconcile_redrives_unsettled_row() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("reconcile", 0);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    let outcome = process_replay(
        &pool,
        &FailingReplaySender,
        request(fixture.id, Some("retry")),
    )
    .await
    .unwrap();
    let replay_id = match outcome {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected Accepted, got {other:?}"),
    };
    let row = fetch_row(&pool, replay_id).await.unwrap().unwrap();
    assert_eq!(row.status, STATUS_MATERIALIZING);
    assert_eq!(replay_id, replay_instance_id(fixture.id, row.signal_id));

    let sender = CapturingReplaySender::new();
    assert_eq!(
        reconcile_orphan_replay_rows(&pool, &sender).await.unwrap(),
        1
    );
    assert_eq!(
        fetch_row(&pool, replay_id).await.unwrap().unwrap().status,
        STATUS_RELEASED
    );
}

fn capture(name: &str, jsonpath: &str) -> wf::CaptureDeclaration {
    wf::CaptureDeclaration {
        name: name.to_string(),
        from: Some(wf::CaptureSource {
            source: Some(wf::capture_source::Source::Trigger(
                wf::capture_source::Trigger {
                    jsonpath: jsonpath.to_string(),
                },
            )),
        }),
    }
}

fn definition_for(
    fixture: &ArchivedFixture,
    version: i64,
    captures: Vec<wf::CaptureDeclaration>,
    outputs: Vec<String>,
) -> wf::WorkflowDefinition {
    wf::WorkflowDefinition {
        id: fixture.workflow_id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "replay-fixture".to_string(),
        name: fixture.projection.workflow_name.clone(),
        version,
        tasks: vec![
            task_definition(fixture.a, fixture.workflow_id, "a", outputs),
            task_definition(fixture.b, fixture.workflow_id, "b", Vec::new()),
        ],
        captures,
        status: wf::WorkflowStatus::Inactive as i32,
        ..Default::default()
    }
}

async fn insert_workflow_definition(
    pool: &PgPool,
    definition: &wf::WorkflowDefinition,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO workflows
            (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
         VALUES ($1, $2, $3, $4, $5, 'Building', $6, $7, $8, '')",
    )
    .bind(Uuid::parse_str(&definition.id)?)
    .bind(definition.version)
    .bind(&definition.namespace)
    .bind(&definition.slug)
    .bind(&definition.name)
    .bind(format!("hash-{}", definition.version))
    .bind(format!("cos-{}", definition.version))
    .bind(definition_proto_to_json(definition)?)
    .execute(pool)
    .await?;
    Ok(())
}

fn shadow(key: &str, value: serde_json::Value) -> HashMap<String, serde_json::Value> {
    [(key.to_string(), value)].into()
}

#[tokio::test]
async fn inputs_shadows_declared_capture_and_audits_name_only() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("shadow", 1);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    insert_workflow_definition(
        &pool,
        &definition_for(
            &fixture,
            1,
            vec![capture("api_credential", "$.cred")],
            vec!["build_digest".to_string()],
        ),
    )
    .await
    .unwrap();

    let sender = CapturingReplaySender::new();
    let outcome = process_replay(
        &pool,
        &sender,
        ReplayRequest {
            inputs: shadow("api_credential", serde_json::json!("fresh-token")),
            ..request(fixture.id, Some("shadow-1"))
        },
    )
    .await
    .unwrap();
    let replay_id = match outcome {
        ReplayIngress::Accepted {
            replay_instance_id, ..
        } => replay_instance_id,
        other => panic!("expected Accepted, got {other:?}"),
    };
    assert_eq!(
        fetch_row(&pool, replay_id)
            .await
            .unwrap()
            .unwrap()
            .shadowed_keys,
        vec!["api_credential"]
    );
    let plan = &sender.rehydrations().await[0].1;
    assert_eq!(plan.shadowed.len(), 1);
    let envelope: Envelope = serde_json::from_slice(&plan.shadowed[0].bytes).unwrap();
    assert!(matches!(envelope.producer, Producer::Signal { .. }));
    assert_eq!(envelope.value, serde_json::json!("fresh-token"));
}

#[tokio::test]
async fn inputs_undeclared_or_task_produced_keys_typed_reject() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("shadow-reject", 1);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    insert_workflow_definition(
        &pool,
        &definition_for(
            &fixture,
            1,
            vec![capture("api_credential", "$.cred")],
            vec!["build_digest".to_string()],
        ),
    )
    .await
    .unwrap();
    let sender = CapturingReplaySender::new();

    for (key, expected_task_output) in [("not_a_capture", false), ("build_digest", true)] {
        let error = process_replay(
            &pool,
            &sender,
            ReplayRequest {
                inputs: shadow(key, serde_json::json!("forged")),
                ..request(fixture.id, Some(key))
            },
        )
        .await
        .expect_err("invalid shadow key rejects");
        if expected_task_output {
            assert!(
                matches!(error, ReplayError::ShadowTaskProduced { key: actual } if actual == key)
            );
        } else {
            assert!(
                matches!(error, ReplayError::ShadowUndeclared { key: actual } if actual == key)
            );
        }
    }
}

#[tokio::test]
async fn shadow_validates_against_archived_version_not_latest() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let fixture = terminal_fixture("version-pinned", 1);
    insert_archived_run(&pool, &fixture, serde_json::json!([]))
        .await
        .unwrap();
    insert_workflow_definition(
        &pool,
        &definition_for(&fixture, 1, vec![capture("cred", "$.cred")], vec![]),
    )
    .await
    .unwrap();
    insert_workflow_definition(
        &pool,
        &definition_for(&fixture, 2, vec![capture("rotated", "$.rotated")], vec![]),
    )
    .await
    .unwrap();
    let sender = CapturingReplaySender::new();

    assert!(matches!(
        process_replay(
            &pool,
            &sender,
            ReplayRequest {
                inputs: shadow("cred", serde_json::json!("refreshed")),
                ..request(fixture.id, Some("v1-cred"))
            }
        )
        .await,
        Ok(ReplayIngress::Accepted { .. })
    ));
    assert!(matches!(
        process_replay(
            &pool,
            &sender,
            ReplayRequest {
                inputs: shadow("rotated", serde_json::json!("x")),
                ..request(fixture.id, Some("v2-only"))
            }
        )
        .await,
        Err(ReplayError::ShadowUndeclared { key }) if key == "rotated"
    ));
}

/// A chained replay resolves an archived task producer through the parent run's
/// task-instance row. Both generations are union projections; no aggregate
/// supplies the old task-mapping shortcut.
#[tokio::test]
async fn chained_replay_rehydrates_through_union_archive_rows() {
    let Some((_guard, pool)) = start_pg().await else {
        return;
    };
    let parent = terminal_fixture("chain-parent", 0);
    let parent_task_instance = Uuid::new_v4();
    insert_archived_run(&pool, &parent, serde_json::json!([]))
        .await
        .unwrap();
    insert_task_instance(&pool, &parent, parent_task_instance, parent.a)
        .await
        .unwrap();

    let mut child = terminal_fixture("chain-child", 0);
    // Replays retain the workflow's graph-slot ids. Make the second archived
    // generation use the parent's published task identities before inserting it.
    let old_a = child.a;
    let old_b = child.b;
    child.workflow_id = parent.workflow_id;
    child.a = parent.a;
    child.b = parent.b;
    child.projection.workflow_id = parent.workflow_id.to_string();
    let runnable = child
        .projection
        .runnable
        .as_mut()
        .expect("runnable section");
    for task in &mut runnable.tasks {
        task.workflow_id = parent.workflow_id.to_string();
        if task.id == old_a.to_string() {
            task.id = parent.a.to_string();
        } else if task.id == old_b.to_string() {
            task.id = parent.b.to_string();
        }
    }
    for node in &mut runnable.graph.as_mut().expect("runnable graph").nodes {
        if node.id == old_a.to_string() {
            node.id = parent.a.to_string();
        } else if node.id == old_b.to_string() {
            node.id = parent.b.to_string();
        }
    }
    for edge in &mut runnable.graph.as_mut().expect("runnable graph").edges {
        for id in edge.sources.iter_mut().chain(edge.targets.iter_mut()) {
            if *id == old_a.to_string() {
                *id = parent.a.to_string();
            } else if *id == old_b.to_string() {
                *id = parent.b.to_string();
            }
        }
    }
    child.projection.triggered_by = Some(ip::TriggerProvenanceView {
        kind: "Replay".to_string(),
        source_instance: Some(ip::IdentityRef {
            id: parent.id.to_string(),
            code: String::new(),
        }),
        ..Default::default()
    });
    let output = Envelope::new(
        "string",
        serde_json::json!("from-parent"),
        false,
        Producer::Task {
            task_id: parent_task_instance.to_string(),
            task_name: "a".to_string(),
        },
    );
    let child_prefix = sanitize_segment(&child.id.to_string());
    insert_archived_run(
        &pool,
        &child,
        serde_json::json!([
            { "key": format!("{child_prefix}/out_a"), "envelope": output },
            { "key": format!("{child_prefix}/tickr_replay/hydrated"), "envelope": {} }
        ]),
    )
    .await
    .unwrap();

    let sender = CapturingReplaySender::new();
    assert!(matches!(
        process_replay(&pool, &sender, request(child.id, Some("chain"))).await,
        Ok(ReplayIngress::Accepted { .. })
    ));
    let plan = &sender.rehydrations().await[0].1;
    assert_eq!(plan.carried.len(), 1);
    assert_eq!(plan.carried[0].name, "out_a");
}
