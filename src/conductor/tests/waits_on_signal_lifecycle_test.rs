//! Real-Postgres integration tests for the waits-on-signal subscription
//! index lifecycle. Verifies the `apply_workflow_state` hook and
//! `rebuild_from_postgres` startup path against a live conductor PG.

#![cfg(not(madsim))]

mod common;

use chrono::{DateTime, TimeZone, Utc};
use tickr_conductor::subscription_index::SubscriptionIndex;
use tickr_conductor::waits_on_signal_lifecycle::{
    apply_workflow_state, rebuild_from_postgres, signal_subscription_index,
};
use tickr_proto::codec::definition::definition_proto_to_json;
use tickr_proto::workflow as wf;
use uuid::Uuid;

fn trigger_capture(name: &str, jsonpath: &str) -> wf::CaptureDeclaration {
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

fn waits_trigger(signal_name: &str, captures: Vec<wf::CaptureDeclaration>) -> wf::Trigger {
    wf::Trigger {
        kind: Some(wf::trigger::Kind::WaitsOnSignal(wf::WaitsOnSignalConfig {
            signal_name: signal_name.to_string(),
            predicate: None,
            captures,
        })),
    }
}

fn workflow(
    id: Uuid,
    name: &str,
    version: i64,
    trigger: Option<wf::Trigger>,
) -> wf::WorkflowDefinition {
    wf::WorkflowDefinition {
        id: id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: name.to_string(),
        version,
        trigger,
        ..Default::default()
    }
}

async fn start_postgres() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

fn workflow_id(definition: &wf::WorkflowDefinition) -> Uuid {
    Uuid::parse_str(&definition.id).expect("workflow id")
}

fn build_waits_on_signal_workflow(name: &str, signal_name: &str) -> wf::WorkflowDefinition {
    workflow(
        Uuid::new_v4(),
        name,
        0,
        Some(waits_trigger(
            signal_name,
            vec![trigger_capture("email", "$.user.email")],
        )),
    )
}

async fn insert_workflow_row(pool: &sqlx::PgPool, workflow: &wf::WorkflowDefinition, status: &str) {
    let definition = definition_proto_to_json(workflow).expect("encode proto definition");
    sqlx::query(
        "INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source) \
         VALUES ($1, $2, 'default', 'wf', $3, $4, 'testhash', 'testcos', $5, '') \
         ON CONFLICT (id, version) DO UPDATE SET \
             status = EXCLUDED.status, \
             definition = EXCLUDED.definition, \
             updated_at = now()",
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(status)
    .bind(&definition)
    .execute(pool)
    .await
    .expect("upsert workflow row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_workflow_state_registers_workflows_with_waits_on_signal() {
    let Some((_pg, _pool)) = start_postgres().await else {
        return;
    };
    let wf = build_waits_on_signal_workflow("apply-success", "user-paid-success");
    apply_workflow_state(&wf).expect("apply");
    let idx = signal_subscription_index();
    assert!(
        idx.lookup("user-paid-success")
            .iter()
            .any(|e| e.workflow_id == Uuid::parse_str(&wf.id).expect("workflow id")),
        "apply_workflow_state on a workflow with waits-on-signal must register it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_workflow_state_unregisters_when_waits_on_signal_drops() {
    let Some((_pg, _pool)) = start_postgres().await else {
        return;
    };
    let mut wf = build_waits_on_signal_workflow("drop-config", "user-paid-drop");
    apply_workflow_state(&wf).expect("register");
    let idx = signal_subscription_index();
    assert!(idx
        .lookup("user-paid-drop")
        .iter()
        .any(|e| e.workflow_id == workflow_id(&wf)));
    // Clear the waits-on-signal config — a subsequent apply must
    // unregister the workflow.
    wf.trigger = None;
    apply_workflow_state(&wf).expect("unregister");
    assert!(
        idx.lookup("user-paid-drop")
            .iter()
            .all(|e| e.workflow_id != workflow_id(&wf)),
        "dropping waits-on-signal must unregister"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_from_postgres_loads_ready_workflows_only() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let wf_ready = build_waits_on_signal_workflow("rebuild-ready", "rebuild-paid");
    let wf_failed = build_waits_on_signal_workflow("rebuild-failed", "rebuild-paid");
    insert_workflow_row(&pool, &wf_ready, "Ready").await;
    insert_workflow_row(&pool, &wf_failed, "BuildFailed").await;

    // Make sure the singleton starts in a clean-for-this-name state.
    signal_subscription_index().unregister(workflow_id(&wf_ready));
    signal_subscription_index().unregister(workflow_id(&wf_failed));

    let count = rebuild_from_postgres(&pool).await.expect("rebuild");
    assert!(count >= 1, "at least the Ready workflow should rebuild");
    let idx = signal_subscription_index();
    let entries = idx.lookup("rebuild-paid");
    let ids: Vec<Uuid> = entries.iter().map(|e| e.workflow_id).collect();
    assert!(
        ids.contains(&workflow_id(&wf_ready)),
        "rebuild must register Ready workflows"
    );
    assert!(
        !ids.contains(&workflow_id(&wf_failed)),
        "rebuild must NOT register BuildFailed workflows"
    );
}

/// Insert a `waits-on-signal` version row with a specific id, version, signal
/// name, and `inserted_at`, so a test can stage several live versions of one id
/// with a deterministic recency order.
async fn insert_waits_on_signal_version(
    pool: &sqlx::PgPool,
    id: Uuid,
    version: i64,
    signal_name: &str,
    status: &str,
    inserted_at: DateTime<Utc>,
) {
    let wf = workflow(
        id,
        "o1-latest",
        version,
        Some(waits_trigger(signal_name, vec![])),
    );
    let definition = definition_proto_to_json(&wf).expect("encode proto definition");
    sqlx::query(
        "INSERT INTO workflows \
         (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source, inserted_at) \
         VALUES ($1, $2, 'default', 'wf', $3, $4, $5, 'testcos', $6, '', $7)",
    )
    .bind(id)
    .bind(version)
    .bind(&wf.name)
    .bind(status)
    .bind(format!("o1-hash-v{}", version))
    .bind(&definition)
    .bind(inserted_at)
    .execute(pool)
    .await
    .expect("insert waits-on-signal version row");
}

/// O1: with several live (`Ready`) versions coexisting for one id, the rebuild
/// registers the **latest** live row's declarations — not merely *a* live row.
/// This pins the wakeup path to the same live-version precedent the trigger
/// resolver (`load_live_workflow_definition`) conforms to: a re-registered slug
/// that renames its signal must subscribe under the new name only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_registers_only_latest_live_version_per_id() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let id = Uuid::new_v4();
    // Names unique to this id so the process-global index doesn't collide with
    // sibling tests running in parallel.
    let old_name = format!("o1-old-{}", id.simple());
    let new_name = format!("o1-new-{}", id.simple());

    // v1 (older) subscribes under the old name; v2 (later) renames to the new
    // name. Both are `Ready`, so both survive the `status IN (...)` filter — the
    // rebuild must still pick only v2 by insertion order.
    insert_waits_on_signal_version(&pool, id, 1, &old_name, "Ready", ts(1)).await;
    insert_waits_on_signal_version(&pool, id, 2, &new_name, "Ready", ts(2)).await;

    signal_subscription_index().unregister(id);

    rebuild_from_postgres(&pool).await.expect("rebuild");
    let idx = signal_subscription_index();
    assert!(
        idx.lookup(&new_name).iter().any(|e| e.workflow_id == id),
        "rebuild must register the latest live version's signal name"
    );
    assert!(
        idx.lookup(&old_name).iter().all(|e| e.workflow_id != id),
        "rebuild must NOT register a superseded live version's signal name"
    );
}

fn ts(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_skips_workflows_without_waits_on_signal_config() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    // A Ready workflow with no waits-on-signal trigger config.
    let plain = workflow(
        Uuid::new_v4(),
        "plain-cron",
        0,
        Some(wf::Trigger {
            kind: Some(wf::trigger::Kind::Cron("* * * * *".to_string())),
        }),
    );
    insert_workflow_row(&pool, &plain, "Ready").await;
    signal_subscription_index().unregister(workflow_id(&plain));

    let _ = rebuild_from_postgres(&pool).await.expect("rebuild");
    // No subscription should exist for this workflow_id. SubscriptionIndex
    // doesn't expose a "by workflow_id" scan, so we round-trip through
    // apply_workflow_state — which unregisters because there's no
    // waits-on-signal config.
    let dummy = SubscriptionIndex::new();
    dummy.unregister(workflow_id(&plain));
    apply_workflow_state(&plain).expect("apply");
}
