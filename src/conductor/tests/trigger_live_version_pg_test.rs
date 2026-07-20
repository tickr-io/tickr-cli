//! Real-Postgres integration tests for the conductor's trigger-time
//! definition resolver (`load_live_workflow_definition`).
//!
//! The regression: a slug re-registered with a **renamed** trigger capture as a
//! new live version must extract under the new name on the next trigger,
//! identically to a first registration. Before the fix the extraction loaded a
//! version-blind `WHERE id = $1` row, so registration count silently decided
//! which declarations a trigger extracted against.
//!
//! Both trigger ingresses (the API command-bus path and the NATS external-signal
//! path) reach this resolver through `trigger_pipeline::process_trigger`, so
//! exercising the shared resolver here covers both.
//!
//! Requires the shared Postgres (see `common`); skipped when it is unreachable.

#![cfg(not(madsim))]

mod common;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use tickr_conductor::captures_extractor::extract_captures;
use tickr_conductor::trigger_pipeline::load_live_workflow_definition;
use tickr_ctx::envelope::SignalSource;
use tickr_proto::workflow::{
    capture_source, CaptureDeclaration, CaptureSource, WorkflowDefinition,
};
use uuid::Uuid;

/// Build a workflow carrying a single trigger capture `name` reading `jsonpath`.
/// A re-registration that renames the capture is modelled as the same id, a new
/// version, and a different capture name.
fn workflow_with_trigger_capture(id: Uuid, version: i64, capture_name: &str) -> WorkflowDefinition {
    WorkflowDefinition {
        id: id.to_string(),
        tenant_id: tickr_proto::TenantId::from_slug("test").to_string(),
        namespace: "default".to_string(),
        slug: "wf".to_string(),
        name: "live-version-wf".to_string(),
        version,
        captures: vec![CaptureDeclaration {
            name: capture_name.to_string(),
            from: Some(CaptureSource {
                source: Some(capture_source::Source::Trigger(capture_source::Trigger {
                    jsonpath: "$.who".to_string(),
                })),
            }),
        }],
        ..Default::default()
    }
}

/// Insert one immutable version row with an explicit `inserted_at` so the
/// live-version ordering (`ORDER BY inserted_at DESC`) is deterministic in the
/// test rather than racing two `now()` defaults into the same microsecond.
async fn insert_version(
    pool: &sqlx::PgPool,
    workflow: &WorkflowDefinition,
    status: &str,
    inserted_at: DateTime<Utc>,
) {
    let definition = serde_json::to_value(workflow).expect("serialize workflow definition");
    sqlx::query(
        "INSERT INTO workflows \
         (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source, inserted_at) \
         VALUES ($1, $2, 'default', 'wf', $3, $4, $5, 'testcos', $6, '', $7)",
    )
    .bind(Uuid::parse_str(&workflow.id).expect("workflow id"))
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(status)
    // Distinct content hash per version keeps the rows honest; the resolver
    // never keys on it, but reusing one value across versions would misrepresent
    // the multi-version shape this test depends on.
    .bind(format!("hash-v{}", workflow.version))
    .bind(&definition)
    .bind(inserted_at)
    .execute(pool)
    .await
    .expect("insert workflow version row");
}

fn at(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + secs, 0).single().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renamed_trigger_capture_extracts_new_name_on_next_trigger() {
    let Some((_db, pool)) = common::test_db().await else {
        return;
    };

    let id = Uuid::new_v4();
    // v1 declared `candidate`; the slug is re-registered as v2 with the capture
    // renamed to `spec`. Both versions are live (`Ready`).
    let v1 = workflow_with_trigger_capture(id, 1, "candidate");
    let v2 = workflow_with_trigger_capture(id, 2, "spec");
    insert_version(&pool, &v1, "Ready", at(1)).await;
    insert_version(&pool, &v2, "Ready", at(2)).await;

    let resolved = load_live_workflow_definition(&pool, id)
        .await
        .expect("resolver query")
        .expect("a live version exists");
    assert_eq!(
        resolved.version, 2,
        "the latest live version must be resolved, not an arbitrary row"
    );

    // Extraction against the resolved declarations must populate the NEW name
    // and no longer produce the old one.
    let payload = json!({ "who": "alice" });
    let extracted = extract_captures(
        &payload,
        &resolved.captures,
        Uuid::new_v4(),
        SignalSource::Manual,
    )
    .expect("extract captures");

    let names: Vec<&str> = extracted.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["spec"],
        "the renamed capture must extract under the new name only"
    );
    let spec = extracted
        .iter()
        .find(|e| e.name == "spec")
        .expect("spec envelope present");
    assert!(spec.envelope.present, "the new capture must be populated");
    assert_eq!(spec.envelope.value, json!("alice"));
    assert!(
        !names.contains(&"candidate"),
        "the old version's capture name must not survive a re-registration"
    );

    // Persist the capture row stamped with the version the resolver landed on,
    // then read the stamped column straight back. The diagnostic signal is the
    // stored version itself, not something inferred from a downstream run: a
    // future version/Event-variable mismatch must be visible in this column.
    let signal_id = Uuid::new_v4();
    let row_envelopes: Vec<tickr_conductor::signal_captures::NamedEnvelope> = extracted
        .iter()
        .map(|e| tickr_conductor::signal_captures::NamedEnvelope {
            name: e.name.clone(),
            envelope: e.envelope.clone(),
        })
        .collect();
    tickr_conductor::signal_captures::insert(
        &pool,
        signal_id,
        id,
        Some(resolved.version),
        &row_envelopes,
    )
    .await
    .expect("insert signal_captures row");

    let (stamped,): (Option<i64>,) =
        sqlx::query_as("SELECT workflow_version FROM signal_captures WHERE signal_id = $1")
            .bind(signal_id)
            .fetch_one(&pool)
            .await
            .expect("read stamped workflow_version");
    assert_eq!(
        stamped,
        Some(resolved.version),
        "the persisted capture row must stamp the live version the extraction resolved"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn building_version_above_live_ready_is_not_selected() {
    let Some((_db, pool)) = common::test_db().await else {
        return;
    };

    let id = Uuid::new_v4();
    // A higher version, inserted later, is still `Building` above the live
    // `Ready` one. A naive `MAX(version)` / `ORDER BY version DESC` would pick
    // the mid-build declarations against the server's live graph — the same
    // desync, triggered by a build in flight. The live-version resolver skips it.
    let live = workflow_with_trigger_capture(id, 1, "candidate");
    let building = workflow_with_trigger_capture(id, 2, "spec");
    insert_version(&pool, &live, "Ready", at(1)).await;
    insert_version(&pool, &building, "Building", at(2)).await;

    let resolved = load_live_workflow_definition(&pool, id)
        .await
        .expect("resolver query")
        .expect("a live version exists");
    assert_eq!(
        resolved.version, 1,
        "the live Ready version must win over a higher mid-Building one"
    );
    let names: Vec<&str> = resolved.captures.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["candidate"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_live_version_resolves_to_none() {
    let Some((_db, pool)) = common::test_db().await else {
        return;
    };

    let id = Uuid::new_v4();
    // Only a `Building` row exists: the server holds no runnable graph for the
    // id, so there is deliberately no latest-inserted fallback.
    let building = workflow_with_trigger_capture(id, 1, "candidate");
    insert_version(&pool, &building, "Building", at(1)).await;

    let resolved = load_live_workflow_definition(&pool, id)
        .await
        .expect("resolver query");
    assert!(
        resolved.is_none(),
        "with no live version the resolver returns None rather than a Building row"
    );
}
