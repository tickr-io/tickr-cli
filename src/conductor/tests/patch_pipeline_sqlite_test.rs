#![cfg(not(madsim))]

use anyhow::Result;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;
use tempfile::TempDir;
use tickr_conductor::patch_pipeline::{
    correlate_outcome, patch_key, process_patch, redrive_unsettled, OutcomeCorrelation,
    ParsedPatch, PatchIngress, PatchProvenance, PatchRelaySender, PatchSource,
};
use tickr_migrations::backend::RepositoryFactory;
use tickr_proto::config::DataPlaneSql;
use tickr_proto::patch as pp;
use tokio::sync::Mutex;
use uuid::Uuid;

struct FailingSender;

#[async_trait::async_trait]
impl PatchRelaySender for FailingSender {
    async fn send(&self, _envelope: &pp::PatchEnvelope) -> Result<()> {
        anyhow::bail!("relay unavailable")
    }
}

#[derive(Default)]
struct CountingSender(Mutex<Vec<pp::PatchEnvelope>>);

#[async_trait::async_trait]
impl PatchRelaySender for CountingSender {
    async fn send(&self, envelope: &pp::PatchEnvelope) -> Result<()> {
        self.0.lock().await.push(envelope.clone());
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_failure_and_redelivery_preserve_one_sqlite_ingress_row() {
    let directory = TempDir::new().unwrap();
    let url = format!("sqlite://{}", directory.path().join("patches.db").display());
    let migration_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(
        tickr_migrations::MigrationTarget::Conductor,
        &migration_pool,
    )
    .await
    .unwrap();
    migration_pool.close().await;

    let factory = RepositoryFactory::new(DataPlaneSql::Sqlite { url: url.clone() });
    let writer = factory.open_writer().await.unwrap();
    let workflow_instance_id = Uuid::new_v4();
    let patch_id = Uuid::new_v4();
    let source = r#"{"ops":[{"RemoveNode":{"node_id":"aB3d"}}]}"#;
    let parsed = ParsedPatch {
        ops: Vec::new(),
        operation: None,
        reason: Some("relay failure law".to_owned()),
        stall_ttl: None,
        source: PatchSource::json(source),
    };

    assert!(matches!(
        process_patch(
            &writer,
            &FailingSender,
            workflow_instance_id,
            patch_id,
            parsed.clone(),
            PatchProvenance::SelfEmitted,
        )
        .await
        .unwrap(),
        PatchIngress::Accepted { .. }
    ));
    writer.close().await;

    let reader = factory.open_read_only().await.unwrap();
    let status = reader.patch_status(patch_id).await.unwrap().unwrap();
    assert_eq!(status.status.as_str(), "Validating");
    let retained = reader
        .patch_source(patch_id)
        .await
        .unwrap()
        .unwrap()
        .source
        .unwrap();
    assert_eq!(retained.text, source);
    reader.close().await;

    let reopened_writer = factory.open_writer().await.unwrap();
    let sender = CountingSender::default();
    match process_patch(
        &reopened_writer,
        &sender,
        workflow_instance_id,
        patch_id,
        parsed,
        PatchProvenance::SelfEmitted,
    )
    .await
    .unwrap()
    {
        PatchIngress::Replayed { row } => {
            assert_eq!(row.patch_key, patch_key(workflow_instance_id, patch_id));
            assert_eq!(row.status, "Validating");
        }
        other => panic!("redelivery did not replay the durable row: {other:?}"),
    }
    assert!(sender.0.lock().await.is_empty());

    assert_eq!(
        redrive_unsettled(&reopened_writer, &sender, Duration::ZERO)
            .await
            .unwrap(),
        1
    );
    let sent = sender.0.lock().await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].patch_key,
        patch_key(workflow_instance_id, patch_id).to_string()
    );
    assert_eq!(
        sent[0].workflow_instance_id,
        workflow_instance_id.to_string()
    );
    drop(sent);

    let outcome = pp::PatchOutcome {
        workflow_instance_id: workflow_instance_id.to_string(),
        patch_key: patch_key(workflow_instance_id, patch_id).to_string(),
        outcome: Some(pp::PatchOutcomeKind {
            kind: Some(pp::patch_outcome_kind::Kind::Applied(
                pp::patch_outcome_kind::Applied { version: 1 },
            )),
        }),
        reshaped_graph_json: None,
    };
    assert_eq!(
        correlate_outcome(&reopened_writer, &outcome).await.unwrap(),
        OutcomeCorrelation::Settled
    );
    let correlated_reader = factory.open_read_only().await.unwrap();
    assert_eq!(
        correlated_reader
            .patch_status(patch_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .as_str(),
        "Applied"
    );
    correlated_reader.close().await;
    assert_eq!(
        redrive_unsettled(&reopened_writer, &sender, Duration::ZERO)
            .await
            .unwrap(),
        0,
        "a terminal Patch is never reopened or driven again"
    );
    assert_eq!(sender.0.lock().await.len(), 1);
    reopened_writer.close().await;
}
