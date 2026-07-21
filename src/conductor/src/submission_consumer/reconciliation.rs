//! Boot-time reconciliation: republish a submission message for every
//! workflow row currently at `Ready`. Runs exactly once at conductor
//! startup, before the submission consumer subscribes. No periodic
//! re-scan in steady state.

use crate::submission_consumer::consumer::publish_submission;
use crate::submission_consumer::message::SubmissionMessage;
use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::definition_repository::DefinitionSubmissionReconciliationOutcome;

#[async_trait::async_trait]
trait SubmissionPublisher: Send + Sync {
    async fn publish(&self, message: &SubmissionMessage) -> Result<()>;
}

struct NatsSubmissionPublisher<'a> {
    nats: &'a NatsClient,
}

#[async_trait::async_trait]
impl SubmissionPublisher for NatsSubmissionPublisher<'_> {
    async fn publish(&self, message: &SubmissionMessage) -> Result<()> {
        publish_submission(self.nats, message).await
    }
}

/// Republish a `SubmissionMessage` per definition that remains at `Ready`.
/// Returns the number of messages published. Logs and continues on per-row
/// publish failures so startup does not wedge on a transient NATS hiccup.
pub async fn reconcile_orphan_ready_rows(
    repositories: &WriterRepositoryBundle,
    nats: &NatsClient,
) -> Result<usize> {
    reconcile_with_publisher(repositories, &NatsSubmissionPublisher { nats }).await
}

async fn reconcile_with_publisher(
    repositories: &WriterRepositoryBundle,
    publisher: &dyn SubmissionPublisher,
) -> Result<usize> {
    let pointers = repositories
        .definition_submission_reconciliation_candidates()
        .await
        .context("scan definitions for orphan Ready rows")?;

    let total = pointers.len();
    let mut published = 0usize;
    for pointer in pointers {
        match repositories
            .definition_submission_reconciliation_outcome(pointer)
            .await
            .context("recheck orphan Ready row before publication")?
        {
            DefinitionSubmissionReconciliationOutcome::Ready => {}
            DefinitionSubmissionReconciliationOutcome::NotReady(_)
            | DefinitionSubmissionReconciliationOutcome::Absent => continue,
        }

        let message = SubmissionMessage {
            workflow_id: pointer.workflow_id,
            workflow_version: pointer.workflow_version,
        };
        match publisher.publish(&message).await {
            Ok(()) => published += 1,
            Err(error) => {
                eprintln!(
                    "boot reconciliation: failed to republish ({}, {}): {}",
                    pointer.workflow_id, pointer.workflow_version, error
                );
            }
        }
    }
    if total > 0 {
        println!(
            "Boot-time reconciliation: republished {published}/{total} orphan Ready rows onto submission queue"
        );
    }
    Ok(published)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use sqlx::sqlite::SqlitePoolOptions;
    use tickr_migrations::backend::RepositoryFactory;
    use tickr_migrations::definition_repository::{
        DefinitionBuildSettlementOutcome, DefinitionLifecycleStatus, DefinitionRegistrationInput,
        DefinitionRegistrationOutcome, DefinitionSubmissionCandidate, DefinitionSubmissionPointer,
        DefinitionSubmissionReconciliationOutcome, DefinitionSubmissionSettlementOutcome,
        DefinitionTaskBuildResult,
    };
    use tickr_migrations::{apply_sqlite, sqlite_writer_options, MigrationTarget};
    use tickr_proto::config::DataPlaneSql;
    use tickr_proto::workflow as wf;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;

    #[derive(Default)]
    struct RecordingPublisher {
        messages: Mutex<Vec<SubmissionMessage>>,
        settle_before_first_publish:
            Mutex<Option<(Arc<WriterRepositoryBundle>, DefinitionSubmissionPointer)>>,
    }

    impl RecordingPublisher {
        fn settling(
            repositories: Arc<WriterRepositoryBundle>,
            pointer: DefinitionSubmissionPointer,
        ) -> Self {
            Self {
                messages: Mutex::new(Vec::new()),
                settle_before_first_publish: Mutex::new(Some((repositories, pointer))),
            }
        }
    }

    #[async_trait::async_trait]
    impl SubmissionPublisher for RecordingPublisher {
        async fn publish(&self, message: &SubmissionMessage) -> Result<()> {
            let settlement = self.settle_before_first_publish.lock().await.take();
            if let Some((repositories, pointer)) = settlement {
                assert_eq!(
                    repositories
                        .settle_definition_submission(
                            pointer.workflow_id,
                            pointer.workflow_version,
                        )
                        .await?,
                    DefinitionSubmissionSettlementOutcome::Submitted
                );
            }
            self.messages.lock().await.push(message.clone());
            Ok(())
        }
    }

    fn sqlite_url(path: &Path) -> String {
        format!("sqlite://{}", path.display())
    }

    async fn register_definition(repositories: &WriterRepositoryBundle, workflow_id: Uuid) -> Uuid {
        let task_id = Uuid::from_u128(workflow_id.as_u128() + 100);
        let outcome = repositories
            .register_definition(DefinitionRegistrationInput {
                definition: wf::WorkflowDefinition {
                    id: workflow_id.to_string(),
                    tenant_id: Uuid::from_u128(999).to_string(),
                    namespace: "default".to_string(),
                    slug: format!("restart-{workflow_id}"),
                    name: "Restart recovery".to_string(),
                    tasks: vec![wf::TaskDefinition {
                        id: task_id.to_string(),
                        workflow_id: workflow_id.to_string(),
                        name: "build".to_string(),
                        nix_expression_path: "/nix/store/build".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                content_hash: format!("content-{workflow_id}"),
                cosmetic_hash: format!("cosmetic-{workflow_id}"),
                nickel_source: "restart-recovery".to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            DefinitionRegistrationOutcome::Inserted {
                workflow_version: 1,
                ..
            }
        ));
        task_id
    }

    async fn make_ready(repositories: &WriterRepositoryBundle, workflow_id: Uuid, task_id: Uuid) {
        assert!(matches!(
            repositories
                .settle_definition_task_build(
                    workflow_id,
                    1,
                    task_id,
                    DefinitionTaskBuildResult::Success,
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::Ready(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_republishes_only_definitions_that_remain_ready() {
        let directory = tempfile::tempdir().unwrap();
        let url = sqlite_url(&directory.path().join("reconciliation.db"));
        let migration_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlite_writer_options(&url, true).unwrap())
            .await
            .unwrap();
        apply_sqlite(MigrationTarget::Conductor, &migration_pool)
            .await
            .unwrap();
        migration_pool.close().await;

        let selection = DataPlaneSql::Sqlite { url };
        let repositories = RepositoryFactory::new(selection.clone())
            .open_writer()
            .await
            .unwrap();

        let interrupted_id = Uuid::from_u128(1);
        let concurrently_settled_id = Uuid::from_u128(2);
        let submitted_id = Uuid::from_u128(3);
        let failed_id = Uuid::from_u128(4);
        let building_id = Uuid::from_u128(5);

        let interrupted_task = register_definition(&repositories, interrupted_id).await;
        make_ready(&repositories, interrupted_id, interrupted_task).await;
        let concurrent_task = register_definition(&repositories, concurrently_settled_id).await;
        make_ready(&repositories, concurrently_settled_id, concurrent_task).await;
        let submitted_task = register_definition(&repositories, submitted_id).await;
        make_ready(&repositories, submitted_id, submitted_task).await;
        assert_eq!(
            repositories
                .settle_definition_submission(submitted_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionSettlementOutcome::Submitted
        );
        let failed_task = register_definition(&repositories, failed_id).await;
        assert_eq!(
            repositories
                .settle_definition_task_build(
                    failed_id,
                    1,
                    failed_task,
                    DefinitionTaskBuildResult::Failure {
                        error: "build failed",
                    },
                )
                .await
                .unwrap(),
            DefinitionBuildSettlementOutcome::BuildFailed
        );
        register_definition(&repositories, building_id).await;

        // The process exits after committing Ready but before publishing.
        repositories.close().await;
        let repositories = Arc::new(
            RepositoryFactory::new(selection)
                .open_writer()
                .await
                .unwrap(),
        );

        let concurrent_pointer = DefinitionSubmissionPointer {
            workflow_id: concurrently_settled_id,
            workflow_version: 1,
        };
        let publisher = RecordingPublisher::settling(Arc::clone(&repositories), concurrent_pointer);
        assert_eq!(
            reconcile_with_publisher(repositories.as_ref(), &publisher)
                .await
                .unwrap(),
            1
        );
        let messages = publisher.messages.lock().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].workflow_id, interrupted_id);
        assert_eq!(messages[0].workflow_version, 1);
        drop(messages);

        assert!(matches!(
            repositories
                .definition_submission_candidate(interrupted_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::Ready(_)
        ));
        let first = repositories.settle_definition_submission(interrupted_id, 1);
        let duplicate = repositories.settle_definition_submission(interrupted_id, 1);
        let (first, duplicate) = tokio::join!(first, duplicate);
        let outcomes = [first.unwrap(), duplicate.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == DefinitionSubmissionSettlementOutcome::Submitted)
                .count(),
            1
        );

        let duplicate_restart = RecordingPublisher::default();
        assert_eq!(
            reconcile_with_publisher(repositories.as_ref(), &duplicate_restart)
                .await
                .unwrap(),
            0
        );
        assert!(duplicate_restart.messages.lock().await.is_empty());
        assert_eq!(
            repositories
                .definition_submission_reconciliation_outcome(concurrent_pointer)
                .await
                .unwrap(),
            DefinitionSubmissionReconciliationOutcome::NotReady(
                DefinitionLifecycleStatus::Submitted,
            )
        );
        assert_eq!(
            repositories
                .definition_submission_candidate(failed_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::NotReady(DefinitionLifecycleStatus::BuildFailed)
        );
        assert_eq!(
            repositories
                .definition_submission_candidate(building_id, 1)
                .await
                .unwrap(),
            DefinitionSubmissionCandidate::NotReady(DefinitionLifecycleStatus::Building)
        );
        assert_eq!(
            repositories
                .definition_submission_reconciliation_outcome(DefinitionSubmissionPointer {
                    workflow_id: Uuid::from_u128(9999),
                    workflow_version: 1,
                })
                .await
                .unwrap(),
            DefinitionSubmissionReconciliationOutcome::Absent
        );
        repositories.close().await;
    }
}
