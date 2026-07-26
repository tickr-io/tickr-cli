//! Observable ScopeStore laws shared by fresh all-NATS and Tickr Lite.

#![cfg(not(madsim))]

use chrono::Utc;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_ctx::nats_scope::{NatsScopeError, NatsScopeStore, MAX_SCOPE_VALUE_BYTES};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::encoding::encode_uuid;
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, DeleteTickrCtxScopeInput, ScopeCleanupOutcome, ScopeCreationOutcome,
    ScopeMutationRejection, ScopeReadOutcome, ScopeSnapshotOutcome, ScopeValueInput,
    ScopeWriteOutcome, WriteTickrCtxScopeInput,
};
use tickr_proto::config::DataPlaneSql;
use uuid::Uuid;

const INITIAL: &[u8] = br#"{ "v": 2, "type": "string", "value": "original", "secret": false, "producer": { "kind": "task", "task_id": "task-7", "task_name": "extract" }, "created_at": "2026-07-23T00:00:00Z", "sha256": "lineage-a" }"#;
const REPLACEMENT: &[u8] = br#"{  "v": 2, "type": "string", "value": "replacement", "secret": false, "producer": { "kind": "task", "task_id": "task-7", "task_name": "extract" }, "created_at": "2026-07-23T00:00:01Z", "sha256": "lineage-b" }"#;

#[derive(Clone, Copy, Debug)]
enum BackendKind {
    AllNats,
    Lite,
}

struct SnapshotEvidence {
    bytes: Vec<u8>,
    digest: String,
}

enum LawBackend {
    AllNats {
        store: NatsScopeStore,
        _container: testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    },
    Lite {
        writer: WriterRepositoryBundle,
        url: String,
        _directory: TempDir,
    },
}

impl LawBackend {
    async fn start(kind: BackendKind) -> Option<Self> {
        match kind {
            BackendKind::AllNats => {
                let command = NatsServerCmd::default().with_jetstream();
                let container = match Nats::default().with_cmd(&command).start().await {
                    Ok(container) => container,
                    Err(error) => {
                        eprintln!("skipping fresh all-NATS ScopeStore laws: {error}");
                        return None;
                    }
                };
                let port = container.get_host_port_ipv4(4222).await.ok()?;
                let nats = async_nats::connect(format!("nats://127.0.0.1:{port}"))
                    .await
                    .ok()?;
                let bucket = tickr_ctx::scope::bucket_for_namespace("default");
                let kv = async_nats::jetstream::new(nats)
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket,
                        history: 1,
                        max_value_size: tickr_ctx::store::MAX_VALUE_SIZE,
                        storage: async_nats::jetstream::stream::StorageType::File,
                        ..Default::default()
                    })
                    .await
                    .expect("create fresh versioned ScopeStore bucket");
                Some(Self::AllNats {
                    store: NatsScopeStore::new(kv, "default").unwrap(),
                    _container: container,
                })
            }
            BackendKind::Lite => {
                let directory = TempDir::new().unwrap();
                let url = format!(
                    "sqlite://{}",
                    directory.path().join("scope-law.db").display()
                );
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
                    .await
                    .unwrap();
                tickr_migrations::apply_sqlite(tickr_migrations::MigrationTarget::Conductor, &pool)
                    .await
                    .unwrap();
                pool.close().await;
                let writer = open_writer(&url).await;
                Some(Self::Lite {
                    writer,
                    url,
                    _directory: directory,
                })
            }
        }
    }

    async fn create(&self, owner: Uuid, envelope: &[u8]) {
        let key = scope_key(owner);
        match self {
            Self::AllNats { store, .. } => {
                store.ensure_scope(&owner.to_string()).await.unwrap();
                store.put(key, envelope.to_vec()).await.unwrap();
            }
            Self::Lite { writer, .. } => {
                assert_eq!(
                    writer
                        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                            scope_id: owner,
                            namespace: "default",
                            run_id: &owner.to_string(),
                            claim_id: Uuid::new_v4(),
                            values: &[ScopeValueInput {
                                key: &key,
                                envelope,
                            }],
                            now: Utc::now(),
                        })
                        .await
                        .unwrap(),
                    ScopeCreationOutcome::Created
                );
            }
        }
    }

    async fn replace(&self, owner: Uuid, envelope: &[u8]) {
        let key = scope_key(owner);
        match self {
            Self::AllNats { store, .. } => store.put(key, envelope.to_vec()).await.unwrap(),
            Self::Lite { writer, .. } => assert!(matches!(
                writer
                    .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                        scope_id: owner,
                        claim_id: Uuid::new_v4(),
                        values: &[ScopeValueInput {
                            key: &key,
                            envelope,
                        }],
                        now: Utc::now(),
                    })
                    .await
                    .unwrap(),
                ScopeWriteOutcome::Applied {
                    inserted: 0,
                    updated: 1
                }
            )),
        }
    }

    async fn read(&self, owner: Uuid) -> Vec<(String, Vec<u8>)> {
        match self {
            Self::AllNats { store, .. } => {
                let prefix = format!("{owner}/");
                let mut values = Vec::new();
                for key in store.keys(&prefix).await.unwrap() {
                    values.push((key.clone(), store.get(&key).await.unwrap().unwrap()));
                }
                values
            }
            Self::Lite { writer, .. } => match writer
                .read_tickr_ctx_scope(owner, Utc::now())
                .await
                .unwrap()
            {
                ScopeReadOutcome::Present(values) => values
                    .into_iter()
                    .map(|value| (value.key, value.envelope))
                    .collect(),
                outcome => panic!("scope must be present, got {outcome:?}"),
            },
        }
    }

    async fn delete(&self, owner: Uuid) {
        let key = scope_key(owner);
        match self {
            Self::AllNats { store, .. } => assert!(store.delete(&key).await.unwrap()),
            Self::Lite { writer, .. } => assert!(matches!(
                writer
                    .delete_tickr_ctx_scope_value(DeleteTickrCtxScopeInput {
                        scope_id: owner,
                        claim_id: Uuid::new_v4(),
                        key: &key,
                        now: Utc::now(),
                    })
                    .await
                    .unwrap(),
                tickr_migrations::scope_repository::ScopeDeleteOutcome::Deleted
            )),
        }
    }

    async fn reject_oversized_replace(&self, owner: Uuid, envelope: &[u8]) {
        let key = scope_key(owner);
        match self {
            Self::AllNats { store, .. } => assert!(matches!(
                store.put(key, envelope.to_vec()).await,
                Err(NatsScopeError::ValueLimit { .. })
            )),
            Self::Lite { writer, .. } => assert!(matches!(
                writer
                    .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                        scope_id: owner,
                        claim_id: Uuid::new_v4(),
                        values: &[ScopeValueInput {
                            key: &key,
                            envelope,
                        }],
                        now: Utc::now(),
                    })
                    .await
                    .unwrap(),
                ScopeWriteOutcome::Rejected(ScopeMutationRejection::Bound(_))
            )),
        }
    }

    async fn snapshot(&self, owner: Uuid) -> SnapshotEvidence {
        match self {
            Self::AllNats { store, .. } => {
                let snapshot = store.snapshot(&owner.to_string()).await.unwrap();
                SnapshotEvidence {
                    bytes: snapshot.bytes,
                    digest: snapshot.digest,
                }
            }
            Self::Lite { writer, .. } => {
                let outcome = writer
                    .snapshot_tickr_ctx_scope(owner, Utc::now())
                    .await
                    .unwrap();
                let snapshot = match outcome {
                    ScopeSnapshotOutcome::Committed(snapshot)
                    | ScopeSnapshotOutcome::Idempotent(snapshot) => snapshot,
                    outcome => panic!("scope must snapshot, got {outcome:?}"),
                };
                SnapshotEvidence {
                    bytes: snapshot.bytes,
                    digest: snapshot.digest,
                }
            }
        }
    }

    async fn cleanup_before_archive_is_rejected(&self, owner: Uuid) {
        match self {
            Self::AllNats { store, .. } => {
                assert!(store.cleanup_archived(&owner.to_string()).await.is_err())
            }
            Self::Lite { writer, .. } => assert_eq!(
                writer
                    .cleanup_tickr_ctx_scope(owner, Utc::now())
                    .await
                    .unwrap(),
                ScopeCleanupOutcome::SnapshotRequired
            ),
        }
    }

    async fn commit_archive_and_cleanup(&self, owner: Uuid, digest: &str) {
        match self {
            Self::AllNats { store, .. } => {
                store
                    .mark_archive_committed(&owner.to_string(), digest)
                    .await
                    .unwrap();
                store.cleanup_archived(&owner.to_string()).await.unwrap();
                assert!(store.keys(&format!("{owner}/")).await.unwrap().is_empty());
            }
            Self::Lite { writer, .. } => assert_eq!(
                writer
                    .cleanup_tickr_ctx_scope(owner, Utc::now())
                    .await
                    .unwrap(),
                ScopeCleanupOutcome::Cleaned
            ),
        }
    }

    async fn assert_missing_and_corrupt_fail(&mut self) {
        let missing = Uuid::new_v4();
        match self {
            Self::AllNats { store, .. } => assert!(matches!(
                store.snapshot(&missing.to_string()).await,
                Err(NatsScopeError::Missing(_))
            )),
            Self::Lite { writer, .. } => assert_eq!(
                writer
                    .snapshot_tickr_ctx_scope(missing, Utc::now())
                    .await
                    .unwrap(),
                ScopeSnapshotOutcome::Missing
            ),
        }

        let corrupt = Uuid::new_v4();
        self.create(corrupt, INITIAL).await;
        let key = scope_key(corrupt);
        match self {
            Self::AllNats { store, .. } => {
                store
                    .raw_store()
                    .put(&key, br#"{"v":99,"opaque":"future"}"#.as_slice().into())
                    .await
                    .unwrap();
                assert!(matches!(
                    store.snapshot(&corrupt.to_string()).await,
                    Err(NatsScopeError::Corrupt(_))
                ));
            }
            Self::Lite { writer, url, .. } => {
                writer.close().await;
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(tickr_migrations::sqlite_writer_options(url, false).unwrap())
                    .await
                    .unwrap();
                sqlx::query("UPDATE tickr_ctx_scope_values SET envelope = ?2 WHERE scope_id = ?1")
                    .bind(encode_uuid(corrupt))
                    .bind(br#"{"v":99,"opaque":"future"}"#.as_slice())
                    .execute(&pool)
                    .await
                    .unwrap();
                pool.close().await;
                *writer = open_writer(url).await;
                assert!(matches!(
                    writer
                        .snapshot_tickr_ctx_scope(corrupt, Utc::now())
                        .await
                        .unwrap(),
                    ScopeSnapshotOutcome::Quarantined { .. }
                ));
            }
        }
    }

    async fn assert_namespace_limit(&self) {
        let oversized = "n".repeat(129);
        match self {
            Self::AllNats { store, .. } => assert!(matches!(
                NatsScopeStore::new(store.raw_store().clone(), &oversized),
                Err(NatsScopeError::InvalidNamespace)
            )),
            Self::Lite { writer, .. } => assert!(writer
                .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                    scope_id: Uuid::new_v4(),
                    namespace: &oversized,
                    run_id: "run",
                    claim_id: Uuid::new_v4(),
                    values: &[],
                    now: Utc::now(),
                })
                .await
                .is_err()),
        }
    }
}

async fn open_writer(url: &str) -> WriterRepositoryBundle {
    RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: url.to_owned(),
    })
    .open_writer()
    .await
    .unwrap()
}

fn scope_key(owner: Uuid) -> String {
    format!("{owner}/result")
}

async fn exercise_backend(kind: BackendKind) {
    let Some(mut backend) = LawBackend::start(kind).await else {
        return;
    };
    backend.assert_namespace_limit().await;

    let owner = Uuid::new_v4();
    backend.create(owner, INITIAL).await;
    assert_eq!(
        backend.read(owner).await,
        vec![(scope_key(owner), INITIAL.to_vec())]
    );

    backend.replace(owner, REPLACEMENT).await;
    assert_eq!(
        backend.read(owner).await,
        vec![(scope_key(owner), REPLACEMENT.to_vec())],
        "replacement must retain exact lineage envelope bytes for {kind:?}"
    );

    let mut oversized = br#"{"v":2,"type":"string","value":""#.to_vec();
    oversized.resize(MAX_SCOPE_VALUE_BYTES + 1, b'x');
    backend.reject_oversized_replace(owner, &oversized).await;
    assert_eq!(
        backend.read(owner).await,
        vec![(scope_key(owner), REPLACEMENT.to_vec())],
        "limit rejection must retain accepted scope for {kind:?}"
    );

    backend.cleanup_before_archive_is_rejected(owner).await;
    let first = backend.snapshot(owner).await;
    let repeated = backend.snapshot(owner).await;
    assert_eq!(first.digest, repeated.digest, "stable digest for {kind:?}");
    assert_eq!(first.bytes, repeated.bytes, "stable snapshot for {kind:?}");
    backend
        .commit_archive_and_cleanup(owner, &first.digest)
        .await;

    let deleted_owner = Uuid::new_v4();
    backend.create(deleted_owner, INITIAL).await;
    backend.delete(deleted_owner).await;
    assert!(backend.read(deleted_owner).await.is_empty());

    backend.assert_missing_and_corrupt_fail().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_all_nats_obeys_scope_store_laws() {
    exercise_backend(BackendKind::AllNats).await;
}

#[tokio::test]
async fn tickr_lite_obeys_scope_store_laws() {
    exercise_backend(BackendKind::Lite).await;
}
