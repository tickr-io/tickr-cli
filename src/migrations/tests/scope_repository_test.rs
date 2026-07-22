#![cfg(not(madsim))]

use chrono::{Duration, Utc};
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_migrations::encoding::{encode_timestamp, encode_uuid};
use tickr_migrations::scope_repository::{
    CreateTickrCtxScopeInput, DeleteTickrCtxScopeInput, ScopeBoundViolation, ScopeCleanupOutcome,
    ScopeCreationOutcome, ScopeDeleteOutcome, ScopeReadOutcome, ScopeSnapshotOutcome,
    ScopeValueInput, ScopeWriteOutcome, WriteTickrCtxScopeInput, MAX_SCOPE_AGE_SECONDS,
};
use tickr_proto::config::DataPlaneSql;
use uuid::Uuid;

const TASK_ENVELOPE: &[u8] = br#"{ "v": 2, "type": "string", "value": "secret-value", "secret": true, "producer": { "kind": "task", "task_id": "task-7", "task_name": "extract" }, "created_at": "2026-07-22T00:00:00Z", "sha256": "lineage-a" }"#;
const UPDATED_TASK_ENVELOPE: &[u8] = br#"{"v":2,"type":"string","value":"replacement","secret":false,"producer":{"kind":"task","task_id":"task-7","task_name":"extract"},"created_at":"2026-07-22T00:00:01Z","sha256":"lineage-b"}"#;
const LEGACY_ENVELOPE: &[u8] = br#"{"v":1,"type":"bool","value":true,"secret":false,"producer_task":"task-1","producer_task_name":"legacy","created_at":"2026-07-22T00:00:00Z","sha256":"legacy"}"#;

async fn migrated_database(name: &str) -> (TempDir, String) {
    let directory = TempDir::new().unwrap();
    let url = format!("sqlite://{}", directory.path().join(name).display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, true).unwrap())
        .await
        .unwrap();
    tickr_migrations::apply_sqlite(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .unwrap();
    pool.close().await;
    (directory, url)
}

async fn open_writer(url: &str) -> WriterRepositoryBundle {
    RepositoryFactory::new(DataPlaneSql::Sqlite {
        url: url.to_owned(),
    })
    .open_writer()
    .await
    .unwrap()
}

#[tokio::test]
async fn empty_run_scope_is_archivable() {
    let (_directory, url) = migrated_database("empty-scope.db").await;
    let writer = open_writer(&url).await;
    let scope_id = Uuid::new_v4();
    let now = Utc::now();

    assert_eq!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id,
                namespace: "default",
                run_id: "run-without-context-values",
                claim_id: Uuid::new_v4(),
                values: &[],
                now,
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::Created
    );
    let ScopeSnapshotOutcome::Committed(snapshot) = writer
        .snapshot_tickr_ctx_scope(scope_id, now)
        .await
        .unwrap()
    else {
        panic!("empty active scope must commit a snapshot");
    };
    assert_eq!(snapshot.row_count, 0);
    writer.close().await;
}

#[tokio::test]
async fn opaque_scope_claims_updates_snapshot_and_cleanup_survive_restarts() {
    let (_directory, url) = migrated_database("scope.db").await;
    let scope_id = Uuid::new_v4();
    let creation_claim = Uuid::new_v4();
    let now = Utc::now();
    let initial = [ScopeValueInput {
        key: "run/key.with-punctuation",
        envelope: TASK_ENVELOPE,
    }];

    let writer = open_writer(&url).await;
    assert_eq!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id,
                namespace: "default",
                run_id: "run/identity",
                claim_id: creation_claim,
                values: &initial,
                now,
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::Created
    );
    writer.close().await;

    let writer = open_writer(&url).await;
    assert_eq!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id,
                namespace: "default",
                run_id: "run/identity",
                claim_id: creation_claim,
                values: &initial,
                now,
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::Idempotent
    );
    let collision_id = Uuid::new_v4();
    assert_eq!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: collision_id,
                namespace: "default",
                run_id: "run/identity",
                claim_id: Uuid::new_v4(),
                values: &initial,
                now,
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::Collision {
            existing_scope_id: scope_id
        }
    );
    assert_eq!(
        writer
            .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                scope_id: Uuid::new_v4(),
                namespace: "other",
                run_id: "other-run",
                claim_id: creation_claim,
                values: &initial,
                now,
            })
            .await
            .unwrap(),
        ScopeCreationOutcome::ClaimConflict
    );

    let ScopeReadOutcome::Present(before_update) =
        writer.read_tickr_ctx_scope(scope_id, now).await.unwrap()
    else {
        panic!("created scope must be readable");
    };
    assert_eq!(before_update.len(), 1);
    assert_eq!(before_update[0].key, "run/key.with-punctuation");
    assert_eq!(before_update[0].envelope, TASK_ENVELOPE);
    let stable_value_identity = before_update[0].value_identity.clone();

    let update_claim = Uuid::new_v4();
    let updates = [
        ScopeValueInput {
            key: "run/key.with-punctuation",
            envelope: UPDATED_TASK_ENVELOPE,
        },
        ScopeValueInput {
            key: "run/legacy",
            envelope: LEGACY_ENVELOPE,
        },
    ];
    assert_eq!(
        writer
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: update_claim,
                values: &updates,
                now: now + Duration::seconds(1),
            })
            .await
            .unwrap(),
        ScopeWriteOutcome::Applied {
            inserted: 1,
            updated: 1
        }
    );
    writer.close().await;

    let writer = open_writer(&url).await;
    assert_eq!(
        writer
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: update_claim,
                values: &updates,
                now: now + Duration::seconds(2),
            })
            .await
            .unwrap(),
        ScopeWriteOutcome::Idempotent
    );
    let ScopeReadOutcome::Present(after_update) = writer
        .read_tickr_ctx_scope(scope_id, now + Duration::seconds(2))
        .await
        .unwrap()
    else {
        panic!("updated scope must be readable");
    };
    assert_eq!(after_update.len(), 2);
    assert_eq!(after_update[0].value_identity, stable_value_identity);
    assert_eq!(after_update[0].envelope, UPDATED_TASK_ENVELOPE);
    assert_eq!(after_update[1].envelope, LEGACY_ENVELOPE);
    assert_eq!(
        writer
            .cleanup_tickr_ctx_scope(scope_id, now + Duration::seconds(3))
            .await
            .unwrap(),
        ScopeCleanupOutcome::SnapshotRequired
    );
    let ScopeSnapshotOutcome::Committed(snapshot) = writer
        .snapshot_tickr_ctx_scope(scope_id, now + Duration::seconds(3))
        .await
        .unwrap()
    else {
        panic!("active scope must commit a snapshot");
    };
    assert_eq!(snapshot.row_count, 2);
    assert_eq!(snapshot.digest.len(), 64);
    assert!(snapshot
        .bytes
        .windows(UPDATED_TASK_ENVELOPE.len())
        .any(|window| window == UPDATED_TASK_ENVELOPE));
    writer.close().await;

    let writer = open_writer(&url).await;
    let ScopeSnapshotOutcome::Idempotent(restarted_snapshot) = writer
        .snapshot_tickr_ctx_scope(scope_id, now + Duration::seconds(4))
        .await
        .unwrap()
    else {
        panic!("restart must recover the committed snapshot");
    };
    assert_eq!(restarted_snapshot, snapshot);
    assert_eq!(
        writer
            .write_tickr_ctx_scope(WriteTickrCtxScopeInput {
                scope_id,
                claim_id: update_claim,
                values: &updates,
                now: now + Duration::seconds(4),
            })
            .await
            .unwrap(),
        ScopeWriteOutcome::Idempotent
    );
    let read_after_snapshot = writer
        .read_tickr_ctx_scope(scope_id, now + Duration::seconds(4))
        .await
        .unwrap();
    assert!(matches!(
        &read_after_snapshot,
        ScopeReadOutcome::Archived(archived) if archived == &snapshot
    ));
    assert_eq!(
        writer
            .cleanup_tickr_ctx_scope(scope_id, now + Duration::seconds(4))
            .await
            .unwrap(),
        ScopeCleanupOutcome::Cleaned
    );
    assert_eq!(
        writer
            .cleanup_tickr_ctx_scope(scope_id, now + Duration::seconds(5))
            .await
            .unwrap(),
        ScopeCleanupOutcome::AlreadyCleaned
    );
    writer.close().await;

    let writer = open_writer(&url).await;
    let read_after_cleanup = writer
        .read_tickr_ctx_scope(scope_id, now + Duration::seconds(6))
        .await
        .unwrap();
    assert!(matches!(
        &read_after_cleanup,
        ScopeReadOutcome::Archived(archived) if archived == &snapshot
    ));
    writer.close().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    let value_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tickr_ctx_scope_values WHERE scope_id = ?1")
            .bind(encode_uuid(scope_id))
            .fetch_one(&pool)
            .await
            .unwrap();
    let claim_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tickr_ctx_scope_claims WHERE scope_id = ?1")
            .bind(encode_uuid(scope_id))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(value_count, 0);
    assert_eq!(claim_count, 0);
    pool.close().await;
}

#[tokio::test]
async fn missing_corrupt_unknown_and_over_age_scope_never_become_empty() {
    let (_directory, url) = migrated_database("scope-failures.db").await;
    let writer = open_writer(&url).await;
    assert_eq!(
        writer
            .read_tickr_ctx_scope(Uuid::new_v4(), Utc::now())
            .await
            .unwrap(),
        ScopeReadOutcome::Missing
    );

    let now = Utc::now();
    let unknown_scope = Uuid::new_v4();
    let malformed_scope = Uuid::new_v4();
    let unreadable_scope = Uuid::new_v4();
    let aged_scope = Uuid::new_v4();
    for (scope_id, run_id) in [
        (unknown_scope, "unknown"),
        (malformed_scope, "malformed"),
        (unreadable_scope, "unreadable"),
        (aged_scope, "aged"),
    ] {
        assert_eq!(
            writer
                .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
                    scope_id,
                    namespace: "default",
                    run_id,
                    claim_id: Uuid::new_v4(),
                    values: &[ScopeValueInput {
                        key: "run/value",
                        envelope: TASK_ENVELOPE,
                    }],
                    now,
                })
                .await
                .unwrap(),
            ScopeCreationOutcome::Created
        );
    }
    writer.close().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    sqlx::query("UPDATE tickr_ctx_scope_values SET envelope = ?2 WHERE scope_id = ?1")
        .bind(encode_uuid(unknown_scope))
        .bind(br#"{"v":99,"opaque":"future"}"#.as_slice())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tickr_ctx_scope_values SET envelope = ?2 WHERE scope_id = ?1")
        .bind(encode_uuid(malformed_scope))
        .bind(b"not-json".as_slice())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tickr_ctx_scope_values SET envelope = 42 WHERE scope_id = ?1")
        .bind(encode_uuid(unreadable_scope))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tickr_ctx_scopes SET created_at = ?2 WHERE scope_id = ?1")
        .bind(encode_uuid(aged_scope))
        .bind(encode_timestamp(
            now - Duration::seconds(MAX_SCOPE_AGE_SECONDS + 1),
        ))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let writer = open_writer(&url).await;
    for scope_id in [unknown_scope, malformed_scope] {
        let ScopeReadOutcome::Quarantined {
            scope_id: quarantined_id,
            diagnostic,
        } = writer.read_tickr_ctx_scope(scope_id, now).await.unwrap()
        else {
            panic!("invalid envelope must quarantine its scope");
        };
        assert_eq!(quarantined_id, scope_id);
        assert!(!diagnostic.is_empty());
        assert!(matches!(
            writer.read_tickr_ctx_scope(scope_id, now).await.unwrap(),
            ScopeReadOutcome::Quarantined { scope_id: repeated, .. } if repeated == scope_id
        ));
    }
    assert!(writer
        .read_tickr_ctx_scope(unreadable_scope, now)
        .await
        .is_err());
    assert!(matches!(
        writer.read_tickr_ctx_scope(aged_scope, now).await.unwrap(),
        ScopeReadOutcome::Bound(ScopeBoundViolation::ScopeAgeSeconds { .. })
    ));
    writer.close().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(tickr_migrations::sqlite_writer_options(&url, false).unwrap())
        .await
        .unwrap();
    let aged_values: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM tickr_ctx_scope_values WHERE scope_id = ?1")
            .bind(encode_uuid(aged_scope))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        aged_values, 1,
        "age refusal must retain accepted scope state"
    );
    pool.close().await;
}

#[tokio::test]
async fn scope_value_deletion_is_claimed_atomic_and_restart_safe() {
    let (_directory, url) = migrated_database("scope-delete.db").await;
    let writer = open_writer(&url).await;
    let scope_id = Uuid::new_v4();
    let now = Utc::now();
    let initial = [
        ScopeValueInput {
            key: "run/keep",
            envelope: LEGACY_ENVELOPE,
        },
        ScopeValueInput {
            key: "run/remove",
            envelope: TASK_ENVELOPE,
        },
    ];
    writer
        .create_tickr_ctx_scope(CreateTickrCtxScopeInput {
            scope_id,
            namespace: "default",
            run_id: "run",
            claim_id: Uuid::new_v4(),
            values: &initial,
            now,
        })
        .await
        .unwrap();

    let claim_id = Uuid::new_v4();
    let deletion = DeleteTickrCtxScopeInput {
        scope_id,
        claim_id,
        key: "run/remove",
        now,
    };
    assert_eq!(
        writer.delete_tickr_ctx_scope_value(deletion).await.unwrap(),
        ScopeDeleteOutcome::Deleted
    );
    assert_eq!(
        writer.delete_tickr_ctx_scope_value(deletion).await.unwrap(),
        ScopeDeleteOutcome::Idempotent
    );
    assert_eq!(
        writer
            .delete_tickr_ctx_scope_value(DeleteTickrCtxScopeInput {
                scope_id,
                claim_id,
                key: "run/keep",
                now,
            })
            .await
            .unwrap(),
        ScopeDeleteOutcome::ClaimConflict
    );
    writer.close().await;

    let writer = open_writer(&url).await;
    let ScopeReadOutcome::Present(values) =
        writer.read_tickr_ctx_scope(scope_id, now).await.unwrap()
    else {
        panic!("scope must remain readable after a claimed deletion");
    };
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].key, "run/keep");
    writer.close().await;
}
