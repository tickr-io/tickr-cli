//! Integration tests for the terminal-time patch settlement audit against
//! ephemeral PG.
//!
//! The audit reconciles the durable patch ledger (`workflow_patches`) against
//! a terminal instance's applied-patch log and writes a loud, durable
//! discrepancy record for anything that did not settle cleanly:
//! - an instance whose every ledger patch settled (an `Applied` patch present
//!   in the applied-patch log, plus a decided `Rejected` patch) produces zero
//!   discrepancy records;
//! - a patch left unsettled (`Submitted`) at terminal produces exactly one;
//! - the audit is idempotent under compaction redelivery (re-running upserts
//!   the same single row).

#![cfg(not(madsim))]

mod common;

use sqlx::PgPool;
use std::collections::HashSet;
use tickr_migrations::backend::WriterRepositoryBundle;
use tickr_migrations::patch_repository::PatchLifecycleStatus;
use uuid::Uuid;

async fn start_pg() -> Option<(common::DbGuard, sqlx::PgPool)> {
    common::test_db().await
}

/// Seed one `workflow_patches` ledger row with the given status.
async fn seed_ledger_row(pool: &PgPool, instance_id: Uuid, patch_key: Uuid, status: &str) {
    sqlx::query(
        r#"
        INSERT INTO workflow_patches
            (patch_key, patch_id, workflow_instance_id, status, ops)
        VALUES ($1, $2, $3, $4, '[]'::jsonb)
        "#,
    )
    .bind(patch_key)
    .bind(Uuid::new_v4())
    .bind(instance_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow_patches row");
}

/// All ledger patches settled cleanly → zero discrepancy records. An `Applied`
/// patch present in the applied-patch log and a decided `Rejected` patch both
/// settle without a terminal-time gap, so neither is flagged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fully_settled_instance_records_no_discrepancy() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let repository = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    let instance_id = Uuid::new_v4();
    let applied_key = Uuid::new_v4();
    let rejected_key = Uuid::new_v4();

    seed_ledger_row(&pool, instance_id, applied_key, "Applied").await;
    seed_ledger_row(&pool, instance_id, rejected_key, "Rejected").await;

    // The applied-patch log carries the Applied patch's key; the Rejected
    // patch is legitimately absent from it.
    let applied: HashSet<Uuid> = [applied_key].into_iter().collect();

    let recorded = repository
        .audit_patch_settlement(instance_id, &applied)
        .await
        .expect("audit runs");

    assert!(
        recorded.is_empty(),
        "no discrepancies for a fully-settled instance"
    );
    assert!(repository
        .patch_settlement_discrepancies(instance_id)
        .await
        .expect("read discrepancies")
        .is_empty());
}

/// A patch left unsettled (`Submitted`) at terminal compaction → exactly one
/// discrepancy record, capturing the observed ledger status. Re-running the
/// audit (compaction redelivery) upserts the same single row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsettled_patch_records_exactly_one_discrepancy() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let repository = WriterRepositoryBundle::from_postgres_pool(pool.clone());
    let instance_id = Uuid::new_v4();
    let unsettled_key = Uuid::new_v4();

    seed_ledger_row(&pool, instance_id, unsettled_key, "Submitted").await;

    // Applied-patch log is empty — the patch never reached an outcome.
    let applied: HashSet<Uuid> = HashSet::new();

    let recorded = repository
        .audit_patch_settlement(instance_id, &applied)
        .await
        .expect("audit runs");
    assert_eq!(
        recorded.len(),
        1,
        "the unsettled patch is the sole discrepancy"
    );
    let discrepancies = repository
        .patch_settlement_discrepancies(instance_id)
        .await
        .expect("read discrepancies");
    assert_eq!(discrepancies.len(), 1);
    assert_eq!(
        discrepancies[0].ledger_status,
        PatchLifecycleStatus::Submitted
    );

    // Idempotent under redelivery: a second audit pass upserts, not duplicates.
    let recorded_again = repository
        .audit_patch_settlement(instance_id, &applied)
        .await
        .expect("audit re-runs");
    assert_eq!(recorded_again.len(), 1);
    assert_eq!(
        repository
            .patch_settlement_discrepancies(instance_id)
            .await
            .expect("read discrepancies")
            .len(),
        1
    );
}
