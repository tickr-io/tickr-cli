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
use tickr_conductor::system_tasks::compaction_receiver::audit_patch_settlement;
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

async fn discrepancy_count(pool: &PgPool, instance_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_patch_discrepancies WHERE workflow_instance_id = $1",
    )
    .bind(instance_id)
    .fetch_one(pool)
    .await
    .expect("count discrepancy rows")
}

/// All ledger patches settled cleanly → zero discrepancy records. An `Applied`
/// patch present in the applied-patch log and a decided `Rejected` patch both
/// settle without a terminal-time gap, so neither is flagged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fully_settled_instance_records_no_discrepancy() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let instance_id = Uuid::new_v4();
    let applied_key = Uuid::new_v4();
    let rejected_key = Uuid::new_v4();

    seed_ledger_row(&pool, instance_id, applied_key, "Applied").await;
    seed_ledger_row(&pool, instance_id, rejected_key, "Rejected").await;

    // The applied-patch log carries the Applied patch's key; the Rejected
    // patch is legitimately absent from it.
    let applied: HashSet<Uuid> = [applied_key].into_iter().collect();

    let recorded = audit_patch_settlement(&pool, instance_id, &applied)
        .await
        .expect("audit runs");

    assert_eq!(recorded, 0, "no discrepancies for a fully-settled instance");
    assert_eq!(discrepancy_count(&pool, instance_id).await, 0);
}

/// A patch left unsettled (`Submitted`) at terminal compaction → exactly one
/// discrepancy record, capturing the observed ledger status. Re-running the
/// audit (compaction redelivery) upserts the same single row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsettled_patch_records_exactly_one_discrepancy() {
    let Some((_container, pool)) = start_pg().await else {
        return;
    };
    let instance_id = Uuid::new_v4();
    let unsettled_key = Uuid::new_v4();

    seed_ledger_row(&pool, instance_id, unsettled_key, "Submitted").await;

    // Applied-patch log is empty — the patch never reached an outcome.
    let applied: HashSet<Uuid> = HashSet::new();

    let recorded = audit_patch_settlement(&pool, instance_id, &applied)
        .await
        .expect("audit runs");
    assert_eq!(recorded, 1, "the unsettled patch is the sole discrepancy");
    assert_eq!(discrepancy_count(&pool, instance_id).await, 1);

    let status: String = sqlx::query_scalar(
        "SELECT ledger_status FROM workflow_patch_discrepancies
          WHERE workflow_instance_id = $1 AND patch_key = $2",
    )
    .bind(instance_id)
    .bind(unsettled_key)
    .fetch_one(&pool)
    .await
    .expect("fetch discrepancy row");
    assert_eq!(status, "Submitted");

    // Idempotent under redelivery: a second audit pass upserts, not duplicates.
    let recorded_again = audit_patch_settlement(&pool, instance_id, &applied)
        .await
        .expect("audit re-runs");
    assert_eq!(recorded_again, 1);
    assert_eq!(discrepancy_count(&pool, instance_id).await, 1);
}
