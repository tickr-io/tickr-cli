//! Shared workflow-registration pipeline.
//!
//! Registration has two transports today: the HTTP `POST
//! /api/workflows/register` route and the API component's command bus. They
//! differ only at the transport edge (request body shape, response
//! projection); the work in between is identical — Nickel parse with a 30s
//! timeout, version-novelty check, the single Postgres transaction that
//! inserts the `workflows` row at `Building` plus one `workflow_task_builds`
//! row per task, and the publish-after-commit of one `TaskBuildJob` per task
//! onto the build queue.
//!
//! This module is that shared middle layer, mirroring `trigger_pipeline`.
//! Callers build a [`RegisterRequest`], invoke [`process_register`], and adapt
//! the resulting [`RegisterOutcome`] / [`RegisterError`] to their response
//! shape.

use anyhow::{Context, Result};
use async_nats::Client as NatsClient;
use sqlx::PgPool;
use std::time::Duration;
use tickr_proto::workflow as wf;
use tickr_proto::TenantId;
use tokio::time::timeout;
use uuid::Uuid;

use crate::parser::Parser;

/// Nickel evaluation budget. A submitted source that doesn't evaluate within
/// this window surfaces as [`RegisterError::Timeout`] (HTTP 408).
const NICKEL_EVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Producer intent the transport-specific caller assembles.
pub struct RegisterRequest {
    pub nickel_source: String,
    /// Grouping segment supplied at registration (not in the source). Empty
    /// normalises to `default` before identity derivation. Qualifies the
    /// author's slug so `namespace.slug` is the workflow's identity.
    pub namespace: String,
}

/// Outcome of [`process_register`]. Both arms carry the fully-rendered
/// `message` string so the HTTP wrapper and the command-bus arm project
/// byte-identical bodies without re-deriving it.
///
/// The version is system-assigned: the conductor derives the workflow's content
/// hash and compares it to the latest stored version. Re-submitting identical
/// content is a clean [`NoOp`](RegisterOutcome::NoOp), not a conflict — so a CD
/// pipeline can blindly re-submit on every merge.
pub enum RegisterOutcome {
    /// A new version row + per-task rows committed and the `TaskBuildJob`s were
    /// published. `workflow_version` is the system-assigned integer. Maps to
    /// HTTP 202.
    Inserted {
        workflow_id: Uuid,
        workflow_version: i64,
        task_count: usize,
        message: String,
    },
    /// Content matched the latest successfully-built version but a cosmetic
    /// field changed; the latest row's cosmetic columns + archived source were
    /// updated in place. No version bump. Maps to HTTP 200.
    Refreshed {
        workflow_id: Uuid,
        workflow_version: i64,
        message: String,
    },
    /// Content matched the latest version whose build had failed; that version's
    /// failed task builds were re-enqueued on the same row. No version bump.
    /// Maps to HTTP 202.
    BuildRequeued {
        workflow_id: Uuid,
        workflow_version: i64,
        task_count: usize,
        message: String,
    },
    /// The incoming content matched the latest stored version's hash (and a
    /// build is settled/in-flight); no storage mutation. Maps to HTTP 200 — a
    /// success, not a conflict.
    NoOp {
        workflow_id: Uuid,
        workflow_version: i64,
        message: String,
    },
}

/// Failure modes the pipeline distinguishes for the caller. The `Display`
/// strings are the exact HTTP messages today's handler returns, so both
/// callers reproduce them by rendering `err.to_string()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("Failed to parse workflow: {0}")]
    Parse(String),
    #[error("Timeout while evaluating Nickel source")]
    Timeout,
    #[error("Workflow parsed successfully, but failed to persist: {0}")]
    Persist(#[source] anyhow::Error),
}

/// Run the shared registration pipeline. On [`RegisterOutcome::Inserted`] the
/// Postgres transaction has committed and the per-task `TaskBuildJob`s have
/// been published onto the build queue (best-effort, after commit).
pub async fn process_register(
    pool: &PgPool,
    nats: &NatsClient,
    req: RegisterRequest,
) -> Result<RegisterOutcome, RegisterError> {
    // 1. Parse the submitted Nickel source with a bounded eval budget. A parse
    //    error is a 400; exceeding the budget is a 408. The conductor is pinned
    //    to one tenant, read from its environment here at the ingress boundary
    //    and threaded into the identity seed so this fleet's ids are all
    //    tenant-scoped (no hard-coded tenant).
    let tenant = TenantId::from_env();
    let mut workflow = match timeout(
        NICKEL_EVAL_TIMEOUT,
        Parser::parse_workflow(&req.nickel_source, tenant, &req.namespace),
    )
    .await
    {
        Ok(Ok(workflow)) => workflow,
        Ok(Err(e)) => return Err(RegisterError::Parse(e.to_string())),
        Err(_) => return Err(RegisterError::Timeout),
    };

    let name = workflow.name.clone();
    let workflow_id = Uuid::parse_str(&workflow.id).expect("parser mints a valid workflow id");

    // 2. Derive the content + cosmetic hashes and resolve the register decision
    //    against the latest stored version. The four outcomes: a first/changed
    //    registration Inserts at the next integer version; a content match either
    //    NoOps, Refreshes cosmetics in place, or re-runs a failed build.
    use crate::version_resolver::RegisterDecision;
    let incoming_hash = crate::content_hash::content_hash(&workflow);
    let incoming_cosmetic = crate::content_hash::cosmetic_hash(&workflow);
    let latest = fetch_latest_row(pool, workflow_id)
        .await
        .map_err(RegisterError::Persist)?;
    let decision =
        crate::version_resolver::resolve(&incoming_hash, &incoming_cosmetic, latest.as_ref());

    let version = match decision {
        RegisterDecision::NoOp { version } => {
            let message = format!(
                "Workflow '{}' v{} unchanged; no-op (content matches the latest version)",
                name, version
            );
            return Ok(RegisterOutcome::NoOp {
                workflow_id,
                workflow_version: version,
                message,
            });
        }
        RegisterDecision::Refreshed { version } => {
            // Identity-affecting content is unchanged; only cosmetics (display
            // name / tags) differ. Update the latest row's cosmetic columns and
            // archived source in place — per-version content stays immutable.
            workflow.version = version;
            refresh_latest_cosmetics(pool, &workflow, &incoming_cosmetic, &req.nickel_source)
                .await
                .map_err(RegisterError::Persist)?;
            let message = format!(
                "Workflow '{}' v{} refreshed; display fields updated in place (no version bump)",
                name, version
            );
            return Ok(RegisterOutcome::Refreshed {
                workflow_id,
                workflow_version: version,
                message,
            });
        }
        RegisterDecision::BuildRequeued { version } => {
            // Content matches a version whose build failed; re-run that version's
            // failed task builds on the same row so re-running CI recovers a
            // flaky build without polluting version history.
            let task_count = requeue_failed_builds(pool, nats, workflow_id, version)
                .await
                .map_err(RegisterError::Persist)?;
            let message = format!(
                "Workflow '{}' v{} build requeued; {} failed task build(s) re-enqueued",
                name, version, task_count
            );
            return Ok(RegisterOutcome::BuildRequeued {
                workflow_id,
                workflow_version: version,
                task_count,
                message,
            });
        }
        RegisterDecision::Insert { version } => version,
    };

    // 3. Stamp the system-assigned version, then write the new version row +
    //    one per-task build-tracking row, all in one transaction.
    workflow.version = version;
    let jobs = write_workflow_and_per_task_rows(
        pool,
        &workflow,
        &incoming_hash,
        &incoming_cosmetic,
        &req.nickel_source,
    )
    .await
    .map_err(RegisterError::Persist)?;

    // 4. Publish-after-commit: one TaskBuildJob per task onto the build queue,
    //    only after the Postgres transaction committed. A best-effort publish
    //    failure leaves the per-task row at `pending`; boot-time reconciliation
    //    covers that gap.
    for job in &jobs {
        match bincode::serialize(job) {
            Ok(payload) => {
                if let Err(e) = nats
                    .publish(crate::build_pipeline::BUILD_QUEUE_SUBJECT, payload.into())
                    .await
                {
                    eprintln!(
                        "Failed to publish TaskBuildJob for task {}: {}",
                        job.task_id, e
                    );
                }
            }
            Err(e) => {
                eprintln!("Failed to serialize TaskBuildJob: {}", e);
            }
        }
    }

    let task_count = jobs.len();
    let message = format!(
        "Workflow '{}' v{} accepted; {} per-task builds queued",
        name, version, task_count
    );
    Ok(RegisterOutcome::Inserted {
        workflow_id,
        workflow_version: version,
        task_count,
        message,
    })
}

/// Fetch the latest (highest-version) row's `(version, content_hash)` for a
/// `workflow_id`, or `None` when the id has no rows yet. The resolver compares
/// the incoming hash only against this latest snapshot — never older rows.
async fn fetch_latest_row(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<crate::version_resolver::LatestRow>> {
    let row: Option<(i64, String, String, String)> = sqlx::query_as(
        "SELECT version, content_hash, cosmetic_hash, status \
         FROM workflows WHERE id = $1 ORDER BY version DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(version, content_hash, cosmetic_hash, status)| {
        crate::version_resolver::LatestRow {
            version,
            content_hash,
            cosmetic_hash,
            status,
        }
    }))
}

/// Inserts a workflow row at status `Building` (with its system-assigned
/// integer `version` and `content_hash`) and one per-task build-tracking row at
/// status `pending` for each task, all in a single Postgres transaction.
///
/// The version-resolver has already guaranteed this `(id, version)` is novel
/// (the next integer above the latest, or `1`), so there is no conflict path:
/// the row always inserts. `content_hash` is persisted on the row so the next
/// registration's resolver can compare against it.
///
/// On success, no outbox row is inserted — the outbox-row insert migrates to
/// the build pipeline's last-one-out finalizer so the cross-plane hand-off is
/// gated on the lifecycle transition rather than racing the per-task builds.
/// The caller publishes the returned `TaskBuildJob` list onto the build queue
/// *after* the transaction commits, satisfying publish-after-commit ordering.
pub(crate) async fn write_workflow_and_per_task_rows(
    pool: &PgPool,
    workflow: &wf::WorkflowDefinition,
    content_hash: &str,
    cosmetic_hash: &str,
    nickel_source: &str,
) -> Result<Vec<crate::build_pipeline::TaskBuildJob>> {
    use crate::build_pipeline::TaskBuildJob;

    let id = Uuid::parse_str(&workflow.id).context("parse workflow id")?;
    let version = workflow.version;
    let name = workflow.name.as_str();
    // `namespace` and `slug` are denormalized display metadata. Identity is
    // derived once and definitions retain both values for rendering.
    let namespace = workflow.namespace.clone();
    let slug = workflow.slug.clone();
    // `status` is the source-of-truth lifecycle column
    // (Building | Ready | BuildFailed | Submitted). The firing trigger lives
    // in `definition` (the `Workflow.trigger` enum); no denormalised column.
    // `nickel_source` is the author's submitted source, persisted verbatim in
    // the same transaction as its parsed `definition` — a conductor-local
    // archival fact read only by the workflow detail surface, never relayed.
    let definition = tickr_proto::codec::definition::definition_proto_to_json(workflow)?;

    let mut tx = pool.begin().await?;

    sqlx::query(
        r#"
        INSERT INTO workflows (id, version, namespace, slug, name, status, content_hash, cosmetic_hash, definition, nickel_source)
        VALUES ($1, $2, $3, $4, $5, 'Building', $6, $7, $8, $9)
        "#,
    )
    .bind(id)
    .bind(version)
    .bind(&namespace)
    .bind(&slug)
    .bind(name)
    .bind(content_hash)
    .bind(cosmetic_hash)
    .bind(&definition)
    .bind(nickel_source)
    .execute(&mut *tx)
    .await?;

    // One per-task tracking row per task in the workflow. The finalizer
    // queries this table to decide whether the workflow's `Building →
    // Ready` flip is eligible.
    let mut jobs = Vec::with_capacity(workflow.tasks.len());
    for task in &workflow.tasks {
        let task_id = Uuid::parse_str(&task.id).context("parse task id")?;
        sqlx::query(
            r#"
            INSERT INTO workflow_task_builds (workflow_id, workflow_version, task_id, status)
            VALUES ($1, $2, $3, 'pending')
            "#,
        )
        .bind(id)
        .bind(version)
        .bind(task_id)
        .execute(&mut *tx)
        .await?;

        // Unified task-spec store write: enrichment reads declared
        // routing-variable specs from this one `task_id`-keyed table for
        // registered and patched-in tasks alike (one lookup, one fail-closed
        // rule). Task ids are minted fresh per registration, so the insert
        // never conflicts in practice; DO NOTHING keeps a replayed
        // registration idempotent.
        let routing_vars = serde_json::to_value(&task.routing_vars)?;
        sqlx::query(
            r#"
            INSERT INTO task_specs (task_id, routing_vars)
            VALUES ($1, $2)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task_id)
        .bind(&routing_vars)
        .execute(&mut *tx)
        .await?;

        jobs.push(TaskBuildJob {
            workflow_id: id,
            workflow_version: version,
            task_id,
            nix_expression_path: task.nix_expression_path.clone(),
        });
    }

    tx.commit().await?;
    Ok(jobs)
}

/// Refresh the latest version row's cosmetic columns + archived source in place
/// for a [`RegisterDecision::Refreshed`](crate::version_resolver::RegisterDecision::Refreshed).
/// The identity-affecting content is unchanged (per-version content is
/// immutable); only the display `name`, the `cosmetic_hash`, and the persisted
/// `definition`/`nickel_source` (which reflect the new cosmetic fields) update.
/// No new row, no build re-enqueue. `workflow.version` must already be the
/// matched latest version.
async fn refresh_latest_cosmetics(
    pool: &PgPool,
    workflow: &wf::WorkflowDefinition,
    cosmetic_hash: &str,
    nickel_source: &str,
) -> Result<()> {
    // Per-version content — the task graph and its task ids included — is
    // immutable. A cosmetic refresh therefore patches ONLY the cosmetic fields
    // (display `name` and `tags`, the pair `cosmetic_hash` covers) into the
    // stored definition; it must never re-serialize the freshly parsed workflow
    // over it. The new parse carries fresh random task ids (`Uuid::new_v4`), and
    // overwriting the definition with them desyncs the immutable
    // `workflow_task_builds` rows — silently breaking a later BuildRequeue,
    // which would then re-enqueue zero tasks.
    let id = Uuid::parse_str(&workflow.id).context("parse workflow id")?;
    let mut definition: serde_json::Value =
        sqlx::query_scalar("SELECT definition FROM workflows WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(workflow.version)
            .fetch_one(pool)
            .await?;
    let cosmetic = tickr_proto::codec::definition::definition_proto_to_json(workflow)?;
    if let (Some(target), Some(source)) = (definition.as_object_mut(), cosmetic.as_object()) {
        for key in ["name", "tags"] {
            if let Some(value) = source.get(key) {
                target.insert(key.to_string(), value.clone());
            }
        }
    }
    sqlx::query(
        r#"
        UPDATE workflows
           SET name = $3,
               cosmetic_hash = $4,
               definition = $5,
               nickel_source = $6,
               updated_at = now()
         WHERE id = $1 AND version = $2
        "#,
    )
    .bind(id)
    .bind(workflow.version)
    .bind(&workflow.name)
    .bind(cosmetic_hash)
    .bind(&definition)
    .bind(nickel_source)
    .execute(pool)
    .await?;
    Ok(())
}

/// Re-enqueue the failed task builds of a `BuildFailed` version for a
/// [`RegisterDecision::BuildRequeued`](crate::version_resolver::RegisterDecision::BuildRequeued).
/// Resets the workflow row `BuildFailed → Building` and every `failure` per-task
/// row back to `pending` (in one transaction), then publishes a fresh
/// `TaskBuildJob` for each reset task after commit. Returns how many builds were
/// re-enqueued. The reset is guarded on the row still being `BuildFailed`, so a
/// concurrent requeue is benign (the loser resets zero rows).
async fn requeue_failed_builds(
    pool: &PgPool,
    nats: &NatsClient,
    workflow_id: Uuid,
    version: i64,
) -> Result<usize> {
    use crate::build_pipeline::TaskBuildJob;

    let mut tx = pool.begin().await?;

    // Flip BuildFailed -> Building (guarded). If another conductor already
    // requeued, this matches zero rows and the failure rows are already pending,
    // so the RETURNING below yields nothing — a benign no-op.
    sqlx::query(
        r#"
        UPDATE workflows
           SET status = 'Building', updated_at = now()
         WHERE id = $1 AND version = $2 AND status = 'BuildFailed'
        "#,
    )
    .bind(workflow_id)
    .bind(version)
    .execute(&mut *tx)
    .await?;

    // Reset the failed per-task rows to pending and learn which tasks they were.
    let reset: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        UPDATE workflow_task_builds
           SET status = 'pending', error = NULL, built_at = NULL
         WHERE workflow_id = $1 AND workflow_version = $2 AND status = 'failure'
        RETURNING task_id
        "#,
    )
    .bind(workflow_id)
    .bind(version)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    if reset.is_empty() {
        return Ok(0);
    }

    // Load the stored definition to recover each task's build inputs. The build
    // rows key on the *stored* task ids, so the jobs must use those — not the
    // freshly-parsed (random-id) incoming workflow.
    let definition = crate::build_pipeline::load_workflow_definition(pool, workflow_id, version)
        .await
        .context("load definition for build requeue")?;

    let mut count = 0usize;
    for (task_id,) in reset {
        let needle = task_id.to_string();
        let Some(task) = definition.tasks.iter().find(|t| t.id == needle) else {
            continue;
        };
        let job = TaskBuildJob {
            workflow_id,
            workflow_version: version,
            task_id,
            nix_expression_path: task.nix_expression_path.clone(),
        };
        match bincode::serialize(&job) {
            Ok(payload) => {
                if let Err(e) = nats
                    .publish(crate::build_pipeline::BUILD_QUEUE_SUBJECT, payload.into())
                    .await
                {
                    eprintln!(
                        "Failed to publish requeued TaskBuildJob for task {}: {}",
                        task_id, e
                    );
                }
            }
            Err(e) => eprintln!("Failed to serialize requeued TaskBuildJob: {}", e),
        }
        count += 1;
    }
    Ok(count)
}
