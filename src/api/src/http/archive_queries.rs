//! Archive read layer. Reads the conductor's canonical `workflows` table and
//! its terminal archive tables (`workflow_instances`, `task_instances`,
//! `workflow_run_info`), rehydrating JSONB columns back to the typed structs
//! that the rest of the workspace operates on.
//!
//! Each function takes the pool as an argument so the integration tests can
//! drive a `testcontainers`-backed Postgres without crate-wide globals.

use std::collections::HashMap;

use anyhow::Result;
use sqlx::{PgPool, Row};
use tickr_proto::instance as ip;
use tickr_proto::workflow as wf;
use uuid::Uuid;

/// One workflow definition projected for the list view, collapsed to a single
/// row per workflow id (the `workflows` table is `(id, version)`-PK'd, so a
/// workflow with N registered versions has N rows). `workflow` is the latest
/// registration's protobuf definition; `build_status`/`build_version` describe
/// that latest row's build lifecycle; `live_version` is the latest
/// registration whose status is live (`Ready`/`Submitted`) — the version that
/// would run if triggered today, or `None` if nothing has ever built.
pub struct WorkflowListRow {
    pub workflow: wf::WorkflowDefinition,
    /// Raw `workflows.status` of the latest row (`Building|Ready|BuildFailed|
    /// Submitted`); mapped to the wire `WorkflowBuildStatus` at the handler.
    pub build_status: String,
    /// System-assigned version of the latest row regardless of build outcome.
    pub build_version: i64,
    /// Latest live (`Ready`/`Submitted`) version, or `None` if never built.
    pub live_version: Option<i64>,
}

/// Reads every registered workflow definition from the conductor's `workflows`
/// table, collapsed to one row per workflow id. The `definition` JSONB column
/// was written by the register handler as `serde_json::to_value(&Workflow)`,
/// so `serde_json::from_value` round-trips it back to the same struct.
///
/// `build_status`/`build_version` come from the latest row per id (by
/// `inserted_at`); `live_version` is the latest row whose `status` is live
/// (`Ready` or `Submitted`) — `Submitted` counts because a submitted workflow
/// built successfully and is live, so it must not read as "never built".
///
/// Ordering: newest registration first (`inserted_at DESC`) so UI list views
/// show the most recently added workflows at the top by default.
pub async fn list_workflow_defs(pool: &PgPool) -> Result<Vec<WorkflowListRow>> {
    let rows: Vec<(serde_json::Value, String, i64, Option<i64>)> = sqlx::query_as(
        r#"
        WITH latest_overall AS (
            SELECT DISTINCT ON (id) id, version, status, definition, inserted_at
            FROM workflows
            ORDER BY id, inserted_at DESC
        ),
        latest_live AS (
            SELECT DISTINCT ON (id) id, version AS live_version
            FROM workflows
            WHERE status IN ('Ready', 'Submitted')
            ORDER BY id, inserted_at DESC
        )
        SELECT lo.definition, lo.status, lo.version, ll.live_version
        FROM latest_overall lo
        LEFT JOIN latest_live ll ON ll.id = lo.id
        ORDER BY lo.inserted_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(definition, build_status, build_version, live_version)| {
            Ok(WorkflowListRow {
                workflow: tickr_proto::codec::definition::definition_proto_from_json(definition)?,
                build_status,
                build_version,
                live_version,
            })
        })
        .collect()
}

/// Count the distinct workflow definitions registered under this tenant. The
/// `workflows` table holds one row per (id, version), and `list_workflow_defs`
/// collapses to one row per `id` via `DISTINCT ON (id)` — so the count must be
/// `count(DISTINCT id)` to equal the length of that list, not `count(*)` (which
/// would over-count by every version). Tenant scoping is implicit: the API
/// component binds one tenant's pool.
pub async fn count_workflow_defs(pool: &PgPool) -> Result<i64> {
    let count: i64 = sqlx::query_scalar("SELECT count(DISTINCT id) FROM workflows")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// Resolves the **Default version** of a single workflow — the version the
/// detail surface shows when the caller supplies no explicit `?version`.
///
/// The rule: the latest *live* version (`status IN ('Ready','Submitted')`) by
/// `inserted_at`; if none has built yet, the latest version overall by
/// `inserted_at`. `Submitted` counts as live because a submitted workflow built
/// successfully and was acknowledged by the server, so it is "what runs today"
/// just as much as a `Ready` one. The fallback guarantees a brand-new workflow
/// still stuck `Building` resolves to *some* version rather than nothing.
///
/// Returns `None` only when the workflow id has no rows at all (unknown id),
/// which the detail endpoint maps to a 404.
///
/// The single `ORDER BY` expresses both tiers at once: the boolean
/// live-membership sorts live rows ahead of non-live (`DESC` puts `true`
/// first), then `inserted_at DESC` picks the newest within whichever tier wins.
/// The bulk list endpoint deliberately does **not** consume this resolver — it
/// computes `build_version` (latest registration) and `live_version` as two
/// distinct per-row concepts in one set-based query, which a per-workflow call
/// would turn into an N+1; this resolver is the detail endpoint's single
/// consumer today.
pub async fn default_version(pool: &PgPool, workflow_id: Uuid) -> Result<Option<(i64, String)>> {
    let row: Option<(i64, String)> = sqlx::query_as(
        r#"
        SELECT version, status
        FROM workflows
        WHERE id = $1
        ORDER BY (status IN ('Ready', 'Submitted')) DESC, inserted_at DESC
        LIMIT 1
        "#,
    )
    .bind(workflow_id)
    .fetch_optional(pool)
    .await?;

    Ok(row)
}

/// Whether any row exists for this workflow id. The calendar handler uses it to
/// 404 an unknown id (rather than return a valid-looking empty calendar).
pub async fn workflow_exists(pool: &PgPool, workflow_id: Uuid) -> Result<bool> {
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM workflows WHERE id = $1 LIMIT 1")
        .bind(workflow_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// The durable lifecycle row of a submitted Patch — the record a submitter
/// polls for the asynchronous outcome after the synchronous ingress ack. The
/// apply itself is authoritative on the server; this row is the conductor's
/// pollable projection of where the Patch is (`Validating → Building →
/// Submitted → Applied | Rejected | BuildFailed`).
pub struct PatchStatusRow {
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    pub status: String,
    /// Human-readable terminal detail (rejection reason, or `applied`); null
    /// until terminal.
    pub outcome: Option<String>,
    /// The submitter's why-string, echoed back.
    pub reason: Option<String>,
    /// The Instance version a successful apply produced; null until Applied.
    pub applied_version: Option<i64>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Read one Patch's lifecycle row by `patch_id`. `None` for an unknown id (the
/// handler 404s). The submitter polls this after the POST's 202 ack.
pub async fn get_patch_status(pool: &PgPool, patch_id: Uuid) -> Result<Option<PatchStatusRow>> {
    #[allow(clippy::type_complexity)]
    let row: Option<(
        Uuid,
        Uuid,
        String,
        Option<String>,
        Option<String>,
        Option<i64>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        r#"
        SELECT patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at
        FROM workflow_patches
        WHERE patch_id = $1
        "#,
    )
    .bind(patch_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(patch_id, workflow_instance_id, status, outcome, reason, applied_version, updated_at)| {
            PatchStatusRow {
                patch_id,
                workflow_instance_id,
                status,
                outcome,
                reason,
                applied_version,
                updated_at,
            }
        },
    ))
}

/// A Patch's retained authored source — exactly what the author submitted
/// (Nickel for an external patch, the JSON document for a self-patch), the
/// conductor-side half of the two-sided patch record. `applied_version` (and
/// `workflow_instance_id`) let a reader join this authored source to the
/// server-side applied-patch effect by patch/version.
pub struct PatchSourceRow {
    pub patch_id: Uuid,
    pub workflow_instance_id: Uuid,
    /// The verbatim submitted source; null for a row predating source retention.
    pub source: Option<String>,
    /// `nickel` | `json` — the language the source is written in, so the code
    /// tab renders it correctly. Pairs with `source` (both set, or both null).
    pub source_format: Option<String>,
    /// The Instance version a successful apply produced; null until Applied.
    pub applied_version: Option<i64>,
}

/// Read one Patch's retained authored source by `patch_id` — the read path the
/// code tab uses, mirroring how a workflow version's `nickel_source` is served
/// from the conductor's Postgres. `None` for an unknown id (the handler 404s);
/// a known patch whose `source` is null (predates retention) still returns a
/// row with `source: None`.
pub async fn get_patch_source(pool: &PgPool, patch_id: Uuid) -> Result<Option<PatchSourceRow>> {
    // (patch_id, workflow_instance_id, source, source_format, applied_version)
    type SourceTuple = (Uuid, Uuid, Option<String>, Option<String>, Option<i64>);
    let row: Option<SourceTuple> = sqlx::query_as(
        r#"
        SELECT patch_id, workflow_instance_id, source, source_format, applied_version
        FROM workflow_patches
        WHERE patch_id = $1
        "#,
    )
    .bind(patch_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(patch_id, workflow_instance_id, source, source_format, applied_version)| PatchSourceRow {
            patch_id,
            workflow_instance_id,
            source,
            source_format,
            applied_version,
        },
    ))
}

/// One registered version of a workflow, projected for the Version picker.
/// `inserted_at` stays a typed timestamp here; the handler renders it RFC3339.
pub struct VersionRow {
    pub version: i64,
    pub status: String,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
}

/// Lists every registered version of a workflow, newest registration first
/// (`inserted_at DESC`) — the source the detail endpoint hands the Version
/// picker. Empty vec for an unknown workflow id (the handler 404s earlier on
/// the version load, so this is only reached for a known id).
pub async fn list_workflow_versions(pool: &PgPool, workflow_id: Uuid) -> Result<Vec<VersionRow>> {
    let rows: Vec<(i64, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        r#"
        SELECT version, status, inserted_at
        FROM workflows
        WHERE id = $1
        ORDER BY inserted_at DESC
        "#,
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(version, status, inserted_at)| VersionRow {
            version,
            status,
            inserted_at,
        })
        .collect())
}

/// The per-version artifacts the detail page renders: the raw build `status`,
/// the opaque parsed-`Workflow` `definition` blob (passed through untyped — the
/// UI walks it), and the author's persisted `nickel_source`.
pub struct WorkflowVersionDetail {
    pub status: String,
    pub definition: serde_json::Value,
    pub nickel_source: String,
}

/// Loads the `(status, definition, nickel_source)` for one `(workflow_id,
/// version)`. `None` when no such row exists — an unknown id, or a `?version`
/// that names a version this workflow never had (both map to 404 at the
/// handler). `definition` is returned as raw `serde_json::Value`: the detail
/// endpoint forwards it opaque rather than rehydrating to `Workflow`, since the
/// UI owns the walk and the parsed shape is still in flux.
pub async fn get_workflow_version(
    pool: &PgPool,
    workflow_id: Uuid,
    version: i64,
) -> Result<Option<WorkflowVersionDetail>> {
    let row: Option<(String, serde_json::Value, String)> = sqlx::query_as(
        r#"
        SELECT status, definition, nickel_source
        FROM workflows
        WHERE id = $1 AND version = $2
        "#,
    )
    .bind(workflow_id)
    .bind(version)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(status, definition, nickel_source)| WorkflowVersionDetail {
            status,
            definition,
            nickel_source,
        },
    ))
}

/// Count terminal (`Completed` ∪ `Failed`) instances per workflow id from the
/// conductor's archive, in one grouped aggregate. The archive holds only
/// terminal rows, so this is a cheap `GROUP BY` over the `workflow_id` index.
/// Workflows with no terminal runs are simply absent from the map (the caller
/// defaults them to `0`).
pub async fn completed_run_counts(pool: &PgPool) -> Result<HashMap<Uuid, i64>> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_id, COUNT(*) AS n
        FROM workflow_instances
        WHERE state IN ('Completed', 'Failed')
        GROUP BY workflow_id
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get::<Uuid, _>("workflow_id"), row.get::<i64, _>("n")))
        .collect())
}

/// Looks up a single archived workflow instance by id, rehydrated as the
/// archive-grade projection (the data-plane-visible allowlist — control-plane
/// internals are structurally absent from what is returned). Returns `None`
/// when no row exists — the route handler treats that as "the instance is not
/// yet terminal" and falls back to a live query against the coordinator.
///
/// The projection is derived by the server's data-plane read seam from the
/// `instance` JSONB blob plus this instance's archived task-instance blobs, so
/// the archived read path derives byte-identical content to the live read path
/// (only the read-time `storage` indicator, stamped by the handler, differs).
pub async fn get_workflow_instance(
    pool: &PgPool,
    instance_id: Uuid,
) -> Result<Option<ip::ArchivedInstance>> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT instance
        FROM workflow_instances
        WHERE id = $1
        "#,
    )
    .bind(instance_id)
    .fetch_optional(pool)
    .await?;

    let Some((instance_json,)) = row else {
        return Ok(None);
    };
    Ok(Some(
        tickr_proto::codec::archive::archived_instance_from_json(instance_json)?,
    ))
}

/// Lists every archived workflow instance for a given workflow id, projected to
/// the slim archive read row. Returns the rows newest-first (`archived_at DESC`)
/// so list views show the most recent runs at the top. Each `instance` JSONB
/// blob is reduced to the published list row, with `task_count` taken from the
/// run's archived task-instance rows.
pub async fn list_workflow_instances_by_workflow(
    pool: &PgPool,
    workflow_id: Uuid,
) -> Result<Vec<ip::ArchivedInstanceRow>> {
    let rows: Vec<(serde_json::Value, i64)> = sqlx::query_as(
        r#"
        SELECT wi.instance,
               (SELECT count(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id) AS task_count
        FROM workflow_instances wi
        WHERE wi.workflow_id = $1
        ORDER BY wi.archived_at DESC
        "#,
    )
    .bind(workflow_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(json, task_count)| {
            tickr_proto::codec::archive::archived_instance_row_from_json(json, task_count as u64)
        })
        .collect()
}

/// Like [`list_workflow_instances_by_workflow`], but scoped to a single local
/// date: only terminal rows whose `scheduled_at`, bucketed into the supplied
/// IANA `tz`, lands on `date` (`YYYY-MM-DD`). Powers the Run calendar's
/// click-through. Postgres handles the IANA name + DST natively via
/// `AT TIME ZONE`.
pub async fn list_workflow_instances_on_date(
    pool: &PgPool,
    workflow_id: Uuid,
    date: &str,
    tz: &str,
) -> Result<Vec<ip::ArchivedInstanceRow>> {
    let rows: Vec<(serde_json::Value, i64)> = sqlx::query_as(
        r#"
        SELECT wi.instance,
               (SELECT count(*) FROM task_instances ti
                WHERE ti.workflow_instance_id = wi.id) AS task_count
        FROM workflow_instances wi
        WHERE wi.workflow_id = $1
          AND (wi.scheduled_at AT TIME ZONE $2)::date = $3::date
        ORDER BY wi.archived_at DESC
        "#,
    )
    .bind(workflow_id)
    .bind(tz)
    .bind(date)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(json, task_count)| {
            tickr_proto::codec::archive::archived_instance_row_from_json(json, task_count as u64)
        })
        .collect()
}

/// One calendar day's terminal-state rollup, bucketed in the client's tz.
pub struct CalendarDayRollup {
    /// `YYYY-MM-DD` in the requested tz.
    pub date: String,
    pub completed: i64,
    pub failed: i64,
}

/// Per-day terminal counts (`completed` / `failed`) for one workflow across a
/// year, each row's `scheduled_at` bucketed into the client's IANA `tz`. A
/// single grouped aggregate with conditional sums; Postgres handles the IANA
/// name and DST transitions natively. Only terminal rows live in this archive,
/// so `Scheduled` / `InProgress` never appear here — those come from the live
/// source at the handler.
pub async fn calendar_terminal_rollup(
    pool: &PgPool,
    workflow_id: Uuid,
    year: i32,
    tz: &str,
) -> Result<Vec<CalendarDayRollup>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT
            (scheduled_at AT TIME ZONE $2)::date::text AS day,
            COUNT(*) FILTER (WHERE state = 'Completed') AS completed,
            COUNT(*) FILTER (WHERE state = 'Failed') AS failed
        FROM workflow_instances
        WHERE workflow_id = $1
          AND state IN ('Completed', 'Failed')
          AND EXTRACT(YEAR FROM (scheduled_at AT TIME ZONE $2)) = $3
        GROUP BY day
        "#,
    )
    .bind(workflow_id)
    .bind(tz)
    .bind(year)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(date, completed, failed)| CalendarDayRollup {
            date,
            completed,
            failed,
        })
        .collect())
}

/// The compaction enrichment for one archived instance: the
/// `workflow_run_info.ctx_envelope` JSON array of `{ key, envelope }` pairs
/// dumped from the tenant's ctx KV at compaction time. `None` when no
/// run-info row exists (e.g. the instance pre-dates the enrichment).
pub async fn get_run_info_ctx_envelope(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Option<serde_json::Value>> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        r#"
        SELECT ctx_envelope
        FROM workflow_run_info
        WHERE workflow_instance_id = $1
        "#,
    )
    .bind(workflow_instance_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(v,)| v))
}

/// Lists every archived task instance belonging to a given workflow instance,
/// projected to the published task read row. The task archive table preserves
/// the established completion order (`archived_at ASC`); each JSONB payload is
/// the published snapshot task projection, while the parent identifiers remain
/// indexed in columns.
pub async fn list_task_instances(
    pool: &PgPool,
    workflow_instance_id: Uuid,
) -> Result<Vec<ip::ArchivedTaskInstance>> {
    let rows: Vec<(Uuid, Uuid, serde_json::Value)> = sqlx::query_as(
        r#"
        SELECT workflow_instance_id, workflow_id, task_instance
        FROM task_instances
        WHERE workflow_instance_id = $1
        ORDER BY archived_at ASC
        "#,
    )
    .bind(workflow_instance_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|(workflow_instance_id, workflow_id, task_instance)| {
            let task: ip::SnapshotTaskInstance = serde_json::from_value(task_instance)?;
            Ok(ip::ArchivedTaskInstance {
                id: task.id,
                task_id: task.task_id,
                workflow_instance_id: workflow_instance_id.to_string(),
                workflow_id: workflow_id.to_string(),
                name: task.name,
                task_type: task.task_type,
                state: task.state,
                executor_id: task.executor_id,
                attempt: task.attempt,
            })
        })
        .collect()
}

/// One row per archived workflow instance for the day-clock. The parent
/// workflow's name is snapshotted on the instance, and the instance `id` is
/// carried so the clock can dedup against the live half by id and the side
/// sheet can identify each run.
#[derive(Debug, Clone)]
pub struct DashboardInstanceRow {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub state: String,
}

/// Lists archived workflow instances whose `scheduled_at` falls in the window
/// — the same axis the clock buckets on, so the archive half lines up with the
/// live half. The parent workflow's name is read from the instance's
/// snapshotted `workflow_name` field in the JSONB column; legacy rows archived
/// before the snapshot existed carry none, so a `LEFT JOIN workflows` supplies
/// it as a COALESCE fallback. `state` is the verbatim substrate state.
pub async fn list_dashboard_instances(
    pool: &PgPool,
    start: Option<chrono::DateTime<chrono::Utc>>,
    end: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<Vec<DashboardInstanceRow>> {
    let rows = sqlx::query_as::<
        _,
        (
            Uuid,
            Uuid,
            String,
            Option<chrono::DateTime<chrono::Utc>>,
            String,
        ),
    >(
        r#"
        SELECT wi.id,
               wi.workflow_id,
               COALESCE(NULLIF(wi.instance->>'workflow_name', ''), w.name, '') AS workflow_name,
               wi.scheduled_at,
               wi.state
        FROM workflow_instances wi
        LEFT JOIN workflows w ON wi.workflow_id = w.id
        WHERE ($1::timestamptz IS NULL OR wi.scheduled_at >= $1)
          AND ($2::timestamptz IS NULL OR wi.scheduled_at <= $2)
        ORDER BY wi.scheduled_at DESC NULLS LAST
        "#,
    )
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, workflow_id, workflow_name, scheduled_at, state)| DashboardInstanceRow {
                id,
                workflow_id,
                workflow_name,
                scheduled_at,
                state,
            },
        )
        .collect())
}

/// One row of the tenant events projection, as the Event log endpoint
/// serves it. `seq` is the poll cursor (insertion order — commit-ordered by
/// the conductor pull cycle's advisory lock); `ts` is occurrence time. The two
/// axes are distinct: the cursor stays on arrival (`seq`) order, so paging is
/// gap-free and duplicate-free even when a late-arriving event from a slow
/// control-plane source lands out of `ts` order mid-buffer; the client sorts the
/// visible buffer by `ts` for display. That is why `seq`, not `ts`, is the
/// cursor — arrival order is stable, occurrence order is not.
pub struct EventRow {
    pub seq: i64,
    pub id: Uuid,
    pub ts: chrono::DateTime<chrono::Utc>,
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// Rehydrate one projection row into an `EventRow`. Shared by the unscoped
/// Event log read and the per-instance filtered reads, so every events
/// surface projects the same column set identically.
fn event_row_from(row: sqlx::postgres::PgRow) -> EventRow {
    EventRow {
        seq: row.get("seq"),
        id: row.get("id"),
        ts: row.get("ts"),
        event_type: row.get("event_type"),
        payload: row.get("payload"),
    }
}

/// Read the newest events from the tenant events projection, newest-first by
/// `seq`. `after = None` is the first page load (latest `limit` rows);
/// `after = Some(seq)` returns only rows strictly newer — the UI's 5s poll
/// passes the highest `seq` it has seen.
pub async fn list_events(pool: &PgPool, after: Option<i64>, limit: i64) -> Result<Vec<EventRow>> {
    let rows = sqlx::query(
        r#"
        SELECT seq, id, ts, event_type, payload
        FROM events
        WHERE ($1::bigint IS NULL OR seq > $1)
        ORDER BY seq DESC
        LIMIT $2
        "#,
    )
    .bind(after)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(event_row_from).collect())
}

/// Read one workflow instance's events from the tenant events projection,
/// same cursor/ordering as `list_events` (newest-first by `seq`; `after`
/// returns strictly-newer rows). The only added predicate is the instance
/// filter — there is no new table and no JOIN; this is the same projection
/// `/api/events` serves, scoped.
///
/// The payload is the externally-tagged event enum — a single-key object
/// `{ "<EventType>": { ...ids } }` — so the instance id sits one level in,
/// under whatever variant named it. `jsonb_each(payload)` yields that single
/// inner object regardless of its key, and we read `workflow_instance_id` off
/// it; this needs no per-event-type case and stays correct as new event types
/// arrive (and stays portable across Postgres versions — no jsonpath). The
/// `jsonb_typeof = 'object'` guard skips unit-variant events (serialized as a
/// bare string), which carry no instance id anyway. Tenant scoping is
/// implicit: the API component binds one tenant's pool, so the instance-id
/// predicate is the only filter required.
///
/// Accepted gap (not silently shipped): `TaskInstanceCreated` / `TaskQueued`
/// for a not-yet-delivered task carry only `task_instance_id`, so this
/// workflow-instance rollup drops them. They are fully served on the
/// task-instance endpoint and every task is listed on the Tasks tab; the
/// clean fix (enriching those payloads with `workflow_instance_id`) is an
/// event-contract change, out of scope here.
pub async fn list_workflow_instance_events(
    pool: &PgPool,
    workflow_instance_id: Uuid,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows = sqlx::query(
        r#"
        SELECT seq, id, ts, event_type, payload
        FROM events
        WHERE ($1::bigint IS NULL OR seq > $1)
          AND jsonb_typeof(payload) = 'object'
          AND (
                SELECT value->>'workflow_instance_id'
                FROM jsonb_each(payload)
                LIMIT 1
              ) = $3
        ORDER BY seq DESC
        LIMIT $2
        "#,
    )
    .bind(after)
    .bind(limit)
    .bind(workflow_instance_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(event_row_from).collect())
}

/// Read one task instance's events from the tenant events projection. Same
/// cursor/ordering and same single-predicate model as
/// `list_workflow_instance_events`, reading `task_instance_id` off the inner
/// object instead.
pub async fn list_task_instance_events(
    pool: &PgPool,
    task_instance_id: Uuid,
    after: Option<i64>,
    limit: i64,
) -> Result<Vec<EventRow>> {
    let rows = sqlx::query(
        r#"
        SELECT seq, id, ts, event_type, payload
        FROM events
        WHERE ($1::bigint IS NULL OR seq > $1)
          AND jsonb_typeof(payload) = 'object'
          AND (
                SELECT value->>'task_instance_id'
                FROM jsonb_each(payload)
                LIMIT 1
              ) = $3
        ORDER BY seq DESC
        LIMIT $2
        "#,
    )
    .bind(after)
    .bind(limit)
    .bind(task_instance_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(event_row_from).collect())
}

/// One replay lifecycle row of a source run, projected for the reverse-link
/// list. Only the operator-facing fields ship — the seed witness and re-drive
/// bookkeeping stay server-side.
pub struct ReplayRow {
    pub replay_instance_id: Uuid,
    pub source_instance_id: Uuid,
    pub status: String,
    pub name: Option<String>,
    pub resume_from: Vec<Uuid>,
    /// Names of the declared trigger captures the replay shadowed — names only,
    /// never values (a shadowed value may be a secret).
    pub shadowed_keys: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// List a source run's replays for the reverse "list its replays" link, newest
/// first. Served from the `workflow_replays` row **indexed by
/// `source_instance_id`** (`workflow_replays_source_idx`) and avoids an unbounded
/// scan of all instances, which is the priced-out read path this endpoint
/// exists to avoid.
pub async fn list_replays_for_source(
    pool: &PgPool,
    source_instance_id: Uuid,
) -> Result<Vec<ReplayRow>> {
    let rows = sqlx::query(
        r#"
        SELECT replay_instance_id, source_instance_id, status, name,
               resume_from, shadowed_keys, created_at
        FROM workflow_replays
        WHERE source_instance_id = $1
        ORDER BY created_at DESC
        "#,
    )
    .bind(source_instance_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| ReplayRow {
            replay_instance_id: r.get("replay_instance_id"),
            source_instance_id: r.get("source_instance_id"),
            status: r.get("status"),
            name: r.get("name"),
            resume_from: serde_json::from_value(r.get("resume_from")).unwrap_or_default(),
            shadowed_keys: serde_json::from_value(r.get("shadowed_keys")).unwrap_or_default(),
            created_at: r.get("created_at"),
        })
        .collect())
}
