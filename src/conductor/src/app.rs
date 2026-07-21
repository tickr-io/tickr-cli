use crate::build_pipeline::{start_build_worker, BuildExecutor, NixBuildExecutor};
use crate::nats_ingress;
use crate::relay;
use crate::signal_captures_cleanup;
use crate::submission_consumer;
use crate::system_tasks;
use crate::waits_on_signal_lifecycle;
use anyhow::{Context, Result};
use async_nats;
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub async fn run_conductor(shutdown_token: CancellationToken) -> Result<()> {
    println!("Starting conductor client...");

    // Resolve the Core DSL import search path used when evaluating submitted
    // Nickel source. Empty/unset is non-fatal: registration still accepts
    // requests, but `nickel export` cannot resolve `import "task.ncl"`.
    match crate::parser::nickel::dsl_import_path() {
        Some(paths) => println!("Core DSL search path ({}): {}", crate::parser::nickel::DSL_PATHS_ENV, paths),
        None => eprintln!(
            "WARNING: {} is unset or empty — workflow registration will fail to resolve the Core DSL until it is set",
            crate::parser::nickel::DSL_PATHS_ENV
        ),
    }

    // Resolve and verify the complete selected writer role before opening NATS
    // consumers, reconciliation loops, or the Compaction drain.
    let selection =
        tickr_proto::config::data_plane_sql().context("resolving data-plane SQL configuration")?;
    println!(
        "Opening {} data-plane SQL writer...",
        selection.implementation()
    );
    let definition_repository = Arc::new(
        crate::repository::configure_writer(selection)
            .await
            .context("opening selected data-plane SQL writer")?,
    );
    println!("Data-plane SQL writer schema verified.");

    // Connect to NATS only after SQL selection and verification succeed.
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;

    let stream_shutdown = shutdown_token.clone();
    let stream_definitions = Arc::clone(&definition_repository);
    let streaming_handle = tokio::spawn(async move {
        if let Err(e) = relay::run_streaming(stream_shutdown, stream_definitions).await {
            eprintln!("Streaming error: {}", e);
        }
    });

    // Compaction drain: consumes staged compaction jobs off the per-tenant
    // NATS work queue and performs the archival (log upload from the Log
    // staging stream, tickr-ctx scope read, three-table archive
    // transaction, signal-captures cleanup, log-subject purge). The relay
    // handler only stages + ACKs; any conductor instance can drain any
    // staged job, so a worker runs on every instance.
    let drain_shutdown = shutdown_token.clone();
    let drain_nats = nats.clone();
    let drain_repositories = Arc::clone(&definition_repository);
    let drain_storage = system_tasks::production_log_storage()?;
    let compaction_drain_handle = tokio::spawn(async move {
        if let Err(e) = system_tasks::run_compaction_drain(
            drain_nats,
            drain_repositories,
            drain_storage,
            drain_shutdown,
        )
        .await
        {
            eprintln!("compaction drain error: {}", e);
        }
    });

    // Rebuild the waits-on-signal subscription index from the selected
    // repository before starting steady-state consumers.
    if let Err(e) =
        waits_on_signal_lifecycle::rebuild_from_repository(definition_repository.as_ref()).await
    {
        eprintln!("waits-on-signal index rebuild failed at startup: {}", e);
    }

    // Boot-time reconciliation: republish a submission pointer per
    // workflow row currently at `Ready` BEFORE the submission consumer
    // subscribes. Bounds the commit-then-publish dual-write hazard
    // where the build pipeline's finalizer committed `Building -> Ready`
    // but the post-commit NATS publish dropped. Runs exactly once on
    // startup — no periodic reconciliation in steady state.
    if let Err(e) =
        submission_consumer::reconcile_orphan_ready_rows(definition_repository.as_ref(), &nats)
            .await
    {
        eprintln!("submission queue boot reconciliation failed: {}", e);
    }

    // Boot-time replay reconcile: re-drive any replay pipeline row a prior
    // conductor left `Materializing` (Trigger relayed but the ctx re-hydration
    // / release never landed before it died). Runs exactly once on startup,
    // following the orphan-ready-row precedent above — new machinery, not a
    // reuse. The sender it drives through is reused by the steady-state re-drive
    // loop below.
    let replay_sender: Arc<dyn crate::replay_pipeline::ReplayRelaySender> =
        Arc::new(crate::replay_pipeline::DefaultReplayRelaySender { nats: nats.clone() });
    match crate::replay_pipeline::reconcile_orphan_replay_rows(
        definition_repository.as_ref(),
        replay_sender.as_ref(),
    )
    .await
    {
        Ok(0) => {}
        Ok(n) => println!("replay boot reconciliation: re-drove {n} unsettled replay(s)"),
        Err(e) => eprintln!("replay boot reconciliation failed: {}", e),
    }

    // Rebuild the per-instance gate index from the configured coordinator.
    // An unavailable coordinator degrades to an empty index; relay updates
    // repopulate it after connectivity returns.
    let dispatched_count = crate::gate_index_lifecycle::rebuild_from_server(
        &tickr_proto::config::coordinator_http_url(),
        tickr_proto::TenantId::from_env(),
    )
    .await;
    if dispatched_count > 0 {
        println!(
            "gate_index rebuild: repopulated {} dispatched gate(s) from server",
            dispatched_count
        );
    }

    // Per-task build worker: consumes `TaskBuildJob` messages off the
    // build queue (one per task per workflow), invokes the production
    // Nix-shelling executor, records per-task outcomes, runs the
    // last-one-out finalizer, and (on the winning Ready flip) publishes
    // a submission pointer onto the submission queue.
    let build_worker_shutdown = shutdown_token.clone();
    let build_worker_nats = nats.clone();
    let build_worker_repositories = Arc::clone(&definition_repository);
    let build_executor: Arc<dyn BuildExecutor> = Arc::new(NixBuildExecutor);
    let build_worker_handle = tokio::spawn(async move {
        if let Err(e) = start_build_worker(
            build_worker_nats,
            build_worker_repositories,
            build_executor,
            build_worker_shutdown,
        )
        .await
        {
            eprintln!("build worker error: {}", e);
        }
    });

    // Patch build worker: the patch-keyed sibling of the per-task build
    // worker. Consumes `PatchTaskBuildJob` messages (one per never-built
    // task a Patch's `AddNode` introduces), builds via the same Nix
    // executor, records patch-keyed outcomes, and runs the patch finalizer
    // (build success ships the single validate+apply envelope; failure settles
    // the row BuildFailed conductor-internally, no envelope).
    let patch_build_shutdown = shutdown_token.clone();
    let patch_build_nats = nats.clone();
    let patch_build_repositories = Arc::clone(&definition_repository);
    let patch_build_executor: Arc<dyn BuildExecutor> = Arc::new(NixBuildExecutor);
    let patch_build_handle = tokio::spawn(async move {
        if let Err(e) = crate::patch_pipeline::start_patch_build_worker(
            patch_build_nats,
            patch_build_repositories,
            patch_build_executor,
            Arc::new(crate::patch_pipeline::DefaultPatchRelaySender),
            patch_build_shutdown,
        )
        .await
        {
            eprintln!("patch build worker error: {}", e);
        }
    });

    // Submission consumer: subscribes to the submission queue with
    // queue-group semantics across replicas. Ships the SubmitWorkflow
    // envelope cross-plane via the relay and flips the workflow row
    // `Ready -> Submitted`. The boot-time reconciliation scan above
    // republishes any orphan Ready rows so the consumer picks them up
    // on startup.
    let submission_shutdown = shutdown_token.clone();
    let submission_nats = nats.clone();
    let submission_repositories = Arc::clone(&definition_repository);
    let submission_consumer_handle = tokio::spawn(async move {
        if let Err(e) = submission_consumer::start_submission_consumer(
            submission_nats,
            submission_repositories,
            submission_shutdown,
        )
        .await
        {
            eprintln!("submission consumer error: {}", e);
        }
    });

    // NATS-side ingress translator. Consumes v=1 envelopes from
    // `tickr.external.signals`, decodes them, mints signal_id, applies the
    // idempotency cache, and forwards Signals over the existing relay
    // outbound channel. Single durable pull consumer per conductor;
    // serial processing matches the v1 throughput posture.
    let ingress_shutdown = shutdown_token.clone();
    let ingress_nats = nats.clone();
    let ingress_repositories = Arc::clone(&definition_repository);
    let nats_ingress_handle = tokio::spawn(async move {
        if let Err(e) =
            nats_ingress::run_translator(ingress_nats, ingress_repositories, ingress_shutdown).await
        {
            eprintln!("NATS ingress translator error: {}", e);
        }
    });

    // API command-bus subscriber. Consumes `ApiCommandRequest`s from the
    // API component over NATS core request/reply on `tickr.api.commands` and
    // dispatches each to the matching write pipeline (register / trigger /
    // cancel / wakeup), replying with an `ApiCommandResponse`. Serial
    // processing matches the conductor's other NATS subscribers.
    let api_commands_shutdown = shutdown_token.clone();
    let api_commands_state = crate::api_commands_consumer::ApiCommandsState {
        definition_repository: Arc::clone(&definition_repository),
        nats: nats.clone(),
        relay_sender: Arc::new(crate::wakeup_translator::DefaultRelaySender),
        patch_relay_sender: Arc::new(crate::patch_pipeline::DefaultPatchRelaySender),
        gate_index: crate::gate_index_lifecycle::gate_index(),
    };
    let api_commands_handle = tokio::spawn(async move {
        if let Err(e) =
            crate::api_commands_consumer::start(api_commands_state, api_commands_shutdown).await
        {
            eprintln!("API command-bus subscriber error: {}", e);
        }
    });

    // Pull tenant-visible coordinator Events through the selected repository.
    // Concurrent replicas may duplicate fetches; atomic idempotent insertion
    // preserves one contiguous public projection.
    let events_pull_shutdown = shutdown_token.clone();
    let events_pull_repositories = Arc::clone(&definition_repository);
    let events_pull_handle = tokio::spawn(system_tasks::run_events_pull(
        events_pull_repositories,
        tickr_proto::config::coordinator_http_url(),
        // Pull only this conductor's own tenant slice from the shared archive.
        tickr_proto::TenantId::from_env().as_uuid(),
        events_pull_shutdown,
    ));

    // Patch re-drive loop: re-sends unsettled patch lifecycle rows
    // (persist-at-ingress + re-drive-to-settlement) until the server's
    // outcome envelope closes them. Safe under redelivery — the patch_key
    // dedup absorbs duplicates on both sides.
    let patch_redrive_shutdown = shutdown_token.clone();
    let patch_redrive_repositories = Arc::clone(&definition_repository);
    let patch_redrive_handle = tokio::spawn(crate::patch_pipeline::run_patch_redrive(
        patch_redrive_repositories,
        Arc::new(crate::patch_pipeline::DefaultPatchRelaySender),
        patch_redrive_shutdown,
    ));

    // Replay re-drive loop: the patch re-drive's sibling for replay pipeline
    // rows. Re-drives any `Materializing` row whose materialise + re-hydrate +
    // release didn't complete (persist-at-ingress + re-drive-to-settlement)
    // until it settles `Released`. Idempotent under redelivery — the
    // deterministic instance id and verbatim ctx puts absorb duplicates.
    let replay_redrive_shutdown = shutdown_token.clone();
    let replay_redrive_repositories = Arc::clone(&definition_repository);
    let replay_redrive_handle = tokio::spawn(crate::replay_pipeline::run_replay_redrive(
        replay_redrive_repositories,
        Arc::clone(&replay_sender),
        replay_redrive_shutdown,
    ));

    // Periodic grace-window sweep for terminal `signal_captures` rows. The
    // compaction hook flips `terminal_at` at run-terminal time; this sweep
    // deletes the row after the grace window so the audit trail survives
    // briefly but storage doesn't grow unbounded.
    let (sweep_shutdown_tx, sweep_shutdown_rx) = watch::channel(false);
    let sweep_repositories = Arc::clone(&definition_repository);
    let sweep_nats = nats.clone();
    let sweep_handle = signal_captures_cleanup::spawn_periodic_sweep(
        sweep_repositories,
        sweep_nats,
        signal_captures_cleanup::DEFAULT_SWEEP_INTERVAL,
        chrono::Duration::from_std(signal_captures_cleanup::DEFAULT_GRACE)
            .expect("DEFAULT_GRACE fits in chrono::Duration"),
        sweep_shutdown_rx,
    );

    shutdown_token.cancelled().await;
    println!("Shutdown signal received, stopping conductor...");

    // Signal the periodic sweep to shut down.
    let _ = sweep_shutdown_tx.send(true);

    // Wait for all tasks to finish
    if let Err(e) = streaming_handle.await {
        eprintln!("Error waiting for streaming task: {}", e);
    }

    if let Err(e) = compaction_drain_handle.await {
        eprintln!("Error waiting for compaction drain: {}", e);
    }

    if let Err(e) = build_worker_handle.await {
        eprintln!("Error waiting for build worker: {}", e);
    }

    if let Err(e) = patch_build_handle.await {
        eprintln!("Error waiting for patch build worker: {}", e);
    }

    if let Err(e) = submission_consumer_handle.await {
        eprintln!("Error waiting for submission consumer: {}", e);
    }

    if let Err(e) = sweep_handle.await {
        eprintln!("Error waiting for signal_captures sweep: {}", e);
    }

    if let Err(e) = events_pull_handle.await {
        eprintln!("Error waiting for events pull cycle: {}", e);
    }

    if let Err(e) = nats_ingress_handle.await {
        eprintln!("Error waiting for NATS ingress translator: {}", e);
    }

    if let Err(e) = api_commands_handle.await {
        eprintln!("Error waiting for API command-bus subscriber: {}", e);
    }

    if let Err(e) = patch_redrive_handle.await {
        eprintln!("Error waiting for patch re-drive loop: {}", e);
    }

    if let Err(e) = replay_redrive_handle.await {
        eprintln!("Error waiting for replay re-drive loop: {}", e);
    }

    definition_repository.close().await;

    println!("Conductor stopped gracefully.");
    Ok(())
}
