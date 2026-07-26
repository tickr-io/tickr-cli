use crate::build_pipeline::{BuildExecutor, NixBuildExecutor};
use crate::ingress_idempotency::IngressCoordinator;
use crate::lifecycle_work::{all_nats_lifecycle_work, LifecycleWork};
use crate::nats_ingress;
use crate::relay;
use crate::signal_applied_notifier::{
    all_nats_signal_applied_notifications, SignalAppliedNotificationRoles,
};
use crate::signal_captures_cleanup;
use crate::submission_consumer;
use crate::system_tasks;
use crate::waits_on_signal_lifecycle;
use anyhow::{Context, Result};
use async_nats;
use std::sync::Arc;
use tickr_migrations::scope_repository::ScopeStore;
use tickr_proto::coord::{
    CompactionStaging, TaskCancellationAckConsumer, TaskCancellationPublisher,
    TaskDispatchPublisher, TaskEventConsumer, TaskEventWriter,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub async fn open_conductor_repositories(
) -> Result<Arc<tickr_migrations::backend::WriterRepositoryBundle>> {
    let selection =
        tickr_proto::config::data_plane_sql().context("resolving data-plane SQL configuration")?;
    println!(
        "Opening {} data-plane SQL writer...",
        selection.implementation()
    );
    let repositories = Arc::new(
        crate::repository::configure_writer(selection)
            .await
            .context("opening selected data-plane SQL writer")?,
    );
    println!("Data-plane SQL writer schema verified.");
    Ok(repositories)
}

pub async fn run_conductor(shutdown_token: CancellationToken) -> Result<()> {
    let repositories = open_conductor_repositories().await?;
    let command_bus = Arc::new(
        crate::api_commands_consumer::NatsCommandBusConsumer::connect(
            &tickr_proto::config::nats_url(),
        )
        .await?,
    );
    run_conductor_with_repositories(shutdown_token, repositories, command_bus).await
}

pub async fn run_conductor_with_repositories(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    command_bus: Arc<dyn crate::api_commands_consumer::CommandBusConsumer>,
) -> Result<()> {
    run_conductor_composed(
        shutdown_token,
        definition_repository,
        command_bus,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub async fn run_conductor_with_task_events(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    command_bus: Arc<dyn crate::api_commands_consumer::CommandBusConsumer>,
    task_event_consumer: Arc<dyn TaskEventConsumer>,
    task_event_writer: Arc<dyn TaskEventWriter>,
) -> Result<()> {
    run_conductor_composed(
        shutdown_token,
        definition_repository,
        command_bus,
        Some((task_event_consumer, task_event_writer)),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub async fn run_conductor_with_roles(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    command_bus: Arc<dyn crate::api_commands_consumer::CommandBusConsumer>,
    task_event_consumer: Arc<dyn TaskEventConsumer>,
    task_event_writer: Arc<dyn TaskEventWriter>,
    task_dispatch: Arc<dyn TaskDispatchPublisher>,
    task_cancellation: Arc<dyn TaskCancellationPublisher>,
    cancellation_acknowledgements: Arc<dyn TaskCancellationAckConsumer>,
    compaction_staging: Arc<dyn CompactionStaging>,
    log_streams: Arc<dyn system_tasks::CompactionLogStaging>,
    scope_store: Arc<dyn ScopeStore>,
    event_ingress: Arc<dyn nats_ingress::EventIngress>,
    ingress_coordinator: IngressCoordinator,
    signal_applied: SignalAppliedNotificationRoles,
    lifecycle_work: LifecycleWork,
) -> Result<()> {
    run_conductor_composed(
        shutdown_token,
        definition_repository,
        command_bus,
        Some((task_event_consumer, task_event_writer)),
        Some(task_dispatch),
        Some((task_cancellation, cancellation_acknowledgements)),
        Some((compaction_staging, log_streams, scope_store)),
        Some((event_ingress, ingress_coordinator)),
        Some(signal_applied),
        Some(lifecycle_work),
    )
    .await
}

async fn run_conductor_composed(
    shutdown_token: CancellationToken,
    definition_repository: Arc<tickr_migrations::backend::WriterRepositoryBundle>,
    command_bus: Arc<dyn crate::api_commands_consumer::CommandBusConsumer>,
    task_events: Option<(Arc<dyn TaskEventConsumer>, Arc<dyn TaskEventWriter>)>,
    task_dispatch: Option<Arc<dyn TaskDispatchPublisher>>,
    task_cancellation: Option<(
        Arc<dyn TaskCancellationPublisher>,
        Arc<dyn TaskCancellationAckConsumer>,
    )>,
    compaction: Option<(
        Arc<dyn CompactionStaging>,
        Arc<dyn system_tasks::CompactionLogStaging>,
        Arc<dyn ScopeStore>,
    )>,
    event_ingress: Option<(Arc<dyn nats_ingress::EventIngress>, IngressCoordinator)>,
    signal_applied: Option<SignalAppliedNotificationRoles>,
    lifecycle_work: Option<LifecycleWork>,
) -> Result<()> {
    println!("Starting conductor client...");

    // Resolve the Core DSL import search path used when evaluating submitted
    // Nickel source. Empty/unset is non-fatal: registration still accepts
    // requests, but `nickel export` cannot resolve `import "task.ncl"`.
    match crate::parser::nickel::dsl_import_path() {
        Some(paths) => println!(
            "Core DSL search path ({}): {}",
            crate::parser::nickel::DSL_PATHS_ENV,
            paths
        ),
        None => eprintln!(
            "WARNING: {} is unset or empty — workflow registration will fail to resolve the Core DSL until it is set",
            crate::parser::nickel::DSL_PATHS_ENV
        ),
    }

    // Formation resources are admitted by the process composer only after
    // this writer role has proved its schema.
    let nats = async_nats::connect(tickr_proto::config::nats_url()).await?;
    let signal_applied = match signal_applied {
        Some(signal_applied) => signal_applied,
        None => all_nats_signal_applied_notifications(nats.clone())
            .await
            .context("constructing all-NATS SignalAppliedNotifier adapter")?,
    };
    let signal_applied_notifier = signal_applied.notifier();
    let signal_applied_notifications = signal_applied.reconciliation();

    let lifecycle_work = match lifecycle_work {
        Some(lifecycle_work) => lifecycle_work,
        None => all_nats_lifecycle_work(&nats)
            .await
            .context("constructing all-NATS LifecycleWork adapter")?,
    };
    let (mut lifecycle_source, lifecycle_wakeups, lifecycle_inputs) = lifecycle_work.into_parts();
    let (
        definition_build_notifications,
        patch_notifications,
        submission_notifications,
        lifecycle_claim_admission,
    ) = lifecycle_inputs.into_parts();
    let lifecycle_wakeup_shutdown = shutdown_token.clone();
    let lifecycle_wakeup_handle = tokio::spawn(async move {
        if let Err(error) = lifecycle_source
            .run(lifecycle_wakeups, lifecycle_wakeup_shutdown)
            .await
        {
            eprintln!("LifecycleWork wakeup source error: {error}");
        }
    });

    let stream_shutdown = shutdown_token.clone();
    let stream_definitions = Arc::clone(&definition_repository);

    let stream_task_events = task_events;
    let stream_task_dispatch = task_dispatch;
    let stream_task_cancellation = task_cancellation;
    let stream_compaction_staging = compaction.as_ref().map(|(staging, _, _)| staging.clone());
    let stream_signal_applied_notifier = Arc::clone(&signal_applied_notifier);
    let streaming_handle = tokio::spawn(async move {
        let result = match (
            stream_task_events,
            stream_task_dispatch,
            stream_task_cancellation,
            stream_compaction_staging,
        ) {
            (
                Some((consumer, writer)),
                Some(dispatch),
                Some((cancellation, acknowledgements)),
                Some(compaction_staging),
            ) => {
                relay::run_streaming_with_roles(
                    stream_shutdown,
                    stream_definitions,
                    consumer,
                    writer,
                    dispatch,
                    cancellation,
                    acknowledgements,
                    compaction_staging,
                    stream_signal_applied_notifier,
                )
                .await
            }
            (Some((consumer, writer)), None, None, None) => {
                relay::run_streaming_with_task_events(
                    stream_shutdown,
                    stream_definitions,
                    consumer,
                    writer,
                    stream_signal_applied_notifier,
                )
                .await
            }
            (None, None, None, None) => {
                relay::run_streaming(
                    stream_shutdown,
                    stream_definitions,
                    stream_signal_applied_notifier,
                )
                .await
            }
            _ => unreachable!("selected Task and Compaction roles require the complete role set"),
        };
        if let Err(e) = result {
            eprintln!("Streaming error: {}", e);
        }
    });

    let selected_ingress_scope_store = compaction
        .as_ref()
        .map(|(_, _, scope_store)| scope_store.clone());

    // Every Conductor drains through the selected Compaction, Log, and Scope
    // role interfaces; fresh all-NATS retains its existing adapter and path.
    let drain_shutdown = shutdown_token.clone();
    let drain_repositories = Arc::clone(&definition_repository);
    let drain_storage = system_tasks::production_log_storage()?;
    let drain_nats = nats.clone();
    let compaction_drain_handle = tokio::spawn(async move {
        let result = match compaction {
            Some((staging, logs, scopes)) => {
                system_tasks::run_selected_compaction_drain(
                    staging,
                    logs,
                    scopes,
                    drain_repositories,
                    drain_storage,
                    drain_shutdown,
                )
                .await
            }
            None => {
                system_tasks::run_compaction_drain(
                    drain_nats,
                    drain_repositories,
                    drain_storage,
                    drain_shutdown,
                )
                .await
            }
        };
        if let Err(error) = result {
            eprintln!("compaction drain error: {error}");
        }
    });

    // Rebuild the waits-on-signal subscription index from the selected
    // repository before starting steady-state consumers.
    if let Err(e) =
        waits_on_signal_lifecycle::rebuild_from_repository(definition_repository.as_ref()).await
    {
        eprintln!("waits-on-signal index rebuild failed at startup: {}", e);
    }

    // Boot-time replay reconcile: re-drive any replay pipeline row a prior
    // conductor left `Materializing` (Trigger relayed but the ctx re-hydration
    // / release never landed before it died). Runs exactly once on startup,
    // independently from the steady-state re-drive loop whose sender it shares.
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

    // Definition-build reconciliation receives only its role wakeup stream and
    // the shared claim-admission fence. Startup and periodic bounded SQL scans
    // remain authoritative when the selected notification source is silent.
    let build_worker_shutdown = shutdown_token.clone();
    let build_worker_repositories = Arc::clone(&definition_repository);
    let build_executor: Arc<dyn BuildExecutor> = Arc::new(NixBuildExecutor);
    let build_claim_admission = Arc::clone(&lifecycle_claim_admission);
    let build_worker_handle = tokio::spawn(async move {
        if let Err(e) =
            crate::build_pipeline::start_local_definition_build_worker_with_claim_admission(
                build_worker_repositories,
                build_executor,
                format!("conductor-build-{}", uuid::Uuid::new_v4()),
                definition_build_notifications,
                build_claim_admission,
                crate::build_pipeline::LocalDefinitionBuildWorkerConfig::default(),
                build_worker_shutdown,
            )
            .await
        {
            eprintln!("build worker error: {}", e);
        }
    });

    // Patch-build and Patch-lifecycle share one advisory wakeup but perform
    // separate read-only discovery and fenced SQL lease operations.
    let patch_build_shutdown = shutdown_token.clone();
    let patch_build_repositories = Arc::clone(&definition_repository);
    let patch_build_executor: Arc<dyn BuildExecutor> = Arc::new(NixBuildExecutor);
    let patch_claim_admission = Arc::clone(&lifecycle_claim_admission);
    let patch_build_handle = tokio::spawn(async move {
        if let Err(e) = crate::patch_pipeline::local::start_local_patch_worker_with_claim_admission(
            patch_build_repositories,
            patch_build_executor,
            Arc::new(crate::patch_pipeline::DefaultPatchRelaySender),
            format!("conductor-patch-{}", uuid::Uuid::new_v4()),
            patch_notifications,
            patch_claim_admission,
            crate::patch_pipeline::local::PatchReconcilerConfig::default(),
            patch_build_shutdown,
        )
        .await
        {
            eprintln!("patch build worker error: {}", e);
        }
    });

    // Definition submission likewise treats the notification as an early-scan
    // request and conditionally settles only its committed SQL lease.
    let submission_shutdown = shutdown_token.clone();
    let submission_repositories = Arc::clone(&definition_repository);
    let submission_consumer_handle = tokio::spawn(async move {
        if let Err(e) =
            submission_consumer::start_local_definition_submission_worker_with_claim_admission(
                submission_repositories,
                format!("conductor-submission-{}", uuid::Uuid::new_v4()),
                submission_notifications,
                lifecycle_claim_admission,
                submission_consumer::LocalDefinitionSubmissionWorkerConfig::default(),
                submission_shutdown,
            )
            .await
        {
            eprintln!("submission consumer error: {}", e);
        }
    });

    // Transport selection is complete before the consumer starts. The
    // steady-state path receives no Redis or NATS client.
    let (event_ingress, ingress_coordinator, ingress_working_set): (
        Arc<dyn nats_ingress::EventIngress>,
        IngressCoordinator,
        Arc<dyn nats_ingress::IngressWorkingSet>,
    ) = match event_ingress {
        Some((event_ingress, ingress_coordinator)) => (
            event_ingress,
            ingress_coordinator,
            Arc::new(nats_ingress::ScopeStoreIngressWorkingSet::new(
                selected_ingress_scope_store
                    .context("selected EventIngress requires the selected ScopeStore")?,
            )),
        ),
        None => {
            let event_ingress = Arc::new(
                nats_ingress::NatsEventIngress::connect(&nats)
                    .await
                    .context("constructing all-NATS EventIngress adapter")?,
            );
            let ingress_coordinator = event_ingress.ingress_coordinator();
            (
                event_ingress,
                ingress_coordinator,
                Arc::new(nats_ingress::NatsIngressWorkingSet::new(nats.clone())),
            )
        }
    };
    let ingress_shutdown = shutdown_token.clone();
    let ingress_repositories = Arc::clone(&definition_repository);
    let event_ingress_handle = tokio::spawn(async move {
        if let Err(error) = nats_ingress::run_event_consumer(
            event_ingress,
            ingress_coordinator,
            ingress_repositories,
            ingress_working_set,
            Arc::new(nats_ingress::GlobalRelaySender),
            ingress_shutdown,
        )
        .await
        {
            eprintln!("EventIngress consumer error: {error}");
        }
    });

    // Serve API Commands through the formation-selected request/reply consumer
    // and dispatch each to the matching write pipeline. The shared dispatcher
    // preserves the existing typed response and HTTP-equivalent status.
    let api_commands_shutdown = shutdown_token.clone();
    let api_commands_state = crate::api_commands_consumer::ApiCommandsState {
        definition_repository: Arc::clone(&definition_repository),
        nats: nats.clone(),
        signal_applied_notifications,
        relay_sender: Arc::new(crate::wakeup_translator::DefaultRelaySender),
        patch_relay_sender: Arc::new(crate::patch_pipeline::DefaultPatchRelaySender),
        gate_index: crate::gate_index_lifecycle::gate_index(),
    };
    let api_commands_handler: Arc<dyn crate::api_commands_consumer::CommandBusHandler> =
        Arc::new(api_commands_state);
    let api_commands_handle = tokio::spawn(async move {
        if let Err(e) = command_bus
            .serve(api_commands_handler, api_commands_shutdown)
            .await
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

    if let Err(e) = lifecycle_wakeup_handle.await {
        eprintln!("Error waiting for LifecycleWork wakeup source: {}", e);
    }

    if let Err(e) = sweep_handle.await {
        eprintln!("Error waiting for signal_captures sweep: {}", e);
    }

    if let Err(e) = events_pull_handle.await {
        eprintln!("Error waiting for events pull cycle: {}", e);
    }

    if let Err(e) = event_ingress_handle.await {
        eprintln!("Error waiting for EventIngress consumer: {}", e);
    }

    if let Err(e) = api_commands_handle.await {
        eprintln!("Error waiting for API command-bus subscriber: {}", e);
    }

    if let Err(e) = replay_redrive_handle.await {
        eprintln!("Error waiting for replay re-drive loop: {}", e);
    }

    definition_repository.close().await;

    println!("Conductor stopped gracefully.");
    Ok(())
}
