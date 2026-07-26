//! SignalAppliedNotifier laws shared by fresh all-NATS and Tickr Lite.

#![cfg(not(madsim))]

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::cancel_pipeline::{
    process_cancel, process_cancel_local, CancelOutcome, CancelRequest, CancelTargetBody,
};
use tickr_conductor::signal_applied_notifier::{
    all_nats_signal_applied_notifications, signal_applied_notifications,
    SharedSignalAppliedReconciliationStream, SignalAppliedNotificationRoles,
};
use tickr_migrations::backend::{RepositoryFactory, WriterRepositoryBundle};
use tickr_proto::codec::signal::decode_signal;
use tickr_proto::config::DataPlaneSql;
use tickr_proto::ConductorRelayMessage;
use tokio::sync::mpsc;
use uuid::Uuid;

mod common;

#[derive(Clone, Copy, Debug)]
enum BackendKind {
    AllNats,
    Lite,
}

enum LawBackend {
    AllNats {
        writer: Arc<WriterRepositoryBundle>,
        nats: async_nats::Client,
        notifications: SharedSignalAppliedReconciliationStream,
        _roles: SignalAppliedNotificationRoles,
        _container: testcontainers_modules::testcontainers::ContainerAsync<Nats>,
        _database: common::DbGuard,
    },
    Lite {
        writer: Arc<WriterRepositoryBundle>,
        notifications: SharedSignalAppliedReconciliationStream,
        _roles: SignalAppliedNotificationRoles,
        _directory: TempDir,
    },
}

impl LawBackend {
    async fn start(kind: BackendKind) -> Option<Self> {
        match kind {
            BackendKind::AllNats => {
                let (database, pool) = common::test_db().await?;
                let command = NatsServerCmd::default().with_jetstream();
                let container = match Nats::default().with_cmd(&command).start().await {
                    Ok(container) => container,
                    Err(error) => {
                        eprintln!("skipping fresh all-NATS Signal notifier laws: {error}");
                        return None;
                    }
                };
                let port = container.get_host_port_ipv4(4222).await.ok()?;
                let nats = async_nats::connect(format!("nats://127.0.0.1:{port}"))
                    .await
                    .ok()?;
                let roles = all_nats_signal_applied_notifications(nats.clone())
                    .await
                    .ok()?;
                Some(Self::AllNats {
                    writer: Arc::new(WriterRepositoryBundle::from_postgres_pool(pool)),
                    nats,
                    notifications: roles.reconciliation(),
                    _roles: roles,
                    _container: container,
                    _database: database,
                })
            }
            BackendKind::Lite => {
                let directory = TempDir::new().unwrap();
                let url = format!(
                    "sqlite://{}",
                    directory.path().join("signal-notifier-law.db").display()
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
                let writer = RepositoryFactory::new(DataPlaneSql::Sqlite { url })
                    .open_writer()
                    .await
                    .unwrap();
                let (notifier, notifications) =
                    signal_applied_notifications(NonZeroUsize::new(1).unwrap());
                let roles = SignalAppliedNotificationRoles::new(notifier, notifications);
                Some(Self::Lite {
                    writer: Arc::new(writer),
                    notifications: roles.reconciliation(),
                    _roles: roles,
                    _directory: directory,
                })
            }
        }
    }

    fn writer(&self) -> Arc<WriterRepositoryBundle> {
        match self {
            Self::AllNats { writer, .. } | Self::Lite { writer, .. } => Arc::clone(writer),
        }
    }

    async fn cancel(&self, request: CancelRequest) -> Result<CancelOutcome, String> {
        match self {
            Self::AllNats {
                writer,
                nats,
                notifications,
                ..
            } => process_cancel(writer, nats, notifications.as_ref(), request)
                .await
                .map_err(|error| error.to_string()),
            Self::Lite {
                writer,
                notifications,
                ..
            } => process_cancel_local(writer, notifications.as_ref(), request)
                .await
                .map_err(|error| error.to_string()),
        }
    }
}

async fn exercise_backend(kind: BackendKind) {
    let Some(backend) = LawBackend::start(kind).await else {
        return;
    };
    let (relay_tx, mut relay_rx) = mpsc::channel::<ConductorRelayMessage>(1);
    tickr_conductor::relay::init_relay_tx(relay_tx).await;

    let materialization_repository = backend.writer();
    let materialize = tokio::spawn(async move {
        let message = tokio::time::timeout(Duration::from_secs(2), relay_rx.recv())
            .await
            .expect("cancel reaches relay")
            .expect("relay remains open");
        let signal = decode_signal(&message.payload).expect("decode Cancel Signal");
        let signal_id = Uuid::parse_str(&signal.signal_id).expect("Signal identity is a UUID");

        assert!(tickr_conductor::signal_cancels::materialize(
            materialization_repository.as_ref(),
            signal_id,
            7,
        )
        .await
        .expect("persist materialization"));
        assert!(!tickr_conductor::signal_cancels::materialize(
            materialization_repository.as_ref(),
            signal_id,
            99,
        )
        .await
        .expect("duplicate materialization converges"));
        signal_id
    });

    let started = tokio::time::Instant::now();
    let outcome = backend
        .cancel(CancelRequest {
            target: CancelTargetBody::ByTag {
                filter: HashMap::from([("env".to_owned(), "prod".to_owned())]),
            },
            note: Some("role law".to_owned()),
            idempotency_key: None,
        })
        .await
        .unwrap_or_else(|error| panic!("{kind:?} cancel failed: {error}"));
    let signal_id = materialize.await.expect("materialization worker");

    match outcome {
        CancelOutcome::ByTag {
            signal_id: outcome_id,
            instances_matched,
        } => {
            assert_eq!(outcome_id, signal_id);
            assert_eq!(instances_matched, 7);
        }
        _ => panic!("{kind:?} returned the wrong cancel outcome"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{kind:?} waited for a suppressed notification instead of bounded reconciliation"
    );
    assert_eq!(
        tickr_conductor::signal_cancels::materialized_count(backend.writer().as_ref(), signal_id,)
            .await
            .expect("read durable materialization"),
        Some(7)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suppressed_notifications_reconcile_from_durable_signal_state_for_every_backend() {
    for kind in [BackendKind::AllNats, BackendKind::Lite] {
        exercise_backend(kind).await;
    }
}
