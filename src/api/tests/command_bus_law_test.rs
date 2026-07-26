//! Observable Command-bus laws shared by the real all-NATS and local transports.

#![cfg(not(madsim))]

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use prost::Message as _;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_api::commands::client::{bus_error_response, BusError, CommandBus};
use tickr_api::commands::local::LocalCommandBusConfig;
use tickr_proto::coord::command_bus::DEFAULT_MAX_PAYLOAD_BYTES;
use tickr_proto::tickr_api as api;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
enum BackendKind {
    AllNats,
    Local,
}

#[derive(Clone, Copy)]
enum HandlerMode {
    Success,
    TypedFailure,
    Malformed,
    Blocked,
}

#[derive(Clone)]
struct LawHandler {
    mode: Arc<Mutex<HandlerMode>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl LawHandler {
    fn new() -> Self {
        Self {
            mode: Arc::new(Mutex::new(HandlerMode::Success)),
            calls: Arc::new(AtomicUsize::new(0)),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn set_mode(&self, mode: HandlerMode) {
        *self.mode.lock().expect("law handler mutex") = mode;
    }

    fn reset_calls(&self) {
        self.calls.store(0, Ordering::SeqCst);
    }

    async fn handle(&self, _payload: Vec<u8>) -> Vec<u8> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = *self.mode.lock().expect("law handler mutex");
        match mode {
            HandlerMode::Success => success_response(),
            HandlerMode::TypedFailure => api::ApiCommandResponse {
                status_code: 422,
                payload: Some(api::api_command_response::Payload::Error(
                    api::ErrorPayload {
                        code: api::CommandErrorCode::BadRequest as i32,
                        message: "typed command failure".to_string(),
                    },
                )),
            }
            .encode_to_vec(),
            HandlerMode::Malformed => vec![0xff],
            HandlerMode::Blocked => {
                self.entered.notify_one();
                self.release.notified().await;
                success_response()
            }
        }
    }
}

struct RunningBackend {
    bus: CommandBus,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    nats: Option<async_nats::Client>,
    _container: Option<testcontainers_modules::testcontainers::ContainerAsync<Nats>>,
}

impl RunningBackend {
    async fn start(
        kind: BackendKind,
        handler: LawHandler,
        in_flight_limit: NonZeroUsize,
    ) -> Option<Self> {
        let cancel = CancellationToken::new();
        match kind {
            BackendKind::Local => {
                let config = LocalCommandBusConfig {
                    capacity: in_flight_limit,
                    max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_PAYLOAD_BYTES)
                        .expect("non-zero constant"),
                };
                let (bus, writer) = CommandBus::local(config);
                let writer_cancel = cancel.clone();
                let task = tokio::spawn(async move {
                    writer
                        .run(writer_cancel, move |payload| {
                            let handler = handler.clone();
                            async move { handler.handle(payload).await }
                        })
                        .await;
                });
                Some(Self {
                    bus,
                    cancel,
                    task: Some(task),
                    nats: None,
                    _container: None,
                })
            }
            BackendKind::AllNats => {
                let command = NatsServerCmd::default().with_jetstream();
                let container = match Nats::default().with_cmd(&command).start().await {
                    Ok(container) => container,
                    Err(error) => {
                        eprintln!("skipping all-NATS Command-bus laws: {error}");
                        return None;
                    }
                };
                let port = container.get_host_port_ipv4(4222).await.ok()?;
                let nats = async_nats::connect(format!("nats://127.0.0.1:{port}"))
                    .await
                    .ok()?;
                let bus = CommandBus::nats_with_in_flight_limit(nats.clone(), in_flight_limit);
                let consumer_cancel = cancel.clone();
                let consumer_nats = nats.clone();
                let task = tokio::spawn(async move {
                    tickr_conductor::api_commands_consumer::start_with_handler(
                        consumer_nats,
                        consumer_cancel,
                        move |payload| {
                            let handler = handler.clone();
                            async move { handler.handle(payload).await }
                        },
                    )
                    .await
                    .expect("all-NATS command consumer");
                });

                let mut ready = false;
                for _ in 0..20 {
                    match bus.send(ping_request(), Duration::from_millis(250)).await {
                        Ok(_) => {
                            ready = true;
                            break;
                        }
                        Err(BusError::Unavailable) => {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                        Err(error) => panic!("all-NATS readiness failed: {error:?}"),
                    }
                }
                assert!(ready, "all-NATS command consumer did not become live");
                Some(Self {
                    bus,
                    cancel,
                    task: Some(task),
                    nats: Some(nats),
                    _container: Some(container),
                })
            }
        }
    }

    async fn stop(mut self) -> Self {
        self.cancel.cancel();
        self.task
            .take()
            .expect("command backend task")
            .await
            .expect("command backend task");
        if let Some(nats) = &self.nats {
            nats.flush().await.expect("flush consumer unsubscribe");
        }
        self
    }
}

fn ping_request() -> api::ApiCommandRequest {
    api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Ping(api::PingRequest {})),
    }
}

fn success_response() -> Vec<u8> {
    api::ApiCommandResponse {
        status_code: 200,
        payload: Some(api::api_command_response::Payload::Ping(
            api::PingPayload {},
        )),
    }
    .encode_to_vec()
}

async fn exercise_backend(kind: BackendKind) {
    let handler = LawHandler::new();
    let Some(backend) = RunningBackend::start(
        kind,
        handler.clone(),
        NonZeroUsize::new(2).expect("non-zero constant"),
    )
    .await
    else {
        return;
    };
    handler.reset_calls();

    let success = backend
        .bus
        .send(ping_request(), Duration::from_secs(1))
        .await
        .expect("typed success");
    assert_eq!(success.status_code, 200, "backend: {kind:?}");

    handler.set_mode(HandlerMode::TypedFailure);
    let failure = backend
        .bus
        .send(ping_request(), Duration::from_secs(1))
        .await
        .expect("typed failure reply");
    assert_eq!(failure.status_code, 422, "backend: {kind:?}");
    assert!(matches!(
        failure.payload,
        Some(api::api_command_response::Payload::Error(_))
    ));

    handler.set_mode(HandlerMode::Malformed);
    assert!(matches!(
        backend
            .bus
            .send(ping_request(), Duration::from_secs(1))
            .await,
        Err(BusError::Malformed)
    ));

    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let duplicate_correlation = Uuid::new_v4();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(
                ping_request(),
                Duration::from_secs(1),
                duplicate_correlation,
            )
            .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(
                ping_request(),
                Duration::from_secs(1),
                duplicate_correlation,
            )
            .await,
        Err(BusError::DuplicateCorrelation)
    ));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);

    handler.release.notify_one();
    first
        .await
        .expect("blocked request task")
        .expect("first reply");

    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let first_correlation = Uuid::new_v4();
    let expired_correlation = Uuid::new_v4();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(1), first_correlation)
                .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(
                ping_request(),
                Duration::from_millis(20),
                expired_correlation,
            )
            .await,
        Err(BusError::Timeout)
    ));
    handler.release.notify_one();
    first
        .await
        .expect("blocked request task")
        .expect("first reply");
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        1,
        "expired command reached mutation handler on {kind:?}"
    );

    handler.set_mode(HandlerMode::Success);
    let mut cleaned = false;
    for _ in 0..20 {
        let reply = backend
            .bus
            .send_with_correlation(ping_request(), Duration::from_secs(1), expired_correlation)
            .await
            .expect("correlation cleanup reply");
        if reply.status_code == 200 {
            cleaned = true;
            break;
        }
        assert_eq!(reply.status_code, 409, "backend: {kind:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cleaned, "expired correlation was not cleaned on {kind:?}");

    let oversized = api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Register(
            api::RegisterRequest {
                nickel_source: "x".repeat(DEFAULT_MAX_PAYLOAD_BYTES + 1),
                namespace: String::new(),
            },
        )),
    };
    assert!(matches!(
        backend.bus.send(oversized, Duration::from_secs(1)).await,
        Err(BusError::TooLarge)
    ));

    let stopped = backend.stop().await;
    let unavailable = stopped
        .bus
        .send(ping_request(), Duration::from_millis(250))
        .await;
    assert!(
        matches!(unavailable, Err(BusError::Unavailable)),
        "stopped backend remained routable on {kind:?}: {unavailable:?}"
    );
}

async fn exercise_saturation(kind: BackendKind) {
    let handler = LawHandler::new();
    let Some(backend) = RunningBackend::start(
        kind,
        handler.clone(),
        NonZeroUsize::new(1).expect("non-zero constant"),
    )
    .await
    else {
        return;
    };
    handler.set_mode(HandlerMode::Blocked);
    handler.reset_calls();
    let entered = handler.entered.notified();
    let first = {
        let bus = backend.bus.clone();
        tokio::spawn(async move {
            bus.send_with_correlation(ping_request(), Duration::from_secs(1), Uuid::new_v4())
                .await
        })
    };
    entered.await;
    assert!(matches!(
        backend
            .bus
            .send_with_correlation(ping_request(), Duration::from_secs(1), Uuid::new_v4(),)
            .await,
        Err(BusError::Unavailable)
    ));
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    handler.release.notify_one();
    first
        .await
        .expect("saturation request task")
        .expect("first reply");
    backend.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_transports_obey_the_same_command_bus_law() {
    exercise_backend(BackendKind::AllNats).await;
    exercise_saturation(BackendKind::AllNats).await;
    exercise_backend(BackendKind::Local).await;
    exercise_saturation(BackendKind::Local).await;

    assert_eq!(
        bus_error_response(BusError::DuplicateCorrelation)
            .into_response()
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        bus_error_response(BusError::Unavailable)
            .into_response()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}
