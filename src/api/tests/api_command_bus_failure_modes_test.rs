//! Transport-failure mapping for the command-bus client.
//!
//! Exercises the four edges the API synthesizes itself rather than forwarding
//! a conductor status:
//!   * no responder on the subject       -> 503 (BusError::Unavailable)
//!   * reply deadline expires             -> 504 (BusError::Timeout)
//!   * reply doesn't decode as a response -> 502 (BusError::Malformed)
//!   * conductor replies UNSUPPORTED      -> 501 forwarded verbatim
//!
//! The first three use lightweight raw NATS responders (no conductor, no PG);
//! the 501 case drives the real conductor subscriber with an empty-body
//! envelope, whose dispatch falls through to the unsupported-command arm
//! regardless of which command arms are wired.
//!
//! Requires Docker (testcontainers NATS). Skipped automatically when
//! unavailable.

#![cfg(not(madsim))]

use std::sync::Arc;
use std::time::Duration;

use async_nats::Client as NatsClient;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tokio_util::sync::CancellationToken;

use tickr_api::commands::client::{
    bus_error_response, error_payload_response, send_command, BusError, COMMAND_SUBJECT,
};
use tickr_proto::tickr_api as api;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    NatsClient,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..20 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((container, client.expect("nats connect")))
}

async fn start_postgres() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    sqlx::PgPool,
)> {
    let container = match Postgres::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: Postgres testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .ok()?;
    tickr_migrations::apply_target(tickr_migrations::MigrationTarget::Conductor, &pool)
        .await
        .ok()?;
    Some((container, pool))
}

/// A raw responder on the command subject that replies with `reply_bytes`
/// after `delay`. Used to simulate a slow responder (504) and a malformed
/// reply (502). Returns once subscribed so callers can send immediately.
async fn spawn_raw_responder(nats: NatsClient, delay: Duration, reply_bytes: Vec<u8>) {
    let mut sub = nats
        .subscribe(COMMAND_SUBJECT)
        .await
        .expect("raw responder subscribe");
    // Flush so the subscription is registered before we return.
    nats.flush().await.expect("flush sub");
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let _ = nats.publish(reply, reply_bytes.clone().into()).await;
                let _ = nats.flush().await;
            }
        }
    });
}

fn register_request() -> api::ApiCommandRequest {
    api::ApiCommandRequest {
        body: Some(api::api_command_request::Body::Register(
            api::RegisterRequest {
                nickel_source: "irrelevant".to_string(),
                namespace: String::new(),
            },
        )),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_responder_maps_to_503() {
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    // No subscriber on the subject: NATS reports no responders.
    let err = send_command(&nats, register_request(), Duration::from_secs(2))
        .await
        .expect_err("expected a transport failure");
    assert!(matches!(err, BusError::Unavailable), "got {:?}", err);
    assert_eq!(
        bus_error_response(BusError::Unavailable)
            .into_response()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_expiry_maps_to_504() {
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    // Responder replies only after 3s; the client deadline is 200ms.
    spawn_raw_responder(nats.clone(), Duration::from_secs(3), vec![0x08]).await;
    let err = send_command(&nats, register_request(), Duration::from_millis(200))
        .await
        .expect_err("expected a timeout");
    assert!(matches!(err, BusError::Timeout), "got {:?}", err);
    assert_eq!(
        bus_error_response(BusError::Timeout)
            .into_response()
            .status(),
        StatusCode::GATEWAY_TIMEOUT
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn undecodable_reply_maps_to_502() {
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    // Reply is a single field-key byte with no value — invalid protobuf.
    spawn_raw_responder(nats.clone(), Duration::ZERO, vec![0x08]).await;
    let err = send_command(&nats, register_request(), Duration::from_secs(2))
        .await
        .expect_err("expected a decode failure");
    assert!(matches!(err, BusError::Malformed), "got {:?}", err);
    assert_eq!(
        bus_error_response(BusError::Malformed)
            .into_response()
            .status(),
        StatusCode::BAD_GATEWAY
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_command_is_forwarded_as_501() {
    let Some((_pg, pool)) = start_postgres().await else {
        return;
    };
    let Some((_nats, nats)) = start_nats().await else {
        return;
    };
    // Real conductor subscriber. An empty-body envelope dispatches to the
    // unsupported-command arm no matter which command arms are wired.
    let cancel = CancellationToken::new();
    let pool = Arc::new(pool);
    let definition_repository = Arc::new(
        tickr_migrations::backend::WriterRepositoryBundle::from_postgres_pool(
            pool.as_ref().clone(),
        ),
    );
    let state = tickr_conductor::api_commands_consumer::ApiCommandsState {
        definition_repository,
        nats: nats.clone(),
        signal_applied_notifications:
            tickr_conductor::signal_applied_notifier::all_nats_signal_applied_notifications(
                nats.clone(),
            )
            .await
            .unwrap()
            .reconciliation(),
        relay_sender: Arc::new(tickr_conductor::wakeup_translator::DefaultRelaySender),
        patch_relay_sender: Arc::new(tickr_conductor::patch_pipeline::DefaultPatchRelaySender),
        gate_index: tickr_conductor::gate_index_lifecycle::gate_index(),
    };
    let token = cancel.clone();
    tokio::spawn(async move {
        let _ = tickr_conductor::api_commands_consumer::start(state, token).await;
    });
    tokio::time::sleep(Duration::from_millis(800)).await;

    let empty = api::ApiCommandRequest { body: None };
    let resp = send_command(&nats, empty, Duration::from_secs(5))
        .await
        .expect("conductor replies");
    assert_eq!(resp.status_code, 501);
    match resp.payload {
        Some(api::api_command_response::Payload::Error(ep)) => {
            assert_eq!(ep.code, api::CommandErrorCode::UnsupportedCommand as i32);
            // The API renders this as 501 with the proto code name.
            let rendered = error_payload_response(resp.status_code, ep.code, ep.message);
            assert_eq!(
                rendered.into_response().status(),
                StatusCode::NOT_IMPLEMENTED
            );
        }
        other => panic!("expected ErrorPayload, got {:?}", other),
    }
    cancel.cancel();
}
