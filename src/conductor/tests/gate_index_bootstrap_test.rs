//! Boot-time gate-index bootstrap coverage. Drives
//! `gate_index_lifecycle::rebuild_from_server` directly — the same
//! snapshot call the conductor makes on every relay reconnect to
//! reconcile its in-memory gate index against the server's
//! authoritative published state — and asserts the process-wide
//! `gate_index()` singleton afterwards.
//!
//! No HTTP wrapper sits between the test and the code under test: the
//! only HTTP here is a tiny fake coordinator stub the rebuild path calls
//! over the wire, exactly as production reaches the real coordinator's
//! `/api/internal/dispatched-gates` route.
//!
//! Requires nothing beyond an ephemeral loopback port. No Postgres /
//! NATS — the bootstrap path touches neither.

#![cfg(not(madsim))]

use std::net::SocketAddr;

use tickr_conductor::gate_index_lifecycle::{gate_index, rebuild_from_server};
use tickr_conductor::relay::dispatch_gates::{
    DispatchGatesClient, DispatchGatesError, DispatchedGate,
};
use tickr_proto::workflow as wf;
use tickr_proto::TenantId;
use uuid::Uuid;

const TEST_BEARER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const TEST_AUTHORIZATION: &str = "Bearer AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Stand up a stub HTTP server mirroring the coordinator's
/// `/api/internal/dispatched-gates` shape — a JSON-encoded
/// `Vec<DispatchedGate>` — so the rebuild path can be exercised
/// without the real coordinator binary.
async fn spawn_fake_dispatched_gates(gates: Vec<DispatchedGate>) -> String {
    let app = axum::Router::new().route(
        "/api/internal/dispatched-gates",
        axum::routing::get(move |headers: axum::http::HeaderMap| {
            let body = gates.clone();
            async move {
                assert_eq!(
                    headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some(TEST_AUTHORIZATION)
                );
                axum::Json(body)
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind stub");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_index_rebuilds_from_coordinator_on_startup() {
    // Seed a stale entry so the rebuild proves it clears existing
    // state before repopulating.
    let stale_wi = Uuid::new_v4();
    gate_index()
        .register(stale_wi, Uuid::new_v4(), "stale", None, vec![])
        .expect("seed stale entry");
    assert_eq!(gate_index().lookup_by_signal_name("stale").len(), 1);

    let fresh_wi = Uuid::new_v4();
    let fresh_edge = Uuid::new_v4();
    let base = spawn_fake_dispatched_gates(vec![DispatchedGate {
        workflow_instance_id: fresh_wi,
        edge_id: fresh_edge,
        signal_name: "fresh".to_string(),
        predicate: Some("$[?@.ok == true]".to_string()),
        captures_spec: vec![wf::CaptureDeclaration {
            name: "v".to_string(),
            from: Some(wf::CaptureSource {
                source: Some(wf::capture_source::Source::Trigger(
                    wf::capture_source::Trigger {
                        jsonpath: "$.v".to_string(),
                    },
                )),
            }),
        }],
    }])
    .await;

    let client = DispatchGatesClient::new(&base, TEST_BEARER_TOKEN, true).unwrap();
    let count = rebuild_from_server(&client, TenantId::from_slug("test"))
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Stale entry is gone; fresh entry is present with the declared
    // predicate + captures.
    assert!(
        gate_index().lookup_by_signal_name("stale").is_empty(),
        "rebuild must clear pre-existing entries"
    );
    let fresh = gate_index().lookup_by_signal_name("fresh");
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0].workflow_instance_id, fresh_wi);
    assert_eq!(fresh[0].edge_id, fresh_edge);
    assert!(fresh[0].predicate.is_some());
    assert_eq!(fresh[0].captures_spec.len(), 1);
    assert_eq!(fresh[0].captures_spec[0].name, "v");

    // Cleanup for test isolation.
    gate_index().sweep_instance(fresh_wi);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_index_rebuild_degrades_to_empty_when_coordinator_unreachable() {
    let stale_wi = Uuid::new_v4();
    gate_index()
        .register(stale_wi, Uuid::new_v4(), "stale-unreach", None, vec![])
        .expect("seed");

    // Point at an unreachable port — the rebuild must degrade to empty
    // rather than propagating the error (the next inbound
    // DispatchPrecondition restocks the index).
    let client = DispatchGatesClient::new("http://127.0.0.1:1", TEST_BEARER_TOKEN, true).unwrap();
    let error = rebuild_from_server(&client, TenantId::from_slug("test"))
        .await
        .unwrap_err();
    assert_eq!(error, DispatchGatesError::Unavailable);
    assert!(gate_index()
        .lookup_by_signal_name("stale-unreach")
        .is_empty());
}
