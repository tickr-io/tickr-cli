//! Smoke tests for the API component's trivial endpoints: `GET /` (hello) and
//! `GET /health` (readiness). Both are stateless, so the test serves the
//! `meta_router` on an ephemeral port and hits it with reqwest — no Postgres or
//! NATS required.

#![cfg(not(madsim))]

use std::net::SocketAddr;
use tickr_api::http::routes::meta_router;

/// Serve the given router on a random loopback port; return its base URL.
async fn spawn(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap_or(());
    });
    format!("http://{}", addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hello_returns_named_greeting() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn(meta_router()).await;

    let resp = reqwest::get(format!("{}/", base)).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["message"], "Hello from Tickr API");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_returns_ok() -> Result<(), Box<dyn std::error::Error>> {
    let base = spawn(meta_router()).await;

    let resp = reqwest::get(format!("{}/health", base)).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["status"], "ok");
    Ok(())
}
