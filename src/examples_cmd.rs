use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use reqwest::{Client, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use tickr::lite_supervisor::LiteSupervisor;
use tickr::migrate_cmd::{self, MigrationFormation};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const BUILD_TIMEOUT: Duration = Duration::from_secs(60);
const RUN_TIMEOUT: Duration = Duration::from_secs(120);
const LOG_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Args, Clone, Debug)]
pub(crate) struct ExamplesArgs {
    #[command(subcommand)]
    command: ExampleCommand,
}

#[derive(Clone, Debug, Subcommand)]
enum ExampleCommand {
    /// Register and run a bundled example against Tickr Lite.
    Run {
        /// Bundled example name.
        #[arg(value_parser = ["hello-world"])]
        example: String,
    },
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    readiness: Option<Readiness>,
}

#[derive(Debug, Deserialize)]
struct Readiness {
    ready: bool,
}

#[derive(Debug, Deserialize)]
struct RegisterResponse {
    workflow_id: String,
    workflow_version: i64,
}

#[derive(Debug, Deserialize)]
struct WorkflowRow {
    id: String,
    build_status: String,
    build_version: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TriggerResponse {
    signal_id: String,
}

#[derive(Debug, Deserialize)]
struct SignalResponse {
    status: String,
    workflow_instance_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskResponse {
    id: String,
    name: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct TaskLogsResponse {
    logs: String,
}

#[derive(Debug)]
struct HelloResult {
    workflow_id: String,
    workflow_version: i64,
    workflow_instance_id: String,
    logs: String,
}

pub(crate) async fn run(args: ExamplesArgs, shutdown: CancellationToken) -> Result<()> {
    match args.command {
        ExampleCommand::Run { example } if example == "hello-world" => {
            run_hello_world(shutdown).await
        }
        ExampleCommand::Run { example } => bail!("unsupported bundled example `{example}`"),
    }
}

async fn run_hello_world(shutdown: CancellationToken) -> Result<()> {
    let api_url = env::var("TICKR_API_URL").unwrap_or_else(|_| "http://127.0.0.1:6000".to_owned());
    let release_home = env::var_os("TICKR_HOME")
        .map(PathBuf::from)
        .context("Tickr setup profile is absent; run `tickr setup` first")?;
    let source_path = release_home.join("examples/hello-world.ncl");
    let nickel_source = tokio::fs::read_to_string(&source_path)
        .await
        .with_context(|| format!("reading bundled example {}", source_path.display()))?;
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("building the local Tickr API client")?;

    let mut owned_lite = None;
    match health(&client, &api_url).await {
        Ok(_) => {}
        Err(error) if error.is_connect() => {
            migrate_cmd::run(MigrationFormation::TickrLite)
                .await
                .context("preparing Tickr Lite state for the example")?;
            let lite_shutdown = shutdown.child_token();
            let supervisor_shutdown = lite_shutdown.clone();
            let handle =
                tokio::spawn(async move { LiteSupervisor::new(supervisor_shutdown).run().await });
            owned_lite = Some((lite_shutdown, handle));
        }
        Err(error) => return Err(error).context("probing the local Tickr API"),
    }

    let readiness = wait_for_readiness(&client, &api_url);
    if let Some((_, handle)) = owned_lite.as_mut() {
        tokio::select! {
            ready = readiness => ready?,
            stopped = handle => {
                match stopped.context("joining Tickr Lite during startup")? {
                    Ok(()) => bail!("Tickr Lite stopped before becoming ready"),
                    Err(error) => {
                        return Err(error).context("Tickr Lite stopped before becoming ready")
                    }
                }
            }
        }
    } else {
        readiness.await?;
    }

    println!("Running bundled example `hello-world`:");
    println!("  Tickr Lite: ready");
    let result = run_against_api(&client, &api_url, &nickel_source).await;

    if let Some((lite_shutdown, handle)) = owned_lite {
        lite_shutdown.cancel();
        handle
            .await
            .context("joining Tickr Lite after the example")??;
    }

    let result = result?;
    println!(
        "  Workflow: Ready ({} version {})",
        result.workflow_id, result.workflow_version
    );
    println!("  Run: Completed ({})", result.workflow_instance_id);
    println!("Output:");
    println!("{}", result.logs.trim_end());
    Ok(())
}

async fn run_against_api(
    client: &Client,
    api_url: &str,
    nickel_source: &str,
) -> Result<HelloResult> {
    let register: RegisterResponse = response_json(
        client
            .post(format!("{api_url}/api/workflows/register"))
            .json(&json!({
                "namespace": "default",
                "nickel_source": nickel_source,
            }))
            .send()
            .await
            .context("submitting the bundled Hello workflow")?,
        "registering the bundled Hello workflow",
    )
    .await?;

    wait_for_build(
        client,
        api_url,
        &register.workflow_id,
        register.workflow_version,
    )
    .await?;

    let trigger: TriggerResponse = response_json(
        client
            .post(format!(
                "{api_url}/api/workflows/{}/trigger",
                register.workflow_id
            ))
            .json(&json!({"name": "Hello from Tickr"}))
            .send()
            .await
            .context("triggering the bundled Hello workflow")?,
        "triggering the bundled Hello workflow",
    )
    .await?;
    let workflow_instance_id = wait_for_signal(client, api_url, &trigger.signal_id).await?;
    let task = wait_for_task(client, api_url, &workflow_instance_id).await?;
    let logs = wait_for_logs(
        client,
        api_url,
        &register.workflow_id,
        &workflow_instance_id,
        &task.id,
    )
    .await?;

    Ok(HelloResult {
        workflow_id: register.workflow_id,
        workflow_version: register.workflow_version,
        workflow_instance_id,
        logs,
    })
}

async fn health(client: &Client, api_url: &str) -> reqwest::Result<Response> {
    client.get(format!("{api_url}/api/health")).send().await
}

async fn wait_for_readiness(client: &Client, api_url: &str) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("Tickr Lite did not become ready within 60 seconds");
        }
        match health(client, api_url).await {
            Ok(response) if response.status().is_success() => {
                let health: HealthResponse = response
                    .json()
                    .await
                    .context("decoding Tickr Lite health")?;
                match health.readiness {
                    Some(Readiness { ready: true }) => return Ok(()),
                    Some(Readiness { ready: false }) => {}
                    None => bail!("the local API is not a Tickr Lite formation"),
                }
            }
            Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => {}
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("Tickr Lite health returned HTTP {status}: {body}");
            }
            Err(error) if error.is_connect() => {}
            Err(error) => return Err(error).context("checking Tickr Lite readiness"),
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_build(
    client: &Client,
    api_url: &str,
    workflow_id: &str,
    workflow_version: i64,
) -> Result<()> {
    let deadline = Instant::now() + BUILD_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("Hello workflow build did not settle within 60 seconds");
        }
        let workflows: Vec<WorkflowRow> = response_json(
            client
                .get(format!("{api_url}/api/workflows"))
                .send()
                .await
                .context("reading workflow build state")?,
            "reading workflow build state",
        )
        .await?;
        if let Some(workflow) = workflows
            .into_iter()
            .find(|workflow| workflow.id == workflow_id)
        {
            if workflow.build_version == Some(workflow_version) {
                match workflow.build_status.as_str() {
                    "Ready" => return Ok(()),
                    "BuildFailed" => {
                        bail!("Hello workflow version {workflow_version} failed to build")
                    }
                    _ => {}
                }
            }
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_signal(client: &Client, api_url: &str, signal_id: &str) -> Result<String> {
    let deadline = Instant::now() + RUN_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("Hello trigger Signal did not materialize within 120 seconds");
        }
        let signal: SignalResponse = response_json(
            client
                .get(format!("{api_url}/api/signals/{signal_id}"))
                .send()
                .await
                .context("resolving the Hello trigger Signal")?,
            "resolving the Hello trigger Signal",
        )
        .await?;
        if let Some(instance_id) = signal.workflow_instance_id {
            if matches!(signal.status.as_str(), "materialized" | "terminal") {
                return Ok(instance_id);
            }
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_task(
    client: &Client,
    api_url: &str,
    workflow_instance_id: &str,
) -> Result<TaskResponse> {
    let deadline = Instant::now() + RUN_TIMEOUT;
    loop {
        if Instant::now() >= deadline {
            bail!("Hello Task did not complete within 120 seconds");
        }
        let tasks: Vec<TaskResponse> = response_json(
            client
                .get(format!(
                    "{api_url}/api/workflows/instances/{workflow_instance_id}/tasks"
                ))
                .send()
                .await
                .context("reading the Hello Task state")?,
            "reading the Hello Task state",
        )
        .await?;
        if let Some(task) = tasks.into_iter().find(|task| task.name == "hello") {
            match task.state.as_str() {
                "Completed" => return Ok(task),
                "Failed" | "Cancelled" | "Canceled" | "Killed" => {
                    bail!("Hello Task reached terminal state {}", task.state)
                }
                _ => {}
            }
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_logs(
    client: &Client,
    api_url: &str,
    workflow_id: &str,
    workflow_instance_id: &str,
    task_id: &str,
) -> Result<String> {
    let deadline = Instant::now() + LOG_TIMEOUT;
    let url = format!(
        "{api_url}/api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_id}/logs"
    );
    loop {
        if Instant::now() >= deadline {
            bail!("Hello Task logs were not available within 15 seconds");
        }
        let response = client
            .get(&url)
            .send()
            .await
            .context("reading the Hello Task logs")?;
        if response.status() == StatusCode::NOT_FOUND {
            sleep(POLL_INTERVAL).await;
            continue;
        }
        let logs: TaskLogsResponse = response_json(response, "reading the Hello Task logs").await?;
        if !logs.logs.contains("hello from Tickr") {
            bail!("Hello Task completed without the expected `hello from Tickr` output");
        }
        return Ok(logs.logs);
    }
}

async fn response_json<T: DeserializeOwned>(response: Response, operation: &str) -> Result<T> {
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("reading response while {operation}"))?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        bail!("{operation} returned HTTP {status}: {body}");
    }
    serde_json::from_slice(&bytes).with_context(|| format!("decoding response while {operation}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, State};
    use axum::http::StatusCode as AxumStatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct FakeApi {
        calls: Mutex<Vec<String>>,
    }

    async fn record(state: &Arc<FakeApi>, call: &str) {
        state.calls.lock().await.push(call.to_owned());
    }

    async fn register(State(state): State<Arc<FakeApi>>) -> (AxumStatusCode, Json<Value>) {
        record(&state, "register").await;
        (
            AxumStatusCode::ACCEPTED,
            Json(json!({
                "workflow_id": "workflow-1",
                "workflow_version": 1,
                "status": "Building",
                "message": "queued",
                "task_count": 1
            })),
        )
    }

    async fn workflows(State(state): State<Arc<FakeApi>>) -> Json<Value> {
        record(&state, "build").await;
        Json(json!([{
            "id": "workflow-1",
            "build_status": "Ready",
            "build_version": 1
        }]))
    }

    async fn trigger(
        State(state): State<Arc<FakeApi>>,
        Path(_workflow): Path<String>,
    ) -> (AxumStatusCode, Json<Value>) {
        record(&state, "trigger").await;
        (
            AxumStatusCode::ACCEPTED,
            Json(json!({"signal_id": "signal-1"})),
        )
    }

    async fn signal(State(state): State<Arc<FakeApi>>, Path(_signal): Path<String>) -> Json<Value> {
        record(&state, "signal").await;
        Json(json!({
            "status": "materialized",
            "workflow_instance_id": "instance-1"
        }))
    }

    async fn tasks(
        State(state): State<Arc<FakeApi>>,
        Path(_instance): Path<String>,
    ) -> Json<Value> {
        record(&state, "tasks").await;
        Json(json!([{
            "id": "task-1",
            "name": "hello",
            "state": "Completed"
        }]))
    }

    async fn logs(State(state): State<Arc<FakeApi>>) -> Json<Value> {
        record(&state, "logs").await;
        Json(json!({"logs": "hello from Tickr\n"}))
    }

    #[tokio::test]
    async fn hello_runner_follows_every_durable_transition() {
        let state = Arc::new(FakeApi::default());
        let app = Router::new()
            .route("/api/workflows/register", post(register))
            .route("/api/workflows", get(workflows))
            .route("/api/workflows/{workflow}/trigger", post(trigger))
            .route("/api/signals/{signal}", get(signal))
            .route("/api/workflows/instances/{instance}/tasks", get(tasks))
            .route(
                "/api/workflows/{workflow}/instances/{instance}/tasks/{task}/logs",
                get(logs),
            )
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Client::new();

        let result = run_against_api(
            &client,
            &format!("http://{address}"),
            "let workflow = {} in workflow",
        )
        .await
        .unwrap();

        assert_eq!(result.workflow_id, "workflow-1");
        assert_eq!(result.workflow_instance_id, "instance-1");
        assert_eq!(result.logs, "hello from Tickr\n");
        assert_eq!(
            *state.calls.lock().await,
            ["register", "build", "trigger", "signal", "tasks", "logs"]
        );
        server.abort();
    }
}
