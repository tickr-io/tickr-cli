mod catalog;
mod prompt;

use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use futures_util::stream::{FuturesUnordered, StreamExt};
use reqwest::{Client, Response, StatusCode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{CompletionType, Config, Editor};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use tickr::migrate_cmd::{self, MigrationFormation};
use tickr::terminal::{TerminalStyle, Tone};

use catalog::{ExampleSpec, EXAMPLES};
use prompt::{PromptHelper, SessionCommand};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Args, Clone, Debug)]
pub(crate) struct ExamplesArgs {
    #[command(subcommand)]
    command: ExampleCommand,
}

#[derive(Clone, Debug, Subcommand)]
enum ExampleCommand {
    /// Run one or more bundled examples against Tickr Lite.
    Run {
        /// Bundled examples to register and trigger.
        #[arg(
            required = true,
            num_args = 1..,
            value_parser = ["hello-world", "runtime-patch", "polyglot"]
        )]
        examples: Vec<String>,
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
struct TenantResponse {
    slug: String,
}

#[derive(Clone, Debug, Deserialize)]
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
struct WorkflowInstanceResponse {
    id: String,
    state: String,
}

#[derive(Clone, Debug)]
struct RegisteredWorkflow {
    workflow_id: String,
    workflow_version: i64,
}

#[derive(Debug)]
struct RunResult {
    workflow_instance_id: String,
}

#[derive(Debug)]
struct ExampleState {
    registration: RegisteredWorkflow,
    run_count: usize,
    last_state: String,
}

#[derive(Debug)]
struct InitialOutcome {
    example: ExampleSpec,
    registration: Option<RegisteredWorkflow>,
    result: Result<RunResult>,
}

struct ExampleSession {
    client: Client,
    api_url: String,
    release_home: PathBuf,
    states: HashMap<&'static str, ExampleState>,
    failures: Vec<String>,
    style: TerminalStyle,
    error_style: TerminalStyle,
}

impl ExampleSession {
    fn new(client: Client, api_url: String, release_home: PathBuf) -> Self {
        Self {
            client,
            api_url,
            release_home,
            states: HashMap::new(),
            failures: Vec::new(),
            style: TerminalStyle::stdout(),
            error_style: TerminalStyle::stderr(),
        }
    }

    async fn run_initial(&mut self, names: &[String]) {
        println!(
            "{}",
            self.style
                .paint(Tone::Strong, "Starting onboarding examples:")
        );
        let mut pending = FuturesUnordered::new();
        for name in names {
            let example = catalog::find(name).expect("clap validates bundled example names");
            pending.push(run_unregistered(
                self.client.clone(),
                self.api_url.clone(),
                self.release_home.clone(),
                example,
            ));
        }

        while let Some(outcome) = pending.next().await {
            if let Some(registration) = outcome.registration {
                self.states.insert(
                    outcome.example.name,
                    ExampleState {
                        registration,
                        run_count: usize::from(outcome.result.is_ok()),
                        last_state: if outcome.result.is_ok() {
                            "Completed".to_owned()
                        } else {
                            "Failed".to_owned()
                        },
                    },
                );
            }
            match outcome.result {
                Ok(result) => println!(
                    "  {} {:<14} Run completed ({})",
                    self.style.paint(Tone::Success, "✓"),
                    outcome.example.name,
                    result.workflow_instance_id
                ),
                Err(error) => {
                    let failure = format!("{}: {error:#}", outcome.example.name);
                    eprintln!("  {} {failure}", self.error_style.paint(Tone::Error, "✗"));
                    self.failures.push(failure);
                }
            }
        }
        println!();
    }

    async fn run_named(&mut self, name: &str) -> Result<()> {
        let example = catalog::find(name).with_context(|| {
            format!("unknown bundled example `{name}`; run `list` to see available examples")
        })?;
        let registration = match self.states.get(example.name) {
            Some(state) => {
                println!(
                    "Using registered Workflow version {}...",
                    state.registration.workflow_version
                );
                state.registration.clone()
            }
            None => {
                println!("Registering {}...", example.name);
                let registration =
                    register_example(&self.client, &self.api_url, &self.release_home, example)
                        .await?;
                self.states.insert(
                    example.name,
                    ExampleState {
                        registration: registration.clone(),
                        run_count: 0,
                        last_state: "Ready".to_owned(),
                    },
                );
                registration
            }
        };

        println!("Triggering {}...", example.name);
        let result = trigger_example(&self.client, &self.api_url, example, &registration).await;
        let state = self
            .states
            .get_mut(example.name)
            .expect("registration was inserted before triggering");
        match result {
            Ok(result) => {
                state.run_count += 1;
                state.last_state = "Completed".to_owned();
                println!(
                    "{} ({})",
                    self.style.paint(Tone::Success, "Run: Completed"),
                    result.workflow_instance_id
                );
                Ok(())
            }
            Err(error) => {
                state.last_state = "Failed".to_owned();
                Err(error)
            }
        }
    }

    fn print_catalog(&self) {
        println!("{}", self.style.paint(Tone::Strong, "Available examples:"));
        for example in EXAMPLES {
            let status = match self.states.get(example.name) {
                Some(state) if state.run_count == 1 => {
                    format!("completed once; {}", example.description)
                }
                Some(state) if state.run_count > 1 => {
                    format!(
                        "completed {} times; {}",
                        state.run_count, example.description
                    )
                }
                Some(state) => format!("{}; {}", state.last_state, example.description),
                None => format!("not run; {}", example.description),
            };
            println!("  {:<14} {status}", example.name);
        }
    }

    fn suggestion(&self) -> String {
        EXAMPLES
            .iter()
            .find(|example| !self.states.contains_key(example.name))
            .map(|example| format!("run {}", example.name))
            .unwrap_or_else(|| "list".to_owned())
    }

    async fn interactive_loop(&mut self, shutdown: &CancellationToken) -> Result<()> {
        println!("Commands: run <example>, list, open, help, quit");
        println!(
            "{}",
            self.style
                .paint(Tone::Muted, "Press Ctrl-C to end the session.")
        );
        let config = Config::builder()
            .completion_type(CompletionType::List)
            .build();
        let helper = PromptHelper::new(catalog::names());
        let mut editor: Editor<PromptHelper, DefaultHistory> =
            Editor::with_config(config).context("starting the interactive example prompt")?;
        editor.set_helper(Some(helper));

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            editor
                .helper_mut()
                .expect("prompt helper is installed")
                .set_suggestion(self.suggestion());
            let input = tokio::task::block_in_place(|| editor.readline("tickr › "));
            let line = match input {
                Ok(line) => line,
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    println!();
                    return Ok(());
                }
                Err(error) => return Err(error).context("reading the interactive example prompt"),
            };
            if !line.trim().is_empty() {
                editor
                    .add_history_entry(line.as_str())
                    .context("recording interactive command history")?;
            }

            match prompt::parse(&line) {
                SessionCommand::Empty => {}
                SessionCommand::Help => {
                    println!(
                        "  run <example>  Register if needed, then trigger with onboarding inputs"
                    );
                    println!("  list           Show packaged examples and session state");
                    println!("  open           Open the Tickr Console");
                    println!("  quit           End this session");
                }
                SessionCommand::List => self.print_catalog(),
                SessionCommand::Open => open_console(&self.api_url),
                SessionCommand::Quit => return Ok(()),
                SessionCommand::Run(name) => {
                    let result = tokio::select! {
                        result = self.run_named(&name) => result,
                        _ = shutdown.cancelled() => return Ok(()),
                    };
                    if let Err(error) = result {
                        let failure = format!("{name}: {error:#}");
                        eprintln!(
                            "{}: {failure}",
                            self.error_style.paint(Tone::Error, "Run failed")
                        );
                        self.failures.push(failure);
                    }
                }
                SessionCommand::Invalid(command) => {
                    eprintln!(
                        "{} `{command}`. Type `help` for available commands.",
                        self.error_style.paint(Tone::Warning, "Unknown command")
                    );
                }
            }
        }
    }

    fn finish(self) -> Result<()> {
        if let Some(failure) = self.failures.first() {
            bail!(
                "{} example operation(s) failed; first failure: {failure}",
                self.failures.len()
            );
        }
        Ok(())
    }
}

pub(crate) async fn run(args: ExamplesArgs, shutdown: CancellationToken) -> Result<()> {
    match args.command {
        ExampleCommand::Run { examples } => run_session(examples, shutdown).await,
    }
}

async fn run_session(examples: Vec<String>, shutdown: CancellationToken) -> Result<()> {
    reject_duplicate_examples(&examples)?;
    let api_url = env::var("TICKR_API_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned())
        .trim_end_matches('/')
        .to_owned();
    let release_home = env::var_os("TICKR_HOME")
        .map(PathBuf::from)
        .context("Tickr setup profile is absent; run `tickr-cli setup` first")?;
    let expected_tenant_slug = env::var("TICKR_TENANT_SLUG")
        .context("Tickr setup profile is absent; run `tickr-cli setup` first")?;
    let client = Client::builder()
        .timeout(OPERATION_TIMEOUT)
        .build()
        .context("building the local Tickr API client")?;
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();

    let mut owned_lite = start_lite_if_absent(&client, &api_url, &release_home).await?;
    let readiness = wait_for_readiness(&client, &api_url, &expected_tenant_slug);
    let readiness_result = if let Some(child) = owned_lite.as_mut() {
        tokio::select! {
            ready = readiness => ready,
            stopped = child.wait() => match stopped {
                Ok(status) => Err(anyhow::anyhow!(
                    "tickr-lite stopped before becoming ready ({status})"
                )),
                Err(error) => Err::<(), _>(error).context("waiting for tickr-lite during startup"),
            }
        }
    } else {
        readiness.await
    };
    if let Err(error) = readiness_result {
        let shutdown_result = match owned_lite.as_mut() {
            Some(child) => stop_owned_lite(child).await,
            None => Ok(()),
        };
        return preserve_operation_result(Err(error), shutdown_result);
    }

    let style = TerminalStyle::stdout();
    println!(
        "{}: {}",
        style.paint(Tone::Accent, "Tickr Lite"),
        style.paint(Tone::Success, "ready")
    );
    println!("{}: {api_url}/", style.paint(Tone::Accent, "Console"));
    if interactive {
        open_console(&api_url);
    }

    let mut session = ExampleSession::new(client, api_url, release_home);
    tokio::select! {
        _ = session.run_initial(&examples) => {}
        _ = shutdown.cancelled() => {}
    }

    let prompt_result = if interactive && !shutdown.is_cancelled() {
        session.interactive_loop(&shutdown).await
    } else {
        Ok(())
    };
    let operation_result = match prompt_result {
        Ok(()) => session.finish(),
        Err(error) => Err(error),
    };
    let shutdown_result = match owned_lite.as_mut() {
        Some(child) => stop_owned_lite(child).await,
        None => Ok(()),
    };
    preserve_operation_result(operation_result, shutdown_result)
}

fn reject_duplicate_examples(examples: &[String]) -> Result<()> {
    let mut unique = HashSet::new();
    for example in examples {
        if !unique.insert(example) {
            bail!("bundled example `{example}` was requested more than once");
        }
    }
    Ok(())
}

async fn start_lite_if_absent(
    client: &Client,
    api_url: &str,
    release_home: &Path,
) -> Result<Option<Child>> {
    match health(client, api_url).await {
        Ok(_) => Ok(None),
        Err(error) if error.is_connect() => {
            migrate_cmd::run(MigrationFormation::TickrLite)
                .await
                .context("preparing Tickr Lite state for the examples")?;
            let executable = release_home.join("tickr-lite");
            if !executable.is_file() {
                bail!(
                    "Tickr Lite executable is missing: {}; reinstall this release",
                    executable.display()
                );
            }
            let child = Command::new(&executable)
                .current_dir(release_home)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .kill_on_drop(false)
                .spawn()
                .with_context(|| format!("starting {}", executable.display()))?;
            Ok(Some(child))
        }
        Err(error) => Err(error).context("probing the local Tickr API"),
    }
}

async fn stop_owned_lite(child: &mut Child) -> Result<()> {
    if let Some(status) = child
        .try_wait()
        .context("checking the owned tickr-lite process")?
    {
        if status.success() {
            return Ok(());
        }
        bail!("owned tickr-lite process exited unexpectedly ({status})");
    }

    let pid = child.id().context("owned tickr-lite process has no PID")?;
    let signal_result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGINT) };
    if signal_result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("interrupting the owned tickr-lite process");
        }
    }

    match timeout(OPERATION_TIMEOUT, child.wait()).await {
        Ok(status) => {
            let status = status.context("waiting for the owned tickr-lite process")?;
            if status.success() {
                Ok(())
            } else {
                bail!("owned tickr-lite process failed during shutdown ({status})")
            }
        }
        Err(_) => {
            child
                .kill()
                .await
                .context("killing an unresponsive owned tickr-lite process")?;
            let _ = child.wait().await;
            bail!(
                "owned tickr-lite process did not stop within {} seconds",
                OPERATION_TIMEOUT.as_secs()
            )
        }
    }
}

fn preserve_operation_result(operation: Result<()>, shutdown: Result<()>) -> Result<()> {
    match (operation, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(shutdown)) => Err(shutdown),
        (Err(operation), Ok(())) => Err(operation),
        (Err(operation), Err(shutdown)) => {
            eprintln!("tickr-lite shutdown also failed after the example error: {shutdown:#}");
            Err(operation)
        }
    }
}

async fn run_unregistered(
    client: Client,
    api_url: String,
    release_home: PathBuf,
    example: ExampleSpec,
) -> InitialOutcome {
    let registration = match register_example(&client, &api_url, &release_home, example).await {
        Ok(registration) => registration,
        Err(error) => {
            return InitialOutcome {
                example,
                registration: None,
                result: Err(error),
            }
        }
    };
    let result = trigger_example(&client, &api_url, example, &registration).await;
    InitialOutcome {
        example,
        registration: Some(registration),
        result,
    }
}

async fn register_example(
    client: &Client,
    api_url: &str,
    release_home: &Path,
    example: ExampleSpec,
) -> Result<RegisteredWorkflow> {
    let source_path = release_home.join(example.source);
    let nickel_source = tokio::fs::read_to_string(&source_path)
        .await
        .with_context(|| format!("reading bundled example {}", source_path.display()))?;
    register_source(client, api_url, example, &nickel_source).await
}

async fn register_source(
    client: &Client,
    api_url: &str,
    example: ExampleSpec,
    nickel_source: &str,
) -> Result<RegisteredWorkflow> {
    let register: RegisterResponse = response_json(
        client
            .post(format!("{api_url}/api/workflows/register"))
            .json(&serde_json::json!({
                "namespace": "default",
                "nickel_source": nickel_source,
            }))
            .send()
            .await
            .with_context(|| format!("submitting bundled example `{}`", example.name))?,
        &format!("registering bundled example `{}`", example.name),
    )
    .await?;
    let registration = RegisteredWorkflow {
        workflow_id: register.workflow_id,
        workflow_version: register.workflow_version,
    };
    wait_for_build(client, api_url, example, &registration, POLL_INTERVAL).await?;
    Ok(registration)
}

async fn trigger_example(
    client: &Client,
    api_url: &str,
    example: ExampleSpec,
    registration: &RegisteredWorkflow,
) -> Result<RunResult> {
    let trigger: TriggerResponse = response_json(
        client
            .post(format!(
                "{api_url}/api/workflows/{}/trigger",
                registration.workflow_id
            ))
            .json(&example.trigger_body())
            .send()
            .await
            .with_context(|| format!("triggering bundled example `{}`", example.name))?,
        &format!("triggering bundled example `{}`", example.name),
    )
    .await?;
    let workflow_instance_id =
        wait_for_signal(client, api_url, example, &trigger.signal_id).await?;
    wait_for_instance(
        client,
        api_url,
        example,
        &registration.workflow_id,
        &workflow_instance_id,
    )
    .await?;
    Ok(RunResult {
        workflow_instance_id,
    })
}

async fn health(client: &Client, api_url: &str) -> reqwest::Result<Response> {
    client.get(format!("{api_url}/api/health")).send().await
}

async fn wait_for_readiness(
    client: &Client,
    api_url: &str,
    expected_tenant_slug: &str,
) -> Result<()> {
    loop {
        match health(client, api_url).await {
            Ok(response) if response.status().is_success() => {
                let health: HealthResponse = response
                    .json()
                    .await
                    .context("decoding Tickr Lite health")?;
                match health.readiness {
                    Some(Readiness { ready: true }) => {
                        return verify_api_tenant(client, api_url, expected_tenant_slug).await;
                    }
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

async fn verify_api_tenant(
    client: &Client,
    api_url: &str,
    expected_tenant_slug: &str,
) -> Result<()> {
    let tenant: TenantResponse = response_json(
        client
            .get(format!("{api_url}/api/tenant"))
            .send()
            .await
            .context("reading the local Tickr API Tenant")?,
        "reading the local Tickr API Tenant",
    )
    .await?;
    if tenant.slug != expected_tenant_slug {
        bail!(
            "the local Tickr API belongs to Tenant `{}`, but this installation belongs to Tenant `{expected_tenant_slug}`; stop the other Tickr Lite process or configure a different TICKR_API_URL",
            tenant.slug
        );
    }
    Ok(())
}

async fn wait_for_build(
    client: &Client,
    api_url: &str,
    example: ExampleSpec,
    registration: &RegisteredWorkflow,
    poll_interval: Duration,
) -> Result<()> {
    loop {
        let workflows: Vec<WorkflowRow> = response_json(
            client
                .get(format!("{api_url}/api/workflows"))
                .send()
                .await
                .with_context(|| format!("reading `{}` build state", example.name))?,
            &format!("reading `{}` build state", example.name),
        )
        .await?;
        if let Some(workflow) = workflows
            .into_iter()
            .find(|workflow| workflow.id == registration.workflow_id)
        {
            if workflow.build_version == Some(registration.workflow_version) {
                match workflow.build_status.as_str() {
                    "Ready" => return Ok(()),
                    "BuildFailed" => bail!(
                        "{} Workflow version {} failed to build",
                        example.name,
                        registration.workflow_version
                    ),
                    _ => {}
                }
            }
        }
        sleep(poll_interval).await;
    }
}

async fn wait_for_signal(
    client: &Client,
    api_url: &str,
    example: ExampleSpec,
    signal_id: &str,
) -> Result<String> {
    loop {
        let signal: SignalResponse = response_json(
            client
                .get(format!("{api_url}/api/signals/{signal_id}"))
                .send()
                .await
                .with_context(|| format!("resolving `{}` trigger Signal", example.name))?,
            &format!("resolving `{}` trigger Signal", example.name),
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

async fn wait_for_instance(
    client: &Client,
    api_url: &str,
    example: ExampleSpec,
    workflow_id: &str,
    workflow_instance_id: &str,
) -> Result<()> {
    loop {
        let instances: Vec<WorkflowInstanceResponse> = response_json(
            client
                .get(format!("{api_url}/api/workflows/{workflow_id}/instances"))
                .send()
                .await
                .with_context(|| format!("reading `{}` Run state", example.name))?,
            &format!("reading `{}` Run state", example.name),
        )
        .await?;
        if let Some(instance) = instances
            .into_iter()
            .find(|instance| instance.id == workflow_instance_id)
        {
            match instance.state.as_str() {
                "Completed" => return Ok(()),
                "Failed" | "Cancelled" | "Canceled" | "Killed" => {
                    bail!(
                        "{} Run reached terminal state {}",
                        example.name,
                        instance.state
                    )
                }
                _ => {}
            }
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn open_console(api_url: &str) {
    #[cfg(target_os = "macos")]
    let program = "open";
    #[cfg(target_os = "linux")]
    let program = "xdg-open";
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        eprintln!("Open the Tickr Console at {api_url}/");
        return;
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    match StdCommand::new(program)
        .arg(format!("{api_url}/"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(_) => println!("Opened the Tickr Console in your browser."),
        Err(error) => eprintln!("Could not open the Tickr Console: {error}. Open {api_url}/"),
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
    use serde_json::{json, Value};
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
            Json(json!({"workflow_id": "workflow-1", "workflow_version": 1})),
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

    async fn delayed_workflows(State(state): State<Arc<FakeApi>>) -> Json<Value> {
        let build_poll = {
            let mut calls = state.calls.lock().await;
            calls.push("build".to_owned());
            calls.iter().filter(|call| call.as_str() == "build").count()
        };
        Json(json!([{
            "id": "workflow-1",
            "build_status": if build_poll == 1 { "Building" } else { "Ready" },
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

    async fn instances(
        State(state): State<Arc<FakeApi>>,
        Path(_workflow): Path<String>,
    ) -> Json<Value> {
        record(&state, "instance").await;
        Json(json!([{"id": "instance-1", "state": "Completed"}]))
    }

    async fn ready_health() -> Json<Value> {
        Json(json!({"readiness": {"ready": true}}))
    }

    async fn other_tenant() -> Json<Value> {
        Json(json!({
            "slug": "lite-test",
            "id": "tenant-id",
            "workflow_count": 1
        }))
    }

    #[tokio::test]
    async fn bundled_example_follows_every_durable_transition() {
        let state = Arc::new(FakeApi::default());
        let app = Router::new()
            .route("/api/workflows/register", post(register))
            .route("/api/workflows", get(workflows))
            .route("/api/workflows/{workflow}/trigger", post(trigger))
            .route("/api/signals/{signal}", get(signal))
            .route("/api/workflows/{workflow}/instances", get(instances))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Client::new();
        let example = catalog::find("hello-world").unwrap();

        let registration = register_source(
            &client,
            &format!("http://{address}"),
            example,
            "let workflow = {} in workflow",
        )
        .await
        .unwrap();
        let result = trigger_example(
            &client,
            &format!("http://{address}"),
            example,
            &registration,
        )
        .await
        .unwrap();

        assert_eq!(result.workflow_instance_id, "instance-1");
        assert_eq!(
            *state.calls.lock().await,
            ["register", "build", "trigger", "signal", "instance"]
        );
        server.abort();
    }

    #[tokio::test]
    async fn build_poller_notifies_after_a_later_ready_transition() {
        let state = Arc::new(FakeApi::default());
        let app = Router::new()
            .route("/api/workflows", get(delayed_workflows))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let registration = RegisteredWorkflow {
            workflow_id: "workflow-1".to_owned(),
            workflow_version: 1,
        };

        wait_for_build(
            &Client::new(),
            &format!("http://{address}"),
            catalog::find("hello-world").unwrap(),
            &registration,
            Duration::from_millis(1),
        )
        .await
        .unwrap();

        assert_eq!(*state.calls.lock().await, ["build", "build"]);
        server.abort();
    }

    #[tokio::test]
    async fn readiness_rejects_another_installations_tenant() {
        let app = Router::new()
            .route("/api/health", get(ready_health))
            .route("/api/tenant", get(other_tenant));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let error = wait_for_readiness(&Client::new(), &format!("http://{address}"), "lite-test-1")
            .await
            .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("local Tickr API belongs to Tenant `lite-test`"));
        assert!(message.contains("this installation belongs to Tenant `lite-test-1`"));
        server.abort();
    }

    #[test]
    fn example_requests_and_pollers_never_block_for_more_than_five_seconds() {
        assert!(OPERATION_TIMEOUT <= Duration::from_secs(5));
        assert!(POLL_INTERVAL <= Duration::from_secs(5));
    }

    #[test]
    fn duplicate_initial_examples_are_rejected() {
        let examples = vec!["hello-world".to_owned(), "hello-world".to_owned()];
        assert!(reject_duplicate_examples(&examples)
            .unwrap_err()
            .to_string()
            .contains("requested more than once"));
    }

    #[test]
    fn example_failure_takes_precedence_over_shutdown_failure() {
        let error = preserve_operation_result(
            Err(anyhow::anyhow!("example operation failed")),
            Err(anyhow::anyhow!("shutdown failed")),
        )
        .unwrap_err();

        assert_eq!(error.to_string(), "example operation failed");
    }
}
