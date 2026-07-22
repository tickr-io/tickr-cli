//! `tickr-ctx` CLI surface.
//!
//! Exit codes:
//! - 0 success
//! - 2 usage error / missing required env
//! - 3 not found (`get` without `--default`)
//! - 4 contract / type / size assertion failed
//! - 5 NATS unreachable / transient
//! - 124 `--wait` timed out

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;
use std::io::{IsTerminal, Read, Write};
use std::time::Duration;

use crate::envelope::{Envelope, Producer};
use crate::scope::Scope;
use crate::store::{Store, MAX_VALUE_SIZE};

#[derive(Parser, Debug)]
#[command(
    name = "tickr-ctx",
    about = "Inter-task context store for tickr workflows"
)]
pub struct Cli {
    #[command(flatten)]
    pub scope: ScopeArgs,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Args, Debug, Clone)]
pub struct ScopeArgs {
    /// Override TICKR_NS.
    #[arg(long, global = true)]
    pub ns: Option<String>,
    /// Override TICKR_RUN_ID.
    #[arg(long, global = true)]
    pub run: Option<String>,
    /// Override TICKR_TASK_ID. Only used as the producer-id stamp on the
    /// envelope; keys are run-scoped, not task-scoped, in the MVP.
    #[arg(long, global = true)]
    pub task: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Publish a value into the run's context store.
    Capture(CaptureArgs),
    /// Read a value from the run's context store.
    Get(GetArgs),
    /// List keys in the run's context store.
    Ls(LsArgs),
    /// Stream put/delete events on the run's context store.
    Tail(TailArgs),
    /// Delete a key from the run's context store.
    Rm(RmArgs),
    /// Dump the entire run scope (KEY=value lines or JSON).
    Export(ExportArgs),
}

#[derive(Args, Debug)]
pub struct CaptureArgs {
    /// Logical key name (must be in TICKR_OUTPUTS unless --allow-undeclared).
    pub key: String,
    /// Positional value. Mutually exclusive with --json/--int/--float/--bool/--file/--stdin.
    pub value: Option<String>,
    #[arg(long, conflicts_with_all = ["int", "float", "bool", "file", "stdin", "value"])]
    pub json: Option<String>,
    #[arg(long, conflicts_with_all = ["json", "float", "bool", "file", "stdin", "value"])]
    pub int: Option<i64>,
    #[arg(long, conflicts_with_all = ["json", "int", "bool", "file", "stdin", "value"])]
    pub float: Option<f64>,
    #[arg(long, conflicts_with_all = ["json", "int", "float", "file", "stdin", "value"])]
    pub bool: Option<bool>,
    #[arg(long, conflicts_with_all = ["json", "int", "float", "bool", "stdin", "value"])]
    pub file: Option<String>,
    #[arg(long, conflicts_with_all = ["json", "int", "float", "bool", "file", "value"])]
    pub stdin: bool,
    /// Mark this value as a secret. Reads on a TTY refuse without --reveal.
    #[arg(long)]
    pub secret: bool,
    /// Bypass the TICKR_OUTPUTS strict check.
    #[arg(long)]
    pub allow_undeclared: bool,
}

#[derive(Args, Debug)]
pub struct GetArgs {
    pub key: String,
    /// Default value to print (and exit 0) if the key is missing.
    #[arg(long)]
    pub default: Option<String>,
    /// Block (KV watch) until the key materializes, up to this duration.
    /// Examples: 30s, 5m, 1h.
    #[arg(long)]
    pub wait: Option<String>,
    /// Parse-and-emit canonical JSON. Errors (exit 4) if the stored type isn't json.
    #[arg(long)]
    pub json: bool,
    /// Required to print a `--secret` value to a TTY.
    #[arg(long)]
    pub reveal: bool,
    /// Read from the trigger-signal namespace instead of the run-scoped one.
    /// Resolves the key against `<signal_id>/<name>` for inputs declared
    /// `from.trigger` in the DSL. Errors (exit 2) when this run wasn't
    /// trigger-originated (no `TICKR_TRIGGER_SIGNAL_ID` env injection).
    #[arg(long)]
    pub signal: bool,
}

#[derive(Args, Debug)]
pub struct LsArgs {
    /// Filter by key prefix (within the run scope).
    #[arg(long)]
    pub prefix: Option<String>,
}

#[derive(Args, Debug)]
pub struct TailArgs {
    #[arg(long)]
    pub prefix: Option<String>,
}

#[derive(Args, Debug)]
pub struct RmArgs {
    pub key: String,
}

#[derive(Args, Debug)]
pub struct ExportArgs {
    /// Output format. `dotenv` writes `KEY=value` lines (one per key).
    /// `json` writes a single JSON object mapping key -> rendered value.
    #[arg(long, default_value = "dotenv")]
    pub format: String,
}

pub async fn run(cli: Cli) -> Result<i32> {
    let scope = Scope::resolve(
        cli.scope.ns.clone(),
        cli.scope.run.clone(),
        cli.scope.task.clone(),
    )?;

    match cli.command {
        Command::Capture(a) => capture(scope, a).await,
        Command::Get(a) => get(scope, a).await,
        Command::Ls(a) => ls(scope, a).await,
        Command::Tail(a) => tail(scope, a).await,
        Command::Rm(a) => rm(scope, a).await,
        Command::Export(a) => export(scope, a).await,
    }
}

async fn capture(scope: Scope, args: CaptureArgs) -> Result<i32> {
    // Strict-by-default validation against the DSL-declared outputs the
    // executor injected via TICKR_OUTPUTS. If TICKR_OUTPUTS is unset (i.e.
    // running outside the executor) we don't enforce.
    if !args.allow_undeclared && !scope.outputs.is_empty() && !scope.outputs.contains(&args.key) {
        eprintln!(
            "tickr-ctx: key {:?} is not in this task's declared outputs ({:?}). \
             Add it to the task's `outputs = [...]` in the DSL, or pass --allow-undeclared.",
            args.key, scope.outputs
        );
        return Ok(4);
    }

    if scope.task_id.is_empty() {
        eprintln!(
            "tickr-ctx: capture needs TICKR_TASK_ID (the executor injects this) or --task. \
             Bare-shell captures from outside a task are not allowed."
        );
        return Ok(2);
    }

    // Materialize the value into a JSON Value + type tag.
    let (kind, value) = read_capture_value(&args)?;

    // `capture` is the executor-driven path: env-var-resolved task scope is
    // the only legitimate producer here. Signal-derived captures arrive
    // through the conductor and are stamped with `Producer::Signal` there.
    let env = Envelope::new(
        kind,
        value,
        args.secret,
        Producer::Task {
            task_id: scope.task_id.clone(),
            task_name: scope.task_name.clone(),
        },
    );
    let bytes = serde_json::to_vec(&env)?;

    if bytes.len() > MAX_VALUE_SIZE as usize {
        eprintln!(
            "tickr-ctx: serialized value is {} bytes (limit {}). \
             Object Store spill is deferred to Phase 3 — split or shrink the payload.",
            bytes.len(),
            MAX_VALUE_SIZE
        );
        return Ok(4);
    }

    let store = Store::open(&scope).await?;
    let key = scope.key(&args.key);
    store.put(key, bytes).await?;
    Ok(0)
}

fn read_capture_value(args: &CaptureArgs) -> Result<(&'static str, serde_json::Value)> {
    if let Some(s) = &args.json {
        let v: serde_json::Value =
            serde_json::from_str(s).context("--json value is not valid JSON")?;
        return Ok(("json", v));
    }
    if let Some(n) = args.int {
        return Ok(("int", serde_json::Value::Number(n.into())));
    }
    if let Some(f) = args.float {
        let n = serde_json::Number::from_f64(f)
            .ok_or_else(|| anyhow!("--float must be finite (no NaN/Inf)"))?;
        return Ok(("float", serde_json::Value::Number(n)));
    }
    if let Some(b) = args.bool {
        return Ok(("bool", serde_json::Value::Bool(b)));
    }
    if let Some(path) = &args.file {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read --file {}", path))?;
        return Ok(("string", serde_json::Value::String(s)));
    }
    if args.stdin {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read stdin")?;
        return Ok(("string", serde_json::Value::String(buf)));
    }
    if let Some(v) = &args.value {
        return Ok(("string", serde_json::Value::String(v.clone())));
    }
    Err(anyhow!(
        "no value provided. Pass a positional VALUE, or one of --json/--int/--float/--bool/--file/--stdin"
    ))
}

async fn get(scope: Scope, args: GetArgs) -> Result<i32> {
    let store = Store::open(&scope).await?;
    // Strict-source declarations bypass the ambient resolver:
    // `--signal` reads `<trigger_signal_id>/<name>` directly. No
    // flag means walk three scopes via the ambient resolver. The
    // resolver errors loudly on multi-scope collisions — operators
    // hit a clear message naming the colliding scopes instead of
    // a silent winner.
    if args.signal {
        let key = match scope.signal_key(&args.key) {
            Some(k) => k,
            None => {
                eprintln!(
                    "tickr-ctx: --signal requires a trigger-originated run \
                     (TICKR_TRIGGER_SIGNAL_ID is unset for this task)"
                );
                return Ok(2);
            }
        };
        return get_explicit_key(scope, args, store, key).await;
    }

    // Ambient bare-string path. Probe each candidate scope key
    // sequentially against NATS KV, build the in-memory hit map,
    // then let the pure resolver enforce the fail-loud rule.
    let scopes = scope.ambient_scopes();
    let mut hits: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    let trigger_key = scopes
        .trigger
        .as_ref()
        .map(|sid| format!("{}/{}", crate::scope::sanitize_segment(sid), args.key));
    let gate_keys: Vec<String> = scopes
        .gates
        .iter()
        .map(|sid| format!("{}/{}", crate::scope::sanitize_segment(sid), args.key))
        .collect();
    let run_key = scope.key(&args.key);
    let mut probes: Vec<String> = Vec::new();
    if let Some(k) = &trigger_key {
        probes.push(k.clone());
    }
    probes.extend(gate_keys.iter().cloned());
    probes.push(run_key.clone());
    for key in &probes {
        if let Some(bytes) = store.get(key).await? {
            hits.insert(key.clone(), bytes);
        }
    }

    struct MapFetcher<'a>(&'a std::collections::HashMap<String, Vec<u8>>);
    impl<'a> crate::ambient::KvFetcher for MapFetcher<'a> {
        fn fetch(&self, key: &str) -> Option<Vec<u8>> {
            self.0.get(key).cloned()
        }
    }

    let bytes: Vec<u8> =
        match crate::ambient::resolve_ambient(&args.key, &scopes, &MapFetcher(&hits)) {
            Ok(b) => b,
            Err(crate::ambient::AmbientError::MultiScopeCollision { name, scopes }) => {
                eprintln!(
                    "tickr-ctx: bare-string `{}` resolves in multiple scopes ({}); \
                 disambiguate with `--signal`, declare `from.signal = <gate>` / \
                 `from.trigger = true` / `from.task = \"<name>\"` on the input \
                 in your workflow definition, or rename the colliding capture.",
                    name,
                    scopes
                        .iter()
                        .map(|s| match s {
                            crate::ambient::ScopeHit::Trigger => "trigger".to_string(),
                            crate::ambient::ScopeHit::Gate(id) => format!("gate({})", id),
                            crate::ambient::ScopeHit::Run => "run".to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                return Ok(5);
            }
            Err(crate::ambient::AmbientError::NotFound { .. }) => {
                // Fall through to the watch / default / not-found
                // branch in `get_explicit_key` by treating the run-
                // scope key as the operative key.
                return get_explicit_key(scope, args, store, run_key).await;
            }
        };

    finish_get(args, bytes)
}

/// Decode the stored value as a `tickr-ctx` envelope, enforce the
/// TTY-vs-secret and `--json` guardrails, and emit the rendered
/// bytes to stdout. Shared between the ambient `get` path and the
/// explicit `get_explicit_key` path so both producers reach the
/// same exit-code surface.
fn finish_get(args: GetArgs, bytes: Vec<u8>) -> Result<i32> {
    let env: Envelope =
        serde_json::from_slice(&bytes).context("stored value is not a tickr-ctx envelope")?;

    if env.secret && std::io::stdout().is_terminal() && !args.reveal {
        eprintln!(
            "tickr-ctx: refusing to print secret {:?} to a TTY. Pass --reveal if you really want it.",
            args.key
        );
        return Ok(4);
    }

    if args.json && env.kind != "json" {
        eprintln!(
            "tickr-ctx: --json requested but stored type is {:?}",
            env.kind
        );
        return Ok(4);
    }

    let out = env.render()?;
    std::io::stdout().write_all(&out).ok();
    Ok(0)
}

async fn get_explicit_key(_scope: Scope, args: GetArgs, store: Store, key: String) -> Result<i32> {
    let entry = store.get(&key).await?;

    let bytes: Vec<u8> = match (entry, &args.wait) {
        (Some(b), _) => b.to_vec(),
        (None, Some(d)) => {
            // Block on KV watch until key shows up or timeout.
            let dur = parse_duration(d)?;
            match wait_for_key(&store, &key, dur).await? {
                Some(b) => b,
                None => return Ok(124),
            }
        }
        (None, None) => {
            if let Some(default) = &args.default {
                std::io::stdout().write_all(default.as_bytes()).ok();
                return Ok(0);
            }
            eprintln!("tickr-ctx: key {:?} not found in run scope", args.key);
            return Ok(3);
        }
    };

    let env: Envelope =
        serde_json::from_slice(&bytes).context("stored value is not a tickr-ctx envelope")?;

    if env.secret && std::io::stdout().is_terminal() && !args.reveal {
        eprintln!(
            "tickr-ctx: refusing to print secret {:?} to a TTY. Pass --reveal if you really want it.",
            args.key
        );
        return Ok(4);
    }

    if args.json && env.kind != "json" {
        eprintln!(
            "tickr-ctx: --json requested but stored type is {:?}",
            env.kind
        );
        return Ok(4);
    }

    let out = env.render()?;
    std::io::stdout().write_all(&out).ok();
    Ok(0)
}

async fn ls(scope: Scope, args: LsArgs) -> Result<i32> {
    let store = Store::open(&scope).await?;
    let run_prefix = format!("{}/", crate::scope::sanitize_segment(&scope.run_id));
    let user_prefix = args.prefix.as_deref().unwrap_or("");
    let prefix = format!("{run_prefix}{user_prefix}");

    for key in store.keys(&prefix).await? {
        let inner = &key[run_prefix.len()..];
        let label = match store.get(&key).await? {
            Some(bytes) => match serde_json::from_slice::<Envelope>(&bytes) {
                Ok(envelope) if envelope.secret => "<redacted>".to_string(),
                Ok(envelope) => envelope.kind,
                Err(_) => "<unparseable>".to_string(),
            },
            None => "<gone>".to_string(),
        };
        println!("{}\t{}", inner, label);
    }
    Ok(0)
}

async fn tail(scope: Scope, args: TailArgs) -> Result<i32> {
    let store = Store::open(&scope).await?;
    let run_prefix = format!("{}/", crate::scope::sanitize_segment(&scope.run_id));
    let user_prefix = args.prefix.as_deref().unwrap_or("");

    let mut watch = store.watch_all().await?;
    while let Some(item) = watch.next().await {
        let entry = item?;
        if !entry.key.starts_with(&run_prefix) {
            continue;
        }
        let inner = &entry.key[run_prefix.len()..];
        if !inner.starts_with(user_prefix) {
            continue;
        }
        let label = match serde_json::from_slice::<Envelope>(&entry.value) {
            Ok(envelope) if envelope.secret => "<redacted>".to_string(),
            Ok(envelope) => format!("{} {}b", envelope.kind, entry.value.len()),
            Err(_) => "<unparseable>".to_string(),
        };
        println!("{:?}\t{}\t{}", entry.operation, inner, label);
    }
    Ok(0)
}

async fn rm(scope: Scope, args: RmArgs) -> Result<i32> {
    let store = Store::open(&scope).await?;
    let key = scope.key(&args.key);
    store.delete(&key).await?;
    Ok(0)
}

async fn export(scope: Scope, args: ExportArgs) -> Result<i32> {
    let store = Store::open(&scope).await?;
    let run_prefix = format!("{}/", crate::scope::sanitize_segment(&scope.run_id));

    let mut entries: Vec<(String, Envelope)> = Vec::new();
    for key in store.keys(&run_prefix).await? {
        let inner = key[run_prefix.len()..].to_string();
        if let Some(bytes) = store.get(&key).await? {
            if let Ok(envelope) = serde_json::from_slice::<Envelope>(&bytes) {
                entries.push((inner, envelope));
            }
        }
    }

    match args.format.as_str() {
        "dotenv" => {
            for (key, envelope) in &entries {
                if envelope.secret {
                    eprintln!("# skipping secret: {}", key);
                    continue;
                }
                let value = String::from_utf8_lossy(&envelope.render()?).into_owned();
                let escaped = value.replace('\'', "'\\''");
                println!("{}='{}'", key, escaped);
            }
        }
        "json" => {
            let mut object = serde_json::Map::new();
            for (key, envelope) in &entries {
                if !envelope.secret {
                    object.insert(key.clone(), envelope.value.clone());
                }
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::Value::Object(object))?
            );
        }
        other => {
            eprintln!(
                "tickr-ctx: unknown --format {:?} (use 'dotenv' or 'json')",
                other
            );
            return Ok(2);
        }
    }
    Ok(0)
}

fn parse_duration(s: &str) -> Result<Duration> {
    // Tiny parser: supports `<n>s`, `<n>m`, `<n>h`. Avoids pulling in a
    // dedicated crate for one knob.
    let s = s.trim();
    let (num, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| s.split_at(i))
        .unwrap_or((s, "s"));
    let n: u64 = num
        .parse()
        .with_context(|| format!("invalid duration: {:?}", s))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        other => return Err(anyhow!("unknown duration unit {:?} (use s/m/h)", other)),
    };
    Ok(Duration::from_secs(secs))
}

async fn wait_for_key(store: &Store, key: &str, timeout: Duration) -> Result<Option<Vec<u8>>> {
    let mut watch = store.watch_key(key).await?;
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => return Ok(None),
            item = watch.next() => match item {
                Some(Ok(entry)) if entry.operation == crate::store::StoreOperation::Put => {
                    return Ok(Some(entry.value));
                }
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Err(error),
                None => return Ok(None),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("42").unwrap(), Duration::from_secs(42));
    }
}
