use anyhow::{anyhow, Context, Result};
use std::io::Write;
use tokio::process::Command;

/// Env var naming the directories to search for Nickel imports (the Core
/// DSL's `task.ncl` lives in one of them). Colon-separated, like `PATH`.
/// Deliberately tickr-named rather than reusing Nickel's own
/// `NICKEL_IMPORT_PATH` so operators running other Nickel tooling on the
/// same host don't collide with the conductor's search path.
pub const DSL_PATHS_ENV: &str = "TICKR_DSL_PATHS";

/// The conductor's resolved Nickel import search path, or `None` when
/// `TICKR_DSL_PATHS` is unset or empty. Mapped onto the child process's
/// `NICKEL_IMPORT_PATH` when invoking `nickel export`.
pub fn dsl_import_path() -> Option<String> {
    match std::env::var(DSL_PATHS_ENV) {
        Ok(paths) if !paths.trim().is_empty() => Some(paths),
        _ => None,
    }
}

/// Evaluates a Nickel source string to JSON by shelling out to
/// `nickel export`. The source is written to a tempfile (so `import
/// "task.ncl"` resolves against the configured search path rather than
/// relative to a caller-provided path), evaluated, and its stdout
/// captured as JSON on success. On non-zero exit the raw stderr blob is
/// passed through verbatim — the conductor does not parse Nickel's
/// diagnostics, it forwards them to the author unchanged.
pub async fn nickel_eval(nickel_source: &str) -> Result<String> {
    let mut source_file =
        tempfile::NamedTempFile::new().context("failed to create tempfile for Nickel source")?;
    source_file
        .write_all(nickel_source.as_bytes())
        .context("failed to write Nickel source to tempfile")?;

    let mut command = Command::new("nickel");
    command
        .arg("export")
        .arg(source_file.path())
        .arg("--format")
        .arg("json")
        .arg("--error-format=json");

    // tickr-named env var → Nickel's own import-path var on the child only.
    if let Some(import_path) = dsl_import_path() {
        command.env("NICKEL_IMPORT_PATH", import_path);
    }

    let output = command
        .output()
        .await
        .with_context(|| "failed to execute `nickel export` for the submitted source")?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        Err(anyhow!("Nickel export failed: {}", stderr))
    }
}
