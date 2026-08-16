use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Args;
use serde::{Deserialize, Serialize};

use crate::migrate_cmd::{self, MigrationFormation};
use crate::terminal::{TerminalStyle, Tone};

const PROFILE_FORMAT_VERSION: u32 = 1;
const INVITATION_FORMAT_VERSION: u32 = 1;
const SUPPORTED_NICKEL_VERSIONS: [&str; 2] = ["1.16.0", "1.17.0"];
const DEFAULT_CONTROL_PLANE_HTTP_URL: &str = "https://ctrl.tickr.works";
const DEFAULT_CONTROL_PLANE_RELAY_URL: &str = "https://relay.tickr.works";
// Port 6000 is browser-blocked X11; Lite follows the Console entrypoint on 3000.
const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:3000";
const DEFAULT_API_URL: &str = "http://127.0.0.1:3000";
const CONFIG_PATH_ENV: &str = "TICKR_CONFIG_PATH";
const TOKEN_ENV: &str = "TICKR_CONTROL_PLANE_BEARER_TOKEN";

#[derive(Args, Clone, Debug, Default)]
pub struct SetupArgs {
    /// Import an operator-issued Tickr invitation.
    #[arg(long = "from", value_name = "INVITATION")]
    from: Option<PathBuf>,
    /// Directory for Tickr Lite's SQLite database, logs, and durable state.
    #[arg(long, value_name = "PATH")]
    data_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Invitation {
    format_version: u32,
    tenant_slug: String,
    credential: String,
    control_plane_http_url: String,
    control_plane_relay_url: String,
    compatible_lite_version: String,
    expires_at: DateTime<Utc>,
}

impl Invitation {
    fn validate(&self, now: DateTime<Utc>) -> Result<()> {
        if self.format_version != INVITATION_FORMAT_VERSION {
            bail!(
                "unsupported Tickr invitation format {}",
                self.format_version
            );
        }
        validate_tenant_slug(&self.tenant_slug)?;
        validate_bearer_token(&self.credential)?;
        validate_https_endpoint(
            "Control-plane HTTP subquery channel",
            &self.control_plane_http_url,
        )?;
        validate_https_endpoint(
            "Control-plane Conductor relay",
            &self.control_plane_relay_url,
        )?;
        if self.compatible_lite_version != env!("CARGO_PKG_VERSION") {
            bail!(
                "Tickr invitation requires Tickr Lite {}; this executable is {}",
                self.compatible_lite_version,
                env!("CARGO_PKG_VERSION")
            );
        }
        if self.expires_at <= now {
            bail!(
                "Tickr invitation expired at {}",
                self.expires_at.to_rfc3339()
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SetupProfile {
    format_version: u32,
    cli_version: String,
    tenant_slug: String,
    bearer_token: String,
    control_plane_http_url: String,
    control_plane_relay_url: String,
    data_dir: PathBuf,
    release_home: PathBuf,
}

impl SetupProfile {
    fn new(
        tenant_slug: String,
        bearer_token: String,
        control_plane_http_url: String,
        control_plane_relay_url: String,
        data_dir: PathBuf,
        release_home: PathBuf,
    ) -> Self {
        Self {
            format_version: PROFILE_FORMAT_VERSION,
            cli_version: env!("CARGO_PKG_VERSION").to_owned(),
            tenant_slug,
            bearer_token,
            control_plane_http_url,
            control_plane_relay_url,
            data_dir,
            release_home,
        }
    }

    pub fn release_home(&self) -> &Path {
        &self.release_home
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != PROFILE_FORMAT_VERSION {
            bail!(
                "unsupported Tickr setup profile format {}; run `tickr-cli setup` with this release",
                self.format_version
            );
        }
        validate_tenant_slug(&self.tenant_slug)?;
        validate_bearer_token(&self.bearer_token)?;
        validate_https_endpoint(
            "Control-plane HTTP subquery channel",
            &self.control_plane_http_url,
        )?;
        validate_https_endpoint(
            "Control-plane Conductor relay",
            &self.control_plane_relay_url,
        )?;
        if !self.data_dir.is_absolute() {
            bail!("Tickr data directory must be absolute");
        }
        if !self.release_home.is_absolute() {
            bail!("Tickr release directory must be absolute");
        }
        Ok(())
    }

    pub fn apply_to_environment(&self) -> Result<()> {
        let sqlite_path = self.data_dir.join("tickr.db");
        let values = [
            ("TICKR_HOME", self.release_home.as_os_str().to_owned()),
            ("TICKR_STATE_DIR", self.data_dir.as_os_str().to_owned()),
            ("TICKR_TENANT_SLUG", self.tenant_slug.clone().into()),
            (
                "TICKR_CTRL_HTTP_URL",
                self.control_plane_http_url.clone().into(),
            ),
            (
                "TICKR_CTRL_RELAY_URL",
                self.control_plane_relay_url.clone().into(),
            ),
            (TOKEN_ENV, self.bearer_token.clone().into()),
            ("TICKR_SQL_BACKEND", "sqlite".into()),
            ("TICKR_SQL_TOPOLOGY", "single-node".into()),
            (
                "TICKR_CONDUCTOR_SQLITE_URL",
                format!("sqlite://{}", sqlite_path.display()).into(),
            ),
            ("TICKR_API_BIND_ADDR", DEFAULT_API_BIND_ADDR.into()),
            ("TICKR_API_URL", DEFAULT_API_URL.into()),
            (
                "TICKR_DSL_PATHS",
                self.release_home.join("dsl").into_os_string(),
            ),
        ];
        for (name, value) in values {
            if env::var_os(name).is_none() {
                env::set_var(name, value);
            }
        }
        let current_path = env::var_os("PATH").unwrap_or_default();
        let mut search_path = env::split_paths(&current_path)
            .filter(|path| path != &self.release_home)
            .collect::<Vec<_>>();
        search_path.insert(0, self.release_home.clone());
        let search_path =
            env::join_paths(search_path).context("constructing Tickr executable search path")?;
        env::set_var("PATH", search_path);
        Ok(())
    }
}

pub async fn run(args: SetupArgs) -> Result<()> {
    let invitation = args.from.as_deref().map(load_invitation).transpose()?;
    let explicit_existing = match explicit_profile_path()? {
        Some(path) => load_profile_at(&path)?,
        None => None,
    };
    let release_home = resolve_release_home(explicit_existing.as_ref())?;
    let path = profile_path(Some(&release_home))?;
    let existing = match explicit_existing {
        Some(profile) => Some(profile),
        None => load_profile_at(&path)?,
    }
    .map(|profile| {
        profile
            .validate()
            .context("validating the existing Tickr setup profile")?;
        Ok::<_, anyhow::Error>(profile)
    })
    .transpose()?;
    if let (Some(invitation), Some(existing)) = (&invitation, &existing) {
        if invitation.tenant_slug != existing.tenant_slug {
            bail!(
                "Tickr invitation belongs to Tenant `{}`, but this installation profile belongs to Tenant `{}`; use a separate extracted release directory, or set explicit TICKR_CONFIG_PATH and --data-dir overrides",
                invitation.tenant_slug,
                existing.tenant_slug
            );
        }
    }
    verify_release_resources(&release_home)?;
    verify_prerequisites(&release_home)?;

    let from_invitation = invitation.is_some();
    let (tenant_slug, bearer_token, control_plane_http_url, control_plane_relay_url) =
        match invitation {
            Some(invitation) => (
                invitation.tenant_slug,
                invitation.credential,
                invitation.control_plane_http_url,
                invitation.control_plane_relay_url,
            ),
            None => (
                resolve_tenant_slug(existing.as_ref())?,
                resolve_bearer_token(existing.as_ref())?,
                existing
                    .as_ref()
                    .map(|profile| profile.control_plane_http_url.clone())
                    .unwrap_or_else(|| DEFAULT_CONTROL_PLANE_HTTP_URL.to_owned()),
                existing
                    .as_ref()
                    .map(|profile| profile.control_plane_relay_url.clone())
                    .unwrap_or_else(|| DEFAULT_CONTROL_PLANE_RELAY_URL.to_owned()),
            ),
        };
    let requested_data_dir = if from_invitation && args.data_dir.is_none() && existing.is_none() {
        default_data_directory(&release_home)
    } else {
        resolve_data_directory(args.data_dir, existing.as_ref(), &release_home)?
    };
    let data_dir = create_data_directory(&requested_data_dir)?;
    let profile = SetupProfile::new(
        tenant_slug,
        bearer_token,
        control_plane_http_url,
        control_plane_relay_url,
        data_dir,
        release_home,
    );

    write_profile(&path, &profile)?;
    profile.apply_to_environment()?;
    migrate_cmd::run(MigrationFormation::TickrLite)
        .await
        .context("initializing Tickr Lite state")?;

    let style = TerminalStyle::stdout();
    println!(
        "{}",
        style.paint(Tone::Success, "Tickr Lite setup complete.")
    );
    println!(
        "  {}: {}",
        style.paint(Tone::Accent, "configuration"),
        path.display()
    );
    println!(
        "  {}: {}",
        style.paint(Tone::Accent, "data"),
        profile.data_dir.display()
    );
    println!(
        "  {}: {}",
        style.paint(Tone::Accent, "Tenant"),
        profile.tenant_slug
    );
    println!(
        "{}: ./tickr-cli examples run hello-world runtime-patch polyglot",
        style.paint(Tone::Strong, "Next")
    );
    Ok(())
}

pub fn load_and_apply_profile() -> Result<Option<SetupProfile>> {
    let release_home = resolve_installed_release_home()?;
    let profile = load_profile(release_home.as_deref())?;
    if let Some(profile) = profile.as_ref() {
        profile.validate()?;
        profile.apply_to_environment()?;
    }
    Ok(profile)
}

pub fn change_to_release_home(profile: Option<&SetupProfile>) -> Result<Option<PathBuf>> {
    let release_home = match profile {
        Some(profile) => profile.release_home().to_owned(),
        None => match env::var_os("TICKR_HOME") {
            Some(path) => PathBuf::from(path),
            None => return Ok(None),
        },
    };
    verify_release_resources(&release_home)?;
    env::set_current_dir(&release_home).with_context(|| {
        format!(
            "changing to Tickr release directory {}",
            release_home.display()
        )
    })?;
    Ok(Some(release_home))
}

fn load_invitation(path: &Path) -> Result<Invitation> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("reading Tickr invitation {}", path.display()))?;
    let invitation: Invitation = serde_json::from_str(&contents)
        .with_context(|| format!("parsing Tickr invitation {}", path.display()))?;
    invitation
        .validate(Utc::now())
        .with_context(|| format!("validating Tickr invitation {}", path.display()))?;
    Ok(invitation)
}

fn load_profile(release_home: Option<&Path>) -> Result<Option<SetupProfile>> {
    let path = profile_path(release_home)?;
    load_profile_at(&path)
}

fn load_profile_at(path: &Path) -> Result<Option<SetupProfile>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("opening Tickr profile {}", path.display()))
        }
    };
    let metadata = file
        .metadata()
        .with_context(|| format!("inspecting Tickr profile {}", path.display()))?;
    if metadata.permissions().mode() & 0o077 != 0 {
        bail!(
            "Tickr profile {} contains a credential and must have mode 0600",
            path.display()
        );
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("reading Tickr profile {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("parsing Tickr profile {}", path.display()))
        .map(Some)
}

fn write_profile(path: &Path, profile: &SetupProfile) -> Result<()> {
    let parent = path
        .parent()
        .context("Tickr profile path has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "creating Tickr configuration directory {}",
            parent.display()
        )
    })?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "restricting Tickr configuration directory {}",
            parent.display()
        )
    })?;

    let bytes = serde_json::to_vec_pretty(profile).context("serializing Tickr setup profile")?;
    let temporary = parent.join(format!(".config.json.tmp.{}", std::process::id()));
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .with_context(|| format!("creating temporary Tickr profile {}", temporary.display()))?;
        file.write_all(&bytes)
            .context("writing Tickr setup profile")?;
        file.write_all(b"\n")
            .context("terminating Tickr setup profile")?;
        file.sync_all().context("syncing Tickr setup profile")?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "installing Tickr profile {} from {}",
                path.display(),
                temporary.display()
            )
        })?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .with_context(|| {
                format!("syncing Tickr configuration directory {}", parent.display())
            })?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn explicit_profile_path() -> Result<Option<PathBuf>> {
    let Some(path) = env::var_os(CONFIG_PATH_ENV) else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        bail!("{CONFIG_PATH_ENV} must be an absolute path");
    }
    Ok(Some(path))
}

fn profile_path(release_home: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = explicit_profile_path()? {
        return Ok(path);
    }
    Ok(default_profile_path(release_home, &home_directory()?))
}

fn default_profile_path(release_home: Option<&Path>, home: &Path) -> PathBuf {
    match release_home.filter(|path| is_installed_release_directory(path)) {
        Some(release_home) => release_home.join("profile/config.json"),
        None => home.join(".config/tickr/config.json"),
    }
}

fn is_installed_release_directory(path: &Path) -> bool {
    [
        "tickr-cli",
        "tickr-lite",
        "tickr-ctx",
        "INSTALL.md",
        "dsl/lib.ncl",
        "examples/hello-world.ncl",
    ]
    .into_iter()
    .all(|relative| path.join(relative).is_file())
}

fn resolve_installed_release_home() -> Result<Option<PathBuf>> {
    let candidates = [
        env::var_os("TICKR_HOME").map(PathBuf::from),
        env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_owned)),
        env::current_dir().ok(),
    ];
    first_installed_release_home(candidates.into_iter().flatten())
}

fn first_installed_release_home(
    candidates: impl IntoIterator<Item = PathBuf>,
) -> Result<Option<PathBuf>> {
    for candidate in candidates {
        if is_installed_release_directory(&candidate) {
            return candidate.canonicalize().map(Some).with_context(|| {
                format!(
                    "resolving installed Tickr release directory {}",
                    candidate.display()
                )
            });
        }
    }
    Ok(None)
}

fn default_data_directory(release_home: &Path) -> PathBuf {
    release_home.join("data")
}

fn home_directory() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .context("HOME must name an absolute directory")
}

fn resolve_release_home(existing: Option<&SetupProfile>) -> Result<PathBuf> {
    let candidates = [
        env::var_os("TICKR_HOME").map(PathBuf::from),
        existing.map(|profile| profile.release_home.clone()),
        env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_owned)),
        env::current_dir().ok(),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.join("dsl/lib.ncl").is_file()
            && candidate.join("examples/hello-world.ncl").is_file()
        {
            return candidate.canonicalize().with_context(|| {
                format!("resolving Tickr release directory {}", candidate.display())
            });
        }
    }
    bail!(
        "cannot locate Tickr's bundled DSL and examples; run setup from the extracted release directory or set TICKR_HOME"
    )
}

fn verify_release_resources(release_home: &Path) -> Result<()> {
    for relative in [
        "dsl/lib.ncl",
        "examples/hello-world.ncl",
        "examples/runtime-patch.ncl",
        "examples/polyglot.ncl",
        "examples/flake.nix",
    ] {
        let path = release_home.join(relative);
        if !path.is_file() {
            bail!("Tickr release resource is missing: {}", path.display());
        }
    }
    Ok(())
}

fn resolve_tenant_slug(existing: Option<&SetupProfile>) -> Result<String> {
    let value = env::var("TICKR_TENANT_SLUG")
        .ok()
        .or_else(|| existing.map(|profile| profile.tenant_slug.clone()))
        .map(Ok)
        .unwrap_or_else(|| prompt_line("Tenant slug", None))?;
    let value = value.trim().to_owned();
    validate_tenant_slug(&value)?;
    Ok(value)
}

fn validate_tenant_slug(value: &str) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        bail!("Tenant slug must be non-empty and contain no whitespace");
    }
    Ok(())
}

fn resolve_bearer_token(existing: Option<&SetupProfile>) -> Result<String> {
    let value = if let Ok(value) = env::var(TOKEN_ENV) {
        value
    } else if let Some(profile) = existing {
        profile.bearer_token.clone()
    } else {
        prompt_secret("Tenant credential")?
    };
    let value = value.trim().to_owned();
    validate_bearer_token(&value)?;
    Ok(value)
}

fn validate_bearer_token(value: &str) -> Result<()> {
    if value.len() != 43
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        bail!("Tenant credential must be a canonical 32-byte base64url token");
    }
    Ok(())
}

fn resolve_data_directory(
    argument: Option<PathBuf>,
    existing: Option<&SetupProfile>,
    release_home: &Path,
) -> Result<PathBuf> {
    if let Some(path) = argument {
        return expand_home(path);
    }
    if let Some(profile) = existing {
        return Ok(profile.data_dir.clone());
    }
    let default = default_data_directory(release_home);
    let answer = prompt_line("Tickr data directory", Some(&default.display().to_string()))?;
    expand_home(PathBuf::from(answer))
}

fn expand_home(path: PathBuf) -> Result<PathBuf> {
    let rendered = path.to_string_lossy();
    if rendered == "~" {
        return home_directory();
    }
    if let Some(relative) = rendered.strip_prefix("~/") {
        return Ok(home_directory()?.join(relative));
    }
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(env::current_dir()
            .context("reading the current directory")?
            .join(path))
    }
}

fn create_data_directory(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("creating Tickr data directory {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restricting Tickr data directory {}", path.display()))?;
    path.canonicalize()
        .with_context(|| format!("resolving Tickr data directory {}", path.display()))
}

fn prompt_line(label: &str, default: Option<&str>) -> Result<String> {
    if !io::stdin().is_terminal() {
        bail!("{label} is required; use `tickr-cli setup --from invitation.json`");
    }
    match default {
        Some(default) => print!("{label} [{default}]: "),
        None => print!("{label}: "),
    }
    io::stdout().flush().context("flushing setup prompt")?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .with_context(|| format!("reading {label}"))?;
    let answer = answer.trim();
    if answer.is_empty() {
        default
            .map(str::to_owned)
            .with_context(|| format!("{label} cannot be empty"))
    } else {
        Ok(answer.to_owned())
    }
}

fn prompt_secret(label: &str) -> Result<String> {
    if !io::stdin().is_terminal() {
        bail!("{label} is required; use `tickr-cli setup --from invitation.json`");
    }
    print!("{label}: ");
    io::stdout().flush().context("flushing credential prompt")?;
    let _echo = TerminalEcho::disable()?;
    let mut answer = String::new();
    let read = io::stdin()
        .read_line(&mut answer)
        .with_context(|| format!("reading {label}"));
    println!();
    read?;
    Ok(answer)
}

struct TerminalEcho;

impl TerminalEcho {
    fn disable() -> Result<Self> {
        let status = Command::new("stty")
            .arg("-echo")
            .stdin(Stdio::inherit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("disabling terminal echo for the credential prompt")?;
        if !status.success() {
            bail!("cannot disable terminal echo for the credential prompt");
        }
        Ok(Self)
    }
}

impl Drop for TerminalEcho {
    fn drop(&mut self) {
        let _ = Command::new("stty")
            .arg("echo")
            .stdin(Stdio::inherit())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn verify_prerequisites(release_home: &Path) -> Result<()> {
    let nix = Command::new("nix")
        .args(["flake", "--help"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("Nix is not available on PATH")?;
    if !nix.success() {
        bail!("Nix Flakes are unavailable; `nix flake --help` must succeed");
    }

    let bundled_nickel = release_home.join("nickel");
    let nickel_program = if bundled_nickel.is_file() {
        bundled_nickel.as_os_str()
    } else {
        std::ffi::OsStr::new("nickel")
    };
    let nickel = Command::new(nickel_program)
        .arg("--version")
        .output()
        .context("Nickel is not available in the Tickr release or on PATH")?;
    if !nickel.status.success() {
        bail!("`nickel --version` failed");
    }
    let output = String::from_utf8_lossy(&nickel.stdout);
    let version = supported_nickel_version(&output).with_context(|| {
        format!(
            "Tickr {} supports Nickel 1.16.0 or 1.17.0; found `{}`",
            env!("CARGO_PKG_VERSION"),
            output.trim()
        )
    })?;
    println!("Prerequisites:");
    println!("  Nix Flakes: available");
    println!("  Nickel: {version}");
    Ok(())
}
fn supported_nickel_version(output: &str) -> Option<&str> {
    output
        .split_whitespace()
        .find(|part| SUPPORTED_NICKEL_VERSIONS.contains(part))
}

fn validate_https_endpoint(name: &str, value: &str) -> Result<()> {
    let endpoint = reqwest::Url::parse(value).with_context(|| format!("parsing {name}"))?;
    if endpoint.scheme() != "https" || endpoint.host_str().is_none() {
        bail!("{name} must be an HTTPS URL");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENVIRONMENT: Mutex<()> = Mutex::new(());

    fn profile(root: &Path) -> SetupProfile {
        SetupProfile::new(
            "acme-demo".to_owned(),
            "A".repeat(43),
            DEFAULT_CONTROL_PLANE_HTTP_URL.to_owned(),
            DEFAULT_CONTROL_PLANE_RELAY_URL.to_owned(),
            root.join("data"),
            root.join("release"),
        )
    }

    fn installed_profile(root: &Path, tenant_slug: &str) -> SetupProfile {
        SetupProfile::new(
            tenant_slug.to_owned(),
            "A".repeat(43),
            DEFAULT_CONTROL_PLANE_HTTP_URL.to_owned(),
            DEFAULT_CONTROL_PLANE_RELAY_URL.to_owned(),
            root.join("data"),
            root.to_owned(),
        )
    }

    fn create_installed_release(root: &Path) {
        fs::create_dir_all(root.join("dsl")).unwrap();
        fs::create_dir_all(root.join("examples")).unwrap();
        for relative in [
            "tickr-cli",
            "tickr-lite",
            "tickr-ctx",
            "INSTALL.md",
            "dsl/lib.ncl",
            "examples/hello-world.ncl",
        ] {
            fs::write(root.join(relative), "").unwrap();
        }
    }

    #[test]
    fn installed_release_defaults_profile_beside_its_data() {
        let root = tempfile::tempdir().unwrap();
        let release = root.path().join("tickr-lite-v1");
        let home = root.path().join("home");
        create_installed_release(&release);

        assert_eq!(
            default_profile_path(Some(&release), &home),
            release.join("profile/config.json")
        );
        assert_eq!(default_data_directory(&release), release.join("data"));
    }

    #[test]
    fn source_workspace_retains_the_global_profile_default() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let home = root.path().join("home");
        fs::create_dir_all(source.join("dsl")).unwrap();
        fs::create_dir_all(source.join("examples")).unwrap();
        fs::write(source.join("dsl/lib.ncl"), "").unwrap();
        fs::write(source.join("examples/hello-world.ncl"), "").unwrap();

        assert_eq!(
            default_profile_path(Some(&source), &home),
            home.join(".config/tickr/config.json")
        );
    }

    #[test]
    fn installed_release_discovery_does_not_depend_on_current_directory() {
        let root = tempfile::tempdir().unwrap();
        let unrelated = root.path().join("elsewhere");
        let release = root.path().join("tickr-lite-v1");
        fs::create_dir_all(&unrelated).unwrap();
        create_installed_release(&release);

        assert_eq!(
            first_installed_release_home([unrelated, release.clone()]).unwrap(),
            Some(release.canonicalize().unwrap())
        );
    }

    #[test]
    fn installed_releases_load_only_their_own_profiles() {
        let _lock = ENVIRONMENT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let release_a = root.path().join("tickr-lite-a");
        let release_b = root.path().join("tickr-lite-b");
        create_installed_release(&release_a);
        create_installed_release(&release_b);

        let previous_config = env::var_os(CONFIG_PATH_ENV);
        let previous_home = env::var_os("HOME");
        env::remove_var(CONFIG_PATH_ENV);
        env::set_var("HOME", &home);

        let global_path = home.join(".config/tickr/config.json");
        write_profile(
            &global_path,
            &installed_profile(root.path(), "global-tenant"),
        )
        .unwrap();
        let path_a = default_profile_path(Some(&release_a), &home);
        write_profile(&path_a, &installed_profile(&release_a, "tenant-a")).unwrap();

        assert_eq!(
            load_profile(Some(&release_a)).unwrap().unwrap().tenant_slug,
            "tenant-a"
        );
        assert_eq!(load_profile(Some(&release_b)).unwrap(), None);

        let path_b = default_profile_path(Some(&release_b), &home);
        write_profile(&path_b, &installed_profile(&release_b, "tenant-b")).unwrap();
        assert_eq!(
            load_profile(Some(&release_b)).unwrap().unwrap().tenant_slug,
            "tenant-b"
        );

        match previous_config {
            Some(value) => env::set_var(CONFIG_PATH_ENV, value),
            None => env::remove_var(CONFIG_PATH_ENV),
        }
        match previous_home {
            Some(value) => env::set_var("HOME", value),
            None => env::remove_var("HOME"),
        }
    }

    #[test]
    fn explicit_profile_path_overrides_an_installed_release() {
        let _lock = ENVIRONMENT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = tempfile::tempdir().unwrap();
        let release = root.path().join("tickr-lite-v1");
        let explicit = root.path().join("explicit/config.json");
        create_installed_release(&release);

        let previous = env::var_os(CONFIG_PATH_ENV);
        env::set_var(CONFIG_PATH_ENV, &explicit);
        assert_eq!(profile_path(Some(&release)).unwrap(), explicit);
        match previous {
            Some(value) => env::set_var(CONFIG_PATH_ENV, value),
            None => env::remove_var(CONFIG_PATH_ENV),
        }
    }

    #[test]
    fn fresh_setup_defaults_data_to_the_release_directory() {
        let release_home = PathBuf::from("/tickr-lite-v1");

        assert_eq!(
            default_data_directory(&release_home),
            release_home.join("data")
        );
    }

    #[test]
    fn profile_applies_lite_defaults_without_overwriting_explicit_environment() {
        let _lock = ENVIRONMENT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = tempfile::tempdir().unwrap();
        let profile = profile(root.path());
        let names = [
            "TICKR_HOME",
            "TICKR_STATE_DIR",
            "TICKR_TENANT_SLUG",
            "TICKR_CTRL_HTTP_URL",
            "TICKR_CTRL_RELAY_URL",
            TOKEN_ENV,
            "TICKR_SQL_BACKEND",
            "TICKR_SQL_TOPOLOGY",
            "TICKR_CONDUCTOR_SQLITE_URL",
            "TICKR_API_BIND_ADDR",
            "TICKR_API_URL",
            "TICKR_DSL_PATHS",
            "PATH",
        ];
        let previous = names.map(|name| (name, env::var_os(name)));
        for name in names {
            env::remove_var(name);
        }
        env::set_var("TICKR_API_BIND_ADDR", "127.0.0.1:7000");

        profile.apply_to_environment().unwrap();

        assert_eq!(env::var("TICKR_TENANT_SLUG").unwrap(), "acme-demo");
        assert_eq!(env::var("TICKR_SQL_BACKEND").unwrap(), "sqlite");
        assert_eq!(env::var("TICKR_API_BIND_ADDR").unwrap(), "127.0.0.1:7000");
        assert_eq!(env::var("TICKR_API_URL").unwrap(), "http://127.0.0.1:3000");
        assert_eq!(
            env::var("TICKR_CONDUCTOR_SQLITE_URL").unwrap(),
            format!("sqlite://{}", root.path().join("data/tickr.db").display())
        );
        assert_eq!(
            env::split_paths(&env::var_os("PATH").unwrap()).next(),
            Some(profile.release_home.clone())
        );

        for (name, value) in previous {
            match value {
                Some(value) => env::set_var(name, value),
                None => env::remove_var(name),
            }
        }
    }

    #[test]
    fn bearer_token_validation_matches_the_control_plane_contract() {
        assert!(validate_bearer_token(&"a".repeat(43)).is_ok());
        assert!(validate_bearer_token(&format!("{}-", "a".repeat(42))).is_ok());
        assert!(validate_bearer_token(&"a".repeat(42)).is_err());
        assert!(validate_bearer_token(&format!("{}=", "a".repeat(42))).is_err());
    }

    #[test]
    fn invitation_loads_operator_connection_values() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("invitation.json");
        fs::write(
            &path,
            serde_json::json!({
                "format_version": 1,
                "tenant_slug": "acme-demo",
                "credential": "A".repeat(43),
                "control_plane_http_url": "https://control.example.test",
                "control_plane_relay_url": "https://relay.example.test",
                "compatible_lite_version": env!("CARGO_PKG_VERSION"),
                "expires_at": "2999-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();

        let invitation = load_invitation(&path).unwrap();

        assert_eq!(invitation.tenant_slug, "acme-demo");
        assert_eq!(
            invitation.control_plane_http_url,
            "https://control.example.test"
        );
        assert_eq!(
            invitation.control_plane_relay_url,
            "https://relay.example.test"
        );
    }

    #[test]
    fn invitation_rejects_expiry_and_incompatible_lite_versions() {
        let invitation = Invitation {
            format_version: INVITATION_FORMAT_VERSION,
            tenant_slug: "acme-demo".to_owned(),
            credential: "A".repeat(43),
            control_plane_http_url: "https://control.example.test".to_owned(),
            control_plane_relay_url: "https://relay.example.test".to_owned(),
            compatible_lite_version: env!("CARGO_PKG_VERSION").to_owned(),
            expires_at: DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        };
        let now = DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(invitation
            .validate(now)
            .unwrap_err()
            .to_string()
            .contains("expired"));

        let incompatible = Invitation {
            compatible_lite_version: "999.0.0".to_owned(),
            expires_at: DateTime::parse_from_rfc3339("2999-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            ..invitation
        };
        assert!(incompatible
            .validate(now)
            .unwrap_err()
            .to_string()
            .contains("requires Tickr Lite 999.0.0"));
    }
    #[test]
    fn nickel_116_and_117_are_supported() {
        assert_eq!(
            supported_nickel_version("nickel-lang-cli nickel 1.16.0 (rev release)"),
            Some("1.16.0")
        );
        assert_eq!(
            supported_nickel_version("nickel-lang-cli nickel 1.17.0 (rev release)"),
            Some("1.17.0")
        );
        assert_eq!(
            supported_nickel_version("nickel-lang-cli nickel 1.18.0"),
            None
        );
    }

    #[test]
    fn profile_less_lite_start_does_not_require_release_resources() {
        let _lock = ENVIRONMENT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = env::var_os("TICKR_HOME");
        env::remove_var("TICKR_HOME");

        assert_eq!(change_to_release_home(None).unwrap(), None);

        if let Some(previous) = previous {
            env::set_var("TICKR_HOME", previous);
        }
    }

    #[test]
    fn profile_is_installed_with_private_permissions() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config/tickr/config.json");
        write_profile(&path, &profile(root.path())).unwrap();

        let file_mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        let directory_mode = fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);
        assert_eq!(directory_mode, 0o700);
    }
}
