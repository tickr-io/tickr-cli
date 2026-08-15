use std::io::{self, Write as _};

use anyhow::{anyhow, bail, Context, Result};
use chrono::DateTime;
use clap::Subcommand;
use reqwest::{redirect, StatusCode};
use serde::{Deserialize, Serialize};
use tickr_proto::TenantId;
use uuid::Uuid;

const TENANT_ADMINISTRATION_URL: &str = "http://127.0.0.1:7000/v1/tenants";

#[derive(Subcommand)]
pub enum TenantCommand {
    /// Register a Tenant and issue its initial credential.
    Register {
        /// Tenant slug used to derive its stable TenantId.
        #[arg(long)]
        slug: String,
        /// Operator-facing Tenant display name.
        #[arg(long)]
        display_name: String,
        /// Explicit credential expiry as an RFC 3339 timestamp.
        #[arg(long)]
        expires_at: String,
    },
    /// List registered Tenants with current Workflow-definition and credential counts.
    List,
    /// Show one Tenant and its credential lifecycle records.
    Show {
        /// Tenant slug used to derive its stable TenantId.
        slug: String,
    },
    /// Manage credentials for an existing Tenant.
    Credential {
        #[command(subcommand)]
        command: TenantCredentialCommand,
    },
}

#[derive(Subcommand)]
pub enum TenantCredentialCommand {
    /// Issue an additional credential without changing existing credentials.
    Issue {
        /// Tenant slug used to derive its stable TenantId.
        slug: String,
        /// Explicit credential expiry as an RFC 3339 timestamp.
        #[arg(long)]
        expires_at: String,
    },
    /// Revoke one credential and wait for its active Conductor relays to close.
    Revoke {
        /// Tenant slug used to derive its stable TenantId.
        slug: String,
        /// Public credential identifier within the Tenant.
        credential_id: Uuid,
    },
}

#[derive(Serialize)]
struct RegisterTenantRequest<'a> {
    tenant_slug: &'a str,
    display_name: &'a str,
    expires_at: &'a str,
}

#[derive(Serialize)]
struct IssueCredentialRequest<'a> {
    expires_at: &'a str,
}

#[derive(Deserialize, Serialize)]
struct RegisterTenantOutput {
    tenant: RegisteredTenant,
    credential: CredentialOutput,
}

#[derive(Deserialize, Serialize)]
struct RegisteredTenant {
    tenant_id: String,
    tenant_slug: String,
    display_name: String,
    created_at: String,
}

#[derive(Deserialize, Serialize)]
struct TenantSummaryOutput {
    tenant_id: String,
    tenant_slug: String,
    display_name: String,
    created_at: String,
    workflow_definition_count: i64,
    last_tenant_activity: Option<String>,
    credential_count: i64,
}

#[derive(Deserialize, Serialize)]
struct TenantDetailOutput {
    #[serde(flatten)]
    summary: TenantSummaryOutput,
    credentials: Vec<CredentialLifecycleOutput>,
}

#[derive(Deserialize, Serialize)]
struct CredentialOutput {
    credential_id: String,
    token: String,
    created_at: String,
    expires_at: String,
}

#[derive(Deserialize, Serialize)]
struct CredentialLifecycleOutput {
    credential_id: Uuid,
    created_at: String,
    expires_at: String,
    revoked_at: Option<String>,
}

pub async fn run(command: TenantCommand) -> Result<()> {
    match command {
        TenantCommand::Register {
            slug,
            display_name,
            expires_at,
        } => register(slug, display_name, expires_at).await,
        TenantCommand::List => list().await,
        TenantCommand::Show { slug } => show(slug).await,
        TenantCommand::Credential { command } => match command {
            TenantCredentialCommand::Issue { slug, expires_at } => issue(slug, expires_at).await,
            TenantCredentialCommand::Revoke {
                slug,
                credential_id,
            } => revoke(slug, credential_id).await,
        },
    }
}

async fn register(slug: String, display_name: String, expires_at: String) -> Result<()> {
    let tenant_slug = slug.trim();
    if tenant_slug.is_empty() {
        bail!("Tenant slug must not be blank");
    }
    if display_name.trim().is_empty() {
        bail!("Tenant display name must not be blank");
    }
    validate_expiry(&expires_at)?;

    let client = administration_client()?;
    let response = client
        .post(TENANT_ADMINISTRATION_URL)
        .json(&RegisterTenantRequest {
            tenant_slug,
            display_name: &display_name,
            expires_at: &expires_at,
        })
        .send()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API request failed"))?;

    if response.status() != StatusCode::CREATED {
        return Err(registration_failure(response.status()));
    }

    let output = response
        .json::<RegisterTenantOutput>()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API returned an invalid response"))?;
    write_output(&output, "Tenant registration")
}

async fn list() -> Result<()> {
    let response = administration_client()?
        .get(TENANT_ADMINISTRATION_URL)
        .send()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API request failed"))?;

    if response.status() != StatusCode::OK {
        return Err(listing_failure(response.status()));
    }

    let output = response
        .json::<Vec<TenantSummaryOutput>>()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API returned an invalid response"))?;
    write_output(&output, "Tenant listing")
}

async fn show(slug: String) -> Result<()> {
    let tenant_slug = slug.trim();
    if tenant_slug.is_empty() {
        bail!("Tenant slug must not be blank");
    }

    let tenant_id = TenantId::from_slug(tenant_slug);
    let response = administration_client()?
        .get(format!("{TENANT_ADMINISTRATION_URL}/{tenant_id}"))
        .send()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API request failed"))?;

    if response.status() != StatusCode::OK {
        return Err(show_failure(response.status()));
    }

    let output = response
        .json::<TenantDetailOutput>()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API returned an invalid response"))?;
    write_output(&output, "Tenant detail")
}

async fn issue(slug: String, expires_at: String) -> Result<()> {
    let tenant_slug = slug.trim();
    if tenant_slug.is_empty() {
        bail!("Tenant slug must not be blank");
    }
    validate_expiry(&expires_at)?;

    let tenant_id = TenantId::from_slug(tenant_slug);
    let url = format!("{TENANT_ADMINISTRATION_URL}/{tenant_id}/credentials");
    let response = administration_client()?
        .post(url)
        .json(&IssueCredentialRequest {
            expires_at: &expires_at,
        })
        .send()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API request failed"))?;

    if response.status() != StatusCode::CREATED {
        return Err(issuance_failure(response.status()));
    }

    let output = response
        .json::<CredentialOutput>()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API returned an invalid response"))?;
    write_output(&output, "Tenant credential issuance")
}

async fn revoke(slug: String, credential_id: Uuid) -> Result<()> {
    let tenant_slug = slug.trim();
    if tenant_slug.is_empty() {
        bail!("Tenant slug must not be blank");
    }

    let tenant_id = TenantId::from_slug(tenant_slug);
    let url = format!("{TENANT_ADMINISTRATION_URL}/{tenant_id}/credentials/{credential_id}/revoke");
    let response = administration_client()?
        .post(url)
        .send()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API request failed"))?;

    if response.status() != StatusCode::OK {
        return Err(revocation_failure(response.status()));
    }

    let output = response
        .json::<CredentialLifecycleOutput>()
        .await
        .map_err(|_| anyhow!("Private Tenant administration API returned an invalid response"))?;
    write_output(&output, "Tenant credential revocation")
}

fn validate_expiry(expires_at: &str) -> Result<()> {
    DateTime::parse_from_rfc3339(expires_at)
        .map_err(|_| anyhow!("--expires-at must be a valid RFC 3339 timestamp"))?;
    Ok(())
}

fn administration_client() -> Result<reqwest::Client> {
    // Keep every administration command single-shot and reject resubmission
    // through redirects so command outcomes remain explicit.
    reqwest::Client::builder()
        .no_proxy()
        .redirect(redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .map_err(|_| anyhow!("failed to initialize Private Tenant administration API client"))
}

fn write_output(output: &impl Serialize, operation: &str) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, output)
        .with_context(|| format!("writing {operation} response to stdout"))?;
    writeln!(stdout).with_context(|| format!("finishing {operation} response"))?;
    Ok(())
}

fn registration_failure(status: StatusCode) -> anyhow::Error {
    let code = match status {
        StatusCode::BAD_REQUEST => "invalid_tenant_registration",
        StatusCode::CONFLICT => "tenant_already_registered",
        StatusCode::SERVICE_UNAVAILABLE => "credential_authority_unavailable",
        _ => "tenant_registration_failed",
    };
    anyhow!("Tenant registration failed: {code} (HTTP {status})")
}

fn listing_failure(status: StatusCode) -> anyhow::Error {
    let code = match status {
        StatusCode::SERVICE_UNAVAILABLE => "credential_authority_unavailable",
        _ => "tenant_listing_failed",
    };
    anyhow!("Tenant listing failed: {code} (HTTP {status})")
}

fn show_failure(status: StatusCode) -> anyhow::Error {
    let code = match status {
        StatusCode::NOT_FOUND => "tenant_not_found",
        StatusCode::SERVICE_UNAVAILABLE => "credential_authority_unavailable",
        _ => "tenant_detail_failed",
    };
    anyhow!("Tenant detail failed: {code} (HTTP {status})")
}

fn issuance_failure(status: StatusCode) -> anyhow::Error {
    let code = match status {
        StatusCode::BAD_REQUEST => "invalid_credential_issuance",
        StatusCode::NOT_FOUND => "tenant_not_found",
        StatusCode::SERVICE_UNAVAILABLE => "credential_authority_unavailable",
        _ => "tenant_credential_issuance_failed",
    };
    anyhow!("Tenant credential issuance failed: {code} (HTTP {status})")
}

fn revocation_failure(status: StatusCode) -> anyhow::Error {
    let code = match status {
        StatusCode::NOT_FOUND => "credential_not_found",
        StatusCode::SERVICE_UNAVAILABLE => "credential_authority_unavailable",
        _ => "tenant_credential_revocation_failed",
    };
    anyhow!("Tenant credential revocation failed: {code} (HTTP {status})")
}
