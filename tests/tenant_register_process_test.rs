#![cfg(not(madsim))]

use std::fmt::Write as _;
use std::process::Output;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

use axum::{
    extract::{Path, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tickr_proto::TenantId;
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::CancellationToken;

const INITIAL_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const EXPIRES_AT: &str = "2099-01-01T00:00:00Z";
const REPLACEMENT_TOKEN: &str = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
const REPLACEMENT_EXPIRES_AT: &str = "2099-06-01T00:00:00Z";

#[derive(Clone)]
struct FrontendState {
    pool: PgPool,
    registration_requests: Arc<AtomicUsize>,
    listing_requests: Arc<AtomicUsize>,
    show_requests: Arc<AtomicUsize>,
    listing_unavailable: Arc<AtomicBool>,
    issuance_requests: Arc<AtomicUsize>,
    revocation_requests: Arc<AtomicUsize>,
}

#[derive(Deserialize)]
struct RegistrationRequest {
    tenant_slug: String,
    display_name: String,
    expires_at: String,
}

#[derive(Deserialize)]
struct IssuanceRequest {
    expires_at: String,
}

async fn register_tenant(
    State(state): State<FrontendState>,
    Json(request): Json<RegistrationRequest>,
) -> (StatusCode, Json<Value>) {
    state.registration_requests.fetch_add(1, Ordering::SeqCst);
    let tenant_slug = request.tenant_slug.trim();
    let Ok(expires_at) = DateTime::parse_from_rfc3339(&request.expires_at) else {
        return registration_error(StatusCode::BAD_REQUEST, "invalid_tenant_registration");
    };
    let expires_at = expires_at.with_timezone(&Utc);
    if tenant_slug.is_empty() || request.display_name.trim().is_empty() || expires_at <= Utc::now()
    {
        return registration_error(StatusCode::BAD_REQUEST, "invalid_tenant_registration");
    }
    if tenant_slug == "retry-probe" {
        return registration_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        );
    }

    let tenant_id = TenantId::from_slug(tenant_slug).to_string();
    let credential_id = "22670956-423f-41c2-9f91-d075a2958c49";
    let created_at = Utc::now();
    let token_digest = Sha256::digest(INITIAL_TOKEN.as_bytes()).to_vec();
    let inserted = sqlx::query(
        "INSERT INTO frontend_tenant_credentials \
         (tenant_id, tenant_slug, display_name, credential_id, token_sha256, created_at, expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&tenant_id)
    .bind(tenant_slug)
    .bind(&request.display_name)
    .bind(credential_id)
    .bind(token_digest)
    .bind(created_at)
    .bind(expires_at)
    .execute(&state.pool)
    .await;

    if let Err(error) = inserted {
        if error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            == Some("23505")
        {
            return registration_error(StatusCode::CONFLICT, "tenant_already_registered");
        }
        return registration_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        );
    }

    (
        StatusCode::CREATED,
        Json(json!({
            "tenant": {
                "tenant_id": tenant_id,
                "tenant_slug": tenant_slug,
                "display_name": request.display_name,
                "created_at": created_at.to_rfc3339(),
            },
            "credential": {
                "credential_id": credential_id,
                "token": INITIAL_TOKEN,
                "created_at": created_at.to_rfc3339(),
                "expires_at": expires_at.to_rfc3339(),
            },
        })),
    )
}

async fn list_tenants(State(state): State<FrontendState>) -> (StatusCode, Json<Value>) {
    state.listing_requests.fetch_add(1, Ordering::SeqCst);
    if state.listing_unavailable.load(Ordering::SeqCst) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": "credential_authority_unavailable",
                "message": "Tenant summaries are temporarily unavailable",
            })),
        );
    }

    (
        StatusCode::OK,
        Json(json!([
            {
                "tenant_id": TenantId::from_slug("Acme-Private").to_string(),
                "tenant_slug": "Acme-Private",
                "display_name": "Acme Private",
                "created_at": "2026-01-01T00:00:00Z",
                "workflow_definition_count": 2,
                "last_tenant_activity": "2026-02-03T04:05:06Z",
                "credential_count": 1,
            },
            {
                "tenant_id": TenantId::from_slug("Dormant-Private").to_string(),
                "tenant_slug": "Dormant-Private",
                "display_name": "Dormant Private",
                "created_at": "2026-01-02T00:00:00Z",
                "workflow_definition_count": 0,
                "last_tenant_activity": null,
                "credential_count": 0,
            }
        ])),
    )
}

async fn show_tenant(
    State(state): State<FrontendState>,
    Path(tenant_id): Path<String>,
) -> (StatusCode, Json<Value>) {
    state.show_requests.fetch_add(1, Ordering::SeqCst);
    if tenant_id == TenantId::from_slug("retry-probe").to_string() {
        return show_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        );
    }
    if tenant_id != TenantId::from_slug("Acme-Private").to_string() {
        return show_error(StatusCode::NOT_FOUND, "tenant_not_found");
    }

    (
        StatusCode::OK,
        Json(json!({
            "tenant_id": tenant_id,
            "tenant_slug": "Acme-Private",
            "display_name": "Acme Private",
            "created_at": "2026-01-01T00:00:00Z",
            "workflow_definition_count": 2,
            "last_tenant_activity": "2026-02-03T04:05:06Z",
            "credential_count": 2,
            "credentials": [
                {
                    "credential_id": "22670956-423f-41c2-9f91-d075a2958c49",
                    "created_at": "2026-01-01T00:00:00Z",
                    "expires_at": EXPIRES_AT,
                    "revoked_at": null,
                },
                {
                    "credential_id": "89bc1b6b-1d2b-4095-a74a-dd99d545c29a",
                    "created_at": "2026-01-02T00:00:00Z",
                    "expires_at": REPLACEMENT_EXPIRES_AT,
                    "revoked_at": "2026-02-01T00:00:00Z",
                }
            ],
        })),
    )
}

async fn issue_credential(
    State(state): State<FrontendState>,
    Path(tenant_id): Path<String>,
    Json(request): Json<IssuanceRequest>,
) -> (StatusCode, Json<Value>) {
    state.issuance_requests.fetch_add(1, Ordering::SeqCst);
    let Ok(expires_at) = DateTime::parse_from_rfc3339(&request.expires_at) else {
        return issuance_error(StatusCode::BAD_REQUEST, "invalid_credential_issuance");
    };
    let expires_at = expires_at.with_timezone(&Utc);
    if expires_at <= Utc::now() {
        return issuance_error(StatusCode::BAD_REQUEST, "invalid_credential_issuance");
    }
    if tenant_id == TenantId::from_slug("retry-probe").to_string() {
        return issuance_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        );
    }

    let credential_id = "89bc1b6b-1d2b-4095-a74a-dd99d545c29a";
    let created_at = Utc::now();
    let token_digest = Sha256::digest(REPLACEMENT_TOKEN.as_bytes()).to_vec();
    let inserted = sqlx::query(
        "INSERT INTO issued_frontend_credentials \
             (credential_id, tenant_id, token_sha256, created_at, expires_at) \
         SELECT $1, tenant_id, $2, $3, $4 \
         FROM frontend_tenant_credentials \
         WHERE tenant_id = $5",
    )
    .bind(credential_id)
    .bind(token_digest)
    .bind(created_at)
    .bind(expires_at)
    .bind(&tenant_id)
    .execute(&state.pool)
    .await;

    match inserted {
        Ok(result) if result.rows_affected() == 1 => (
            StatusCode::CREATED,
            Json(json!({
                "credential_id": credential_id,
                "token": REPLACEMENT_TOKEN,
                "created_at": created_at.to_rfc3339(),
                "expires_at": expires_at.to_rfc3339(),
            })),
        ),
        Ok(_) => issuance_error(StatusCode::NOT_FOUND, "tenant_not_found"),
        Err(_) => issuance_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        ),
    }
}

async fn revoke_credential(
    State(state): State<FrontendState>,
    Path((tenant_id, credential_id)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    state.revocation_requests.fetch_add(1, Ordering::SeqCst);
    if tenant_id == TenantId::from_slug("retry-probe").to_string() {
        return revocation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        );
    }

    let revoked = sqlx::query_as::<_, (String, DateTime<Utc>, DateTime<Utc>, DateTime<Utc>)>(
        "UPDATE issued_frontend_credentials \
         SET revoked_at = COALESCE(revoked_at, NOW()) \
         WHERE tenant_id = $1 AND credential_id = $2 \
         RETURNING credential_id, created_at, expires_at, revoked_at",
    )
    .bind(tenant_id)
    .bind(credential_id)
    .fetch_optional(&state.pool)
    .await;

    match revoked {
        Ok(Some((credential_id, created_at, expires_at, revoked_at))) => (
            StatusCode::OK,
            Json(json!({
                "credential_id": credential_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "revoked_at": revoked_at,
            })),
        ),
        Ok(None) => revocation_error(StatusCode::NOT_FOUND, "credential_not_found"),
        Err(_) => revocation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "credential_authority_unavailable",
        ),
    }
}

fn revocation_error(status: StatusCode, code: &'static str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "code": code,
            "message": "Tenant credential revocation failed",
        })),
    )
}

fn issuance_error(status: StatusCode, code: &'static str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "code": code,
            "message": "Tenant credential issuance failed",
        })),
    )
}

fn registration_error(status: StatusCode, code: &'static str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "code": code,
            "message": "Tenant registration failed",
        })),
    )
}

fn show_error(status: StatusCode, code: &'static str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({
            "code": code,
            "message": "Tenant detail failed",
        })),
    )
}

async fn protected_tenant_channel(
    State(state): State<FrontendState>,
    Path(asserted_tenant): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    let Some(token) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return StatusCode::UNAUTHORIZED;
    };
    let token_digest = Sha256::digest(token.as_bytes()).to_vec();
    let admitted_tenant = sqlx::query_scalar::<_, String>(
        "SELECT tenant_id FROM frontend_tenant_credentials \
         WHERE token_sha256 = $1 AND expires_at > NOW()",
    )
    .bind(token_digest)
    .fetch_optional(&state.pool)
    .await;

    match admitted_tenant {
        Ok(Some(tenant_id)) if tenant_id == asserted_tenant => StatusCode::OK,
        Ok(Some(_)) => StatusCode::FORBIDDEN,
        Ok(None) => StatusCode::UNAUTHORIZED,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn run_registration(slug: &str, display_name: &str, expires_at: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args([
            "tenant",
            "register",
            "--slug",
            slug,
            "--display-name",
            display_name,
            "--expires-at",
            expires_at,
        ])
        .output()
        .await
        .expect("invoke `tickr tenant register`")
}

async fn run_listing() -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args(["tenant", "list"])
        .output()
        .await
        .expect("invoke `tickr tenant list`")
}

async fn run_show(slug: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args(["tenant", "show", slug])
        .output()
        .await
        .expect("invoke `tickr tenant show`")
}

async fn run_issuance(slug: &str, expires_at: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args([
            "tenant",
            "credential",
            "issue",
            slug,
            "--expires-at",
            expires_at,
        ])
        .output()
        .await
        .expect("invoke `tickr tenant credential issue`")
}

async fn run_revocation(slug: &str, credential_id: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .args(["tenant", "credential", "revoke", slug, credential_id])
        .output()
        .await
        .expect("invoke `tickr tenant credential revoke`")
}

fn assert_secret_safe_failure(output: &Output, digests: &[&str]) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "failure wrote a success document");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains(INITIAL_TOKEN));
    assert!(!stderr.contains(REPLACEMENT_TOKEN));
    for digest in digests {
        assert!(!stderr.contains(digest));
    }
    assert!(!stderr.contains("token_sha256"));
}

fn token_digest_hex(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut digest_hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut digest_hex, "{byte:02x}").unwrap();
    }
    digest_hex
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_registration_process_maps_and_admits_without_retry() {
    let postgres = Postgres::default()
        .start()
        .await
        .expect("start Tenant registration Postgres");
    let postgres_port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("map Tenant registration Postgres port");
    let postgres_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&postgres_url)
        .await
        .expect("connect Tenant registration Postgres");
    sqlx::query(
        "CREATE TABLE frontend_tenant_credentials (\
             tenant_id TEXT PRIMARY KEY,\
             tenant_slug TEXT UNIQUE NOT NULL,\
             display_name TEXT NOT NULL,\
             credential_id TEXT UNIQUE NOT NULL,\
             token_sha256 BYTEA UNIQUE NOT NULL,\
             created_at TIMESTAMPTZ NOT NULL,\
             expires_at TIMESTAMPTZ NOT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("create local Frontend credential authority");
    sqlx::query(
        "CREATE TABLE issued_frontend_credentials (\
             credential_id TEXT PRIMARY KEY,\
             tenant_id TEXT NOT NULL REFERENCES frontend_tenant_credentials (tenant_id),\
             token_sha256 BYTEA UNIQUE NOT NULL,\
             created_at TIMESTAMPTZ NOT NULL,\
             expires_at TIMESTAMPTZ NOT NULL,\
             revoked_at TIMESTAMPTZ NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("create local replacement credential store");

    let registration_requests = Arc::new(AtomicUsize::new(0));
    let listing_requests = Arc::new(AtomicUsize::new(0));
    let show_requests = Arc::new(AtomicUsize::new(0));
    let listing_unavailable = Arc::new(AtomicBool::new(false));
    let issuance_requests = Arc::new(AtomicUsize::new(0));
    let revocation_requests = Arc::new(AtomicUsize::new(0));
    let state = FrontendState {
        pool: pool.clone(),
        registration_requests: registration_requests.clone(),
        listing_requests: listing_requests.clone(),
        show_requests: show_requests.clone(),
        listing_unavailable: listing_unavailable.clone(),
        issuance_requests: issuance_requests.clone(),
        revocation_requests: revocation_requests.clone(),
    };
    let app = Router::new()
        .route("/v1/tenants", get(list_tenants).post(register_tenant))
        .route("/v1/tenants/{tenant_id}", get(show_tenant))
        .route(
            "/v1/tenants/{tenant_id}/credentials",
            post(issue_credential),
        )
        .route(
            "/v1/tenants/{tenant_id}/credentials/{credential_id}/revoke",
            post(revoke_credential),
        )
        .route("/protected/{tenant_id}", get(protected_tenant_channel))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:7000")
        .await
        .expect("bind local Private Tenant administration API");
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
            .expect("serve local Frontend");
    });

    let success = run_registration("  Acme-Private  ", "Acme Private", EXPIRES_AT).await;
    assert!(
        success.status.success(),
        "registration failed: {}",
        String::from_utf8_lossy(&success.stderr)
    );
    assert!(success.stderr.is_empty());
    let stdout = String::from_utf8(success.stdout).expect("registration stdout is UTF-8");
    assert_eq!(
        stdout.lines().count(),
        1,
        "stdout must contain one JSON document"
    );
    assert_eq!(stdout.matches(INITIAL_TOKEN).count(), 1);
    let output: Value = serde_json::from_str(&stdout).expect("parse registration JSON document");
    assert_eq!(output["tenant"]["tenant_slug"], "Acme-Private");
    assert_eq!(output["tenant"]["display_name"], "Acme Private");
    assert_eq!(output["credential"]["token"], INITIAL_TOKEN);
    assert_eq!(registration_requests.load(Ordering::SeqCst), 1);

    let persisted: (String, String, DateTime<Utc>) = sqlx::query_as(
        "SELECT tenant_slug, display_name, expires_at \
         FROM frontend_tenant_credentials WHERE tenant_id = $1",
    )
    .bind(output["tenant"]["tenant_id"].as_str().unwrap())
    .fetch_one(&pool)
    .await
    .expect("read mapped registration request");
    assert_eq!(persisted.0, "Acme-Private");
    assert_eq!(persisted.1, "Acme Private");
    assert_eq!(
        persisted.2,
        DateTime::parse_from_rfc3339(EXPIRES_AT).unwrap()
    );

    let tenant_id = output["tenant"]["tenant_id"].as_str().unwrap();
    let client = reqwest::Client::new();
    let admitted = client
        .get(format!("http://127.0.0.1:7000/protected/{tenant_id}"))
        .bearer_auth(INITIAL_TOKEN)
        .send()
        .await
        .expect("call protected Frontend channel");
    assert_eq!(admitted.status(), StatusCode::OK);
    let mismatched_tenant = TenantId::from_slug("mismatched-tenant");
    let rejected = client
        .get(format!(
            "http://127.0.0.1:7000/protected/{mismatched_tenant}"
        ))
        .bearer_auth(INITIAL_TOKEN)
        .send()
        .await
        .expect("call protected Frontend channel with mismatched Tenant");
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

    let digest_hex = token_digest_hex(INITIAL_TOKEN);
    let replacement_digest_hex = token_digest_hex(REPLACEMENT_TOKEN);

    let listed = run_listing().await;
    assert!(
        listed.status.success(),
        "Tenant listing failed: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    assert!(listed.stderr.is_empty());
    let listed_stdout = String::from_utf8(listed.stdout).expect("listing stdout is UTF-8");
    assert_eq!(
        listed_stdout.lines().count(),
        1,
        "listing stdout must contain one JSON document"
    );
    assert!(!listed_stdout.contains(INITIAL_TOKEN));
    assert!(!listed_stdout.contains(REPLACEMENT_TOKEN));
    assert!(!listed_stdout.contains(&digest_hex));
    assert!(!listed_stdout.contains(&replacement_digest_hex));
    assert!(!listed_stdout.contains("token_sha256"));
    let listed_output: Value =
        serde_json::from_str(&listed_stdout).expect("parse listing JSON document");
    assert_eq!(listed_output.as_array().unwrap().len(), 2);
    assert_eq!(listed_output[0]["tenant_slug"], "Acme-Private");
    assert_eq!(listed_output[0]["workflow_definition_count"], 2);
    assert_eq!(
        listed_output[0]["last_tenant_activity"],
        "2026-02-03T04:05:06Z"
    );
    assert_eq!(listed_output[0]["credential_count"], 1);
    assert!(listed_output[1]["last_tenant_activity"].is_null());
    assert_eq!(listing_requests.load(Ordering::SeqCst), 1);

    listing_unavailable.store(true, Ordering::SeqCst);
    let listing_failure = run_listing().await;
    assert_secret_safe_failure(&listing_failure, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&listing_failure.stderr)
        .contains("credential_authority_unavailable"));
    assert_eq!(listing_requests.load(Ordering::SeqCst), 2);

    let shown = run_show("  Acme-Private  ").await;
    assert!(
        shown.status.success(),
        "Tenant detail failed: {}",
        String::from_utf8_lossy(&shown.stderr)
    );
    assert!(shown.stderr.is_empty());
    let shown_stdout = String::from_utf8(shown.stdout).expect("detail stdout is UTF-8");
    assert_eq!(
        shown_stdout.lines().count(),
        1,
        "detail stdout must contain one JSON document"
    );
    assert!(!shown_stdout.contains(INITIAL_TOKEN));
    assert!(!shown_stdout.contains(REPLACEMENT_TOKEN));
    assert!(!shown_stdout.contains(&digest_hex));
    assert!(!shown_stdout.contains(&replacement_digest_hex));
    assert!(!shown_stdout.contains("token_sha256"));
    let shown_output: Value =
        serde_json::from_str(&shown_stdout).expect("parse detail JSON document");
    assert_eq!(
        shown_output["tenant_id"],
        TenantId::from_slug("Acme-Private").to_string()
    );
    assert_eq!(shown_output["tenant_slug"], "Acme-Private");
    assert_eq!(shown_output["display_name"], "Acme Private");
    assert_eq!(shown_output["workflow_definition_count"], 2);
    assert_eq!(shown_output["last_tenant_activity"], "2026-02-03T04:05:06Z");
    assert_eq!(shown_output["credential_count"], 2);
    assert_eq!(shown_output["credentials"].as_array().unwrap().len(), 2);
    assert!(shown_output["credentials"][0]["revoked_at"].is_null());
    assert_eq!(
        shown_output["credentials"][1]["revoked_at"],
        "2026-02-01T00:00:00Z"
    );
    assert_eq!(shown_output.as_object().unwrap().len(), 8);
    assert_eq!(show_requests.load(Ordering::SeqCst), 1);

    let unknown_detail = run_show("unknown-Tenant").await;
    assert_secret_safe_failure(&unknown_detail, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&unknown_detail.stderr).contains("tenant_not_found"));
    assert_eq!(show_requests.load(Ordering::SeqCst), 2);

    let unavailable_detail = run_show("retry-probe").await;
    assert_secret_safe_failure(&unavailable_detail, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&unavailable_detail.stderr)
        .contains("credential_authority_unavailable"));
    assert_eq!(show_requests.load(Ordering::SeqCst), 3);

    let blank_detail = run_show("   ").await;
    assert_secret_safe_failure(&blank_detail, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&blank_detail.stderr).contains("must not be blank"));
    assert_eq!(show_requests.load(Ordering::SeqCst), 3);

    let old_credential_before: (String, Vec<u8>, DateTime<Utc>, DateTime<Utc>) = sqlx::query_as(
        "SELECT credential_id, token_sha256, created_at, expires_at \
             FROM frontend_tenant_credentials WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&pool)
    .await
    .expect("read initial credential before replacement issuance");

    let issued = run_issuance("  Acme-Private  ", REPLACEMENT_EXPIRES_AT).await;
    assert!(
        issued.status.success(),
        "credential issuance failed: {}",
        String::from_utf8_lossy(&issued.stderr)
    );
    assert!(issued.stderr.is_empty());
    let issued_stdout = String::from_utf8(issued.stdout).expect("issuance stdout is UTF-8");
    assert_eq!(
        issued_stdout.lines().count(),
        1,
        "issuance stdout must contain one JSON document"
    );
    assert_eq!(issued_stdout.matches(REPLACEMENT_TOKEN).count(), 1);
    let issued_output: Value =
        serde_json::from_str(&issued_stdout).expect("parse issuance JSON document");
    assert_eq!(issued_output["token"], REPLACEMENT_TOKEN);
    assert_eq!(issued_output.as_object().unwrap().len(), 4);
    assert_eq!(issuance_requests.load(Ordering::SeqCst), 1);

    let replacement_persisted: (String, Vec<u8>, DateTime<Utc>, Option<DateTime<Utc>>) =
        sqlx::query_as(
            "SELECT tenant_id, token_sha256, expires_at, revoked_at \
             FROM issued_frontend_credentials WHERE credential_id = $1",
        )
        .bind(issued_output["credential_id"].as_str().unwrap())
        .fetch_one(&pool)
        .await
        .expect("read mapped replacement credential request");
    assert_eq!(replacement_persisted.0, tenant_id);
    assert_eq!(
        replacement_persisted.1,
        Sha256::digest(REPLACEMENT_TOKEN.as_bytes()).as_slice()
    );
    assert_eq!(
        replacement_persisted.2,
        DateTime::parse_from_rfc3339(REPLACEMENT_EXPIRES_AT).unwrap()
    );
    assert!(replacement_persisted.3.is_none());
    let old_credential_after: (String, Vec<u8>, DateTime<Utc>, DateTime<Utc>) = sqlx::query_as(
        "SELECT credential_id, token_sha256, created_at, expires_at \
             FROM frontend_tenant_credentials WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_one(&pool)
    .await
    .expect("read initial credential after replacement issuance");
    assert_eq!(old_credential_after, old_credential_before);

    let issued_credential_id = issued_output["credential_id"].as_str().unwrap();
    let wrong_scope = run_revocation("other-Tenant", issued_credential_id).await;
    assert_secret_safe_failure(&wrong_scope, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&wrong_scope.stderr).contains("credential_not_found"));
    assert_eq!(revocation_requests.load(Ordering::SeqCst), 1);

    let revoked = run_revocation("  Acme-Private  ", issued_credential_id).await;
    assert!(
        revoked.status.success(),
        "credential revocation failed: {}",
        String::from_utf8_lossy(&revoked.stderr)
    );
    assert!(revoked.stderr.is_empty());
    let revoked_stdout = String::from_utf8(revoked.stdout).expect("revocation stdout is UTF-8");
    assert_eq!(
        revoked_stdout.lines().count(),
        1,
        "revocation stdout must contain one JSON document"
    );
    assert!(!revoked_stdout.contains(INITIAL_TOKEN));
    assert!(!revoked_stdout.contains(REPLACEMENT_TOKEN));
    assert!(!revoked_stdout.contains(&digest_hex));
    assert!(!revoked_stdout.contains(&replacement_digest_hex));
    assert!(!revoked_stdout.contains("token_sha256"));
    let revoked_output: Value =
        serde_json::from_str(&revoked_stdout).expect("parse revocation JSON document");
    assert_eq!(revoked_output.as_object().unwrap().len(), 4);
    assert_eq!(revoked_output["credential_id"], issued_credential_id);
    assert_eq!(revocation_requests.load(Ordering::SeqCst), 2);
    let persisted_revoked_at: DateTime<Utc> = sqlx::query_scalar(
        "SELECT revoked_at FROM issued_frontend_credentials WHERE credential_id = $1",
    )
    .bind(issued_credential_id)
    .fetch_one(&pool)
    .await
    .expect("read mapped credential revocation");
    assert_eq!(
        revoked_output["revoked_at"],
        serde_json::to_value(persisted_revoked_at).unwrap()
    );

    let repeated = run_revocation("Acme-Private", issued_credential_id).await;
    assert!(repeated.status.success());
    assert_eq!(repeated.stderr, b"");
    let repeated_output: Value = serde_json::from_slice(&repeated.stdout).unwrap();
    assert_eq!(repeated_output, revoked_output);
    assert_eq!(revocation_requests.load(Ordering::SeqCst), 3);

    let unknown_credential =
        run_revocation("Acme-Private", "00000000-0000-4000-8000-000000000001").await;
    assert_secret_safe_failure(&unknown_credential, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&unknown_credential.stderr).contains("credential_not_found"));
    assert_eq!(revocation_requests.load(Ordering::SeqCst), 4);

    let revocation_unavailable = run_revocation("retry-probe", issued_credential_id).await;
    assert_secret_safe_failure(
        &revocation_unavailable,
        &[&digest_hex, &replacement_digest_hex],
    );
    assert!(String::from_utf8_lossy(&revocation_unavailable.stderr)
        .contains("credential_authority_unavailable"));
    assert_eq!(revocation_requests.load(Ordering::SeqCst), 5);

    let unknown = run_issuance("unknown-Tenant", REPLACEMENT_EXPIRES_AT).await;
    assert_secret_safe_failure(&unknown, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("tenant_not_found"));
    assert_eq!(issuance_requests.load(Ordering::SeqCst), 2);

    let issuance_unavailable = run_issuance("retry-probe", REPLACEMENT_EXPIRES_AT).await;
    assert_secret_safe_failure(
        &issuance_unavailable,
        &[&digest_hex, &replacement_digest_hex],
    );
    assert!(String::from_utf8_lossy(&issuance_unavailable.stderr)
        .contains("credential_authority_unavailable"));
    assert_eq!(
        issuance_requests.load(Ordering::SeqCst),
        3,
        "non-idempotent issuance must not be retried"
    );

    let invalid_issuance = run_issuance("never-sent", "not-rfc3339").await;
    assert_secret_safe_failure(&invalid_issuance, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&invalid_issuance.stderr).contains("RFC 3339"));
    assert_eq!(issuance_requests.load(Ordering::SeqCst), 3);

    let non_future = run_issuance("Acme-Private", "2000-01-01T00:00:00Z").await;
    assert_secret_safe_failure(&non_future, &[&digest_hex, &replacement_digest_hex]);
    assert!(String::from_utf8_lossy(&non_future.stderr).contains("invalid_credential_issuance"));
    assert_eq!(issuance_requests.load(Ordering::SeqCst), 4);

    let duplicate = run_registration("Acme-Private", "Acme Private", EXPIRES_AT).await;
    assert_secret_safe_failure(&duplicate, &[&digest_hex]);
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("tenant_already_registered"));
    assert_eq!(registration_requests.load(Ordering::SeqCst), 2);

    let unavailable = run_registration("retry-probe", "Retry Probe", EXPIRES_AT).await;
    assert_secret_safe_failure(&unavailable, &[&digest_hex]);
    assert!(
        String::from_utf8_lossy(&unavailable.stderr).contains("credential_authority_unavailable")
    );
    assert_eq!(
        registration_requests.load(Ordering::SeqCst),
        3,
        "non-idempotent registration must not be retried"
    );

    let invalid = run_registration("never-sent", "Invalid Input", "not-rfc3339").await;
    assert_secret_safe_failure(&invalid, &[&digest_hex]);
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("RFC 3339"));
    assert_eq!(registration_requests.load(Ordering::SeqCst), 3);

    shutdown.cancel();
    server.await.expect("join local Frontend");
    pool.close().await;
    drop(postgres);
}
