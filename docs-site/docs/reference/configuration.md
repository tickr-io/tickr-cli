---
title: Configuration reference
description: Public operational environment variables grouped by responsibility and formation.
sidebar_position: 3
---

# Configuration reference

These variables configure released runtime behavior. Test helpers and variables injected into Task processes are not operator configuration.

## Shared

| Variable | Applies to | Meaning |
| --- | --- | --- |
| `TICKR_TENANT_SLUG` | All | Tenant identity |
| `TICKR_CTRL_HTTP_URL` | All | Control-plane HTTP subquery channel URL |
| `TICKR_CTRL_RELAY_URL` | All | Control-plane Conductor relay URL |
| `TICKR_CONTROL_PLANE_BEARER_TOKEN` | API/Conductor/Lite | Canonical 43-character Tenant bearer token for protected HTTP and relay traffic |
| `TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK` | API/Conductor/Lite | Exact `true` permits development loopback `http://` only |
| `TICKR_API_BIND_ADDR` | API/Lite | HTTP bind address |
| `TICKR_DSL_PATHS` | Conductor/Lite | Core DSL import paths |
| `TICKR_EXECUTOR_CONCURRENCY` | Executor/Lite | Concurrent Task process slots |
| `TICKR_LIVENESS_TIMEOUT_SECS` | Executor | Task liveness timeout |

### Pre-GA migration

`TICKR_COORDINATOR_HTTP_URL` and `TICKR_COORDINATOR_RELAY_URL` are obsolete,
unsupported, and ignored. Set the `TICKR_CTRL_*` replacements explicitly for a
remote Control plane. If either replacement is absent, Tickr retains its existing
loopback default for that channel.

### Control-plane connection security

`TICKR_CONTROL_PLANE_BEARER_TOKEN` must be the canonical unpadded base64url
encoding of exactly 32 random bytes: exactly 43 ASCII characters matching
`[A-Za-z0-9_-]{43}` and unchanged by decode then re-encode. The value is
validated without trimming at process startup whenever a Control-plane endpoint
is configured. Both endpoints otherwise require `https://` with normal
certificate-chain and hostname verification. The loopback opt-in never allows
non-loopback plaintext and never bypasses authentication.

The Control-plane Frontend selects its authority with required
`TICKR_CTRL_CREDENTIALS_FILE`. The readable regular file is strict UTF-8 JSON
with exactly `{"schema_version":1,"credentials":[...]}` at the top level and at
least one record. Each record has exactly `token_sha256`, `tenant_id`,
`expires_at`, and `revoked`: the digest is 64 lowercase hexadecimal characters
containing SHA-256 of the token's exact ASCII bytes, the Tenant ID is a
canonical UUID string, expiry is RFC 3339, and revocation is a JSON boolean.
Unknown, missing, extra, duplicate, raw-token, malformed, unsupported-version,
or empty-list input rejects Frontend startup before listeners bind. Authority
changes require a controlled Frontend restart. Secret delivery and ACLs are
deployment responsibilities; the application checks readability and
regular-file type, not mode bits or platform ACLs.

### Tickr Lite invitation

`tickr-cli setup --from <path>` accepts a strict UTF-8 JSON invitation with these
fields:

```json
{
  "format_version": 1,
  "tenant_slug": "acme-demo",
  "credential": "<43-character base64url token>",
  "control_plane_http_url": "https://ctrl.example.com",
  "control_plane_relay_url": "https://relay.example.com",
  "compatible_lite_version": "0.1.5",
  "expires_at": "2026-12-31T23:59:59Z"
}
```

Unknown or missing fields, unsupported format versions, malformed credentials,
non-HTTPS endpoints, expired invitations, and invitations for another Tickr
Lite version reject setup before the local profile or data directory is
created. The invitation and generated profile both contain the Tenant
credential and must remain private.

For an extracted installation, setup defaults the profile to
`profile/config.json` and durable state to `data/` inside the resolved Tickr
Lite release directory. `TICKR_CONFIG_PATH=<absolute-path>` and
`--data-dir <path>` override those locations. The profile directory and data
directory use mode `0700`; the profile uses mode `0600`. `tickr-cli` and
`tickr-lite` resolve the release-local profile from the executable location,
not the caller's working directory. Source-workspace operation retains the
global `$HOME/.config/tickr/config.json` default.

Rerunning setup for an existing installation profile preserves that profile's
recorded data directory unless the override is supplied. An installed release
does not fall back to a global profile belonging to another Tenant.

## SQL

| Variable | Applies to | Meaning |
| --- | --- | --- |
| `TICKR_SQL_BACKEND` | API/Conductor/Lite | `postgres` or `sqlite` |
| `TICKR_SQL_TOPOLOGY` | SQLite | Must be `single-node` |
| `TICKR_CONDUCTOR_SQLITE_URL` | Lite/single node | SQLite database URL |
| `TICKR_CONDUCTOR_POSTGRES_URL` | Distributed | Postgres connection URL |

Postgres ignores `TICKR_SQL_TOPOLOGY`. SQLite requires the explicit single-node topology.

## all-NATS

| Variable | Meaning |
| --- | --- |
| `TICKR_NATS_URL` | NATS account endpoint used by the admitted all-NATS protocol set |

The presence of this variable does not select the profile.

## all-Redis

| Variable | Meaning |
| --- | --- |
| `TICKR_REDIS_CONNECTION_DESCRIPTOR` | TLS endpoint/topology descriptor |
| `TICKR_REDIS_ROLE_CREDENTIALS` | Per-role authentication material |
| `TICKR_REDIS_NAMESPACE` | Formation namespace identity |
| `TICKR_REDIS_CAPACITY_BYTES` | Admitted capacity limit |

The presence of Redis configuration does not select `all-redis`; pass `--formation all-redis`.

## final-Log storage

| Variable | Meaning |
| --- | --- |
| `TICKR_LOG_STORAGE_ENDPOINT` | S3-compatible endpoint |
| `TICKR_LOG_STORAGE_BUCKET` | final-Log bucket |
| `TICKR_LOG_STORAGE_REGION` | Region |
| `TICKR_LOG_STORAGE_ACCESS_KEY_ID` | Access key identity |
| `TICKR_LOG_STORAGE_SECRET_ACCESS_KEY` | Secret key |
| `TICKR_LOG_BUFFER_CAPACITY` | Staging buffer capacity |
| `TICKR_LOG_RECORD_MAX_BYTES` | Maximum staged record size |
| `TICKR_LOG_FLUSH_DEADLINE_MS` | Flush deadline |
| `TICKR_LOG_PUBLISH_TIMEOUT_MS` | Publish timeout |
| `TICKR_LOG_PUBLISH_BACKOFF_MAX_MS` | Maximum publish backoff |

## Task process environment

Tickr injects Task identity and context such as `TICKR_NS`, `TICKR_RUN_ID`, `TICKR_TASK_ID`, `TICKR_TASK_INSTANCE_ID`, `TICKR_INPUTS`, `TICKR_OUTPUTS`, and the scoped `tickr-ctx` connection. Workflow code consumes these values; operators should not set them globally on runtime components.
