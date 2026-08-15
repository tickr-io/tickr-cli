---
title: Configuration
description: Configure identity, Control-plane connection, storage, and the selected formation.
sidebar_position: 2
---

# Configuration

Configuration is part of formation admission. A value that changes topology, durable identity, or protocol behavior must agree with the selected profile before Tickr starts listeners, consumers, claims, or producers.

## Shared identity and connection

| Variable | Purpose |
| --- | --- |
| `TICKR_TENANT_SLUG` | Tenant identity for this Data plane |
| `TICKR_CTRL_HTTP_URL` | Control-plane HTTP subquery channel for live queries and health reads |
| `TICKR_CTRL_RELAY_URL` | Bidirectional Control-plane Conductor relay endpoint |
| `TICKR_CONTROL_PLANE_BEARER_TOKEN` | One Tenant bearer token sent on every protected HTTP query and relay establishment |
| `TICKR_ALLOW_INSECURE_CONTROL_PLANE_LOOPBACK` | Development-only opt-in for loopback `http://`; never permits non-loopback plaintext |
| `TICKR_API_BIND_ADDR` | API and embedded Console bind address |
| `TICKR_DSL_PATHS` | Release-matched Core DSL import paths |
| `TICKR_EXECUTOR_CONCURRENCY` | Executor process-slot limit |

The bearer token is the canonical unpadded base64url encoding of exactly 32
random bytes: exactly 43 ASCII characters matching `[A-Za-z0-9_-]{43}`, with
decode-then-re-encode identity. Values are not trimmed. API and Conductor
validate it at startup whenever a Control-plane endpoint is configured and keep
it out of URLs and diagnostics.

Remote HTTP and relay endpoints must use `https://` with normal certificate
chain and hostname verification. The loopback setting must be exactly `true`,
applies only to an explicit loopback `http://` endpoint, and never disables
bearer authentication. The obsolete `TICKR_COORDINATOR_HTTP_URL` and
`TICKR_COORDINATOR_RELAY_URL` variables are unsupported and ignored.

### Control-plane Frontend authority

The Control-plane Frontend requires `TICKR_CTRL_CREDENTIALS_FILE`. The selected
readable regular file is strict UTF-8 JSON whose top-level object contains
exactly `schema_version` and `credentials`: `schema_version` is the integer `1`
and `credentials` is a non-empty array. Every record contains exactly:

- `token_sha256`: 64 lowercase hexadecimal characters containing SHA-256 of the
  token's exact ASCII bytes;
- `tenant_id`: a canonical Tenant UUID string;
- `expires_at`: an RFC 3339 timestamp;
- `revoked`: a JSON boolean.

Unknown or missing fields, raw-token fields, duplicate digests, unsupported
versions, invalid values, malformed JSON, or an empty list reject the complete
Frontend startup before a listener binds. File changes take effect only after a
controlled Frontend restart. Deployment must deliver the file and environment
value through its secret mechanism and restrict their ACLs to the service
identity; the application verifies readability and regular-file type but does
not interpret filesystem mode bits or platform ACLs.

Bind the local API to loopback by default. A non-loopback API requires a
separate authenticated TLS ingress design.

## Tickr Lite

| Variable | Required value or role |
| --- | --- |
| `TICKR_STATE_DIR` | Private durable root owned by the process user |
| `TICKR_SQL_BACKEND` | `sqlite` |
| `TICKR_SQL_TOPOLOGY` | `single-node` |
| `TICKR_CONDUCTOR_SQLITE_URL` | SQLite URL below durable local storage |

The state root must be an admitted local filesystem, owned by the effective user, and mode `0700`. Files use mode `0600`. Network and unknown filesystems are rejected.

## Distributed SQL and logs

| Variable | Purpose |
| --- | --- |
| `TICKR_CONDUCTOR_POSTGRES_URL` | Shared Postgres repository |
| `TICKR_LOG_STORAGE_ENDPOINT` | S3-compatible endpoint |
| `TICKR_LOG_STORAGE_BUCKET` | final-Log bucket |
| `TICKR_LOG_STORAGE_REGION` | Storage region |
| `TICKR_LOG_STORAGE_ACCESS_KEY_ID` | Storage credential identity |
| `TICKR_LOG_STORAGE_SECRET_ACCESS_KEY` | Storage credential secret |

All Data-plane processes in one formation must address the same admitted stores and tenant scope.

## all-NATS

`TICKR_NATS_URL` selects the NATS account used by the complete `all-nats` protocol set. Endpoint presence does not select the formation; the CLI profile does.

Tickr verifies the fresh profile-qualified namespace and exact streams, consumers, and key-value buckets before opening runtime work.

## all-Redis

`TICKR_REDIS_CONNECTION_DESCRIPTOR`, `TICKR_REDIS_ROLE_CREDENTIALS`, `TICKR_REDIS_NAMESPACE`, and `TICKR_REDIS_CAPACITY_BYTES` participate in `all-redis` admission.

Redis endpoints must use `rediss`, contain no embedded credentials, and resolve to the admitted Redis OSS 7.4.x standalone writable primary. Credentials and trust roots belong in dedicated secret fields, not URLs or diagnostics.

## Secrets

Keep environment files mode `0600`. Do not commit `.env.local`,
`tickr-lite.env`, the Control-plane bearer token, Redis descriptors, role
credentials, TLS roots, database URLs, or object-store keys.

See the [configuration reference](../reference/configuration.md) for the concise variable table.
