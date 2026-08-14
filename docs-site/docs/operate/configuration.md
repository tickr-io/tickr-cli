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
| `TICKR_API_BIND_ADDR` | API and embedded Console bind address |
| `TICKR_DSL_PATHS` | Release-matched Core DSL import paths |
| `TICKR_EXECUTOR_CONCURRENCY` | Executor process-slot limit |

The obsolete `TICKR_COORDINATOR_HTTP_URL` and `TICKR_COORDINATOR_RELAY_URL`
variables are unsupported and ignored. When either new variable is absent, Tickr
uses its existing loopback default.

Bind loopback by default. A non-loopback API requires an explicit authenticated TLS ingress design.

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

Keep environment files mode `0600`. Do not commit `.env.local`, `tickr-lite.env`, Redis descriptors, role credentials, TLS roots, database URLs, or object-store keys.

See the [configuration reference](../reference/configuration.md) for the concise variable table.
