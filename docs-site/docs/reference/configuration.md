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
| `TICKR_COORDINATOR_HTTP_URL` | All | Coordinator query and health URL |
| `TICKR_COORDINATOR_RELAY_URL` | All | Coordinator relay URL |
| `TICKR_API_BIND_ADDR` | API/Lite | HTTP bind address |
| `TICKR_DSL_PATHS` | Conductor/Lite | Core DSL import paths |
| `TICKR_EXECUTOR_CONCURRENCY` | Executor/Lite | Concurrent Task process slots |
| `TICKR_LIVENESS_TIMEOUT_SECS` | Executor | Task liveness timeout |

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
