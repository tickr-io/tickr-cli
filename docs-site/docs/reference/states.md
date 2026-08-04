---
title: States and statuses
description: Public lifecycle values and forward-compatible client behavior.
sidebar_position: 4
---

# States and statuses

State strings are case-sensitive. Preserve the spelling returned by the API and distinguish lifecycle families.

## Workflow build

| Value | Meaning |
| --- | --- |
| `Building` | Tickr is validating and preparing the definition. |
| `Ready` | The definition can be triggered. |
| `BuildFailed` | Build stopped with a diagnostic; do not trigger this version. |

## Runtime Patch

| Value | Meaning |
| --- | --- |
| `Validating` | Patch source and request are being validated. |
| `Building` | Patch Tasks or graph document are being prepared. |
| `Submitted` | The Patch is durably submitted for application. |
| `Applied` | The live graph change grounded successfully. |
| `Rejected` | The operation was not admissible against the live graph. |
| `BuildFailed` | Patch source or Task build failed. |

## Health

| Value | Meaning |
| --- | --- |
| `healthy` | The component satisfies its current health contract. |
| `degraded` | The component remains observable but has reduced capability. |
| `unhealthy` | The component does not satisfy its health contract. |

Read `readiness.ready` separately. A degraded Control-plane row after startup does not necessarily clear local formation readiness.

## Signal materialization

Signal responses return a string status and may add the resulting `workflow_instance_id`, matched counts, captures, and related workflow identity. A trigger is ready to follow as a Run when its status is `materialized` and `workflow_instance_id` is present.

## Run and Task state

Run and Task projections expose lifecycle strings appropriate to the current runtime contract. Clients should:

- render unknown future values without crashing;
- avoid treating a Task state as the Run state;
- retain attempt/generation identity when correlating retries;
- use terminal projections rather than transient delivery messages as durable authority.

The [OpenAPI reference](/docs/api/tickr-api) is authoritative for the response shape in this release line.
