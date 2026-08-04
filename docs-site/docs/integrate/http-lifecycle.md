---
title: HTTP lifecycle and conventions
description: Handle asynchronous commands, resource identities, readiness, and errors correctly.
sidebar_position: 1
---

# HTTP lifecycle and conventions

The API uses resource identities to separate Command acceptance from eventual outcome. Persist the returned identity and reconcile through its read endpoint.

## Base URL

Tickr Lite binds to loopback by default:

```bash
export TICKR_API_URL="http://127.0.0.1:6000"
```

The embedded Console uses the same origin. Distributed deployments define their own authenticated ingress and TLS boundary.

## Readiness

`GET /api/health` remains available while the formation is unready. Work-producing `/api/*` routes remain behind the formation's admission gate.

Check both:

- formation readiness;
- Control-plane health.

After startup, temporary Control-plane loss degrades its health row without discarding local durable state.

## Asynchronous Commands

### Registration

`POST /api/workflows/register` returns a Workflow definition identity. Read the workflow collection or definition projection until its build is `Ready` or `BuildFailed`.

### Triggering

`POST /api/workflows/{workflow_id}/trigger` returns a Signal identity. Read `/api/signals/{signal_id}` until the Signal materializes and exposes `workflow_instance_id`.

### Runtime Patches

Patch submission returns before the graph operation necessarily reaches `Applied`. Observe the Patch resource and stop on a rejected or failed terminal outcome.

## Timeouts do not imply cancellation

A caller timeout can occur after the Data-plane writer has accepted a mutation. The writer may complete it and discard the late reply. Do not blindly resubmit a non-idempotent request after a timeout; reconcile by the known resource or idempotency identity first.

## API areas

The OpenAPI contract groups routes under:

- workflows and workflow instances;
- Signals;
- Patches;
- Events;
- health;
- tenant and dashboard projections.

Use the [rendered HTTP API reference](/docs/api/tickr-api) for exact request and response schemas in this release line.

## Example: follow a trigger

```bash
trigger_response="$(curl -fsS -X POST \
  "$TICKR_API_URL/api/workflows/$workflow_id/trigger" \
  -H 'Content-Type: application/json' \
  -d '{"name":"integration run"}')"

signal_id="$(jq -er '.signal_id' <<<"$trigger_response")"
curl -fsS "$TICKR_API_URL/api/signals/$signal_id" |
  jq '{status, workflow_instance_id}'
```

Fetch current state at a cadence appropriate to your client. Do not run an unbounded tight polling loop.
