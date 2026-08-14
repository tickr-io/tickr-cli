---
title: Troubleshooting
description: Diagnose readiness, build, Signal, Task, storage, and formation admission failures.
sidebar_position: 7
---

# Troubleshooting

Start from the first lifecycle boundary that is not healthy. Do not reset durable state until you understand which authority rejected or stalled the operation.

## Tickr Lite is not ready

Fetch health once:

```bash
curl -fsS "$TICKR_API_URL/api/health" |
  jq '{readiness, formation, data_plane_sql, control_plane}'
```

Check:

- the selected profile is `lite-local`;
- the state directory owner and mode are correct;
- SQLite migration and formation metadata match this binary;
- the API bind address is available;
- `TICKR_CTRL_HTTP_URL` and `TICKR_CTRL_RELAY_URL` are present and valid;
- every critical child started.

`/api/health` remains available while work-producing routes are closed.

## The data directory is locked

Only one Tickr Lite process can own a state directory. Stop the existing owner cleanly. Do not delete lock or manifest files to bypass admission.

If no expected process owns the directory, inspect the prior process and filesystem state before restarting. Lock acquisition is intentionally not retried behind a partial startup.

## A workflow stays `Building`

Read the workflow projection again after allowing the asynchronous build to progress. If it reaches `BuildFailed`, inspect the diagnostic and validate the exact submitted Nickel source with the release-matched evaluator and Core DSL.

Common causes:

- importing a DSL from another release;
- unknown closed-record fields;
- raw strings where typed Task, Signal, or routing-variable references are required;
- invalid edge gate/kind combinations;
- unavailable Nix input or build failure.

## A trigger has no Run identity

Follow the returned Signal, not the workflow list. A Run identity appears only after the trigger Signal materializes.

```bash
curl -fsS "$TICKR_API_URL/api/signals/$signal_id" |
  jq '{status, workflow_id, workflow_instance_id}'
```

Check Control-plane health and the Signal outcome before resubmitting.

## A runtime Patch does not affect the graph

Read the Patch status. `Submitted` is not `Applied`.

- `Rejected`: inspect the reason and confirm every live identity belongs to this Run/version.
- `BuildFailed`: validate the Patch's Nickel and Task build inputs.
- `Applied`: use `applied_version` to fetch the corresponding graph projection.

## A Task does not progress

Inspect the Task state, attempt identity, gates, inputs, and logs. Distinguish:

- waiting on a predecessor;
- waiting on a Signal, timer, or predicate gate;
- waiting on Executor capacity;
- claimed/running with missing liveness;
- terminal failure with retry attempts remaining;
- terminal failure with no retries remaining.

## A request timed out

Do not assume the mutation was cancelled. Reconcile using the returned resource or idempotency identity. Blindly repeating a request can create a second accepted operation when the first completed after the caller timed out.

## Distributed formation admission fails

Treat admission failures as capability or identity failures, not transient warnings.

For `all-nats`, verify the exact profile namespace and configured stream, consumer, and key-value identities. For `all-redis`, verify TLS, credentials, Redis 7.4.x standalone writable-primary topology, time, durability, role manifests, namespace, and capacity. Mixed or partial profiles are unsupported.

:::danger Destructive reset
Deleting SQLite, Postgres, JetStream, Redis, object-store, journal, or manifest state destroys different parts of the durable formation. Never use a reset as routine troubleshooting. Take a coherent backup and identify the failed authority first.
:::
