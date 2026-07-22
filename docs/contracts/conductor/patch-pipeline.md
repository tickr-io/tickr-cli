# Conductor Patch pipeline contract

**Status:** Current.

**Code:** `src/conductor/src/patch_pipeline.rs`, `src/conductor/src/patch_pipeline/local.rs`, and `src/migrations/src/patch_repository.rs`.

## What this component is

The Patch pipeline owns an accepted Patch document from durable ingress through optional per-Task builds, validate-and-apply relay, and terminal outcome correlation. Patch identity and published request and response shapes do not vary by formation.

The distributed formation transports per-Task build pointers through the `conductor_patch_build_queue` NATS queue group and uses periodic durable Patch re-drive. Tickr Lite does not use that build queue. It selects committed Patch build and lifecycle rows through the one-writer SQLite repository.

## Durable identity and lifecycle

`patch_key = UUIDv5(workflow_instance_id, patch_id)` is the stable identity for ingress deduplication, build finalization, apply re-drive, and outcome correlation. The parsed operations and optional operation descriptor commit on `workflow_patches` before any build or relay effect. `workflow_patch_task_builds` contains one committed `pending` row for every new Task.

A Patch follows the existing lifecycle:

- `Validating → Submitted → Applied | Rejected` when it adds no Task;
- `Building → Submitted → Applied | Rejected` when every added Task builds; or
- `Building → BuildFailed` when any added Task build fails.

`Applied`, `Rejected`, and `BuildFailed` are terminal. A duplicate ingress returns the existing row. A late or duplicate outcome cannot reopen a terminal row.

## Tickr Lite selection and notifications

Tickr Lite runs one startup scan before its steady-state loop. Timer-led scans remain bounded. A bounded in-process notification may request an immediate scan after ingress commits, but a full channel, closed channel, dropped sender, or process exit loses only that latency hint.

Build work is eligible when the per-Task row is `pending`, the parent Patch is `Building`, and its previous lease is absent or expired. Selection orders by `pending_since`, Patch identity, and Task identity.

Lifecycle work is eligible when the Patch is `Validating` or `Submitted`, its backoff age is satisfied, and its previous lifecycle lease is absent or expired. Selection orders by `updated_at` and Patch identity. Startup and notification-led reconciliation use zero minimum age; steady-state scans apply the configured bounded backoff.

Every selection has a fixed batch limit. Selection and lease acquisition commit an owner, opaque token, and expiry through the formation's sole SQLite writer before a build executor or relay sender runs.

## Build lease and finalizer law

Nix realization is idempotent for a Task expression path. Lease expiry may authorize another realization, but only the exact live lease owner and token may settle the per-Task row.

Settlement records the Task result, clears its lease, and evaluates the parent finalizer in one writer transaction. The existing typed outcomes remain authoritative:

- a failed Task atomically wins `Building → BuildFailed`;
- a successful Task returns `AwaitingTasks` while a sibling remains pending;
- the last successful Task atomically wins `Building → Submitted` and returns the single apply intent;
- stale leases return `LeaseLost`; and
- duplicate, late, absent, and already-settled work return their existing typed non-winning outcomes.

Only the `Submitted` winner attempts the validate-and-apply relay. A crash after the finalizer commit but before relay leaves a committed `Submitted` row for lifecycle recovery. A crash before settlement leaves a leased `pending` row that becomes eligible after lease expiry.

## Lifecycle lease and effect law

A lifecycle worker rebuilds the validate-and-apply envelope only from the leased committed row. A successful send conditionally records `Submitted`, refreshes `updated_at`, and clears the exact live lease. A failed send conditionally releases the lease and leaves the durable row eligible for a later scan. A stale worker cannot settle or clear another owner's lease.

A process can fail after the relay accepts an envelope but before local lease settlement. Recovery may therefore send the same envelope again. Every retry carries the same `patch_key`; the existing server-side Patch identity absorbs redelivery so only one Patch application wins. The local lease prevents concurrent ordinary processing, while stable identity resolves the unavoidable post-effect crash ambiguity.

Terminal outcome correlation remains a conditional transition over the same `patch_key` and Workflow-instance identity. Its winning transaction records the terminal state and clears any lifecycle lease. Duplicate outcomes return a typed non-winning correlation and create no second terminal transition.

## Invariants

- **Durable source.** Channels and worker lifetimes never create, acknowledge, or remove Patch work.
- **Stable bounded selection.** Every local scan leases a deterministic, bounded set of committed rows.
- **Commit before execution.** Build or relay work starts only after lease acquisition commits.
- **Lease-guarded settlement.** An expired or superseded lease cannot mutate a Task row, parent finalizer, or lifecycle marker.
- **Exactly one finalizer.** Competing successful Task settlements yield one `Submitted` winner.
- **Stable effect identity.** Ambiguous or repeated relay sends retain one `patch_key` and therefore one winning apply.
- **Terminal monotonicity.** No notification, expired lease, build result, relay retry, or outcome correlation reopens a terminal Patch.

The file-backed SQLite suite covers restart after ingress, claim survival and expiry, build execution, competing finalizers, committed apply intent recovery, terminal settlement, and full or closed notification channels.
