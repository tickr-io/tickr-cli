# Conductor per-task build pipeline contract

**Status:** Current.

**Code:** `src/conductor/src/build_pipeline/` and `src/migrations/src/definition_repository.rs`.

## What this component is

The per-task definition build pipeline. Its work unit is one Task belonging to one committed Workflow definition version. It runs the Task's Nix build through the injected `BuildExecutor`, records the per-Task result, and invokes the definition repository's last-one-out aggregate transition.

The distributed formation transports `TaskBuildJob` pointers through the `conductor_build_queue` NATS queue group. Tickr Lite does not use that queue. It selects the same committed `workflow_task_builds` lifecycle rows through the one-writer SQLite repository.

## Durable lifecycle

Registration commits the `Building` definition row and all `pending` per-Task build rows in one transaction. A queue message or local notification emitted afterward is never the source of build work.

A Tickr Lite build is eligible when:

- its per-Task row remains `pending`;
- its parent definition remains `Building`; and
- it has no lease or its previous lease has expired.

The SQLite writer selects eligible rows by `pending_since`, Workflow identity, version, and Task identity. Every scan has a fixed batch limit. Selection and lease acquisition run through the formation's sole writer connection and commit the lease owner, opaque lease token, and expiry before the executor starts.

The processor scans once during startup and at a bounded steady-state interval. A bounded in-process notification may request an earlier scan after registration commits. A full channel, a closed channel, a dropped sender, or a process exit loses only that latency hint. It does not acknowledge, remove, or settle a lifecycle row.

## Lease and settlement law

Nix realization is the retryable operation behind definition-build leases. It is idempotent for a given expression path. Lease expiry may therefore authorize another realization, but it does not itself mutate the Task result or parent definition.

Settlement is one conditional writer transaction. The caller must still own the exact lease token and owner, and the lease must remain unexpired. A stale worker receives `LeaseLost` and performs no lifecycle or publication side effect. Successful settlement clears the lease fields while recording the Task result.

The existing typed definition finalizer remains authoritative:

- a failed Task records `failure` and atomically wins `Building → BuildFailed`;
- a successful Task records `success` and returns `AwaitingTasks` while a sibling remains unsettled;
- the last successful Task atomically wins `Building → Ready` and returns the decoded definition as the single submission intent;
- duplicate, late, and losing finalizers return typed non-winning outcomes and publish nothing.

A process death before settlement leaves a pending leased row. A later scan may reacquire it only after expiry. A process death after settlement finds no eligible row on restart. Reopening a failed definition clears obsolete leases before its pending work is reconsidered, so an old worker cannot settle into the reopened lifecycle.

Definition-build leases are not task-dispatch claims. They must not be reused for dispatch pickup, process launch, liveness, or cancellation choreography.

## Publication boundary

For the distributed formation, only the winning `Ready` outcome publishes a `SubmissionMessage` after commit.

For Tickr Lite, the committed `Ready` definition is the durable submission intent. Submission reconciliation selects that row independently. Build notification state and worker lifetime are irrelevant to submission recovery.

## Invariants

- **Stable bounded selection.** Local scans lease committed eligible rows in deterministic order and never take an unbounded batch.
- **Commit before execution.** The executor runs only after lease acquisition commits.
- **Notification loss is harmless.** Startup and periodic durable scans recover work without an in-memory receipt.
- **Expired work is safe to reconsider.** Only the idempotent Nix realization can repeat; the lease-guarded finalizer remains conditional.
- **Concurrent-finalizer atomicity.** Simultaneous successful Task settlements yield one `Ready` winner and typed losing outcomes.
- **`BuildFailed` is terminal until explicit re-registration.** Later results cannot revive the definition.
- **Per-task work only.** No whole-Workflow build work item exists.

The file-backed SQLite restart suite covers commit before notification, full/closed/missed notification hints, startup and steady-state recovery, worker death before and after settlement, stable lease ordering, lease expiry, and competing parent finalizers.
