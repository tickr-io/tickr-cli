# Conductor per-task build pipeline contract

**Status:** Current.

**Code:** `src/conductor/src/build_pipeline/` and `src/migrations/src/definition_repository.rs`.

## What this component is

The per-task definition build pipeline. Its work unit is one Task belonging to one committed Workflow definition version. It runs the Task's Nix build through the injected `BuildExecutor`, records the per-Task result, and invokes the definition repository's last-one-out aggregate transition.

The selected SQL repository is the source of definition-build work in every formation. Distributed NATS `TaskBuildJob` delivery and Tickr Lite's bounded in-process channel are advisory notifications only.

## Durable lifecycle

Registration commits the `Building` definition row and all `pending` per-Task build rows in one transaction. A notification emitted afterward is never the source of build work.

A definition build is eligible when:

- its per-Task row remains `pending`;
- its parent definition remains `Building`; and
- it has no lease or its previous lease has expired.

The repository selects eligible rows by `pending_since`, Workflow identity, version, and Task identity with a fixed batch limit. It conditionally commits a lease owner, opaque token, and expiry before the executor starts. Postgres contenders lock eligible rows with skip-locked semantics; Tickr Lite serializes the same operation through its sole SQLite writer.

Every processor scans once during startup and at a bounded steady-state interval. A NATS message or bounded in-process notification may request an earlier scan after registration commits. Loss, duplication, reordering, a full or closed channel, and process exit affect only that latency hint.

## Lease and settlement law

Nix realization is the retryable operation behind definition-build leases. It is idempotent for a given expression path. Lease expiry may therefore authorize another realization, but it does not itself mutate the Task result or parent definition.

Settlement is one conditional repository transaction. The caller must still own the exact lease token and owner, and the lease must remain unexpired. A stale worker receives `LeaseLost` and performs no lifecycle or publication side effect. Successful settlement clears the lease fields while recording the Task result.

The existing typed definition finalizer remains authoritative:

- a failed Task records `failure` and atomically wins `Building → BuildFailed`;
- a successful Task records `success` and returns `AwaitingTasks` while a sibling remains unsettled;
- the last successful Task atomically wins `Building → Ready` and returns the decoded definition as the single submission intent;
- duplicate, late, and losing finalizers return typed non-winning outcomes and publish nothing.

A process death before settlement leaves a pending leased row. A later scan may reacquire it only after expiry. A process death after settlement finds no eligible row on restart. Task settlement and parent finalization share one transaction, so a crash cannot expose a settled last child beneath a still-`Building` parent. Reopening a failed definition clears obsolete leases before its pending work is reconsidered.

Definition-build leases are not task-dispatch claims. They must not be reused for dispatch pickup, process launch, liveness, or cancellation choreography.

## Publication boundary

The winning `Ready` row is authoritative definition-submission work. The submission reconciler independently leases committed `Ready` rows, forwards the unchanged definition, and conditionally settles `Ready → Submitted`.

NATS submission pointers and Tickr Lite submission notifications request earlier scans only. Lost, duplicate, or reordered pointers do not create or settle repository state.

## Invariants

- **Stable bounded selection.** Reconciliation scans lease committed eligible rows in deterministic order and never take an unbounded batch.
- **Commit before execution.** The executor runs only after lease acquisition commits.
- **Notification loss is harmless.** Startup and periodic durable scans recover work without an in-memory receipt.
- **Expired work is safe to reconsider.** Only the idempotent Nix realization can repeat; the lease-guarded finalizer remains conditional.
- **Concurrent-finalizer atomicity.** Simultaneous successful Task settlements yield one `Ready` winner and typed losing outcomes.
- **`BuildFailed` is terminal until explicit re-registration.** Later results cannot revive the definition.
- **Per-task work only.** No whole-Workflow build work item exists.

The file-backed SQLite and Postgres suites cover notification-free startup and steady-state scans, bounded stable ordering, competing claims, real process death before claim, during execution, and before settlement, lease expiry and reclaim, conditional settlement, and one winning parent finalizer.
