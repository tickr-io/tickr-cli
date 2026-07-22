# tickr-ctx scope contract

## Role boundary

`ScopeStore` is the Tickr Lite durable role for tickr-ctx scope values. It is separate from ingress idempotency, the Command bus, task dispatch, and generic repository access. The Conductor-owned SQLite writer performs every mutation; task processes and helper endpoints do not receive a database connection or repository bundle.

A scope has one stable UUID identity and one unique `(namespace, run_id)` identity. Keys remain the exact tickr-ctx keys supplied by the existing scope resolver, including their run-qualified prefix and punctuation. The local store does not sanitize, rename, split, or reinterpret a key.

## Opaque envelopes and lineage

Each value row has a stable identity, its exact key, the accepted envelope bytes, and creation and update times. Accepted v1 and v2 JSON envelopes are stored and returned byte-for-byte. The store reads only the top-level version discriminator needed for admission and quarantine; it does not deserialize or rewrite the value, secret flag, producer lineage, presence flag, timestamp, or value digest.

A key update replaces only its opaque envelope bytes and update time. It retains the value identity and creation time. This preserves the existing last-write behavior while keeping secret and producer metadata under the envelope contract.

Malformed envelopes, envelopes without an integer version, and versions other than v1 or v2 are rejected before acceptance. Invalid bytes discovered at rest quarantine the complete scope under its stable scope identity. No invalid entry is skipped to manufacture a partial or empty scope.

## Atomic claims and collisions

Scope creation atomically commits the scope identity, its non-empty initial value set, and a UUID creation claim. Repeating the same claim with the same scope, namespace, run, keys, and envelope bytes is idempotent, including after an ambiguous acknowledgement or process restart. Reusing the claim for different content is a claim conflict. Creating a different identity for an occupied `(namespace, run_id)` is a collision and returns the existing stable identity.

Each update carries a UUID claim. The claim and all key mutations commit in one SQLite transaction. Repeating a claim with the same scope and request bytes is idempotent; changing the scope or request bytes is a claim conflict. Duplicate keys and an empty mutation request are rejected before the transaction changes accepted state.

Creation and update claims remain durable through snapshot commit. Lifecycle cleanup may remove the per-request claim rows only after the committed snapshot and digest remain available under the stable scope identity.

## Bounds

The local role applies fixed hard maxima:

- one envelope: 1 MiB;
- one mutation request: 128 values and 4 MiB across keys and envelopes;
- one live scope: 4,096 rows and 64 MiB of envelope bytes; and
- one live scope age: 30 days.

Value, request-count, request-byte, scope-row, scope-byte, and scope-age refusal are distinct typed outcomes. Every bound is checked before the mutation commits. A bound refusal leaves all previously accepted values and claims unchanged; it never truncates, evicts, or cleans a scope to fit.

## Local helper endpoint

Tickr Lite task processes reach their assigned scope through a root-local Unix-domain endpoint, not SQLite or a Conductor repository. The formation gives each task exactly one opaque credential plus its endpoint path; every request carries that credential, task identity, namespace, and run identity. The endpoint accepts an operation only when all four values match the active task grant, so a task cannot read or mutate another task's scope.

The endpoint uses a typed, versioned request/response protocol. It preserves the existing `tickr-ctx` CLI, ambient resolution, key layout, opaque envelopes, collision failures, secret behavior, and observable command errors. A missing, stopped, or unready endpoint is an unavailable error; it is never interpreted as an empty scope.

Endpoint binding is permitted only after data-directory admission and scope recovery. Socket permissions are `0600`; readiness remains false until the supervisor has registered its critical children, and clears before endpoint shutdown. Requests and responses have finite frame limits, keys and task identities have finite limits, and scope bounds remain typed failures before any mutation commits.

The endpoint owns no SQLite connection. Reads and mutations cross its bounded writer channel to the Conductor-owned writer; closing that writer yields unavailable. The endpoint removes its socket during shutdown, so restart binds a newly admitted endpoint and task grants rather than reviving stale credentials.

## Read, snapshot, and cleanup

Reading an active scope returns all rows in stable key order. A missing identity returns `Missing`; it never returns an empty collection. An active scope with no accepted values is quarantined rather than presented as an empty scope.

Snapshot commit validates every stored envelope and bound, orders rows by exact key, and writes a versioned binary snapshot containing each key and its original envelope bytes. The SHA-256 digest, row count, envelope-byte count, snapshot bytes, and snapshot time commit atomically before the scope becomes `snapshotted`. Repeating snapshot after an ambiguous acknowledgement or restart returns the same committed bytes and digest.

Cleanup is refused while a scope remains active. For a snapshotted scope, cleanup first verifies the committed snapshot format and digest, then deletes live values and request claims and marks the scope `cleaned` in one transaction. The snapshot and digest remain readable after cleanup. Repeating cleanup returns `AlreadyCleaned` without changing the archive identity.

Compaction resolves a scope by its stable `(namespace, run_id)` identity before snapshotting; it never guesses a UUID from a published envelope. The archive commits the snapshot digest with terminal state before cleanup may delete live values and claims.

Compaction cleanup recovery re-resolves the retained `(namespace, run_id)` identity and requires both the stored scope UUID and committed digest to match its archive completion evidence. It performs this verification for `snapshotted` and `cleaned` scopes before treating cleanup as converged.

## Failure and quarantine

Missing scope state, unreadable SQLite values, invalid timestamps or state, unknown store protocol versions, malformed or unknown envelope versions, missing snapshot fields, unknown snapshot formats, and digest mismatch are errors or quarantine outcomes with the stable scope identity in the diagnostic. Quarantine preserves the stored rows and opaque bytes for diagnosis. Recovery never substitutes an empty scope, silently skips a bad row, or cleans quarantined state.
