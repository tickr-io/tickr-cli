# Data-plane storage contract

## Tickr Lite storage root

All Tickr Lite local storage belongs to one exclusively leased data directory. SQLite state, the formation manifest, journals, staged logs, final logs, temporary files, and quarantine entries are addressed below that already-open root. Storage code does not accept absolute formation paths or path traversal.

`RootRelativePath` is the boundary for dynamic names. Root-relative opens walk each component with no-follow semantics and validate owner, permissions, and device before returning a handle. A symlink at the root, an intermediate directory, or a final file is an error rather than an alternate location.

## SQLite migration

For SQLite, the migration command derives the data-directory root from the parent of the configured absolute on-disk SQLite path. It then:

1. admits and exclusively locks that root;
2. validates the database basename as a root-relative path;
3. opens or creates the database file through the root handle with mode `0600` and syncs a new file and its parent;
4. opens the one-writer SQLite pool only after those checks;
5. applies and verifies migrations while retaining the lease; and
6. closes the pool before releasing the lease.

In-memory, relative, percent-encoded, and non-SQLite locations are not valid Tickr Lite SQLite paths. Runtime ownership uses the same admission primitive and root, so migration and runtime cannot inspect or mutate the same SQLite state concurrently. A losing contender does not create the database.

SQLite remains responsible for its database, WAL, and shared-memory file protocol. The formation remains responsible for ensuring their selected parent is the admitted, locked root and for retaining the lease while SQLite is open.

When migration is explicitly selected for the Tickr Lite formation, it constructs the manifest fingerprint only after `verify_sqlite_current` and full SQLite schema verification succeed. It then installs or verifies the manifest before closing the pool and releasing the lease. The default distributed migration path remains unchanged and does not claim Tickr Lite formation identity.

## Durable file creation and replacement

Formation files and directories require effective-user ownership, mode `0600` for files or `0700` for directories, and the same device as the root.

Creating a file is durable only after the file and its parent directory sync successfully. Replacing a file is durable only after:

1. the temporary file has been opened below the same root and verified on the root device;
2. its contents have been synced;
3. an operating-system atomic rename replaces the destination using open source and destination parent handles; and
4. the destination parent directory has been synced.

A failure at any step is an operation failure. Callers must not publish a manifest, journal frontier, log reference, quarantine record, or SQLite-adjacent identity as installed before the complete replacement sequence succeeds.

The formation manifest uses this replacement protocol. Its temporary record is complete and checksummed before replacement. An orphaned temporary record has no authority; only the installed destination is considered during admission. Restart validation therefore accepts either the previous complete destination or the new complete destination after interruption around temporary-file sync, rename, or parent-directory sync.

## Refusal and corruption boundary

Wrong owner or mode, symlink traversal, path escape, cross-device placement, and lock, sync, or replacement failures are storage-admission errors. They are never treated as missing or empty state. Quarantine is itself beneath the leased root and cannot be used to move a record onto another device or through a symlink.

The manifest's required-file set is verified through root-relative, no-follow opens before installation and on every restart. A missing required file, directory in place of a file, wrong owner, unsafe mode, or different-device entry is a storage failure. Manifest verification never creates the missing entry or substitutes empty state.

## Tickr Lite SQLite writer ownership

The Conductor-owned writer repository opens SQLite through a pool whose maximum connection count is exactly one. It is retained for the lifetime of the local Command writer and serializes Trigger, Cancel, Wakeup, Register, Patch, and replay mutations through that one connection.

The API component may open the verified read-only SQLite role concurrently. That role uses SQLite read-only and `query_only` enforcement, exposes only `ReadOnlyRepositoryBundle` operations, and rejects a direct mutation even while the Conductor writer is open. No Executor, task process, helper, or API mutation path receives `WriterRepositoryBundle`.

The local Command boundary accepts and returns protobuf envelopes only. Repository bundles, SQLite connections, transactions, statements, and rows do not cross it. A bounded in-process queue schedules work but carries no durability claim; the committed SQLite operation remains authoritative.

The role-law integration gate keeps the writer open while the API read-only role is exercised, verifies the writer pool limit is one, and proves a mutation attempted through the concurrent API connection fails. A second role that becomes writable violates formation admission.

## Definition-build lifecycle records

`workflow_task_builds` remains the durable source of per-Task definition-build work. Tickr Lite extends each row with nullable `lease_owner`, `lease_token`, and `lease_expires_at` fields. The three fields are written and cleared together by repository operations; a channel message never changes them.

The one-writer repository exposes two local lifecycle operations:

1. bounded stable selection plus conditional lease acquisition for committed `pending` rows whose parent is `Building`; and
2. lease-guarded Task settlement plus the existing typed parent finalizer in one transaction.

Selection orders by `pending_since`, Workflow identity, version, and Task identity. A row with an unexpired lease is ineligible. Settlement requires the exact owner and token and rejects an expired lease without changing durable state. Successful settlement clears the lease fields. Reopening a `BuildFailed` definition clears obsolete leases before its pending rows can be reconsidered.

These lease operations are SQLite-only Tickr Lite coordination. Postgres retains the distributed build queue protocol, although the paired migration keeps the logical schema identity aligned. Task dispatch cannot call or reinterpret the definition-build lease operations.

## Patch lifecycle records

`workflow_patches` and `workflow_patch_task_builds` remain the durable sources for accepted Patch work. Tickr Lite adds nullable owner, token, and expiry fields to both the per-Task build claim and the Patch lifecycle claim. Each lease triple is either wholly absent or wholly present.

The one-writer repository exposes SQLite-only operations for:

1. bounded stable selection and conditional lease acquisition of `pending` Patch Task builds whose parent remains `Building`;
2. lease-guarded Task settlement plus the existing typed last-one-out Patch finalizer in one transaction;
3. bounded stable selection and conditional lease acquisition of `Validating` and `Submitted` Patch lifecycle rows;
4. lease-guarded `Submitted` settlement after relay acceptance; and
5. conditional release after a failed relay attempt.

Build selection orders by `pending_since`, Patch identity, and Task identity. Lifecycle selection orders by `updated_at` and Patch identity. An unexpired lease makes its row ineligible. Settlement requires the exact owner and token and rejects an expired lease without changing durable state.

The Patch operations are local coordination, not a general queue. Postgres retains the distributed build protocol, while the paired migration keeps logical schema identity aligned. A Patch lifecycle lease cannot authorize task dispatch or process launch.

Terminal Patch correlation and successful per-Task settlement clear the applicable lease fields in the same transaction as their lifecycle mutation. Crash recovery may reacquire only expired unresolved work; terminal rows and settled Task rows are never eligible.

## tickr-ctx scope records

Tickr Lite persists tickr-ctx scope state in three paired-schema tables:

- `tickr_ctx_scopes` owns the stable scope identity, unique namespace/run identity, store protocol, lifecycle state, quarantine diagnostic, and committed snapshot metadata;
- `tickr_ctx_scope_values` owns stable value identities, exact keys, and opaque envelope bytes; and
- `tickr_ctx_scope_claims` owns atomic mutation claims until lifecycle cleanup.

The paired Postgres schema preserves the logical migration identity, but scope operations are a SQLite-only local coordination role. A Postgres repository bundle rejects them. The Conductor-owned SQLite writer is the only role that may create a scope, apply a claimed update, commit a snapshot, quarantine state, or perform cleanup.

Creation commits a non-empty initial value set and its claim in one transaction. Updates commit every value and the request claim together. Bound or claim-conflict outcomes do not partially change rows. Scope values use SQLite `BLOB` and Postgres `bytea` so accepted envelope bytes and snapshot bytes do not pass through a text or JSON re-encoding.

Snapshot bytes, SHA-256 digest, row count, envelope-byte count, and snapshot time commit before live values become cleanup-eligible. Cleanup verifies the committed digest, deletes live values and mutation claims, and retains the scope row plus snapshot. Duplicate snapshot and cleanup operations converge on the same identity. Missing, unreadable, corrupt, or unknown-version state is returned as an error or stable quarantine outcome and is never represented by an empty scope.

## Final Log records

The local final-Log adapter receives a sealed accepted-record set and terminal metadata from Log staging. It writes protocol-identified final-log and exit-metadata documents to distinct root-relative destinations below `logs/final/`; both files use same-device temporary files below `tmp/`, file sync, atomic replacement, and parent-directory sync.

The persisted reference is backend-neutral: stream identity plus the accepted-record, final-log, and exit-metadata SHA-256 digests. Readers reopen both files through the locked root, verify protocol identity and every digest before use, and reject a missing, unreadable, unknown-format, or mismatched file. A mismatching temporary file is durably moved below quarantine and reports a failure; it is never selected as a final record or replaced with an empty payload.

## Local Compaction staging records

The SQLite Compaction staging record stores the published envelope bytes, archive identity, and payload digest before relay acknowledgement. Archive commit changes it to complete in the same writer transaction that persists terminal Workflow-instance and Task-instance rows, run-info, linked Trigger-derived Event-variable/Signal-capture terminal state, scope digest, and final-Log references. Purge clears only envelope bytes after that commit and retains an identity/digest tombstone for late redelivery.

Both `staged` and `complete` rows are eligible for bounded drain selection. A `complete` row carries the still-retained envelope, scope identity/digest, and final-Log references needed to verify and resume cleanup after restart; only `purged` tombstones leave the drain set.
