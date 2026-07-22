# Data-plane formation contract

## Tickr Lite data-directory ownership

A Tickr Lite process owns local durable state through one `DataDirectory` lease. The lease is both the already-open root authority and the operating-system exclusive lock. A second handle or process cannot acquire a lease for the same directory until the owner drops its lease.

The runtime and offline SQLite migration entry point use the same `DataDirectory::admit` operation. Lock acquisition therefore precedes SQLite connection, migration, recovery, formation-file inspection or mutation, listener startup, Control-plane connection, relay work, and task claim. Failure to acquire the lock is a startup failure; contention is never retried behind a partially started formation.

## Admission order and fail-closed behavior

Admission performs these steps in order:

1. Open the configured root as a directory with no-follow and close-on-exec flags.
2. Require the root to be owned by the effective process user with mode `0700`.
3. Identify an admitted local platform/filesystem pair. macOS admits APFS and HFS; Linux admits ext-family, XFS, and Btrfs. Network and unknown filesystems are rejected.
4. Prove the parent-liveness pipe and close-on-exec descriptor behavior required by process containment.
5. Acquire a non-blocking exclusive operating-system lock on the open root directory.
6. On that locked handle, prove same-device temporary placement, file sync, atomic replacement, installed-content visibility, and parent-directory sync.

Admission fails on an unsupported platform, symlink root, wrong owner or mode, network or unknown filesystem, unsupported lock, failed sync, failed replacement, cross-device entry, or missing required capability. Capability probes use reserved temporary names, remove them before returning, and sync the root after cleanup. No formation-owned state is opened before exclusive lock acquisition.

The lease records the admitted platform, filesystem, and the complete capability proof. There is no partial admission: every required capability must be true.

## Root-relative authority

Formation paths are non-empty relative paths composed only of normal path components. Absolute paths, parent traversal, prefixes, and embedded NUL bytes are rejected.

Every directory component is opened relative to the already-open root or its already-open child handle, with no-follow and directory flags. Every opened entry must be owned by the effective process user, use mode `0700` for directories or `0600` for files, and remain on the root device. Newly created directories and files are synced with their parent before use.

The owned layout includes:

- SQLite state and its SQLite-managed sibling files;
- `formation-manifest.json`;
- `journals/`;
- `logs/staged/`;
- `logs/final/`;
- `tmp/`; and
- `quarantine/`.

Callers may extend these roots only through a validated `RootRelativePath` and the root-handle operations. Joining an unchecked path to the configured path is not an admitted file operation.

## Lease lifetime

The exclusive lock remains held for the lifetime of `DataDirectory`. Shutdown must keep the lease alive until formation children, task process groups, SQLite connections, and durable flushes have settled. Dropping the lease releases the lock.

## Formation-manifest identity

`formation-manifest.json` is the versioned identity record for durable state below one admitted data directory. Its normalized fingerprint binds the complete resolved formation descriptor, every behavior-affecting configuration value, the verified logical SQL migration set, every coordination-role implementation and protocol identity, file-format versions, namespace identities, and the required-file set. The manifest checksum also binds the fingerprint and the admitted root's device, inode, platform, and filesystem identity. The checksum detects corruption; it is not a signature or an authenticity claim.

Configuration normalization is deterministic. Ordering differences in maps or SQLite query parameters do not change the fingerprint, while a changed effective value does. Unknown manifest versions, checksum algorithms, protocol identities, migration identities, file-format versions, or namespace identities are not interpreted.

## Manifest admission and replacement

Manifest admission runs while the `DataDirectory` lease is held and after the selected SQLite migration set and schema have been verified. It completes before readiness, listener binding, task claims, helper binding, Control-plane client construction, or relay connection.

- A first installation requires either the explicit offline migration path or evidence from the durable-state owner that every frontier is empty and reconstructible.
- A restart with an identical normalized fingerprint verifies in place and does not rewrite the manifest or durable state.
- A missing manifest, checksum or fingerprint failure, wrong data-directory identity, unsafe ownership or permissions, missing required file, schema disagreement, or unknown identity refuses runtime admission.
- A changed fingerprint refuses ordinary runtime admission. It may be installed only by the explicit offline migration path or after an `EmptyReconstructibleFrontier` proof succeeds.
- Offline migration may replace a valid manifest with a new supported fingerprint. It does not repair, upgrade, or overwrite a corrupt, unknown, or otherwise unverifiable record.

Every successful installation writes a new checksummed record to a same-device temporary file, syncs that file, atomically replaces the destination, and syncs the destination parent. Readiness remains false if any step fails. A process loss at any boundary may expose the previous valid record or the new valid record after restart, never a partially parsed record that admission accepts.

## Tickr Lite Command bus

The resolved Tickr Lite formation selects bounded local request/reply for the API component-to-Conductor Command bus. Distributed formations continue to select NATS Core request/reply on `tickr.api.commands`. Both implementations carry the production `ApiCommandRequest` and `ApiCommandResponse` protobuf envelopes; selecting the formation cannot change the HTTP status or typed outcome.

The local Command bus has one bounded request queue and exactly one receiver owned by the Conductor writer. The receiver dispatches one request at a time through the same production Command dispatcher used by the NATS subscriber. Queue capacity and maximum encoded request size are finite formation inputs. The API component sees only the `CommandBus` request/reply interface; it cannot obtain the receiver, reply channel, repository, connection, row, or transaction.

Each API call supplies its command-specific deadline. A closed or absent writer maps to unavailable, an elapsed deadline maps to timeout, an undecodable response maps to malformed upstream reply, and a request above the configured encoded payload limit maps to payload too large. Cancelling or timing out a caller does not cancel a mutation already accepted by the writer; the writer completes it and discards the late reply.

The local writer is a critical formation child. Readiness requires that it has been constructed alongside the single SQLite writer role. Its return, panic, or cancellation clears formation readiness and enters the common shutdown path.

## Tickr Lite `tickr-ctx` endpoint

After the data-directory lease, SQLite schema verification, manifest admission, and scope recovery succeed, the supervisor binds the root-local Unix-domain `tickr-ctx` endpoint. Binding is a critical startup step: readiness stays false until the bind succeeds and every critical child, including the Conductor-owned scope writer, is registered. Shutdown clears readiness before cancelling the endpoint, then removes the socket before releasing the data-directory lease.

The socket is owned by the admitted user and has mode `0600`. The supervisor grants each launched task one fresh credential for its assigned task, namespace, run, and scope identity. The helper endpoint verifies that full grant on each request and forwards every scope read or mutation through the bounded writer channel. It exposes neither a SQLite connection nor any Conductor-internal interface to task processes.

## Transient Signal-applied notification

Tickr Lite implements `SignalAppliedNotifier` as one bounded, best-effort notification channel used only after ByTag-cancel materialization feedback. A notification contains only the Signal identity needed to prompt reconciliation. Durable Signal state and the existing relay response remain authoritative for the materialized result.

Full, closed, delayed, duplicated, or dropped notifications do not alter Signal audit state, complete relay work, or acknowledge any delivery. The consumer waits for either a notification hint or a bounded reconciliation deadline and then consults durable state. A closed producer retains that scan cadence rather than turning channel closure into lifecycle state.

## Observational Executor capacity

Tickr Lite implements `ExecutorFleetStatus` for exactly one Executor. A snapshot contains that Executor's identity, configured process-slot count, and current in-flight count. In-flight means a process slot is held; the configured count does not change at runtime.

The status interface exposes no permit, reservation, dispatch claim, acknowledgement, queue transition, or lifecycle mutation. Reading a current or stale snapshot cannot change queue semantics. The Executor acquires the real process slot before selecting durable dispatch work, so saturation leaves the oldest pending dispatch unchanged until capacity becomes available.


## Tickr Lite supervision and readiness

`LiteSupervisor` is the sole formation-wide ownership root. It owns the admitted `DataDirectory`, the single SQLite writer and read-only roles, API component, local Conductor roles, exactly one Executor, embedded Console assets, Control-plane query client, existing cross-plane relay, `tickr-ctx` endpoint, local lifecycle workers, and Compaction drain.

Readiness begins false. Before publishing it, the supervisor resolves and admits the complete formation descriptor, acquires the data-directory lease, verifies the schema and formation manifest, recovers local journals and pending Compaction cleanup, reconciles overdue pickup claims, binds the helper and HTTP sockets, validates Control-plane client configuration, and registers every critical child. Work-producing HTTP routes, helper requests, relay work, lifecycle workers, Compaction drain, and task claims remain behind the shared admission gate until those steps complete.

Publishing readiness is one ordered transition: mark the helper ready, publish HTTP readiness, then release the internal work-admission gate. Clearing readiness reverses availability before cancellation. A critical child return, error, or panic clears HTTP and helper readiness, cancels the formation, joins every remaining child, and only then releases repositories and the data-directory lease.

Control-plane reachability is not formation ownership. Loss of the Control plane after admission degrades health while the existing query and relay retry behavior continues; it does not clear, reinterpret, or discard local durable state.

## Fail-stop supervision and bounded shutdown

Every child that owns durable state, accepts traffic, drives local coordination or relay work, serves the helper or HTTP endpoint, or supervises Task processes is critical. Registration completes before readiness. Unexpected success, error, panic, or disappearance of any critical child first clears HTTP and helper readiness, then cancels the one formation root token. There is no role-local fail-open path.

SIGINT, SIGTERM, startup failure after child construction, and critical-child failure enter the same shutdown barrier. The barrier gives all registered children one bounded settlement interval, aborts children that exceed it, and treats a missed deadline as formation failure. Critical-child and deadline failures return non-zero. The `DataDirectory` lease, writer roles, and endpoint handles remain owned until the barrier has joined or aborted every registered child.

A failed liveness renewal stops and reaps the owned Task process group, makes that claimed generation's durable liveness deadline immediately due through the writer, and then fails the Executor child. Readiness is cleared before sibling cancellation. The dying process does not elect a terminal result: restart reconciliation selects the due durable claim and competes through the generation-qualified terminal-election transition.

## Task process parent-death containment

Each Tickr Lite Task launch is parented by a dedicated guardian process. The formation retains the only write end of that guardian's parent-liveness pipe; neither the guardian's workload nor any descendant inherits a write descriptor. Normal cancellation remains handler-owned: the task handler signals and reaps its registered guardian process group, while the guardian forwards termination to and reaps the workload leader.

Formation death closes every retained write descriptor. EOF makes each surviving guardian send SIGTERM to its workload process group, escalate to SIGKILL after a bounded grace, reap the workload leader, and kill any remaining descendants. EOF is the only parent-death signal and cannot create a local terminal outcome; recovery uses the already-durable claim and liveness evidence.

## Formation-aware Health projection

Tickr Lite projects the admitted descriptor onto `GET /api/health` without recomputing profile selection. The projection identifies the Tickr Lite profile, single-node topology, SQLite, local final files, one Conductor-owned writer, exactly one Executor, every coordination role implementation, and every stable protocol identity. It contains no backend location.

Substrate selection is explicit: SQLite is present; Postgres, NATS, Redis, and object storage are absent. Health does not construct or probe an absent substrate. The compatibility NATS row is not green in Tickr Lite and points consumers to the typed substrate selection and local-coordination row.

The Health handler reads the same readiness atomic published and withdrawn by `LiteSupervisor`. `/api/health` remains available while unready, but all other work-producing `/api/*` routes remain behind admission. A critical-child failure withdraws readiness before cancellation, so diagnostics cannot report an admitted formation after sibling cancellation starts.

Control-plane status is independent of formation readiness after admission. A degraded or lost Control-plane connection changes only its Health row while the local SQLite, coordination journals, and staged effects remain authoritative. Reconnection is observed by the next fresh Health request; no cached status table delays recovery.
