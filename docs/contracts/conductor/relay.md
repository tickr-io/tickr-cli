# Conductor relay contract

## Profile boundary

Distributed formations retain the existing NATS dispatch, TaskEvent, cancellation, Compaction, scope, and liveness legs. Tickr Lite replaces only the Data-plane coordination legs. The Conductor-to-Control-plane stream, tenant addressing, published protobuf families, and ack-on-forward boundary are unchanged.

## Inbound Tickr Lite dispatch

The relay hands the server-authored `TaskDispatch` bytes to the bounded local task-pickup writer. Acceptance is durable only when the writer commits the pending SQLite record. Its `task-dispatch-v1` payload digest is stable across ambiguous retries; the same bytes cannot create two pending pickups.

A bounded notification may accelerate the sole Executor, but SQLite is the source of work. Full process capacity prevents selection. Selection is observational and leaves the dispatch pending. Executor capacity status cannot reserve a slot or authorize a claim.

Decode and process-input validation happen after selection and before claim. Poison bytes or inputs are atomically rejected and copied with their reason into durable quarantine. They are never acknowledged and discarded.

## Local claim and TaskEvent staging

One writer transaction changes a pending dispatch to claimed state while it:

- advances the monotonic pickup generation;
- records the sole Executor owner;
- records the generation's liveness deadline; and
- stages the existing published `Assigned` `TaskEvent` bytes in the local outbox.

No process may launch until the claimant arms liveness and proves that exact owner, generation, deadline, and `Assigned` staging record. An ambiguous claim acknowledgement is resolved by that proof. If proof is unavailable, no process launches and recovery never reopens the claimed generation.

After process spawn, the writer stages the existing published `Started` `TaskEvent`. The outbox qualifies both records with the local pickup generation, but the serialized `TaskEvent` bytes remain unchanged and contain no local generation.

## Local terminal-outcome election

Process exit, task-process setup failure after claim, and a due liveness deadline submit the exact claimed generation to one `SafeAttemptOutcomeHandoff` transition. The winner records one local outcome and stages exactly one existing terminal TaskEvent in the same writer transaction: successful exit projects to `Completed`, non-zero exit or setup failure to `Failed`, and liveness expiry to `Unhealthy`. Losers observe the settled outcome and produce no local side effect.

Restart never relaunches a claimed generation. It preserves a not-yet-due claim and submits an overdue unresolved claim to liveness election. Terminal settlement rejects later `Started`, renewal, and terminal staging for that generation.

## Local cancellation fencing

The inbound published `CancelTaskRequest` commits a stable task-identity fence in the local writer before any owner notification. A matching pending task cannot claim after that commit, and a claimed task cannot prove the fenced pickup generation ready to spawn. Fence commit and proof-to-spawn share the task handler's launch gate.

When the fence records the sole Executor as owner, cancellation is directed to that task handler. The handler remains the sole process-group signaler and reaper. Its killed, already-exited, or no-process reconciliation enters the same conditional terminal election as process exit and liveness; it cannot bypass the existing single-winner guard.

Restart settles only durable evidence. An unmatched fence settles as no process, and a terminal generation settles as already exited. A claimed owner without a terminal winner remains fenced until liveness election; restart neither reports an unobserved kill nor relaunches the generation.

Fence settlement stages the unchanged published `CancelTaskAck` bytes under a stable local acknowledgement identity. Relay-channel send is the completion boundary. Outage leaves the row pending, and a crash after send but before completion redelivers the same identity and bytes.


## Inbound Tickr Lite Compaction

The relay persists the published Compaction envelope in the local Compaction staging role before emitting `COMPACTION_ACK`. That ACK means durable staging only. The relay never writes archive rows, final-Log references, or scope state. The drain owns those effects and retains staged bytes until its archive transaction commits.

A committed archive and its `complete` staging state are one SQLite transaction. Restart selects `complete` rows for reference verification and idempotent cleanup until the retained identity/digest tombstone reaches `purged`; it does not replay terminal archive writes.

## Outbound cross-plane behavior

Locally staged TaskEvents remain pending until the Conductor relay forwards their unchanged published envelopes. Forward acknowledgement does not mean Control-plane application. Duplicate and late application remains guarded by the existing Task Manager state-and-kind transitions; this profile adds no server-visible deduplication identity.

For Tickr Lite, relay-channel send is the local forward boundary. The writer marks the staged row forwarded only after that send succeeds. A send failure leaves it pending; a crash after send but before the mark replays the same published bytes. This is ack-on-forward only, not server application acknowledgement.

## Invariants

- Dispatch selection, decode, validation, claim, liveness arm, claim proof, spawn, `Started` staging, and renewal occur in that order.
- A claimed pickup generation is never relaunched, including after ambiguous acknowledgement or restart.
- `Assigned` cannot commit without its claim, and a claim cannot commit without `Assigned`.
- Fence commit precedes owner notification; claim and spawn proof reject the fenced identity and generation.
- The local writer is the sole mutation path; callers receive role results, never a SQLite connection, transaction, statement, row, or channel.
- Exactly one terminal local outcome and one terminal TaskEvent are staged per pickup generation.
- Local duplicate and late contenders are absorbed before relay; cross-plane duplicates remain safe under the existing Task Manager guard.
- The local outbox retains every winner until relay-channel forward completion.
- A staged cancellation acknowledgement remains durable until relay-channel forward completion.
- No local cancellation identity or pickup generation enters the published cancellation family.
