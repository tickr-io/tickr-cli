# Conductor relay contract

## Tickr Lite Command leg

The API component's Trigger, Cancel, Wakeup, Register, Patch, replay, and Ping Commands use bounded local request/reply in Tickr Lite. The API encodes the production `ApiCommandRequest`; the sole Conductor-owned local writer decodes and dispatches it through the same command handlers used by the distributed subscriber, then returns the production `ApiCommandResponse`.

The response carries the Conductor-selected HTTP-equivalent `status_code` and exactly one typed payload. Success, idempotency conflict, unsupported Command, cancellation, parse failure, repository failure, and relay-unavailable outcomes are unchanged. The API forwards decoded status and payload as before. Per-command deadlines and unavailable, timeout, malformed-reply, and payload-limit mappings are transport-independent.

Local request acceptance is not a durability acknowledgement. A mutation becomes authoritative only at its existing SQLite commit boundary. Timing out or cancelling the requesting HTTP call after local acceptance does not roll back or duplicate that mutation; idempotency and typed outcomes remain owned by the production handler and repository.

## Distributed Command leg

Distributed formations retain NATS Core request/reply on the single `tickr.api.commands` subject and queue group `tickr-conductor-api-commands`. No subject, protobuf family, queue-group behavior, or status mapping changes.

## Tickr Lite task-coordination legs

The inbound Tickr Lite dispatch leg durably stages the unchanged published `TaskDispatch` bytes through `LocalTaskPickupWriterClient::stage_dispatch`. A versioned payload digest is the stable local dispatch identity, so retry after ambiguous relay acceptance converges on one pending record. The local wakeup is only a hint; SQLite remains authoritative.

The sole Executor acquires process capacity before observing the oldest pending dispatch. Selection does not release or claim it. Decode and process-input validation precede claim; invalid bytes or inputs transition the dispatch to rejected state and copy the original payload and reason into durable quarantine.

One writer transaction advances the dispatch's monotonic pickup generation, records the sole owner and liveness deadline, stages the unchanged published `Assigned` `TaskEvent` bytes under that generation, and commits claimed state. `Started` is staged only after a proved, armed claim launches a process. Pickup generation remains in local outbox columns and never enters the published `TaskEvent` family.

The local TaskEvent outbox is the durable source for the unchanged cross-plane leg. Forwarding retains the existing ack-on-forward boundary; local staging does not claim Control-plane application.

Process exit, task-process setup failure after claim, and liveness expiry compete through `SafeAttemptOutcomeHandoff`. The winning conditional writer transaction records one generation-qualified local outcome and stages one unchanged published `Completed`, `Failed`, or `Unhealthy` TaskEvent. Duplicate and late contenders observe settlement and stage nothing.

The local TaskEvent drain selects staged rows in `Assigned`, `Started`, terminal order, forwards the unchanged envelope to the existing relay channel, and only then marks the row forwarded. A closed relay leaves the row pending. A crash after channel forward but before local completion redelivers the same bytes; the existing Control-plane state-and-kind guard absorbs that duplicate.

## Tickr Lite cancellation leg

The inbound Tickr Lite cancellation leg commits a stable task-identity fence through the sole local writer before notifying the committed pickup owner. Claim acquisition and spawn proof reject the fenced pickup generation. The task handler remains the sole process-group signaler and reaper; its killed, already-exited, or no-process reconciliation competes through the same single-winner terminal election as process exit and liveness.

Fence settlement stages the unchanged published `CancelTaskAck` bytes in a local outbox. A relay or process outage leaves the acknowledgement pending. The drain forwards the existing `CANCEL_TASK_ACK` envelope and only then marks the row forwarded; a crash between those operations redelivers the same acknowledgement identity and bytes.


## Tickr Lite Signal-applied feedback

Only ByTag-cancel materialization feedback may emit through `SignalAppliedNotifier`. The notification carries the Signal identity as a latency hint; durable Signal state and the existing `SignalApplied` relay response own the result and materialized count.

Notification delivery is outside every relay acknowledgement and audit boundary. Full, closed, delayed, duplicated, or dropped notification cannot complete inbound relay work, settle an outbound effect, change Signal audit state, or suppress the bounded durable-state reconciliation deadline. Distributed formations retain the existing tenant-NATS `signal_applied.<signal_id>` response correlation.

## Tickr Lite Compaction leg

The relay stages the unchanged Compaction envelope in local SQLite and returns `COMPACTION_ACK` only after that commit. The ACK is not archive completion and the relay performs no terminal archive write. The local drain later seals and installs final Logs, snapshots tickr-ctx scope, commits terminal archive state and final references, then purges local sources.

If restart finds the archive transaction already committed, the `complete` staging row remains drain work until scope, Log-journal, and envelope cleanup converge. Recovery verifies the committed scope and final-Log digests and does not archive a second time.

## Cross-plane leg

The Conductor-to-Control-plane relay remains unchanged. Trigger, cancellation, Patch, replay, and lifecycle effects continue to project onto their existing published envelopes and tenant addressing. TaskEvent and other durable outbound legs retain their current ack-on-forward boundary: forwarding to the relay channel does not claim server application acknowledgement.

In Tickr Lite, relay connection failure and stream loss are recoverable transport outages rather than formation-child termination. The relay retries with bounded backoff. Each successful connection replaces only the live channel and restarts drains over the same durable local outboxes; staged TaskEvents, cancellation acknowledgements, replay effects, and other locally committed winners remain pending until forward completion. Reconnection therefore preserves the existing payload identities and ack-on-forward boundary without inventing server-application acknowledgement.

## Invariants

- The API component cannot observe or obtain the local queue, reply channel, SQLite connection, transaction, statement, row, or writer repository.
- Exactly one local receiver handles Commands serially for one Tickr Lite formation.
- Every Command transport carries the same encoded production request and response envelopes.
- The local Command exception does not create local substitutes for the cross-plane relay or published protocol families.
- A staged TaskEvent remains durable until relay-channel forwarding completes locally.
- A staged cancellation acknowledgement remains durable until relay-channel forwarding completes locally.
- Forward completion is ack-on-forward, not Control-plane application acknowledgement.
- Pickup generation and local terminal outcome never enter a published protobuf.
- Cancellation fence identity, owner notification, reconciliation, and pickup generation never enter a published protobuf.
- Relay unavailability or reconnection never mutates, settles, or discards a locally staged effect by itself.
