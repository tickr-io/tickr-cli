# Conductor relay contract

## Tickr Lite Command leg

The API component's Trigger, Cancel, Wakeup, Register, Patch, replay, and Ping Commands use bounded local request/reply in Tickr Lite. The API encodes the production `ApiCommandRequest`; the sole Conductor-owned local writer decodes and dispatches it through the same command handlers used by the distributed subscriber, then returns the production `ApiCommandResponse`.

The response carries the Conductor-selected HTTP-equivalent `status_code` and exactly one typed payload. Success, idempotency conflict, unsupported Command, cancellation, parse failure, repository failure, and relay-unavailable outcomes are unchanged. The API forwards decoded status and payload as before. Per-command deadlines and unavailable, timeout, malformed-reply, and payload-limit mappings are transport-independent.

Local request acceptance is not a durability acknowledgement. A mutation becomes authoritative only at its existing SQLite commit boundary. Timing out or cancelling the requesting HTTP call after local acceptance does not roll back or duplicate that mutation; idempotency and typed outcomes remain owned by the production handler and repository.

## Distributed Command leg

Distributed formations retain NATS Core request/reply on the single `tickr.api.commands` subject and queue group `tickr-conductor-api-commands`. No subject, protobuf family, queue-group behavior, or status mapping changes.

## Hardened all-NATS task-coordination leg

The fresh all-NATS Executor acquires local process capacity before pulling one `TaskDispatch`. A saturated Executor therefore leaves both pulled and unpulled dispatches pending without acknowledgement. Decode and process-input validation occur before claim; invalid payloads are copied with their rejection reason into the durable pickup-handoff bucket before their source delivery is acknowledged, and they never launch a process.

The TaskDispatch stream sequence is the stable all-NATS pickup-operation identity. One version-2 pickup record binds the payload, pickup generation, owner, NATS-server-time deadline, and exact encoded `Assigned` bytes. `Assigned` is durably published under that stable identity, generation-qualified liveness is armed, and the exact state is proved before the source delivery is acknowledged. A lost or ambiguous acknowledgement is resolved from that record. Recovery completes an unfinished handoff and acknowledges redelivery without launching; an existing claim is always treated as potentially launched.

Only a newly proved and source-completed handoff may spawn. After spawn, the Executor stages the existing encoded `Started` event and performs the first conditional renewal against that same dispatch key, pickup generation, and owner. A stale generation, different owner, terminal record, failed `Started` stage, or failed renewal cannot mutate the handoff; post-spawn failure stops the tracked process. Pickup generation, owner, deadline, and stable operation identity remain adapter-local and never enter a published protobuf.

The version-2 TaskEvent stream remains the durable cross-plane source. Its existing Conductor consumer acknowledges at relay-channel forward, not at Control-plane application.

Process exit and post-claim setup failure conditionally settle the version-2 pickup record. A bounded sweeper in each Executor and Conductor reads the same durable, NATS-server-time deadline and competes through that identical generation-and-owner-qualified mutation. The one winner records the outcome and exact encoded `Completed`, `Failed`, or `Unhealthy` TaskEvent bytes. Late contenders read the elected outcome and publish nothing contradictory.

The Conductor reconciliation loop enqueues an elected terminal event onto the version-2 TaskEvent stream under one stable per-generation identity, then records that enqueue. The existing TaskEvent consumer retains the event until relay-channel forward. A crash or ambiguous acknowledgement before the enqueue record or stream acknowledgement retries the same identity and bytes; duplicate forwarding remains possible at the documented forward-versus-apply gap and is absorbed by the Control-plane state-and-kind guard.

Generation-qualified TTL markers are optional scan wakeups only. Periodic scans of the durable pickup deadline remain authoritative when a marker is lost, delayed, duplicated, or unavailable, including deadlines that became due while every Conductor was down.

## Hardened all-NATS cancellation leg

The fresh all-NATS cancellation source remains on the unchanged `CancelTaskRequest` envelope and version-2 cancellation work queue. Before owner delivery, an Executor commits one stable cancellation identity with the current dispatch key, pickup generation, and owner in durable coordination state. A queued or missing Task commits an ownerless fence; a later pickup consumes the queued dispatch without process launch.

Owner notification is an advisory, owner-addressed wakeup. Only the exact generation owner may signal and reap its process group. Claiming, active, exited, missing-process, stale-generation, and already-terminal observations converge through explicit conditional reconciliation, and cancellation competes in the same terminal election as process exit, setup failure, and liveness expiry.

The winning reconciliation stages the exact existing encoded `CancelTaskAck` bytes under the stable acknowledgement identity before the cancellation source is acknowledged. Executor restart, owner death, cancellation-source redelivery, or an ambiguous acknowledgement reconstructs the same fence, election, and acknowledgement bytes. The version-2 cancellation-ack stream remains pending until the Conductor forwards the unchanged `CANCEL_TASK_ACK` envelope to the existing relay channel; duplicate requests replay the same result.

## all-Redis task-coordination leg

The version-`1` TaskDispatch adapter admits encoded dispatches into a role-owned Redis Stream without trimming. Each Executor acquires a local process slot before `XAUTOCLAIM` or `XREADGROUP`; a saturated process therefore leaves both pending and unpulled entries untouched. Decode and process-input validation precede every pickup mutation. Invalid bytes are copied with a bounded rejection reason into durable role state, and only the same fsync-proved operation may complete their Stream entry; they never stage `Assigned` or launch.

The Redis Stream entry ID is the stable pickup identity. One atomic role-local script advances its pickup generation, binds the sole owner, derives the initial deadline from Redis server time, and stores the exact encoded `Assigned` bytes. The adapter proves one primary-local AOF fsync with zero required replica acknowledgements before completing the source entry. Mutation or fsync ambiguity is resolved from that same identity; recovery treats an existing generation as potentially launched, completes unfinished source work, and never launches it again.

Only the newly proved and source-completed generation may spawn. The exact generation and owner condition `Started` staging, immediate renewal, every later renewal, source completion, and terminal election. Process exit, setup failure, liveness expiry, and cancellation compete through the same atomic outcome mutation. A late or stale contender observes the elected result without changing it.

The role accounts accepted dispatch entries, active claims, staged events, and bytes independently. Soft pressure is observable; hard record, claim, staged-event, or byte pressure fences new acceptance or pickup without completing the source. Dispatch charges are released only by fsync-proved source completion, active-claim charges only by generation-qualified terminal settlement, and staged charges only after the staged handoff is safely forwarded. Read-only, OOM, missing identity, inconsistent accounting, or failed local-fsync proof closes capability and preserves pending work.


## all-Redis TaskEvent leg

The all-Redis role stages the exact existing encoded `TaskEvent` bytes in its version-`1` Redis Stream under an adapter-local stable operation identity. The identity binds one payload digest and never enters the published protobuf. Same-identity retry converges, conflicting bytes fail, and producer acceptance follows one primary-local AOF fsync proof with zero required replica acknowledgements.

The Conductor consumes through the role-owned consumer group. Delivery claim and pending accounting are durable before forwarding. Relay closure, Conductor death, or lost ownership leaves the entry pending for consumer-group reclaim. The adapter acknowledges and removes the Stream entry, releases its accepted-record and pending-delivery quota, and proves that completion durable only after the unchanged encoded event has entered the existing relay channel.

A crash after relay-channel forward and before Redis Stream completion redelivers the same bytes. The existing Control-plane terminal/state-and-kind guard remains the duplicate boundary; Redis forwarding does not add Server apply acknowledgement or cross-turn exactly-once delivery. Backlog pressure fences producers before append and never trims an accepted TaskEvent. Redis Pub/Sub has no TaskEvent role.

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
- A fresh all-NATS TaskDispatch source is acknowledged only after the complete `SafePickupHandoff` proof.
- A fresh all-NATS pickup generation records at most one terminal outcome and exact terminal TaskEvent; the shared deadline sweep and process-side contenders use the same conditional election.
- Fresh all-NATS terminal TaskEvents remain durable across Conductor restart until the existing TaskEvent consumer acknowledges relay-channel forwarding.
- all-Redis TaskEvents remain fsync-proved and consumer-group pending across Conductor restart until relay-channel forwarding permits durable Stream completion.
- all-Redis TaskDispatch entries complete only after the exact generation, owner, Redis-server-time deadline, `Assigned` bytes, and primary-local fsync proof exist.
- all-Redis recovery never launches an existing pickup generation; only a newly proved and source-completed generation may spawn and stage `Started`.
- Liveness expiry notifications may accelerate reconciliation but never author a verdict or replace the durable server-time deadline.
- Pickup generation, owner, deadline, stable pickup identity, and terminal outcome never enter a published protobuf.
- Cancellation fence identity, owner notification, reconciliation, and pickup generation never enter a published protobuf.
- Relay unavailability or reconnection never mutates, settles, or discards a locally staged effect by itself.
