# Task liveness watchdog contract

## Formation-specific substrates

Fresh all-NATS stores generation-qualified owner and NATS-server-time deadline state in the version-2 durable pickup record; all-Redis stores the same evidence beside the version-`1` TaskDispatch Stream and its Redis-server-time sorted deadline index; Tickr Lite uses the sole SQLite writer and a durable deadline. All three bind liveness to the pickup generation and owner and project the same existing `Unhealthy` TaskEvent onto the Control plane without changing the published family.

## Safe pickup claim-time liveness

The pickup claim binds an initial deadline with the generation, owner, and exact staged `Assigned` bytes. Tickr Lite commits these fields in one writer transaction. Fresh all-NATS derives the deadline from the NATS-server timestamp of the durable pickup-record revision. All-Redis derives it from `TIME` inside the same atomic pickup script that advances generation and stages `Assigned`. Each distributed adapter uses its substrate server time on renewal. A transient expiry notification may be emitted as an optional wakeup, but its expiry or absence is never verdict evidence.

Before spawn, the claimant conditionally arms liveness for the exact dispatch key, generation, and owner, then proves the same committed claim and serialized `Assigned` event. Source dispatch completion follows that proof. A failed arm, failed proof, or unprovable ambiguous acknowledgement permits zero process launches. Recovery treats every existing claim as potentially launched and completes source acknowledgement without launching again.

The task handler performs the first renewal immediately after process spawn and durable generation-matched `Started` staging, then renews at one quarter of the liveness timeout while the child is active. Every renewal conditionally updates only the exact claimed generation and owner. A stale generation, different owner, or terminal record changes nothing and is a failure.

## Crash behavior

- Before claim commit, the dispatch remains pending and may be claimed after restart.
- After claim commit, recovery treats the generation as potentially launched and never relaunches it, even if source acknowledgement or `Started` staging is absent.
- After spawn, failure to stage generation-matched `Started` or perform any renewal stops the tracked child; the durable claim remains authoritative.
- Deadline expiry and generation-qualified terminal-outcome election never reopen the pickup generation.

## Fresh all-NATS deadline settlement

Each Executor and Conductor runs the same bounded competing scan over version-2 pickup records. A scan obtains NATS server time, selects only armed unresolved records whose durable deadline is due, and submits the exact dispatch key, pickup generation, and owner to `SafeAttemptOutcomeHandoff`. An optional expiry marker may wake the scan early; periodic scanning reconstructs the same due set after marker loss or Conductor outage.
Process exit, post-claim process setup failure, cancellation reconciliation, and a due deadline conditionally mutate the same pickup record. The winner stores one outcome and, where the existing wire contract requires one, the exact unchanged terminal TaskEvent bytes. The Conductor later enqueues TaskEvent bytes under one stable terminal identity; the existing TaskEvent work queue retains them through restart until relay-channel forwarding is acknowledged. A cancellation winner instead stages the existing `CancelTaskAck` through its durable acknowledgement record. A loser or stale/non-owner renewal or cancellation observes settlement and performs no contradictory side effect.

Terminal settlement prevents later renewal, `Started` staging, owner-directed cancellation of that generation, or a second terminal election. Only the Control plane may create a later Attempt. If an Executor owner dies with an unresolved cancellation, the durable server-time deadline remains authoritative; liveness election settles the generation and cancellation reconciliation derives `AlreadyExited` from that result.

## all-Redis deadline settlement

The role-owned sorted deadline index contains only armed, unresolved pickup generations and uses Redis server time for claim, renewal, failure registration, and due selection. A bounded sweeper selects a due dispatch key, reloads its exact generation and owner, and enters the same atomic terminal election as process exit, setup failure, and cancellation. The one winner stores the existing encoded terminal TaskEvent bytes and removes the deadline; stale generations and non-owners cannot renew, complete, start, cancel, fail, or remove the current pickup.

A source entry remains consumer-group pending while capability, quota, read-only, OOM, or local-fsync proof prevents the handoff. After a fsync-proved claim, restart treats that generation as potentially launched, may finish its source completion, and never invokes the process launcher. Terminal settlement releases the active-claim charge; staged event state remains accounted until its safe forwarding boundary.


## Tickr Lite deadline settlement

The recovery scan treats every claimed generation as potentially launched. A claim whose deadline is not yet due remains claimed and is never selected for process launch. Once the deadline is due, the scan submits that exact dispatch key, pickup generation, and owner to `SafeAttemptOutcomeHandoff` as `LivenessExpired`.

Process exit, task-process setup failure after claim, and liveness expiry use the same conditional terminal election. The winner records one local outcome and stages the unchanged published terminal `TaskEvent` in the same writer transaction. A loser observes the recorded outcome and performs no relay, log, scope, or lifecycle side effect.

Terminal settlement prevents later liveness arm, renewal, `Started` staging, or a second terminal event for that generation. A late deadline after process exit therefore changes nothing, and only the Control plane may create a later Attempt.

## Invariants

- The initial durable deadline arm is fail-closed for fresh all-NATS, all-Redis, and Tickr Lite safe pickup; transient expiry notification is not an arm or verdict.
- Liveness cadence derives from the timeout and is not independently configurable.
- Owner and pickup generation qualify every arm, proof, `Started` stage, renewal, and terminal mutation.
- Pickup generation is adapter-local durable evidence only and never appears in the published `TaskEvent`.
- Restart reconciliation never invokes the process launcher for a claimed generation.
- Exactly one terminal outcome and terminal TaskEvent can exist for one pickup generation.
- A due liveness winner projects as the existing identity-only `Unhealthy` TaskEvent.
- Fresh all-NATS expiry is selected from durable deadline state using NATS server time, never from a transient expiry marker.
- all-Redis expiry is selected from the durable sorted deadline index using Redis server time; key expiry and Pub/Sub never author `Unhealthy`.
- Bounded Executor and Conductor sweepers may compete, but the generation-and-owner-qualified pickup-record mutation elects one result.
- Cancellation reconciliation competes through the same generation-and-owner-qualified terminal mutation as liveness and process evidence.
- Owner death cannot turn transient notification loss into `NoProcess`; deadline election provides the durable terminal evidence used to finish the acknowledgement.
- An elected fresh all-NATS terminal TaskEvent remains in durable staging until the Conductor relay forwards it.
- An elected all-Redis terminal TaskEvent remains in accounted durable staging until safe forwarding releases it.
