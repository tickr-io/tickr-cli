# Task liveness watchdog contract

## Formation-specific substrates

Distributed Executors retain the NATS KV heartbeat and expiry-marker behavior. Tickr Lite uses the sole SQLite writer and a durable deadline qualified by local pickup generation. The two implementations project the same existing `Unhealthy` TaskEvent onto the Control plane; neither changes the published family.

## Tickr Lite claim-time liveness

The atomic pickup transaction records an initial liveness deadline with the claimed generation, owner, and staged `Assigned` TaskEvent. Before spawn, the claimant must perform a conditional initial arm through the writer for that exact dispatch key, generation, and owner. The arm succeeds only while the matching `Assigned` outbox record exists.

After the arm, the claimant proves the same committed claim and exact serialized `Assigned` event. A failed arm or failed proof permits zero process launches. An ambiguous claim acknowledgement may continue only when the writer proves the committed owner, generation, deadline, and `Assigned` record; unavailable or contradictory proof permits zero launches.

The task handler performs the first renewal immediately after process spawn and durable `Started` staging, then renews at one quarter of the liveness timeout while the child is active. Every renewal conditionally updates only the exact claimed generation and owner through the writer. A renewal for a stale generation or different owner changes nothing and is a failure.

## Crash behavior

- Before claim commit, the dispatch remains pending and may be claimed after restart.
- After claim commit, recovery treats the generation as potentially launched and never relaunches it, even if `Started` is absent.
- After spawn, failure to stage `Started` or renew liveness stops the tracked child and returns failure to formation supervision; the durable claim remains authoritative.
- Deadline expiry and generation-qualified terminal-outcome election are handled by the local outcome choreography. They never reopen this pickup generation.

## Tickr Lite deadline settlement

The recovery scan treats every claimed generation as potentially launched. A claim whose deadline is not yet due remains claimed and is never selected for process launch. Once the deadline is due, the scan submits that exact dispatch key, pickup generation, and owner to `SafeAttemptOutcomeHandoff` as `LivenessExpired`.

Process exit, task-process setup failure after claim, and liveness expiry use the same conditional terminal election. The winner records one local outcome and stages the unchanged published terminal `TaskEvent` in the same writer transaction. A loser observes the recorded outcome and performs no relay, log, scope, or lifecycle side effect.

Terminal settlement prevents later liveness arm, renewal, `Started` staging, or a second terminal event for that generation. A late deadline after process exit therefore changes nothing, and only the Control plane may create a later Attempt.

## Invariants

- The initial arm is fail-closed in Tickr Lite; the distributed NATS implementation remains unchanged.
- Liveness cadence derives from the timeout and is not independently configurable.
- Owner and pickup generation qualify every arm, proof, and renewal.
- Pickup generation is local durable evidence only and never appears in the published `TaskEvent`.
- Restart reconciliation never invokes the process launcher for a claimed generation.
- Exactly one terminal outcome and terminal TaskEvent can exist for one pickup generation.
- A due liveness winner projects as the existing identity-only `Unhealthy` TaskEvent.
