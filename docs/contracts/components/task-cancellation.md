# Task cancellation contract

## Published boundary

Cancellation consumes the existing published `CancelTaskRequest` and produces the existing published `CancelTaskAck`. Pickup generation, fence identity, owner-notification state, and reconciliation outcome remain local coordination data and never enter a published protobuf.

The cancellation acknowledgement identity is stable for one workflow-instance and task-instance pair. Repeating the same cancellation request converges on the same fence, terminal election, outbox row, and acknowledgement bytes.

## Safe cancellation fence

A `SafeCancellationFence` is the durable generation-qualified barrier between cancellation acceptance and process ownership. Fence commit precedes owner notification. The commit records the matching dispatch key, current pickup generation, and committed owner when a claim exists; an unmatched cancellation records no dispatch, generation, or owner.

Claim acquisition rejects a task identity that already has a cancellation fence. Spawn proof rejects a fence for its exact dispatch and pickup generation. Fence commit and proof-to-spawn execute under the task handler's shared launch gate, so exactly one ordering is possible: either the fence commits before proof and spawn is denied, or the task handler registers the process before the fence commits.

## Owner notification and process teardown

Only the committed owner is notified. The task handler remains the sole owner, signaler, and reaper for a running Task process and its process group. Cancellation requests process-group termination through the handler and waits for its durable reconciliation result:

- `killed` means the handler observed the registered process, signalled its process group, and reaped it;
- `already-exited` means durable terminal evidence existed before cancellation reconciliation; and
- `no-process` means cancellation committed with no owner, or the committed owner was notified before a process was registered.

Owner notification is recorded durably after the fence commit. A notification cannot transfer process ownership or authorize another component to signal or reap.

## Terminal election

Cancellation reconciliation is a contender in the same generation-qualified terminal election as process exit, process setup failure, and liveness expiry. `killed`, `already-exited`, and `no-process` cannot stage a terminal effect without passing the single-winner conditional transition. A late or duplicate contender observes the settled winner and produces no second terminal TaskEvent.

Cancellation outcomes project through the existing Task Manager guard: `killed` uses the existing killed acknowledgement, while `already-exited` and `no-process` use the existing no-such-task acknowledgement. The local reconciliation identity remains internal.

## Restart reconciliation

Restart scans unresolved fences and unresolved claims. An unmatched fence can settle as `no-process`. A fence whose generation already has a durable terminal winner can settle as `already-exited`. A claimed owner without terminal evidence remains fenced and unresolved until its liveness deadline supplies durable evidence; restart never reports it killed and never relaunches that pickup generation.

## Acknowledgement outbox

Fence settlement and acknowledgement staging share one writer transaction. The acknowledgement remains pending through writer, relay-channel, or process outage. Forwarding uses the existing relay envelope and marks the row complete only after relay-channel send succeeds. A crash after send but before the completion mark redelivers the same acknowledgement identity and bytes.

## Invariants

- Fence commit always precedes owner notification.
- A fenced task identity cannot acquire a new claim, and a fenced pickup generation cannot prove spawn readiness.
- The task handler is the sole process-group signaler and reaper.
- Every reconciliation outcome passes through the generic terminal election.
- Restart never converts uncertain ownership into a killed claim and never relaunches a claimed generation.
- Exactly one acknowledgement is staged per stable cancellation identity and remains durable until relay forwarding completes.
