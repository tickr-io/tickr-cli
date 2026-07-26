# Definition submission contract

## Durable authority

A committed definition lifecycle row at `Ready` is the authority for submission work in every formation. NATS pointers and bounded local notifications are latency hints only. Startup reconciliation and bounded steady-state scans select eligible rows even when a notification is lost, duplicated, full, closed, or disabled.

Coordination selection changes only the advisory wakeup protocol. It does not change the selected SQL authority, published workflow definition family, or Conductor-to-Control-plane relay topology.

## Selection and ownership

Every formation selects only committed `Ready` rows in stable `(workflow_id, workflow_version)` order. Postgres contenders use skip-locked selection, while the sole SQLite writer serializes the same conditional lease operation. The repository installs an owner, opaque lease token, and expiry before returning a definition payload. A row with an unexpired lease is not eligible for another worker. Expiry makes the row eligible again because forwarding the same committed definition is idempotent at this boundary.

Settlement is generation-safe: `Ready -> Submitted` succeeds only for the matching unexpired lease. Settlement clears the lease fields and returns one typed outcome: submitted, already settled, absent, or lease lost. Duplicate notifications and competing workers therefore converge on the durable row rather than channel delivery history.

## Relay boundary

A worker forwards the existing encoded workflow definition as `SubmitWorkflow` and conditionally settles the row only after the relay outbound channel accepts it. This is the existing ack-on-forward boundary. It does not assert that the Control plane applied the definition and does not provide cross-turn exactly-once delivery.

Relay unavailability or a failed forward leaves the row at `Ready`. A process exit before forwarding leaves the lease to expire. A process exit after forwarding but before local settlement can cause the same definition to be forwarded again after lease expiry. Those cases prefer duplicate idempotent forwarding over stranding committed work.

## Recovery invariants

- Notification loss cannot strand a committed `Ready` definition.
- Restart never settles a definition merely because it was selected or leased.
- Relay failure and Control-plane disconnection preserve eligibility for bounded re-drive.
- Only successful relay forwarding permits local settlement.
- One unexpired lease owns a row at a time; stale owners cannot settle it.
- Lease identity never enters the published workflow definition or relay envelope.
