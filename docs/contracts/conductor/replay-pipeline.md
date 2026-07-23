# Conductor replay pipeline contract

**Status:** Current.

**Code:** `src/conductor/src/replay_pipeline.rs`, `src/conductor/src/replay_pipeline/local.rs`, `src/conductor/src/replay_rehydration.rs`, and `src/migrations/src/replay_repository.rs`.

## What this component is

The replay pipeline owns one accepted replay from durable ingress through Trigger relay, tickr-ctx rehydration, born-Stall release, and terminal local settlement. Replay identity, ordering, typed API outcomes, and the published Signal family do not vary by formation.

The distributed formation retains its periodic durable re-drive. Tickr Lite selects committed replay lifecycle rows through the one-writer SQLite repository; an in-process notification is only a latency hint.

## Durable identity and lifecycle

`replay_instance_id = UUIDv5(source_instance_id, signal_id)` is committed at ingress with the source identity, replay frontier, pre-grounded nodes, seed witness, optional idempotency key, and shadow audit before any outbound effect. A replay row follows `Materializing → Released`; `VersionUnresolvable` is a terminal ingress park. Neither terminal state can reopen.

The replay Trigger always reuses the row's committed `signal_id`. The born-Stall Resume uses `UUIDv5(replay_instance_id, "release")`. Rehydration uses the committed replay and hydration identities. A re-drive therefore reproduces the same logical Trigger, scope writes, hydration sentinel, and Resume rather than minting a second effect identity.

Replay drive order remains:

1. forward the Trigger that materializes the replay under `replay_instance_id`;
2. apply the rehydration plan and hydration sentinel when required;
3. forward the stable Resume when the replay is born Stalled; and
4. conditionally settle the durable row as `Released`.

The relay sender returning success retains the existing ack-on-forward boundary. It does not assert Control-plane application acknowledgement.

## Tickr Lite selection and notifications

Tickr Lite runs a bounded startup scan before its steady-state loop. Periodic scans remain bounded. A bounded notification may request an immediate scan after ingress commits, but a full channel, closed channel, dropped sender, or process exit loses only that hint.

A replay is eligible when it remains `Materializing`, satisfies the scan's minimum age, and has no lease or an expired lease. Selection orders by `updated_at` and `replay_instance_id`. Startup and notification-led scans use zero minimum age; steady-state scans apply the configured backoff.

Selection and lease acquisition commit an owner, opaque token, and expiry through the formation's sole SQLite writer before replay effects begin. Every selection has a fixed batch limit. Corrupt rows are reported independently and cannot prevent healthy rows from the same bounded selection from progressing.

## Lease, effect, and settlement law

A leased worker reconstructs the drive only from the committed replay row and selected archived source. Identity, frontier, pre-grounding, and seed-witness disagreement fails before an effect. A normal failed drive conditionally releases its lease without acknowledging the row.

Only the exact owner and opaque token of an unexpired lease may settle `Materializing → Released`. Settlement records the existing typed outcome and clears the lease. A stale worker returns `LeaseLost`; a duplicate or late settlement returns the existing typed terminal outcome and creates no second transition. Terminal transitions outside the local worker also clear any lease.

A process can fail after lease commit, after a rehydration effect, or after relay forwarding but before settlement. The row remains durable. It becomes eligible after lease expiry, and recovery reconstructs the same effect identities. The lease prevents concurrent ordinary processing; stable identities and conditional settlement resolve the unavoidable post-effect crash ambiguity.

## Invariants

- **Durable source.** Channels and worker lifetimes never create, acknowledge, or remove replay work.
- **Stable bounded selection.** Every local scan leases a deterministic, bounded set of committed eligible rows.
- **Commit before effects.** Replay work starts only after its lifecycle row and local lease commit.
- **Stable ordering.** Trigger forwarding precedes rehydration; rehydration precedes Resume; settlement follows successful forwarding.
- **Stable effect identity.** Retry preserves the replay, Trigger, hydration, and Resume identities.
- **Lease-guarded settlement.** An expired or superseded lease cannot settle or clear another worker's replay.
- **Terminal monotonicity.** Duplicate wakeups, lease expiry, worker races, and late settlement cannot reopen a terminal replay.
- **Wire compatibility.** The existing Signal family and API response shapes remain unchanged; local lease fields never cross the plane boundary.

The file-backed SQLite suite covers stable bounded selection, duplicate scans, lease survival and expiry, startup recovery, dropped-notification periodic recovery, crashes around effects and relay forwarding, conditional settlement, and restart after settlement.
