# Conductor lifecycle contract

## Formation profiles

Distributed formation remains the default. Its API component and Conductor run as separate processes, Conductor replicas remain externally coordinated and interchangeable, and existing NATS, Postgres, object-store, and cross-plane relay behavior is unchanged.

Tickr Lite composes the logical API component, Conductor, and exactly one Executor under one `LiteSupervisor`. Same-process ownership does not collapse role boundaries: the API reaches Conductor mutations only through the bounded production Command envelope interface, task pickup and cancellation use their local coordination interfaces, and the existing relay remains the only cross-plane transport.

## Recovery before admission

The Tickr Lite Conductor role does not accept work until formation admission completes. While holding the exclusive data-directory lease, the supervisor verifies the selected schema and manifest, opens the sole SQLite writer, replays recoverable local journals, resumes pending Compaction cleanup, and reconciles every overdue pickup claim. It then constructs local writer, scope, lifecycle, relay, Executor, and query roles and registers each as a critical formation child.

Lifecycle workers, relay processing, Compaction drain, and task claiming wait on the common work-admission gate. The API listener may be bound so its critical child can be registered, but work-producing routes return unavailable while readiness is false. The `tickr-ctx` socket likewise rejects task requests until explicitly marked ready.

## Readiness and supervision

Readiness is a formation property owned only by `LiteSupervisor`; no child may publish it independently. The supervisor publishes readiness only after all startup checks, recovery, bindings, and child registrations succeed and no child has exited during registration. It marks the helper ready and HTTP readiness true before releasing the common work-admission gate.

Any critical child return, error, or panic clears HTTP and helper readiness before formation cancellation. Shutdown joins the remaining children and keeps the writer, repositories, helper handle, and data-directory lease alive until those children settle. There is no partially ready fallback formation.

The sole SQLite task-pickup writer, local Command and scope writers, helper endpoint, definition/Patch/replay/submission workers, relay loop, Executor dispatch loop, Compaction drain, and HTTP server are critical children. A return of `Ok`, an error, a panic, or loss before admission is a formation failure. Only bounded notification hints whose authoritative work is reconstructible from durable rows may be non-critical; a closed hint channel disables that receive branch while periodic durable scans continue.

All child settlement is bounded by the formation shutdown deadline. Missing that deadline is itself a failure. The supervisor aborts the remaining child futures only after the deadline and keeps durable owners and the data-directory lease alive until every registered child has joined or been aborted.

A local Executor liveness-renewal failure first records the affected generation by making its durable deadline due, then returns an Executor-child error. The supervisor withdraws readiness before cancelling the other Conductor roles. Restart, not the failed child or supervisor, selects the due claim and invokes the existing single-winner terminal election; parent death never fabricates a Task outcome.

## Recoverable Control-plane outages

Control-plane query failure after startup is reported through health as degraded or unhealthy. Relay connection failure and stream loss remain inside the relay's bounded reconnect loop and do not terminate the formation. Reconnection rebinds the live relay channel and restarts drains from durable local staging, preserving stable payload identities and the existing ack-on-forward boundary.
