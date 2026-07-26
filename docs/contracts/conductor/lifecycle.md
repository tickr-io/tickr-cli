# Conductor lifecycle contract

## Formation profiles

An omitted profile resolves to `all-nats`. The `all-nats` and `all-redis` formations keep the API component and Conductor in separate processes, keep Conductor replicas externally coordinated and interchangeable, retain Postgres and the S3-compatible final-Log store, and retain the existing Conductor relay topology. They differ only through their complete, role-specific Coordination protocol sets; neither can be inferred from endpoint availability or partial role configuration.

The explicit `lite-local` formation composes the logical API component, Conductor, and exactly one Executor under one `LiteSupervisor`. Same-process ownership does not collapse role boundaries: the API reaches Conductor mutations only through the bounded production Command envelope interface, task pickup and cancellation use their local Coordination interfaces, and the existing relay remains the only cross-plane transport.

There is no partially ready fallback formation. Startup, restart, and capability recovery cannot substitute NATS for Redis, combine role protocols across profiles, or continue with an unproved choreography capability.

## Admission and recovery before runtime resources

Every formation resolves and admits one complete descriptor before constructing repositories, substrate clients, listeners, consumers, claims, relay loops, work producers, HTTP endpoints, or notification channels. Admission verifies the profile-owned topology, stores, Executor fleet, ingress, all thirteen Coordination role implementation/protocol pairs, and all three choreography capabilities as one unit. Failure leaves readiness false and creates no runtime resource.

For `all-nats`, the process entry point connects only to the configured NATS endpoint and completes one bounded fresh-resource admission before dispatching to the API component, Conductor, or Executor startup. Admission verifies the hardened protocol-set identity, exact stream and KV configuration, and every durable consumer. Only after that future succeeds may component-specific SQL verification, listeners, reconciliation, consumers, claims, relay loops, or producers start.

An `all-nats` identity mismatch, nonempty fresh state without an identity, exact-resource mismatch, connection failure, or twenty-second admission timeout terminates startup. No component starts against a partial setup, and startup never searches another NATS namespace for state to adopt or repair.

The `all-redis` formation parses its external connection descriptor after descriptor admission and before runtime resource construction. Parsing rejects plaintext, absent trust roots or credentials, credentials or query parameters embedded in endpoints, Sentinel-mediated configuration, and malformed topology without constructing a client or opening a socket.

Probe-only TLS connections then require certificate and hostname validation and prove every configured credential. The probes admit only Redis OSS 7.4.x at exactly one standalone writable primary with cluster mode disabled, required command availability, and usable nondecreasing Redis server time. A failed endpoint, identity, version, role, Cluster, multiple-primary, or time capability terminates startup with a secret-free typed error; probe connections are dropped and are never transferred to runtime roles.

Only the immutable admitted capability may advance to later Redis admission layers. No API component, Conductor, or Executor listener, repository, runtime Redis client, consumer, claim, relay loop, work producer, HTTP endpoint, or notification channel exists before this phase succeeds. Restart or capability recovery reconstructs pending role state behind the closed work-admission gate; readiness can return only after the same descriptor and complete capability class are proved.

After all-Redis external capability and ACL admission succeeds, component-specific Postgres roles may open. The process composer then creates all thirteen role-authenticated Redis clients, binds them to the API component, Conductor, and distributed Executor through role-specific interfaces, registers every role probe and reconstruction callback with the capability monitor, and completes the first reconstruction pass before it opens readiness or starts listeners, consumers, claims, relay loops, or producers. No all-Redis path constructs an NATS Coordination client or touches an NATS Coordination resource; the retained Conductor relay may continue to use its separate cross-plane NATS transport.

The complete capability monitor has shared execution fate with each distributed component. Capability loss first closes readiness and its generation-qualified admission fence, then prevents new durable admission, pickup, and acknowledgement. Recovery re-proves the admitted fingerprint and every role capability, reconstructs pending work for every role, and opens a new generation only after all callbacks succeed.

The `lite-local` Conductor role likewise does not accept work until formation admission completes. While holding the exclusive data-directory lease, the supervisor verifies the selected schema and manifest, opens the sole SQLite writer, replays recoverable local journals, resumes pending Compaction cleanup, and reconciles every overdue pickup claim. It then constructs local writer, scope, lifecycle, relay, Executor, and query roles and registers each as a critical formation child.

Lifecycle workers, relay processing, Compaction drain, and task claiming wait on the common work-admission gate. The API listener may be bound so its critical child can be registered, but work-producing routes return unavailable while readiness is false. The `tickr-ctx` socket likewise rejects task requests until explicitly marked ready.

Definition-build, Patch-build, Patch-apply, and definition-submission progress is reconstructed from the selected SQL repository in every formation. Each reconciler completes an initial bounded scan before entering its steady-state notification/timer loop, then repeats bounded ordered scans while the process remains admitted. NATS delivery and local channels can advance the next scan but cannot authorize execution, settlement, parent finalization, readiness, or a durable receipt. Restart and future capability-gate reopening therefore resume unresolved rows through lease expiry, conditional settlement, and authoritative parent finalizers rather than through notification replay.

## Readiness and supervision

For `lite-local`, readiness is a formation property owned only by `LiteSupervisor`; no child may publish it independently. The supervisor publishes readiness only after all startup checks, recovery, bindings, and child registrations succeed and no child has exited during registration. It marks the helper ready and HTTP readiness true before releasing the common work-admission gate.

For `all-redis`, the process capability monitor owns readiness and the common work-admission generation. API, Conductor, and distributed Executor roots read that shared state rather than deriving readiness from Redis connectivity or component-local progress. Missing role registration, failed reconstruction, or monitor exit keeps readiness closed and fails the component.

Any critical child return, error, or panic clears formation readiness before cancellation. Shutdown joins the remaining children while keeping the admitted descriptor, capability monitor, role clients, repositories, and final-Log operator alive until those children settle. The all-Redis monitor is cancelled and joined after work-producing children stop; its role clients are released last. There is no partially ready fallback formation.

The sole SQLite task-pickup writer, local Command and scope writers, helper endpoint, definition/Patch/replay/submission workers, relay loop, Executor dispatch loop, Compaction drain, and HTTP server are critical children. A return of `Ok`, an error, a panic, or loss before admission is a formation failure. Only bounded notification hints whose authoritative work is reconstructible from durable rows may be non-critical; a closed hint channel disables that receive branch while periodic durable scans continue.

All child settlement is bounded by the formation shutdown deadline. Missing that deadline is itself a failure. The supervisor aborts the remaining child futures only after the deadline and keeps durable owners and the data-directory lease alive until every registered child has joined or been aborted.

A local Executor liveness-renewal failure first records the affected generation by making its durable deadline due, then returns an Executor-child error. The supervisor withdraws readiness before cancelling the other Conductor roles. Restart, not the failed child or supervisor, selects the due claim and invokes the existing single-winner terminal election; parent death never fabricates a Task outcome.

## Recoverable Control-plane outages

Control-plane query failure after startup is reported through health as degraded or unhealthy. Relay connection failure and stream loss remain inside the relay's bounded reconnect loop and do not terminate the formation. Reconnection rebinds the live relay channel and restarts drains from durable local staging, preserving stable payload identities and the existing ack-on-forward boundary.
