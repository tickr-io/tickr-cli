# Data-plane coordination contract

## Scope and authority

A Coordination role is one independently contracted Data-plane responsibility with its own acknowledgement, ordering, recovery, expiry, and loss law. The Resolved formation descriptor selects one versioned protocol identity for each of the thirteen roles below. Callers depend on the role law, not on NATS, Redis, SQLite, journal, key, stream, subject, command, or script types.

The contract set has no universal broker or repository interface. Role-specific implementations may share durability primitives internally, but they do not erase role boundaries or expose substrate clients to callers.

## Formation applicability

| Profile | Role applicability |
| --- | --- |
| `all-nats` | all thirteen roles are enabled through their profile-owned versioned NATS protocols |
| `lite-local` | eleven local roles are enabled; `IngressIdempotencyStore` and `EventIngress` are explicitly disabled |
| `all-redis` | all thirteen roles are enabled through their profile-owned versioned Redis protocols |

Every enabled role has a stable, secret-free protocol name and positive protocol version. Disabled roles retain explicit disabled identities rather than disappearing from the descriptor. Admission requires the complete role set and all three choreography proofs together; it cannot admit a partial or cross-profile combination.

## all-Redis initial capability class

Before any Redis-backed role adapter exists, the selected external capability must prove certificate-validated TLS, Redis OSS 7.4.x identity and required command availability, direct access to exactly one standalone writable primary with cluster mode disabled, and usable Redis server time. Plaintext, an untrusted or hostname-mismatched certificate, missing trust roots, rejected credentials, a replica or read-only role, Cluster, Sentinel-mediated topology, multiple writable endpoints, another version, or a compatibility-only server refuses the complete formation.

This phase returns secret-free capability facts and no Redis client. Role construction therefore cannot observe or recover from a failed probe by opening a listener, consumer, claim, relay loop, producer, or weaker transport. Primary-local AOF durability, role ACL isolation, capacity, namespace identity, and runtime monitoring are additional admission layers; transport-and-topology admission alone makes no fsync, replica-durability, availability, or role-operation claim.

## all-Redis operation manifests

Each Redis role adapter owns one operation manifest at the same protocol identity selected for that Coordination role. The manifest is not an ACL policy or a shared command registry: it is the adapter's versioned declaration of the exact commands, named script SHA-256 identities, key and channel patterns, required-operation canaries, and representative cross-role and administrative operations that admission must deny. Formation admission aggregates these declarations only after all thirteen adapters have supplied them.

Commands, scripts, namespace patterns, and canary references are exact normalized sets. Required canaries may reference only an operation and key or channel pattern registered by that manifest. A cross-role denial probe uses one registered role operation against another Coordination role, while an administrative denial probe names an operation absent from the role's allowed set. Both denial classes are mandatory.

Every key and channel pattern begins with `tickr:{namespace}:<coordination-role>:`. The placeholder is not a concrete namespace or endpoint. Manifest types carry no endpoint, username, password, credential, trust root, certificate content, or other secret/location field, and sensitive or location-bearing values are malformed.

The manifest identity is a SHA-256 digest of its schema, Coordination role, role protocol, commands, script identities, patterns, canaries, and forbidden probes in canonical order. Entry order does not change it; changing any command, script digest, or namespace pattern does. The complete set is ordered by the canonical thirteen-role list and contributes its identities to the formation capability fingerprint.

Missing, duplicate, malformed, cross-role, protocol-mismatched, or incomplete manifest sets fail before namespace inspection, Redis capability or ACL probes, client construction, or any runtime role construction. <!-- enforced-by: tickr-cli/src/redis_operation_manifest.rs::tests; tickr-cli/src/redis_formation_identity.rs::tests -->


## all-NATS protocol versioning

The thirteen `all-nats` role identities are one atomic version-`2` protocol set under the `tickr.all-nats.*` name family. Their subjects, streams, durable consumers, and KV buckets use only the `tickr.all_nats.v2`, `TICKR_ALL_NATS_V2`, and `tickr-all-nats-v2` resource families. A role cannot retain a version-`1` identity or an unqualified resource name while another role advances.

Formation admission installs or verifies the protocol-set identity before provisioning the exact static resources for `TaskDispatch`, `TaskEvents`, both `TaskCancellation` legs, `CompactionStaging`, `LogStaging`, `EventIngress`, the admitted `ScopeStore`, `IngressIdempotencyStore`, `LivenessWatchdog`, and `ExecutorFleetStatus`. Core-NATS `CommandBus`, `LifecycleWork`, and `SignalAppliedNotifier` subjects are namespace-qualified but require no persisted resource.

No role adapter exposes an operation that accepts an arbitrary stream, consumer, bucket, or subject for formation setup. Legacy discovery, enumeration, migration, dual readers, compatibility consumers, and existing-deployment utilities are outside this contract.

## Coordination-role law index

1. **`CommandBus`.** Carries the existing encoded `ApiCommandRequest` and `ApiCommandResponse` envelopes through bounded request/reply. It preserves command deadlines, encoded-payload limits, typed responses, duplicate-correlation behavior, and distinct unavailable, timeout, and malformed-reply outcomes. A request that has expired is not applied.
2. **`TaskDispatch`.** Offers competing Executors bounded dispatch work. Pickup establishes a generation-qualified owner claim, stages `Assigned`, and arms liveness before the source dispatch is irreversibly acknowledged. An ambiguous acknowledgement is resolved by the stable pickup identity and never authorizes a second process launch.
3. **`TaskEvents`.** Durably stages existing encoded TaskEvents before the producer proceeds. Delivery is redeliverable until the Conductor-relay forwarding boundary; forwarding does not claim Server application. Process exit, setup failure, liveness expiry, and cancellation converge on one generation-qualified terminal event.
4. **`TaskCancellation`.** Commits a durable fence against the current pickup generation and owner before owner-directed termination. Delivery, process-group reconciliation, terminal election, and the acknowledgement record survive restart. A stale generation cannot cancel or acknowledge a later Attempt.
5. **`CompactionStaging`.** Durably stages the unchanged `CompactionEnvelope` bytes under a stable identity before sending `CompactionAck`. Drain completion and staging cleanup occur only after the existing Postgres archive commit and S3-compatible final-Log verification.
6. **`LifecycleWork`.** Treats coordination delivery as an advisory latency hint. Definition-build, Patch-build, and submission rows remain authoritative; bounded ordered scans, expiring leases, conditional settlement, and parent finalizers reconstruct progress after notification loss or process restart.
7. **`LogStaging`.** Gives each pickup generation a Log-stream identity and each record a stable sequence identity and content digest. Acceptance rejects identity/content conflicts, records bounded pre-acceptance gaps, and advances a contiguous committed frontier. Compaction seals the full accepted/gap/frontier/terminal state, verifies final installation, commits the archive, and only then purges Accepted Log records while retaining a terminal mutation fence.
8. **`ScopeStore`.** Stores opaque tickr-ctx scope envelopes with atomic create, replace, and delete semantics and bounded values. Scope lineage and Workflow-instance lifetime remain unchanged. Missing or corrupt scope at Compaction is an error, never an empty value.
9. **`IngressIdempotencyStore`.** Keeps producer idempotency identity separate from transport delivery identity. Stable payload hashes, bounded reservations, same-hash deduplication, different-hash conflict, and expired-reservation reclaim survive restart.
10. **`LivenessWatchdog`.** Stores generation-qualified owner deadlines and derives verdicts from durable deadline state rather than transient expiry notifications. Renewal by a stale generation or non-owner is rejected. A competing sweeper enters the same terminal election as process exit and cancellation.
11. **`SignalAppliedNotifier`.** Provides only a bounded advisory hint for ByTag cancellation-materialization feedback. Loss, duplication, delay, or closure cannot acknowledge delivery, alter durable Signal state, or become an audit fact; bounded reconciliation remains authoritative.
12. **`ExecutorFleetStatus`.** Publishes expiring observational capacity metadata. Freshness, configured slots, and observed in-flight counts are diagnostic facts only; a fresh, stale, missing, duplicated, or contradictory observation cannot reserve capacity, authorize or acknowledge Task dispatch, select work, or alter queue semantics.
13. **`EventIngress`.** Separates transport delivery identity from producer idempotency. Delivery is acknowledged only after deterministic effects, Event-variable results, and a recoverable Conductor-relay intent are durable, or after a durable permanent rejection. Transient failure, saturation, or quota pressure preserves redelivery.

## ExecutorFleetStatus law

Every distributed report carries a bounded lifetime owned by its adapter. A report that reaches that lifetime is omitted even when substrate cleanup lags. Tickr Lite computes a request-time observation from its live local capacity state and retains no report between Health reads.

Executors acquire their own process capacity before pulling or selecting durable work. `TaskDispatch` and pickup admission receive that capacity authority directly and never receive an `ExecutorFleetStatus` reader or observation. Missing reports therefore do not stop pickup, while fresh reports never grant it. Duplicate identities converge in the observation adapter, contradictory values may degrade diagnostic detail, and neither case changes a dispatch generation, owner claim, queue order, source acknowledgement, or process launch.

Health labels Executor counts and load as observations, reports the observation detection window, and exposes configured and in-flight counts without deriving or promising guaranteed available capacity.


## CompactionStaging law

The Workflow-instance UUID is the stable Compaction operation identity. Staging stores the unchanged encoded `CompactionEnvelope` bytes immutably under that identity and records their SHA-256 digest. Same-identity/same-payload delivery converges; same-identity/different-payload delivery fails without replacing the accepted bytes.

Fresh all-NATS first receives the file-backed identity-store acknowledgement, then publishes the raw bytes to the versioned WorkQueue stream with one stable NATS message identity and receives its JetStream acknowledgement. Only then may the relay send `CompactionAck`. A lost publish or cross-plane acknowledgement retries through the same identity; a durable queue-evidence marker avoids a second publish once acceptance is known.

The shared durable consumer leaves an unacknowledged delivery pending across Conductor death. Before final-Log installation it verifies the delivery bytes, seals the tickr-ctx scope and every Log stream, and records one immutable Compaction digest over their identities. Every deterministic final-Log object is re-read and verified against a retained path, length, and SHA-256 digest before the existing Postgres archive transaction may commit.

Archive-commit evidence bound to that Compaction digest is the sole purge gate. Cleanup removes Accepted Log records and gaps while retaining terminal fences, cleans the sealed scope, records completion, then removes raw payload and queue evidence before acknowledging the WorkQueue delivery. A crash at any boundary leaves either the pending delivery or retained completion identity sufficient to converge; same-payload redelivery is idempotent and conflicting payload remains a conflict.

## Definition-build LifecycleWork law

Definition registration atomically creates the parent `Building` row and its ordered per-Task `pending` rows. Every enabled definition-build adapter scans those SQL rows at startup and on a bounded interval. NATS `TaskBuildJob` delivery, local channel delivery, and all-Redis LifecycleWork Pub/Sub delivery may request an earlier scan but carry no receipt, ownership, executable specification, or settlement authority.

Competing Conductors acquire expiring owner-and-token leases through bounded stable selection. The executor starts only after the lease commits. Nix realization may repeat after lease expiry because the expression-path operation is idempotent, but Task settlement requires the current unexpired lease and atomically records the result, clears the lease, and runs the existing parent finalizer. A stale worker changes nothing.

The committed parent `Ready` row is authoritative submission work. Definition-submission reconcilers likewise lease bounded ordered rows, relay the unchanged published definition, and conditionally settle `Ready → Submitted`. NATS pointers, local channel hints, and Redis LifecycleWork messages are notifications only; notification loss, duplication, reordering, Conductor death, Redis restart, and relay disconnection leave the SQL row reclaimable.

## Patch-build LifecycleWork law

Patch ingress atomically commits the parent `Building` row and one ordered per-Task `pending` row for every new Task. Every enabled Patch-build adapter scans those SQL rows at startup and on a bounded interval. NATS `PatchTaskBuildJob` delivery, the local channel, and all-Redis LifecycleWork Pub/Sub delivery may request an earlier scan but carry no receipt, ownership, executable specification, or settlement authority.

Competing Conductors acquire expiring owner-and-token leases through bounded selection ordered by pending time, Patch identity, and Task identity. A worker reconstructs the Nix expression path from the committed Patch operation and starts realization only after the lease commits. Lease expiry may authorize another idempotent realization, but only the current unexpired lease may conditionally record its result.

Per-Task settlement, lease clearing, and the Patch parent finalizer commit atomically. One failure wins `Building → BuildFailed`; the last successful child wins `Building → Submitted` and returns one apply intent; stale, duplicate, late, or already-settled contenders produce no Patch effect. Process death before that transaction commits leaves the child pending and reclaimable, while death after commit leaves the parent outcome authoritative.

Committed `Validating` and `Submitted` parents are independent lifecycle work. Reconcilers lease them in bounded `updated_at` and Patch-identity order, rebuild the unchanged validate-and-apply envelope from the row, and conditionally settle or release the exact lease. Relay ambiguity may resend one stable `patch_key`; the existing Patch identity permits only one winning application. NATS, local, and Redis notification loss, duplication, delay, reordering, closure, and restart therefore affect latency only.

### all-Redis LifecycleWork implementation

The version-`1` adapter owns one role-scoped Redis client, three Pub/Sub channel patterns under `tickr:{namespace}:lifecycle-work:wakeup:*`, and advisory-only hint, expiry, and quota keys. Its versioned operation manifest declares the exact `EVAL`, hash, sorted-set, server-time, Pub/Sub commands, script SHA-256, key/channel patterns, required script and pattern-subscription canaries, and representative cross-role and administrative denials. An operation absent from the manifest is inadmissible; the role credential has no SQL, lease, settlement, Stream, or durable-work operation.

One atomic role script purges expired tickets, coalesces at most one queued ticket per pipeline, accounts queued/coalesced/dropped/expired hints, enforces the configured soft and hard hint limits, and publishes a new ticket only when a subscriber exists. A coalesced hint adds no charge. Hard pressure, no subscriber, a closed capability fence, local channel pressure, publication failure, and expiry drop only the hint. Delivery or expiry conditionally releases the exact ticket's capacity, so an old delivery cannot release a newer ticket.

Every reconciler performs bounded read-only SQL discovery before consulting the shared `LifecycleClaimAdmission` boundary. A closed or reconstructing all-Redis generation fence leaves discovery active but prevents definition-build, Patch-build, Patch-lifecycle, and submission lease acquisition. Capability recovery runs the registered authoritative SQL reconstruction before readiness; claims resume only after the complete role probe and reconstruction reopen the fence. <!-- enforced-by: tickr-cli/src/redis_lifecycle_work.rs::tests; tickr-cli/tests/redis_lifecycle_work_law_test.rs::real_redis_lifecycle_laws_bound_hints_and_recover_all_sql_pipelines -->

## TaskDispatch safe-pickup law

Process capacity is acquired before substrate selection or pull. Saturation cannot acknowledge, reject, claim, or otherwise mutate a valid dispatch. Decode and process-input validation precede claim. Poison input converges to a durable rejection containing the original bytes and reason before source completion and cannot stage `Assigned` or launch.

One stable dispatch-operation identity names the pickup across ambiguous client acknowledgements. A successful new pickup binds its next monotonic generation, sole owner, server-time liveness deadline, and exact encoded `Assigned` bytes. The adapter durably stages `Assigned`, conditionally arms liveness, proves all four values, and only then completes the source dispatch. Recovery of an existing claim may finish durable staging and source completion but never authorizes a process launch; unprovable ambiguity therefore yields zero launches.

Process spawn follows that exact proof. `Started` staging and the immediate first renewal are conditional on the same dispatch key, pickup generation, and owner. Every later renewal and terminal mutation retains the fence. A stale generation, different owner, or settled record changes nothing and cannot affect a later Attempt. The coordinator receives only the backend-neutral dispatch, claim, event bytes, deadlines, and typed outcomes; NATS messages, KV stores, SQLite repositories, and adapter resource names remain behind role implementations.

### all-Redis TaskDispatch implementation

The version-`1` adapter owns one Stream and consumer group under `tickr:{namespace}:task-dispatch:*`. Producer identity, payload digest, Stream entry, pickup generation, owner, deadline, staged-event bytes, rejection record, quota counters, and stable mutation identities remain adapter-local. Its versioned operation manifest is the sole declaration of exact commands, the exact script identity, role key patterns, required canary, and representative cross-role and administrative denials; an operation absent from that manifest is inadmissible.

An Executor process slot is held before `XAUTOCLAIM` or `XREADGROUP`. The pulled entry becomes consumer-group pending, but no pickup record changes until decode and process-input validation succeed. A poison entry is copied with its original bytes and a bounded reason into durable rejection state; the same script completes its source only after the mutation crosses primary-local AOF fsync. Valid pickup uses one atomic script to advance generation, bind owner, derive the deadline from Redis `TIME`, arm liveness, and store the exact encoded `Assigned` bytes.

Source completion is a separate generation-and-owner-qualified mutation after exact claim proof. It `XACK`s and removes only the proved Stream entry, releases its dispatch-entry charge, and crosses primary-local fsync before returning success. A timeout or disconnect resolves through the stable per-phase identity. Recovery may complete that mutation for an existing claim but cannot return launch authority. `Started`, first and later renewal, failure registration, cancellation outcome, and terminal election all reject a stale generation, non-owner, or settled record.

Quota state counts dispatch entries, active claims, staged events, and their admitted bytes. Soft pressure is projected before hard record, claim, staged-event, or byte limits. Hard pressure fences append or pickup without source completion. Dispatch capacity is released only after fsync-proved source completion, active-claim capacity only after generation-qualified terminal settlement, and staged capacity only after safe forwarding makes those bytes removable. OOM, read-only Redis, failed local-fsync proof, missing accepted identity, unexpected Stream loss, or inconsistent accounting closes role capability and leaves source work durable. <!-- enforced-by: tickr-cli/src/redis_task_pickup.rs::tests; tickr-cli/tests/redis_task_pickup_law_test.rs -->

## SafeAttemptOutcomeHandoff law

One durable record binds a terminal election to the exact dispatch key, pickup generation, and owner. Process exit, post-claim setup failure, liveness expiry, and cancellation reconciliation submit existing encoded terminal TaskEvent bytes to that election. One conditional mutation records the winner; every late, duplicate, stale-generation, or non-owner contender reads the elected result and performs no contradictory event, Log, scope, cancellation, or lifecycle side effect.

For fresh `all-nats`, the version-2 pickup KV record is the election record and carries a durable NATS-server-time deadline. Bounded sweepers in Executors and Conductors compete on due records through the same conditional mutation used by process-side contenders. Per-key expiry markers are optional wakeups only. A marker cannot author `Unhealthy`, and periodic scanning reconstructs deadlines that became due while Conductors were unavailable.

The elected existing terminal TaskEvent bytes remain durable in the pickup record until they are enqueued under one stable per-generation identity in the version-2 TaskEvent work queue. That queue completes delivery only after the Conductor relay accepts the unchanged envelope. A crash or ambiguous acknowledgement before either local completion step retries the same identity and bytes. Forwarding may still be duplicated across the residual relay-hop/apply gap; it does not imply Server application or cross-turn exactly-once.

Tickr Lite applies the same observable law in one SQLite writer transaction: its generation-qualified outcome and terminal outbox row commit together, and the outbox remains pending until relay-channel forwarding. The substrate-specific transaction shape differs; the winner, loser, restart, redelivery, and published-event laws do not.

## SafeCancellationFence law

One stable acknowledgement identity names every delivery and replay of a cancellation request. Before owner notification or process-group signalling, the selected adapter commits a durable fence containing that identity and the current dispatch key, pickup generation, and owner. A queued or absent Task has no owner at commit; the fence prevents a later claim from launching and binds to the generation that consumes the queued dispatch.

Cancellation reconciliation has explicit `Killed`, `AlreadyExited`, and `NoProcess` results. Queued, claiming, active, exited, missing-process, stale-generation, and already-terminal observations apply one generation-and-owner-qualified conditional transition. The exact owner may signal and reap its process group. A non-owner, a stale generation, or a duplicate delivery cannot signal or settle the current process.

Cancellation kill, process exit, setup failure, and liveness expiry submit to `SafeAttemptOutcomeHandoff`. The first terminal contender wins; later cancellation reconciliation reads that election and derives the existing acknowledgement result without authoring a contradictory TaskEvent.

The fence stores reconciliation and the exact existing encoded `CancelTaskAck` bytes before the cancellation source is completed. Fresh all-NATS enqueues those bytes under the stable acknowledgement identity and records that enqueue; Tickr Lite stages them in its local outbox. A crash before source completion or Conductor-relay forwarding reconstructs the same bytes. Duplicate requests replay the same result, while relay forwarding retains the existing ack-on-forward boundary.

Fresh all-NATS owner notification is an advisory wakeup addressed to the committed owner. Durable fence scans, cancellation-source redelivery, and the generation-qualified liveness election remain authoritative after Executor or Conductor death. Pickup generation, owner, reconciliation, and acknowledgement identity remain adapter-local and do not alter a published envelope.


## Accepted Log staging law

Each pickup generation owns one logical stream. A submitted record binds its stream identity, monotonic sequence, SHA-256 content digest, and bytes. Durable acceptance of the same identity and content is idempotent; different content for an accepted identity is a conflict. This law promises one logical Accepted Log record on replay, not one physical substrate insertion.

The committed frontier advances only across a contiguous prefix covered by accepted records or durable declared pre-acceptance gaps. Replay returns both in identity order through that frontier and retains accepted records beyond a hole until the missing range is covered. A bounded stdout drain assigns sequence before buffering, never waits for staging, and declares an evicted pre-acceptance range before later progress can hide the loss.

Controlled End-of-stream and abnormal closure are mutually exclusive durable terminals. Reconnect and restart rebuild identity, gap, frontier, and terminal state from the role protocol. Compaction records abnormal closure for an otherwise open terminal Task, then seals a canonical digest over stream identity, committed frontier, every Accepted Log record including records beyond a replay hole, every declared gap, and terminal state. The Workflow-instance Compaction seal combines those ordered stream digests with the tickr-ctx scope snapshot digest.

Fresh all-NATS stores versioned role records on the per-Task subject and uses stable message identity to suppress acknowledgement retries. Final installation preserves all Accepted Log bytes through the existing S3-compatible key and reference shapes, retains and verifies object identity, commits the archive, and only then purges Accepted Log records and gaps. One terminal fence per sealed pickup generation remains so a late adapter mutation is rejected after purge. Tickr Lite appends and syncs the same logical records in its admitted local journal and follows the same seal-before-install and commit-before-purge law.

## ScopeStore law

One scope identity binds the selected namespace to one Workflow instance. Keys remain run-scoped and values remain the existing opaque versioned tickr-ctx envelope bytes; adapters may validate the known envelope version but never decode and re-encode an accepted value. Create, replacement, and deletion are atomic per stable key, retain its lineage and owner, and cannot cross a namespace or Workflow-instance identity.

The admitted value, row, namespace-byte, and total-byte limits are checked before mutation. A refused mutation leaves every previously accepted key and byte unchanged. Fresh all-NATS serializes mutation and sealing through its versioned scope metadata record; Tickr Lite applies the equivalent mutation and claim in the SQLite writer transaction. All-Redis uses protocol `tickr.scope-store.redis-opaque-snapshot/1`: one role-local atomic script binds every mutation claim to its payload fingerprint and Workflow-instance owner before changing the scope document or exact quota counters.

Compaction changes an active scope into one immutable snapshot ordered by full key. The snapshot encoding length-prefixes each key and opaque envelope, and its SHA-256 digest is stable across retry and process restart. An existing empty scope seals as an empty snapshot; an absent scope does not. Missing identity, unreadable values, malformed or unknown envelope versions, inconsistent bounds, changed post-seal bytes, and a digest mismatch fail Compaction rather than producing an empty archive.

The immutable snapshot remains available through archive commit and redelivery. Archive enrichment carries both the existing parsed response shape and each accepted envelope's exact bytes; replay copies those bytes rather than serializing the parsed value. Cleanup requires archive-commit evidence bound to the immutable Compaction seal and is completed while fresh all-NATS WorkQueue delivery remains pending, so a crash before or after cleanup redelivers. A duplicate after cleanup reconstructs and verifies the same snapshot from committed exact bytes and the retained digest; it cannot accept a replacement scope.

The Redis adapter stores scope documents, owner records, stable claims, exact per-scope metrics, aggregate quota counters, and cleaned snapshot proofs only under `tickr:{namespace}:scope-store:*`. Its versioned operation manifest declares the exact command set, Lua identity and SHA-256, key patterns, required script canary, and representative LogStaging and administrative denials. Create and replacement enforce value, namespace, scope-record, value-record, and byte bounds atomically; soft pressure is projected and a hard crossing writes neither claim nor value. Every accepted mutation, seal, archive-commit record, and cleanup crosses one primary-local `WAITAOF 1 0` proof while the same capability generation remains open. Ambiguous acknowledgement resolves from the original claim identity and fingerprint.

Redis sealing retains active values and exact accounting through the immutable scope snapshot and verified Workflow/archive commit. Cleanup is ineligible until the archive identity is bound to that snapshot digest. The cleanup script then removes the active document and its quota charge while retaining the owner binding and cleaned exact snapshot proof, so redelivery reconstructs the same digest but cannot create a replacement scope for the archived Workflow instance. Missing documents with retained accounting, unreadable state, conflicting identities, malformed envelopes, and digest disagreement are capability or Compaction failures, never empty-scope results.

The backend-parameterized law suites exercise these outcomes against a real fresh versioned all-NATS ScopeStore, Tickr Lite, and Redis OSS 7.4.x. The Redis suite covers opaque-byte fidelity, create/replace/delete identity conflicts, ambiguous acknowledgements, value and namespace rejection, real soft/hard pressure, restart reconstruction, stable sealing, archive-gated cleanup, corruption, and ACL isolation. Adapter keys, KV revisions, SQLite rows, and metadata encodings are not part of the caller contract. <!-- enforced-by: tickr-cli/tests/redis_scope_store_law_test.rs::real_redis_scope_laws_cover_bytes_atomicity_limits_restart_seal_corruption_and_cleanup -->

## EventIngress and IngressIdempotencyStore law

Transport delivery identity and producer idempotency identity occupy disjoint adapter namespaces. A JetStream stream sequence or Redis Stream entry identifies one delivery; the producer key identifies the logical request and is bound to a stable canonical payload hash. No adapter may synthesize the producer key from transport metadata.

The first producer claim records one stable Signal identity and a bounded owner lease. A live same-hash retry waits without effects, an expired claim is reclaimable under the original Signal identity, a completed same-hash retry deduplicates, and a different-hash retry is a durable permanent conflict. Claim loss or expiry never authorizes a second logical Signal effect.

An acquired claim persists deterministic effects and Event-variable/capture results before retaining the exact unchanged `Signal` and `GateOutcome` bytes as recoverable Conductor-relay intents. Only a complete effect-and-intent record, or a durable permanent-rejection record, permits the transport delivery to be acknowledged. Processing, persistence, relay-intent, saturation, and quota failures preserve redelivery.

Duplicate delivery, producer retry, claim reclaim, and process restart converge on one producer record, one stable Signal identity, and one byte-identical intent set. Transport delivery outcomes are recorded separately so a crash immediately before ACK can settle the redelivery without repeating producer effects. The relay forward-versus-Server-apply gap and all published envelopes remain unchanged.

## TaskEvents staging and forwarding law

The all-Redis TaskEvents adapter owns one version-`1` Stream and consumer group under `tickr:{namespace}:task-events:*`. A producer supplies an adapter-local stable operation identity beside the exact existing encoded `TaskEvent` bytes. One atomic mutation binds that identity to the payload SHA-256, accounts the immutable Stream record, and appends without trimming. Same-identity/same-payload retry replays acceptance; a different payload conflicts without replacing the accepted bytes.

Producer acceptance crosses only after the mutation proves one primary-local AOF fsync and zero required replica acknowledgements while the capability generation remains open. Mutation or fsync ambiguity withholds acceptance and is resolved by the same identity. Redis-local identity, digest, Stream entry, consumer, and quota metadata never enter the published `TaskEvent` family.

Consumer-group delivery and idle-entry reclaim preserve pending work across Conductor restart. Claim accounting becomes durable before relay forwarding. A closed or lost relay leaves the delivery pending; a successful relay-channel forward is the only boundary that permits the adapter to acknowledge and remove the Stream entry. Crash after forward but before that completion may forward the same bytes again. The existing Control-plane terminal/state-and-kind guard absorbs the duplicate; forward still does not mean Server application.

The role accounts accepted Stream bytes and records separately from pending deliveries. Soft pressure is projected before a hard byte or record limit. The hard limit fences a new identity atomically before `XADD`; accepted entries are never trimmed, evicted, or dropped for quota recovery. Only fsync-proved Stream completion after relay forward releases the accepted and pending charges. Missing accepted identity, unexpected Stream deletion, negative or mismatched accounting, OOM, read-only Redis, and failed fsync close the role capability instead of acknowledging progress. <!-- enforced-by: tickr-cli/src/redis_task_events.rs::tests; tickr-cli/tests/redis_task_events_law_test.rs -->

## Command-bus request/reply law

The API admits a Command only while the selected transport proves one live, bounded Conductor mutation path. For `all-nats`, the broker-maintained queue subscription is the consumer lease: an explicitly unsubscribed or absent responder returns unavailable immediately rather than consuming the request deadline. For `lite-local`, the open sole-writer receiver is the lease and a closed or saturated bounded queue returns unavailable. Neither path treats broker connectivity, an open API process, or Health observation alone as mutation-path availability.

The encoded `ApiCommandRequest` and `ApiCommandResponse` bytes do not change. Correlation identity and the absolute caller deadline travel beside those bytes as transport metadata. Admission rejects a missing or malformed correlation/deadline, an oversized payload, a duplicate live correlation, or saturation before dispatch. The serial writer checks expiry again immediately before invoking the Command handler, so a request that expires while queued becomes a terminal rejection and late queue progress cannot apply it.

Payload bytes, admitted requests, and live correlations are hard-bounded by the Resolved formation descriptor. A correlation names at most one live request. Success, typed Conductor failure, malformed reply, duplicate correlation, timeout, payload overflow, and unavailable remain separate observable outcomes; their HTTP projections remain respectively the carried status, the carried typed error, `502`, `409`, `504`, `413`, and `503`. Reply inboxes/channels and correlation entries are released on success, typed failure, malformed reply, timeout, cancellation, expiry, or transport loss.

Capability loss and saturation withhold success and cannot enqueue a hidden mutation. The backend-parameterized role-law suite exercises these outcomes through the real fresh all-NATS and local transports using only the public Command-bus request/reply surface; adapter subjects, inboxes, channels, and correlation storage are not assertions.

For `all-redis`, one CommandBus role credential owns the version-`1` request Stream, consumer group, live-consumer lease index, correlation hashes, reply records, deadline index, and exact quota counters under `tickr:{namespace}:command-bus:*`. API admission uses Redis server time, requires a live unexpired Conductor lease, reserves request, correlation, and reply capacity atomically, and appends one request under the existing correlation identity. Consumer-group delivery and reclaim permit one serial mutation path across competing Conductors. A consumer durably claims the request, rechecks its absolute deadline immediately before handler invocation, and writes a typed expiry rejection instead of invoking the handler when queue delay or restart made it late.

Every Redis request admission, consumer lease change, processing claim, typed expiry rejection, reply transition, and cleanup is an idempotent or conditional script mutation followed by proof of one primary-local AOF fsync and zero required replica acknowledgements. The runtime capability generation fences both admission and completion; read-only, OOM, failed fsync, missing accepted identity, or inconsistent accounting closes the role capability rather than acknowledging the operation. Ambiguous mutation acknowledgement is resolved from the correlation's stable request, claim, or response digest before retry.

The Redis quota counts request records, correlation records, reply records, reply reservations, and their admitted bytes. Soft pressure is projected before the hard limit; the hard byte or record limit fences new requests without trimming accepted work. Reply delivery removes the Stream entry, correlation, deadline, reply, and their quota charge only after the reply transition is durable. Undelivered replies and typed expiry rejections remain until bounded expiry cleanup, while a processing request with a live owner lease is never trimmed. <!-- enforced-by: tickr-cli/src/redis_command_bus.rs::tests; tickr-cli/tests/redis_command_bus_law_test.rs -->

## SignalAppliedNotifier law

A ByTag Cancel Signal is staged in the selected SQL implementation before it is forwarded. The server-authored `SignalApplied` result materializes that same durable row before any notifier is invoked. API completion reads the row on a fixed bounded cadence until materialization or the request deadline; receiving a notification only requests an earlier read. The durable row, not the notification, owns Signal identity, target, matched count, audit projection, and restart reconstruction.

The notification carries only a Signal identity. It is never a receipt for Signal delivery or application, a persistence or audit record, a relay acknowledgement, or evidence that cancellation materialization succeeded. Loss, duplication, delay, reordering, malformed payload, notifier closure, or publication failure cannot change durable Signal application and cannot prevent bounded reconciliation from observing an already-materialized result.

Every notifier implementation has finite memory and non-blocking producer behavior. Tickr Lite uses a fixed-capacity channel whose full or closed result is discarded. Fresh all-NATS uses one request-scoped Core-NATS subscription on a Signal-specific subject; it creates no stream, durable consumer, persisted message, or cross-request backlog, and subscription or flush failure falls back to periodic reads.

The all-Redis version-`1` adapter owns one role-scoped client, advisory metadata keys under `tickr:{namespace}:signal-applied-notifier:*`, and only the channel pattern `tickr:{namespace}:signal-applied-notifier:materialized:*`. Its operation manifest declares the exact hash, sorted-set, server-time, script, `PUBLISH`, and `PSUBSCRIBE` operations, required script and pattern-subscription canaries, and representative TaskCancellation and administrative denials. Any other command, key pattern, or channel pattern is inadmissible.

One atomic script purges expired hints, admits at most one ticket per Signal identity, and accounts admitted, coalesced, omitted, and expired hints against configured soft and hard limits. Coalescing adds no charge. Hard pressure, no subscriber, local producer pressure, capability fencing, publication failure, and expiry may omit only the hint. Matching delivery or expiry releases its charge; an old delivery cannot release a newer ticket. These keys may survive a Redis restart until expiry but are never Signal state, a receipt, an audit record, or recovery authority.

The shared Redis capability fence suppresses publication while closed. Command, Pub/Sub, or accounting failure reports role capability loss and clears formation readiness through the monitor; the adapter does not open NATS or memory-only fallback transport. A lost restart-era Pub/Sub message therefore reaches the same fixed reconciliation deadline as a suppressed, delayed, reordered, duplicated, malformed, or expired message.

No other Coordination role may consume the Signal-applied notification or use a transient notification as its durable record. The backend-parameterized role-law suite suppresses notifications for fresh all-NATS, Tickr Lite, and all-Redis, while the real Redis suite duplicates hints, crosses real soft and hard pressure, restarts the server, exercises ACL denial, and requires the public ByTag Cancel result to converge from the durable SQL materialization. <!-- enforced-by: tickr-cli/src/conductor/tests/signal_applied_notifier_law_test.rs::suppressed_notifications_reconcile_from_durable_signal_state_for_every_backend; tickr-cli/tests/redis_signal_applied_notifier_law_test.rs::real_redis_signal_notifier_laws_cover_acl_pressure_restart_and_reconciliation -->

## Formation-level choreography proofs

- **`SafePickupHandoff`.** For one pickup generation, a durable owner claim, staged `Assigned` event, and armed liveness deadline exist before dispatch acknowledgement or process spawn. Ambiguity may yield zero launches but never a second launch.
- **`SafeAttemptOutcomeHandoff`.** Process exit, setup failure, liveness expiry, and cancellation compete through one durable generation-qualified outcome election. Exactly one terminal TaskEvent is staged; every later contender observes that election.
- **`SafeCancellationFence`.** Cancellation is bound durably to the current pickup generation and owner before termination is requested. Reconciliation and acknowledgement are restartable and cannot target a later generation.

A named profile is inadmissible if any required proof is absent. Proofs describe cross-role observable behavior; they do not imply a transaction spanning role credentials or a shared substrate abstraction. Cross-role choreography uses stable identities, generation fences, and restartable phases.

## Protocol and substrate boundaries

The `all-redis` formation retains distributed topology, Postgres, the S3-compatible final-Log store, enabled HTTP Commands and External Event ingress, and a distributed Executor fleet. Redis Pub/Sub is admissible only for advisory notifications; no durable receipt, queue, Event ingress, accepted Log, scope, cancellation, liveness verdict, or Compaction acknowledgement depends on it.

Selection never falls back from Redis to NATS or from durable coordination to memory. The contract does not define dual protocol operation, online migration, Redis Cluster or Sentinel support, replica durability, Redis-managed high availability, or a generic coordination-driver ABI.
