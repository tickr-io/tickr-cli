# Compaction drain contract

## Stage and acknowledgement

Every selected relay validates and durably stages the unchanged published Compaction envelope before it returns `COMPACTION_ACK`. The acknowledgement means only that the raw envelope bytes are held under the stable Workflow-instance identity; relay handling performs no terminal archive write.

Tickr Lite stores the bytes and SHA-256 digest in local SQLite. Fresh all-NATS stores the raw bytes in its file-backed Compaction identity bucket before publishing the unchanged bytes to the versioned WorkQueue stream under the same stable NATS message identity. Both JetStream acknowledgements precede the cross-plane acknowledgement.

Same-identity/same-payload delivery is idempotent. Same-identity/different-payload delivery is a conflict and cannot replace the staged bytes. An ambiguous all-NATS publish or cross-plane acknowledgement is retried through the retained identity and payload; it never authorizes another logical Compaction. After raw staging bytes are purged, a retained completion digest prevents late redelivery from reopening the operation.

## Drain sequence

The drain decodes one staged envelope, verifies its raw bytes against the stable identity record, seals and verifies the identity-qualified tickr-ctx scope, and seals every Task-instance's generation-qualified Log staging stream. Each stream seal covers its committed frontier, every Accepted Log record including records beyond a replay hole, every declared gap, and its terminal state. One immutable Compaction digest then covers the Workflow-instance identity, scope digest, and ordered stream-seal identities before final-Log installation begins.

Tickr Lite commits the Workflow-instance, Task-instance rows, run-info enrichment containing the scope snapshot/digest and final-Log references, linked Trigger-derived Event-variable/Signal-capture terminal state, and the staging record's `complete` state in one SQLite writer transaction.

Fresh all-NATS retains the immutable Compaction seal and an installation identity containing the existing deterministic final-Log object paths, lengths, and SHA-256 digests. It re-reads every object and verifies that identity before committing the unchanged archive projection through the selected Postgres writer bundle. The successful archive transaction records scope-archive state and is followed by durable archive-commit evidence bound to the Compaction seal.

Only archive-commit evidence matching the verified Compaction seal makes cleanup eligible. Fresh all-NATS purges Accepted Log records and gaps while retaining one terminal fence per pickup generation, then cleans the sealed scope, records stable staging completion, removes raw Compaction bytes and queue evidence, and acknowledges the WorkQueue delivery. All-Redis records the verified Workflow/archive identity against the immutable scope digest, primary-local-fsync proves that record, and only then runs its role-local cleanup script; the script releases active scope and namespace quota while retaining the cleaned snapshot proof and owner binding for redelivery. Cleanup failure leaves Redis Compaction work pending. Tickr Lite applies the equivalent purge eligibility inside its completion transaction.

Both Tickr Lite `staged`/`complete` rows and fresh all-NATS pending WorkQueue deliveries are restart-selectable drain work. A retry before commit recomputes and verifies the same scope and Log seals. A retry after commit verifies the retained Compaction seal and final-Log installation identity, reconstructs a cleaned scope from committed exact archive bytes when necessary, and repeats cleanup without reopening accepted state.

Fresh all-NATS scope sealing orders full keys, length-prefixes each opaque envelope byte sequence, and records one SHA-256 digest while its metadata fence rejects later mutation. All-Redis seals the identical ordered encoding under protocol `tickr.scope-store.redis-opaque-snapshot/1`; the seal, verified archive record, and cleanup each cross primary-local `WAITAOF 1 0`, and ambiguous acknowledgement retries the same stable identity. Archive enrichment retains the existing parsed envelope response and exact accepted bytes. Tickr Lite seals the same snapshot encoding in SQLite. In all three formations absence, unreadable or conflicting state, corruption, digest disagreement, cleanup refusal, or interruption preserves retryable evidence and cannot substitute an empty scope or reopen it for writes. <!-- enforced-by: tickr-cli/tests/redis_scope_store_law_test.rs::real_redis_scope_laws_cover_bytes_atomicity_limits_restart_seal_corruption_and_cleanup -->

## Failure and redelivery

Missing, unreadable, corrupt, unknown-version, or digest-mismatched scope or Log state fails the drain; it is never replaced with an empty scope, empty Log, or synthetic terminal marker. Compaction opens only an existing generation-qualified Log journal, so a missing journal is not created as an empty substitute. A failed drain leaves the staged envelope durable for retry.

Redelivery before or after staging, seal, final-file installation, archive commit, completion, or purge converges on one archive identity, one scope digest, one final-Log reference set, and one purged staging result.
