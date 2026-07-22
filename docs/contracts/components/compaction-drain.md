# Compaction drain contract

## Stage and acknowledgement

The Tickr Lite relay validates and durably stages the unchanged published Compaction envelope before it returns `COMPACTION_ACK`. The acknowledgement means only that the envelope bytes, archive identity, and digest are durable in local SQLite; relay handling performs no terminal archive write.

The staging identity is the Workflow-instance UUID and payload digest. Duplicate bytes are one staged record. A delivery with the same archive identity and different bytes is a conflict. After staging bytes are purged, the retained tombstone keeps that identity and digest so late redelivery cannot create another archive.

## Drain sequence

The drain decodes one staged envelope, resolves the identity-qualified tickr-ctx scope, snapshots and verifies it, seals every Task-instance's generation-qualified Log staging stream, and installs digest-verified final Log files. It then commits the Workflow-instance, Task-instance rows, run-info enrichment containing the scope snapshot/digest and final-Log references, linked Trigger-derived Event-variable/Signal-capture terminal state, and the staging record's `complete` state in one SQLite writer transaction.

Only after that archive transaction commits may the drain clean the snapshotted scope, purge verified Log staging journals, and clear staged envelope bytes. Every cleanup is idempotent. A completed record is never purged before its durable archive references exist.

Both `staged` and `complete` records are restart-selectable drain work. A `complete` record resumes by verifying the retained envelope digest, identity-qualified scope snapshot and digest, and every installed final-Log reference before repeating idempotent cleanup; it never repeats the archive transaction.

## Failure and redelivery

Missing, unreadable, corrupt, unknown-version, or digest-mismatched scope or Log state fails the drain; it is never replaced with an empty scope, empty Log, or synthetic terminal marker. Compaction opens only an existing generation-qualified Log journal, so a missing journal is not created as an empty substitute. A failed drain leaves the staged envelope durable for retry.

Redelivery before or after staging, seal, final-file installation, archive commit, completion, or purge converges on one archive identity, one scope digest, one final-Log reference set, and one purged staging result.
