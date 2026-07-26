# Task log shipper

## Scope

The Task log shipper carries a Task instance's stdout telemetry into its selected Log staging stream. It is independent of task outcome reporting: a staging outage, bounded telemetry loss, or replay delay cannot block or revise the Task instance outcome.

## Accepted Log staging stream

Each pickup generation owns one Log staging stream identity. Every submitted payload carries a monotonic sequence identity, its SHA-256 content digest, and its bytes. Tickr Lite reports acceptance only after its framed journal record is appended and synced under the admitted data-directory contract. Fresh all-NATS reports acceptance only after the role's JetStream acknowledgement; its stable message identity is a duplicate-suppression aid, while replayed protocol identity and digest remain authoritative.

A caller that cannot distinguish completed acceptance from its own timeout reopens the stream and looks up the stable identity before retrying. The same identity and digest is already accepted; the same identity with different content is rejected. Logical replay exposes one Accepted Log record even if a substrate physically contains a duplicate delivery.

The committed frontier is the greatest contiguous sequence range beginning at zero. Replay exposes accepted records and declared gaps through that frontier in identity order. An accepted record beyond a hole remains durable but is not replayable until the missing range is accepted or explicitly covered by a durable pre-acceptance gap.

The stdout drain assigns sequence identity before copying a chunk into bounded memory and never waits for acceptance. An evicted chunk remains pre-acceptance telemetry loss. Before a later record can move the committed frontier across that sequence, the publisher durably declares the lost range as a pre-acceptance gap. A gap cannot overlap accepted data, so Accepted Log bytes are never represented as loss.

The Executor writes the sole controlled End-of-stream record only after stdout production ends and the bounded acceptance flush completes. A stream interrupted without a terminal receives a durable abnormal-closure record carrying its last committed frontier; recovery and Compaction must not manufacture a controlled end. Replay places either terminal record after committed payload and gap records.

## Ordering and lifecycle invariants

- Stdout and stderr drains run concurrently with child-process waiting and never acquire the acceptance path. A slow or unavailable staging backend cannot back-pressure the Task process or revise its outcome.
- The Log path may drop only telemetry that has not crossed the acceptance boundary. It must durably declare such loss before frontier progress hides it.
- A controlled End-of-stream record and abnormal closure are mutually exclusive durable terminal records.
- Reconnect and process restart reconstruct accepted identities, declared gaps, the committed frontier, and terminal state from durable replay.
- Reopening a local journal after a partial trailing write truncates only that incomplete, unaccepted tail. A complete valid record is treated as accepted so an ambiguous caller can resolve by identity lookup.

## Seal and final installation

Compaction seals only a stream with one durable terminal record. The immutable stream digest covers the stream identity, committed frontier, every Accepted Log record identity, content digest, and byte sequence including records beyond a replay hole, every declared pre-acceptance gap, and the terminal state. Repeating a seal over unchanged state returns the same identity; a mismatching recovered seal is corruption. The Workflow-instance Compaction seal combines every stream digest with the sealed tickr-ctx scope digest before final-Log installation begins.

Tickr Lite records the stream seal in its staging journal before installing the protocol-identified record document and separate terminal metadata beneath the locked root. Fresh all-NATS records the Compaction seal in its durable staging identity store, writes the existing deterministic S3-compatible objects, and retains each object's path, length, and SHA-256 digest as installation identity without changing the existing final-Log reference projection.

Every installed file or object is re-read and checked against its retained length and SHA-256 digest before the archive transaction may commit. An interrupted retry verifies the same identity or rewrites the same deterministic object bytes; any digest or identity disagreement fails closed. Purge is forbidden until verified installation and archive-commit evidence exist. Fresh all-NATS removes Accepted Log records and gaps but retains one terminal fence per sealed pickup generation so a late writer remains rejected after purge.
