# Task log shipper

## Scope

The Task log shipper carries a Task instance's stdout telemetry into its selected Log staging stream. It is independent of task outcome reporting: a staging outage, bounded telemetry loss, or replay delay cannot block or revise the Task instance outcome.

## Local Log staging stream

A Tickr Lite stream is identified by the Task-instance identity and pickup generation. Each accepted payload also carries a monotonic record sequence. Acceptance is reported only after the local journal has appended and synced the framed identity and payload under the admitted data-directory contract.

A caller that cannot distinguish a completed acceptance from its own timeout looks up the stable identity before retrying. The same identity with identical bytes is already accepted; the same identity with different bytes is rejected. A retry never appends a second accepted record.

The committed frontier is the greatest contiguous sequence range beginning at zero. Replay exposes only records at or below that frontier, in sequence order. An accepted record beyond a hole remains durable but is not replayable until the missing range is accepted or explicitly covered by a durable pre-acceptance gap.

Telemetry discarded before acceptance is non-blocking loss. The shipper writes a durable pre-acceptance gap before any later record can move the frontier across that sequence range. A gap cannot overlap accepted data, so accepted bytes are never represented as loss.

The Executor writes the sole clean End-of-stream marker after stdout production ends and all known accepted identities are contiguous. On recovery, a stream without that marker receives a durable abnormal-closure record; recovery must not manufacture an End-of-stream marker. Replay places either terminal record after the committed payload and gap records.

## Ordering and lifecycle invariants

- The stdout drain runs concurrently with child-process waiting. Task completion does not wait for local publication or replay.
- The log path may drop only telemetry that has not crossed the acceptance boundary. It must record such loss before frontier progress hides it.
- A clean End-of-stream marker and abnormal closure are mutually exclusive terminal records.
- Reopening a journal after a partial trailing write truncates only that incomplete, unaccepted tail. A complete valid record is treated as accepted so an ambiguous caller can resolve by identity lookup.

## Seal and final installation

Compaction seals only a stream with one durable terminal record. The seal freezes every accepted record identity and bytes, including accepted records beyond a replay frontier, and stores their SHA-256 digest in the staging journal. Repeating a seal for the same stream returns that record set and digest; a mismatching recovered seal is corruption.

Final installation writes a protocol-identified record document and separate terminal metadata beneath the locked root. Each file is first created below `tmp/`, synced, atomically replaced into `logs/final/`, and followed by a destination-parent sync. The resulting reference carries only stream identity and verified record, final-log, and exit-metadata digests; it does not expose a backend location.

An interrupted retry either completes an already-valid temporary file or verifies and re-syncs an already-installed destination. A partial or mismatching temporary file is moved into quarantine and returns an explicit failure for retry. Unknown protocol identity, unreadable files, and any digest or identity disagreement fail closed; no empty final Log is substituted.
