---
title: Storage and durability
description: Understand what each formation persists and how to back it up safely.
sidebar_position: 3
---

# Storage and durability

Durability spans more than the SQL database. Backups and restores must account for every authoritative store owned by the selected formation.

## Tickr Lite data directory

One `DataDirectory` lease owns the local root for the lifetime of Tickr Lite. The layout includes:

- SQLite and its SQLite-managed sibling files;
- `formation-manifest.json`;
- `journals/`;
- `logs/staged/`;
- `logs/final/`;
- `tmp/`;
- `quarantine/`.

A second process cannot acquire the same directory. The lease remains held until critical children, Task process groups, SQLite connections, and durable flushes settle.

A fresh setup defaults the private profile to `profile/config.json` and the
durable root to `data/` inside the extracted Tickr Lite release directory.
Separate installations therefore do not silently share Tenant credentials or
one SQLite database. Explicit `TICKR_CONFIG_PATH` and `--data-dir` overrides
select managed locations, and an existing installation profile continues to
use its recorded directory.

### Backup

Stop Tickr Lite cleanly before taking a simple filesystem backup. If online backup is required, use a SQLite-aware method that includes committed WAL state and preserves the complete admitted directory. Copying only the main database while the writer is active is not valid.

Keep permissions and ownership intact during restore. Do not edit `formation-manifest.json` to force a mismatched binary or configuration to start.

## Distributed formations

Both distributed profiles use:

- Postgres for definitions, builds, terminal Run projections, Signals, Events, Patches, and related SQL state;
- S3-compatible storage for final Task logs;
- a formation-specific durable Coordination substrate.

`all-nats` additionally owns its admitted JetStream streams, consumers, and key-value buckets. `all-redis` owns its admitted role namespaces and append-only durability state.

A Postgres backup alone is not a complete formation backup.

## Restore invariants

A restored formation must preserve:

- tenant and namespace identity;
- selected profile and protocol identities;
- logical migration set;
- durable store consistency;
- final-Log objects;
- pending Coordination work needed for recovery.

Unknown, corrupt, or mismatched identity is a startup failure. Offline migration can install supported metadata after verification; ordinary startup does not repair an unverifiable state root.

## Shutdown

SIGINT, SIGTERM, startup failure after child construction, and critical-child failure enter the same bounded shutdown path. Readiness clears before runtime consumers are cancelled. Treat a non-zero shutdown caused by an unsettled critical child as an operational failure requiring investigation.
