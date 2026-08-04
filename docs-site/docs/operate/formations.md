---
title: Formation profiles
description: Compare the three complete admitted Data-plane topologies.
sidebar_position: 1
---

# Formation profiles

Tickr admits exactly three complete named profiles. A profile owns topology, storage, Executor shape, ingress capabilities, Coordination roles, and protocol identities as one unit.

| Profile | Topology | SQL | final Logs | Executors | HTTP Commands | External Event ingress |
| --- | --- | --- | --- | --- | --- | --- |
| `lite-local` | Single node | SQLite | Local files | Exactly one | Enabled | Disabled |
| `all-nats` | Distributed | Postgres | S3-compatible object store | Distributed fleet | Enabled | Enabled |
| `all-redis` | Distributed | Postgres | S3-compatible object store | Distributed fleet | Enabled | Enabled |

## `lite-local`

Run the complete local formation with:

```bash
./tickr tickr-lite
```

Characteristics:

- one supervised process owns API, Conductor roles, and one Executor;
- one admitted local data directory owns SQLite, journals, staged/final Logs, temporary files, and quarantine;
- no broker, Postgres, or object store is opened;
- one process at a time can own the data directory;
- work-producing routes stay closed until the complete local formation is ready.

## `all-nats`

Select explicitly:

```bash
tickr --formation all-nats conductor
tickr --formation all-nats api
tickr --formation all-nats executor
```

If a distributed formation flag is omitted, the CLI currently resolves to `all-nats`. Deployment manifests should still make the selected profile visible to operators.

`all-nats` requires Postgres, NATS/JetStream, S3-compatible object storage, and the complete hardened all-NATS namespace/protocol set.

## `all-redis`

Select explicitly:

```bash
tickr --formation all-redis conductor
tickr --formation all-redis api
tickr --formation all-redis executor
```

`all-redis` requires Postgres, S3-compatible object storage, and a complete admitted Redis 7.4.x single-primary topology with TLS, role credentials, durability, namespace, operation-manifest, and capacity proofs.

It is not a transparent NATS replacement. Every Coordination role uses its own versioned Redis protocol.

## Rejected combinations

Tickr does not admit:

- mixed NATS and Redis Coordination roles;
- automatic NATS/Redis fallback;
- dual reads or writes;
- Redis Cluster or Sentinel for `all-redis`;
- multiple Tickr Lite writers;
- distributed SQLite;
- external Event ingress in Tickr Lite.

Formation disagreement is a startup failure. Tickr does not start a partial set of consumers and hope the remaining capabilities appear later.
