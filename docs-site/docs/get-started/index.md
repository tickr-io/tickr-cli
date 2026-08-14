---
title: Get started with Tickr Lite
description: Connect an invited local data plane and take a workflow from source to a completed Run.
sidebar_position: 1
---

# Get started with Tickr Lite

Tickr Lite runs one complete Tickr Data plane on your machine: API, Conductor, one Executor, the Console, SQLite state, and local Task logs. It connects to an existing compatible Control plane supplied by your Tickr operator.

:::info Access model
Tickr Lite is local-first, not Control-plane-free. Obtain the tenant and Control-plane connection values described in [Obtain access](./access.md) before expecting a workflow Run to execute.
:::

## What you will do

1. Verify and extract a release archive.
2. install the release-matched Nickel evaluator.
3. Configure a private local state directory and Control-plane connection.
4. Migrate and start Tickr Lite.
5. Register the bundled Hello workflow.
6. Trigger it and inspect the resulting Run.

At the end, the `hello` Task is `Completed` and its terminal log contains:

```text
hello from Tickr
```

## What Tickr Lite owns

| Capability | Tickr Lite |
| --- | --- |
| Runtime processes | One supervised `tickr` process |
| SQL state | SQLite |
| final-Log storage | Local files |
| Executors | Exactly one |
| Console | Embedded at the local API address |
| HTTP Commands | Enabled |
| External Event ingress | Disabled |
| Control plane | External; operator supplied |

Tickr Lite does not start Postgres, NATS, Redis, an object store, or a Control plane.

## Continue

- [Obtain access from your operator](./access.md)
- [Install Tickr Lite](./install-lite.md)
- [Run the Hello workflow](./first-run.md)
