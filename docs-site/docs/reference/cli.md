---
title: CLI reference
description: Commands exposed by the tickr-cli and tickr-lite executables.
sidebar_position: 1
---

# CLI reference

```text
Usage: tickr-cli [OPTIONS] <COMMAND>
```

## Commands

| Command | Purpose |
| --- | --- |
| `tickr-cli setup` | Configure a private Tickr Lite installation |
| `tickr-cli examples run <EXAMPLES>...` | Run packaged examples and enter the interactive example session |
| `tickr-cli tenant` | Administer Tenants through the loopback Control-plane API |
| `tickr-cli conductor` | Run the distributed Conductor component |
| `tickr-cli api` | Run the distributed API component |
| `tickr-cli executor` | Run a distributed Executor component |
| `tickr-cli migrate` | Apply and verify selected Data-plane SQL migrations |
| `tickr-lite` | Run the admitted single-process Tickr Lite formation |
| `tickr-cli help` | Print command help |

## Distributed formation option

```text
--formation <DISTRIBUTED_FORMATION>
```

Possible values:

- `all-nats` — default when the option is omitted;
- `all-redis` — explicit all-Redis formation.

The option precedes the distributed component command:

```bash
tickr-cli --formation all-nats conductor
tickr-cli --formation all-redis api
```

Tickr Lite is a standalone executable, not a distributed formation option:

```bash
tickr-lite
```

## Migrations

```text
Usage: tickr-cli migrate [OPTIONS]

--formation <FORMATION>
  possible values: distributed, tickr-lite
  default: distributed
```

Examples:

```bash
tickr-cli migrate
tickr-cli migrate --formation tickr-lite
```

Use the matched release executables, profile, SQL configuration, and durable state identity for migration and runtime startup.

## Common options

| Option | Meaning |
| --- | --- |
| `-h`, `--help` | Print help |
| `-V`, `--version` | Print the executable version |
