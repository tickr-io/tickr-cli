---
title: CLI reference
description: Commands and formation selection exposed by the tickr executable.
sidebar_position: 1
---

# CLI reference

```text
Usage: tickr [OPTIONS] <COMMAND>
```

## Commands

| Command | Purpose |
| --- | --- |
| `tickr conductor` | Run the distributed Conductor component |
| `tickr api` | Run the distributed API component |
| `tickr executor` | Run a distributed Executor component |
| `tickr tickr-lite` | Run the admitted single-process Tickr Lite formation |
| `tickr migrate` | Apply and verify selected Data-plane SQL migrations |
| `tickr help` | Print command help |

## Distributed formation option

```text
--formation <DISTRIBUTED_FORMATION>
```

Possible values:

- `all-nats` — default when the option is omitted;
- `all-redis` — explicit all-Redis formation.

The option precedes the distributed component command:

```bash
tickr --formation all-nats conductor
tickr --formation all-redis api
```

Tickr Lite is a dedicated command, not a value of the distributed `--formation` option:

```bash
tickr tickr-lite
```

## Migrations

```text
Usage: tickr migrate [OPTIONS]

--formation <FORMATION>
  possible values: distributed, tickr-lite
  default: distributed
```

Examples:

```bash
tickr migrate
tickr migrate --formation tickr-lite
```

Use the same profile, SQL configuration, release binary, and durable state identity for migration and runtime startup.

## Common options

| Option | Meaning |
| --- | --- |
| `-h`, `--help` | Print help |
| `-V`, `--version` | Print the executable version |
