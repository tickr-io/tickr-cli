---
title: tickr-ctx
description: Read and publish run-scoped values from a Task process.
sidebar_position: 2
---

# `tickr-ctx`

`tickr-ctx` is the Task-facing helper for the run context store. Tickr injects the process-private endpoint and scoped credential into launched Tasks; Task code does not receive a database, NATS, or Redis connection.

## Commands

```text
tickr-ctx capture   publish a value
tickr-ctx get       read a value
tickr-ctx ls        list keys
tickr-ctx tail      stream put/delete events
tickr-ctx rm        delete a key
tickr-ctx export    dump the run scope
```

Keys are Run-scoped. `--task` changes the producer identity stamped on an envelope; it does not create a Task-local key namespace.

## Publish an output

```bash
tickr-ctx capture result --json '{"rows": 42, "status": "ready"}'
```

By default, `capture` requires the key to be declared in the Task's `outputs`. Other accepted value inputs include `--int`, `--float`, `--bool`, `--file`, and `--stdin`.

Mark sensitive values explicitly:

```bash
tickr-ctx capture api_response --stdin --secret < response.json
```

Interactive reads of secret values require `--reveal`.

## Read an input

```bash
tickr-ctx get result --json
```

Wait for a value with a bounded duration:

```bash
tickr-ctx get result --wait 30s --json
```

Supply a benign default only when absence is part of the Task's contract:

```bash
tickr-ctx get optional-key --default disabled
```

## Trigger captures

For inputs declared from a trigger Signal, use the trigger namespace:

```bash
tickr-ctx get seed --signal --json
```

This requires a trigger-originated Run. Tickr rejects the operation when no trigger Signal identity was injected.

## Runtime environment

Tickr supplies namespace, Run, Task, endpoint, credential, input, and output context to the launched process. Avoid overriding `--ns`, `--run`, or `--task` in normal workflow code; those flags are primarily diagnostic and must remain within the credential's granted scope.

The command-level helper contract remains stable across formations, while each
formation owns its transport. Tickr Lite uses a process-private Unix-domain
endpoint backed by the local Conductor-owned scope writer. `all-redis` also
presents a process-private endpoint and does not expose Redis credentials or
key access to Tasks.
