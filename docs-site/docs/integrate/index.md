---
title: Integrate with Tickr
description: Drive the Data plane through HTTP, Task context, OpenAPI, and protobuf contracts.
sidebar_position: 5
---

# Integrate with Tickr

The Data-plane API is the public operational boundary used by Console and external clients. It exposes health, workflow definitions, Runs, Tasks, logs, Signals, Events, Patches, and tenant projections.

## Choose the right contract

| Need | Contract |
| --- | --- |
| Register, trigger, inspect, or operate workflows | HTTP API |
| Read and publish values inside a Task | `tickr-ctx` |
| Generate an HTTP client | OpenAPI |
| Implement a compatible cross-process integration | Public protobuf contracts |

Start with the lifecycle guide before generating a client. A generated method signature cannot explain when an accepted Command becomes a materialized Signal, Run, or applied Patch.

- [HTTP lifecycle and conventions](./http-lifecycle.md)
- [`tickr-ctx`](./tickr-ctx.md)
- [Rendered HTTP API reference](/docs/api/tickr-api)
- [OpenAPI source](https://github.com/tickr-io/tickr-cli/blob/main/console/openapi.yaml)
- [Public protobuf source](https://github.com/tickr-io/tickr-cli/tree/main/proto)

## Formation applicability

The HTTP Command contract is shared across `lite-local`, `all-nats`, and `all-redis`. Transport and storage differ behind that boundary. Formation selection must not change a successful HTTP outcome into a different semantic result.

External Event ingress is disabled in `lite-local`; use a distributed formation when that capability is required.
