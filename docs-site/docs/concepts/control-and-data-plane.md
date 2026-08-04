---
title: Control plane and Data plane
description: Understand which responsibilities belong to the operator and to your Tickr formation.
sidebar_position: 1
---

# Control plane and Data plane

Tickr divides tenant-wide coordination from the runtime that authors and executes workflows.

## Control plane

The external Control plane provides the Coordinator endpoints used for live queries and the bidirectional Conductor relay. Your operator provisions access and gives the Data plane its tenant slug and Coordinator URLs.

Tickr Lite does not embed, create, or administer a Control plane.

## Data plane

The Data plane owns tenant workflow operations:

- workflow source registration and build state;
- durable workflow definitions and terminal Run projections;
- live task coordination;
- Task execution and cancellation;
- Task context;
- Events, Signals, Patches, and replays;
- Task logs;
- the HTTP API and Console.

A **formation** is one complete admitted Data-plane profile. Tickr supports `lite-local`, `all-nats`, and `all-redis`. Profiles select a complete topology and role-specific contracts; they are not interchangeable broker settings.

```mermaid
flowchart LR
  O[Tickr operator] -->|tenant + Coordinator values| D
  C[Control plane] <-->|HTTP queries + relay| D[Data-plane formation]
  U[Workflow author] -->|Nickel + HTTP Commands| D
  D --> E[Executor]
  E --> T[Task processes]
  U --> X[Console]
  X --> D
```

## Failure boundary

Formation readiness and Control-plane health are related but distinct. After admission, loss of Control-plane reachability degrades its health component without reinterpreting or discarding local durable state.

Work-producing routes remain governed by formation readiness. `/api/health` stays available for diagnosis when readiness is false.
