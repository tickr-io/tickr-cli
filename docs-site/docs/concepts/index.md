---
title: Concepts
description: The shared mental model behind Tickr authoring, operations, and integration.
sidebar_position: 2
---

# Concepts

Tickr separates authored intent, durable coordination, and Task execution. Learning those boundaries makes API responses, Console state, and failure recovery predictable.

## Start with these distinctions

- A **Workflow definition** is versioned authored source accepted by Tickr.
- A **build** validates and prepares a definition for execution.
- A **Run** is one materialized execution of a ready definition.
- A **Task** is an authored unit of work; a Task attempt is one execution generation.
- A **Signal** is a durable request that materializes into an outcome such as a Run or cancellation.
- An **Event** is an observed lifecycle fact.
- A **runtime Patch** changes the live graph of one Run through a validated graph operation.

## Read by question

| Question | Guide |
| --- | --- |
| Which part runs locally? | [Control plane and Data plane](./control-and-data-plane.md) |
| Why did an accepted request not finish yet? | [Execution lifecycle](./execution-lifecycle.md) |
| When do I use a Signal, Event, or Patch? | [Signals, Events, and runtime Patches](./signals-events-patches.md) |

The same terms appear in the Core DSL, HTTP API, Console, logs, and operator health projections.
