---
title: Core DSL reference
description: Public constructors and combinators exported by lib.ncl.
sidebar_position: 2
---

# Core DSL reference

Import only the release-matched public entrypoint:

```nickel
let tickr = import "lib.ncl" in
```

## Workflow and graph

| Export | Result |
| --- | --- |
| `mkWorkflow` | Workflow definition document |
| `mkTask` | Task value |
| `mkTaskGroup` | Task-group graph value |
| `mkEdge` | Edge between one or more typed Task endpoints |
| `chain` | Definition-time graph fragment with derived control edges |
| `mkLoop` | Loop document |

## Signals, triggers, routing, and gates

| Export | Result |
| --- | --- |
| `mkSignal` | Typed named Signal reference |
| `mkSignalEmit` | Success-side Signal emission from a routing variable |
| `mkSignalEmitOnFailure` | Failure-side Signal emission with Task lineage |
| `mkRoutingVar` | Typed routing-variable declaration |
| `mkSignalGate` | Gate satisfied by a matching Signal |
| `mkTimerGate` | Duration gate |
| `mkPredicateGate` | Routing-variable predicate gate |
| `mkTriggerOn` | Workflow trigger configuration |
| `mkTriggerCapture` | Trigger payload capture declaration |

Routing-variable types:

```text
string · int · bool · bytes · array
```

Predicate operators:

```text
Eq · NotEq · Lt · Le · Gt · Ge
```

## Runtime Patch constructors

| Export | Operation |
| --- | --- |
| `mkInsert` | Insert one Task |
| `mkChain` | Insert an ordered Task scope-tree |
| `mkFork` | Insert parallel arms and barrier join |
| `mkBranch` | Insert branch structure |
| `mkExpand` | Expand addressed structure |
| `mkSwap` | Replace addressed structure |
| `mkCut` | Cut addressed structure |
| `mkPrune` | Prune selected structure |
| `mkTrim` | Trim at a boundary |
| `mkTruncate` | Truncate beyond a boundary |

`chain` and `mkChain` are intentionally different:

- `chain` composes a Workflow definition and returns a graph fragment.
- `mkChain` creates a runtime Patch document against a live Run.

## Primary Task fields

| Field | Contract |
| --- | --- |
| `name` | Identifier |
| `nix_expression_path` | String |
| `args` | Array of strings |
| `outputs` | Array of strings; defaults empty |
| `inputs` | Bare names or structured input bindings; defaults empty |
| `secrets` | Array of strings; defaults empty |
| `routing_vars` | Optional array of `mkRoutingVar` values |
| `emits` | Optional array of Signal emit values |
| `max_attempts` | Integer `1..100`; defaults `3` |
| `timeout` | Optional duration |

The contracts are closed. Unknown fields fail validation rather than passing through silently.
