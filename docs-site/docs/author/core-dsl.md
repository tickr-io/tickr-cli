---
title: Core DSL fundamentals
description: Construct workflows, Tasks, routing variables, Signals, and gates through lib.ncl.
sidebar_position: 1
---

# Core DSL fundamentals

The Core DSL uses closed-record contracts. Unknown fields, malformed identifiers, invalid timeout strings, and wrong reference types fail at the constructor call instead of becoming runtime surprises.

## Task

`mkTask` requires:

- `name`;
- `nix_expression_path`;
- `args`.

It defaults `outputs`, `inputs`, and `secrets` to empty arrays and `max_attempts` to `3`. Optional fields include `routing_vars`, `emits`, and `timeout`.

```nickel
let result = tickr.mkRoutingVar { name = "result", type = "string" } in
let produce = tickr.mkTask {
  name = "produce",
  nix_expression_path = "path:./tasks#produce",
  args = [],
  inputs = [],
  outputs = ["result"],
  routing_vars = [result],
  timeout = "5m",
} in
```

Pass constructor values as typed references. Do not replace a Task reference with its name string at an edge or gate call site.

## Task group

A Task group is a locally complete graph fragment with Tasks and edges:

```nickel
tickr.mkTaskGroup {
  name = "processing",
  args = [],
  outputs = [],
  tasks = [extract, transform, load],
  edges = [
    tickr.mkEdge { from = extract, to = transform },
    tickr.mkEdge { from = transform, to = load },
  ],
}
```

The `chain` combinator removes duplicate ordering declarations for an ungated sequence; see [Compose a graph](./composition.md).

## Workflow

`mkWorkflow` defines the authored root. Its fields include:

| Field | Meaning |
| --- | --- |
| `slug` | Stable URL-safe workflow name |
| `name` | Human-readable identifier |
| `args` | Workflow arguments |
| `tasks` | Task groups or accepted task entries |
| `outputs` | Declared workflow outputs |
| `triggerOn` | Optional trigger configuration |
| `captures` | Trigger capture declarations |
| `timeout` | Optional workflow duration |
| `tags` | Workflow tags |

Defaults are applied by the release-matched constructor. Prefer omitting optional values to inventing sentinel strings.

## Gates

Gates are attached to edges:

- `mkSignalGate` waits for a named Signal and optional predicate/captures.
- `mkTimerGate` waits for a duration.
- `mkPredicateGate` compares a routing variable against a scalar value.

A gated edge must explicitly use `kind = "data"` or `kind = "loop"`. A plain control edge has no gate and does not use those kinds.

## Validate before registration

From a source checkout:

```bash
nickel export workflow.ncl --format json -I dsl
```

From a release archive, point Nickel at the bundled `$TICKR_DSL_PATHS`. A successful export proves DSL evaluation; registration and build still apply Data-plane validation.
