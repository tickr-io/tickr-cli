---
title: Compose a graph
description: Build ordered, gated, branching, and looping workflow structure without duplicating intent.
sidebar_position: 2
---

# Compose a graph

A Task group owns a locally complete set of Tasks and edges. Use typed Task values as endpoints so constructor contracts can validate the graph before registration.

## Sequential control flow

For an ungated sequence, declare order once with the definition-time `chain` combinator:

```nickel
let spine = tickr.chain [extract, transform, load] in

tickr.mkTaskGroup {
  name = "pipeline",
  args = [],
  outputs = [],
  tasks = [spine],
}
```

`chain` returns a graph fragment. `mkTaskGroup` flattens that fragment into Tasks and control edges; the fragment tag never reaches the wire document.

Chains can nest:

```nickel
let prepare = tickr.chain [fetch, normalize] in
let pipeline = tickr.chain [prepare, enrich, publish] in
```

Each boundary connects the last Task of one fragment to the first Task of the next. Internal edges remain intact.

## Explicit edges

Use `mkEdge` when the relationship carries information that array order cannot express:

```nickel
tickr.mkEdge {
  from = produce,
  to = consume,
  kind = "data",
  gate = tickr.mkPredicateGate {
    routing_var = route,
    op = "Eq",
    value = "ready",
  },
}
```

Endpoints accept one Task reference or an array of Task references. Arrays express barrier-style source or target sets.

Rules enforced by the DSL:

- an ungated control edge can omit `kind`;
- a gated edge must explicitly use `data` or `loop`;
- `data` and `loop` require a gate;
- raw Task-name strings are not Task references.

## Forks and barriers at definition time

A fork is represented by edges from one source to multiple downstream Tasks. A join uses an edge with multiple sources so the successor waits on the declared barrier.

```nickel
let split_left = tickr.mkEdge { from = start, to = left } in
let split_right = tickr.mkEdge { from = start, to = right } in
let join = tickr.mkEdge { from = [left, right], to = summarize } in
```

## Loops

Use `mkLoop` for the Core DSL's loop document rather than synthesizing a cycle from ordinary control edges. The `tasks` array is an ordered ring: each Task hands control to the next, and the last hands control back to the head.

One Task is the loop's `producer`. It defaults to the head, but you can select a later Task in the ring. The producer alone owns the reserved `loop_control` routing variable. When it omits that value, the loop continues; `done` exits successfully, while `fail` terminates the loop unsuccessfully. Non-producer Tasks do not emit `loop_control` and park between turns.

```nickel
let inspect = tickr.mkTask {
  name = "inspect",
  nix_expression_path = "path:./tasks#inspect",
  args = [],
} in
let decide = tickr.mkTask {
  name = "decide",
  nix_expression_path = "path:./tasks#decide",
  args = [],
} in
let after_loop = tickr.mkTask {
  name = "after-loop",
  nix_expression_path = "path:./tasks#after-loop",
  args = [],
} in
let review_loop = tickr.mkLoop {
  name = "review-loop",
  tasks = [inspect, decide],
  producer = decide,
  exitTo = after_loop,
} in

tickr.mkTaskGroup {
  name = "review",
  args = [],
  outputs = [],
  tasks = [review_loop, after_loop],
}
```

Here `inspect` remains the head and runs first on every lap. `decide` is the sole Task that may return `loop_control`; choosing `done` sends control to `after-loop`.

## Keep gated seams explicit

`chain` derives ordinary control edges. When one seam needs a gate, split the sequence there and declare that edge explicitly. This keeps the exceptional transition visible and prevents a hand-written edge from overlapping an edge already contributed by a graph fragment.
