---
title: Author workflows
description: Define, validate, register, and evolve Tickr workflow graphs with the Core DSL.
sidebar_position: 3
---

# Author workflows

Tickr workflows are Nickel programs that evaluate to the closed wire shape accepted by the Data plane. Authors import one public entrypoint, `lib.ncl`, and compose typed constructor values.

```nickel
let tickr = import "lib.ncl" in
let hello = tickr.mkTask {
  name = "hello",
  args = ["hello from Tickr"],
  nix_expression_path = "path:./examples#hello",
  outputs = [],
} in
tickr.mkWorkflow {
  slug = "hello-world",
  name = "hello-world",
  args = [],
  outputs = [],
  tasks = [
    tickr.mkTaskGroup {
      name = "hello",
      args = [],
      outputs = [],
      tasks = [hello],
    },
  ],
}
```

## Authoring loop

1. Import the release-matched Core DSL.
2. Construct Tasks and Task groups.
3. Compose control flow and gates.
4. Export with the same Nickel version used by Tickr.
5. Register the exact source through the API.
6. Resolve build diagnostics before triggering.

Validate a source checkout example with:

```bash
just dsl-check examples/hello-world.ncl
```

A released Tickr Lite bundle carries its matching `dsl/` directory. Keep the binary, DSL, Nickel evaluator, and docs on the same release line.

## Public boundary

Import only `lib.ncl`. Files such as `task.ncl`, `edge.ncl`, and `contracts.ncl` are internal modules. Their organization can change without creating a second public import surface.

Continue with:

- [Core DSL fundamentals](./core-dsl.md)
- [Compose a graph](./composition.md)
- [Apply runtime Patches](./runtime-patches.md)
