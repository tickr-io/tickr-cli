---
title: Runtime Patches
description: Change one live Run through validated graph operations.
sidebar_position: 3
---

# Runtime Patches

A runtime Patch changes the materialized graph of one Run. Patch constructors produce request documents; they do not change the authored Workflow definition or later Runs of that definition.

:::warning Wait for application
Patch submission and Patch application are distinct stages. A Task that depends on the changed graph must wait until the Patch reaches `Applied`.
:::

## Address live structure

Runtime operations address graph structure by the short identity code exposed with the live graph, or by its full UUID. Do not copy an identity from another Run: runtime structure belongs to one materialized graph.

## Insert a chain

`mkChain` interposes one or more Tasks after an anchor:

```nickel
tickr.mkChain {
  anchor = "AB12",
  steps = [
    { handle = "prepare", task = prepare_task },
    { handle = "publish", task = publish_task },
  ],
  reason = "expand the publishing path",
}
```

Handles are scope-local author names. Tickr mints runtime node identities for the leaf Tasks and reconnects the anchor's successors.

## Fork and rejoin

`mkFork` grows parallel arms and re-seats the anchor's successors behind a barrier over every arm tail:

```nickel
tickr.mkFork {
  anchor = "AB12",
  arms = [
    { handle = "left", steps = left_steps },
    { handle = "right", steps = right_steps },
  ],
  reason = "process both derived partitions",
}
```

The successor grounds only after all declared arms ground.

## Public Patch constructors

| Constructor | Graph operation |
| --- | --- |
| `mkInsert` | Insert one Task after an anchor |
| `mkChain` | Insert an ordered scope-tree of Tasks |
| `mkFork` | Insert parallel arms with a barrier join |
| `mkBranch` | Insert conditional branch structure |
| `mkExpand` | Expand selected live structure |
| `mkSwap` | Replace live structure |
| `mkCut` | Remove an addressed section and reconnect |
| `mkPrune` | Remove unreachable or selected structure |
| `mkTrim` | Remove structure at a boundary |
| `mkTruncate` | Remove structure beyond a boundary |

The exact closed-record fields are release-specific. Use the [Core DSL reference](../reference/core-dsl.md) for the selected documentation version and validate Patch source with the bundled DSL before submitting it.

## Deterministic example

The release archive includes `examples/runtime-patch.ncl`. Given the same integer seed, it derives the same two arm lengths, applies one `mkFork` Patch, waits for `Applied`, and then runs both arms before the final summary Task.
