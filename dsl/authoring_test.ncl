# tickr/dsl/authoring_test.ncl
#
# Positive-path tests for the bare-verb `chain` combinator and its ingestion by
# `mkTaskGroup`. `chain` returns a graph fragment (`{ kind = "graph-fragment",
# tasks, edges }`) carrying the tasks plus the ungated control edges threading
# them in array order; `mkTaskGroup` flattens that fragment into loose tasks +
# edges before validating, so a `chain`-authored group equals its hand-wired
# twin and the fragment tag never survives. Run via `just dsl-test`.
let { mkTask, mkTaskGroup, mkEdge, chain, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let task = fun n =>
  mkTask {
    name = n,
    args = [],
    nix_expression_path = "x",
    outputs = [],
  }
in
[
  # A flat `chain` returns a graph fragment carrying the tasks plus the ungated
  # control edges `a → b → c`, order derived from array position.
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    chain [ a, b, c ]
    == {
      kind = "graph-fragment",
      tasks = [ a, b, c ],
      edges = [
        mkEdge { from = a, to = b },
        mkEdge { from = b, to = c },
      ],
    }
  ),

  # A single-element chain has no edges — one task, nothing to thread.
  check (
    let a = task "a" in
    chain [ a ] == { kind = "graph-fragment", tasks = [ a ], edges = [] }
  ),

  # `chain` is closed under nesting: an element may itself be a `chain`
  # fragment. `chain [ chain [ a, b ], c ]` concatenates the inner chain's
  # tasks with the bare task and links the one boundary (last-of-inner `b` →
  # first-of-next `c`). The inner edge `a → b` survives unchanged.
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    chain [ chain [ a, b ], c ]
    == {
      kind = "graph-fragment",
      tasks = [ a, b, c ],
      edges = [
        mkEdge { from = a, to = b },
        mkEdge { from = b, to = c },
      ],
    }
  ),

  # Two nested chains side by side: the task lists concatenate, both inner
  # edges (`a → b`, `c → d`) survive, and exactly one boundary edge links the
  # seam (last-of-first `b` → first-of-second `c`) — no boundary doubled, none
  # missed.
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    let d = task "d" in
    chain [ chain [ a, b ], chain [ c, d ] ]
    == {
      kind = "graph-fragment",
      tasks = [ a, b, c, d ],
      edges = [
        mkEdge { from = a, to = b },
        mkEdge { from = c, to = d },
        mkEdge { from = b, to = c },
      ],
    }
  ),

  # A nested chain at the head flattens the same as its fully-flat twin:
  # building a spine out of a leading sub-sequence yields the identical graph
  # fragment as writing every task loose in one `chain` (`a → b → c → d`).
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    let d = task "d" in
    chain [ chain [ a, b ], c, d ] == chain [ a, b, c, d ]
  ),

  # `mkTaskGroup` flattens a `chain` fragment into loose tasks + edges: the
  # group is byte-identical to the hand-wired twin, and no `graph-fragment` tag
  # survives.
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    mkTaskGroup { name = "g", args = [], outputs = [], tasks = [ chain [ a, b, c ] ] }
    == mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ a, b, c ],
      edges = [ mkEdge { from = a, to = b }, mkEdge { from = b, to = c } ],
    }
  ),

  # A `chain` fragment merges with hand-wired edges already on the group: the
  # derived control edges are appended to the group's own edge list.
  check (
    let a = task "a" in
    let b = task "b" in
    let c = task "c" in
    (mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ chain [ a, b ], c ],
      edges = [ mkEdge { from = b, to = c } ],
    }).edges
    == [ mkEdge { from = b, to = c }, mkEdge { from = a, to = b } ]
  ),

  # A fragment-free group is untouched — flattening is a no-op, so the exported
  # shape is exactly what a hand-wirer passed.
  check (
    let a = task "a" in
    let b = task "b" in
    mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ a, b ],
      edges = [ mkEdge { from = a, to = b } ],
    }
    == {
      name = "g",
      args = [],
      tasks = [ a, b ],
      outputs = [],
      edges = [ mkEdge { from = a, to = b } ],
    }
  ),

  # The overlap + name-uniqueness detections are **fragment-gated**: they run only
  # when a `chain` fragment is ingested. A fragment-free group carrying the very
  # shape the detection would reject if a fragment produced it — two parallel
  # edges over the same pair — still validates, so adding the grammar regresses no
  # existing hand-wired file. (The hand-wired parallel-edge footgun stays a
  # pre-existing, separately-tracked surface.)
  check (
    let a = task "a" in
    let b = task "b" in
    (mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ a, b ],
      edges = [ mkEdge { from = a, to = b }, mkEdge { from = a, to = b } ],
    }).edges
    == [ mkEdge { from = a, to = b }, mkEdge { from = a, to = b } ]
  ),
]
