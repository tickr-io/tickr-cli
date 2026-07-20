# tickr/dsl/chain_test.ncl
#
# Positive-path tests for the `mkChain` constructor: the emitted document is the
# `chain` arm of the Patch tagged union, carrying the anchor code and a
# scope-tree of steps (leaf `task`s and nested `steps` sub-chains), authored
# against identity codes with no hand-mapped UUIDs. Run via `just dsl-test`.
let { mkTask, mkChain, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let step_task = fun n =>
  mkTask {
    name = n,
    args = [ "run" ],
    nix_expression_path = "flake#%{n}",
    outputs = [ "report" ],
  }
in
[
  # A single-step chain is the `insert`-equivalent: `{ chain = { anchor, steps
  # = [ { handle, task } ] } }`, authored against the anchor code alone.
  check (
    let a = step_task "a" in
    mkChain { anchor = "AB12", steps = [ { handle = "a", task = a } ] }
    == { chain = { anchor = "AB12", steps = [ { handle = "a", task = a } ] } }
  ),

  # A chain of two leaf tasks threads in declaration order; handles are
  # author-chosen strings, and no UUID is written by hand.
  check (
    let a = step_task "a" in
    let b = step_task "b" in
    (mkChain {
      anchor = "AB12",
      steps = [ { handle = "a", task = a }, { handle = "b", task = b } ],
    }).chain.steps
    == [ { handle = "a", task = a }, { handle = "b", task = b } ]
  ),

  # A nested sub-chain forms a child scope: a step carries `steps` instead of a
  # `task`. The same handle string may recur across sibling scopes.
  check (
    let a = step_task "a" in
    let inner = step_task "a" in
    (mkChain {
      anchor = "AB12",
      steps = [
        { handle = "a", task = a },
        { handle = "grp", steps = [ { handle = "a", task = inner } ] },
      ],
    }).chain.steps
    == [
      { handle = "a", task = a },
      { handle = "grp", steps = [ { handle = "a", task = inner } ] },
    ]
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    let a = step_task "a" in
    mkChain { anchor = "AB12", steps = [ { handle = "a", task = a } ], reason = "extend after AB12" }
    == {
      chain = { anchor = "AB12", steps = [ { handle = "a", task = a } ] },
      reason = "extend after AB12",
    }
  ),

  # An optional `relay = { edge, var }` rides inside the `chain` document: it
  # authorises the variable-absent arm when the anchor sources a gated Data edge.
  check (
    let a = step_task "a" in
    mkChain {
      anchor = "AB12",
      steps = [ { handle = "a", task = a } ],
      relay = { edge = "CD34", var = "verdict" },
    }
    == {
      chain = {
        anchor = "AB12",
        steps = [ { handle = "a", task = a } ],
        relay = { edge = "CD34", var = "verdict" },
      },
    }
  ),
]
