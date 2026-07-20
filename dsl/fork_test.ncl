# tickr/dsl/fork_test.ncl
#
# Positive-path tests for the `mkFork` constructor: the emitted document is the
# `fork` arm of the Patch tagged union, carrying the anchor code and a list of
# arms (each a chain-step scope-tree — leaf `task`s and nested `steps`
# sub-chains), authored against identity codes with no hand-mapped UUIDs. Run via
# `just dsl-test`.
let { mkTask, mkFork, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let arm_task = fun n =>
  mkTask {
    name = n,
    args = [ "run" ],
    nix_expression_path = "flake#%{n}",
    outputs = [ "report" ],
  }
in
[
  # A two-arm fork of single-task arms: `{ fork = { anchor, arms = [ { handle,
  # task }, { handle, task } ] } }`, authored against the anchor code alone.
  check (
    let l = arm_task "l" in
    let r = arm_task "r" in
    mkFork { anchor = "AB12", arms = [ { handle = "l", task = l }, { handle = "r", task = r } ] }
    == { fork = { anchor = "AB12", arms = [ { handle = "l", task = l }, { handle = "r", task = r } ] } }
  ),

  # An arm may itself be a chain: a nested sub-chain forms a multi-task arm. The
  # same handle string may recur across sibling arms.
  check (
    let a = arm_task "a" in
    let b1 = arm_task "b1" in
    let b2 = arm_task "b2" in
    (mkFork {
      anchor = "AB12",
      arms = [
        { handle = "a", task = a },
        { handle = "b", steps = [ { handle = "s", task = b1 }, { handle = "t", task = b2 } ] },
      ],
    }).fork.arms
    == [
      { handle = "a", task = a },
      { handle = "b", steps = [ { handle = "s", task = b1 }, { handle = "t", task = b2 } ] },
    ]
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    let l = arm_task "l" in
    mkFork { anchor = "AB12", arms = [ { handle = "l", task = l } ], reason = "fan out after AB12" }
    == {
      fork = { anchor = "AB12", arms = [ { handle = "l", task = l } ] },
      reason = "fan out after AB12",
    }
  ),
]
