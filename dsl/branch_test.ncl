# tickr/dsl/branch_test.ncl
#
# Positive-path tests for the `mkBranch` constructor: the emitted document is the
# `branch` arm of the Patch tagged union, carrying the anchor code and a list of
# **gated arms** (each a chain-step scope-tree plus a selecting `gate`), authored
# against identity codes with no hand-mapped UUIDs. Run via `just dsl-test`.
let { mkTask, mkBranch, mkSignalGate, mkPredicateGate, mkSignal, mkRoutingVar, .. } =
  import "lib.ncl"
in
let check = fun b => (b | std.test.Assert) in
let arm_task = fun n =>
  mkTask {
    name = n,
    args = [ "run" ],
    nix_expression_path = "flake#%{n}",
    outputs = [ "report" ],
  }
in
let go = fun n => mkSignalGate { signal = mkSignal { name = n } } in
[
  # A two-arm branch of single-task arms, each gated by its own signal:
  # `{ branch = { anchor, arms = [ { handle, gate, task }, … ] } }`, authored
  # against the anchor code alone.
  check (
    let l = arm_task "l" in
    let r = arm_task "r" in
    mkBranch {
      anchor = "AB12",
      arms = [
        { handle = "l", gate = go "go_l", task = l },
        { handle = "r", gate = go "go_r", task = r },
      ],
    }
    == {
      branch = {
        anchor = "AB12",
        arms = [
          { handle = "l", gate = go "go_l", task = l },
          { handle = "r", gate = go "go_r", task = r },
        ],
      },
    }
  ),

  # An arm may itself be a chain: a nested sub-chain forms a multi-task arm. The
  # same handle string may recur across sibling arms. A predicate gate selects.
  check (
    let a = arm_task "a" in
    let b1 = arm_task "b1" in
    let b2 = arm_task "b2" in
    let pg = mkPredicateGate { routing_var = mkRoutingVar { name = "verdict", type = "string" }, op = "Eq", value = "go" } in
    (mkBranch {
      anchor = "AB12",
      arms = [
        { handle = "a", gate = go "go_a", task = a },
        { handle = "b", gate = pg, steps = [ { handle = "s", task = b1 }, { handle = "t", task = b2 } ] },
      ],
    }).branch.arms
    == [
      { handle = "a", gate = go "go_a", task = a },
      { handle = "b", gate = pg, steps = [ { handle = "s", task = b1 }, { handle = "t", task = b2 } ] },
    ]
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    let l = arm_task "l" in
    mkBranch {
      anchor = "AB12",
      arms = [ { handle = "l", gate = go "go_l", task = l } ],
      reason = "gated fan-out after AB12",
    }
    == {
      branch = { anchor = "AB12", arms = [ { handle = "l", gate = go "go_l", task = l } ] },
      reason = "gated fan-out after AB12",
    }
  ),
]
