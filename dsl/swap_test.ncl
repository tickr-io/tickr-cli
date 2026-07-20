# tickr/dsl/swap_test.ncl
#
# Positive-path tests for the `mkSwap` constructor: the emitted document is the
# `swap` arm of the Patch tagged union, carrying the target code and the
# replacement task body, authored against an identity code with no hand-mapped
# UUID. Run via `just dsl-test`.
let { mkTask, mkSwap, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let replacement =
  mkTask {
    name = "replacement",
    args = [ "run" ],
    nix_expression_path = "flake#replacement",
    outputs = [ "report" ],
  }
in
[
  # A swap names the target by code and emits `{ swap = { target, task } }`, the
  # replacement task body carried whole.
  check (
    mkSwap { target = "AB12", task = replacement }
    == { swap = { target = "AB12", task = replacement } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkSwap { target = "AB12", task = replacement, reason = "reconsidered AB12" }
    == {
      swap = { target = "AB12", task = replacement },
      reason = "reconsidered AB12",
    }
  ),
]
