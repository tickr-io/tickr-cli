# tickr/dsl/prune_test.ncl
#
# Positive-path tests for the `mkPrune` constructor: the emitted document is the
# `prune` arm of the Patch tagged union, naming the fan-out arm by the identity
# code of its head with no hand-mapped UUID. `prune` mints no tasks — it carries
# only the arm the server drops as it narrows the join at apply. Run via
# `just dsl-test`.
let { mkPrune, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
[
  # A prune names the arm head by code and emits `{ prune = { arm } }` — no task
  # body, no UUID, just the arm the server drops.
  check (
    mkPrune { arm = "AB12" }
    == { prune = { arm = "AB12" } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkPrune { arm = "AB12", reason = "branch no longer relevant" }
    == {
      prune = { arm = "AB12" },
      reason = "branch no longer relevant",
    }
  ),
]
