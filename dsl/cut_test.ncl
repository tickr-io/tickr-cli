# tickr/dsl/cut_test.ncl
#
# Positive-path tests for the `mkCut` constructor: the emitted document is the
# `cut` arm of the Patch tagged union, naming the mid-path target by identity
# code with no hand-mapped UUID. `cut` mints no tasks — it carries only the
# target the server excises and bridges at apply. Run via `just dsl-test`.
let { mkCut, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
[
  # A cut names the target by code and emits `{ cut = { target } }` — no task
  # body, no UUID, just the region the server excises.
  check (
    mkCut { target = "AB12" }
    == { cut = { target = "AB12" } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkCut { target = "AB12", reason = "no longer needed" }
    == {
      cut = { target = "AB12" },
      reason = "no longer needed",
    }
  ),
]
