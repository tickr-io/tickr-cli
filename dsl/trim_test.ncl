# tickr/dsl/trim_test.ncl
#
# Positive-path tests for the `mkTrim` constructor: the emitted document is the
# `trim` arm of the Patch tagged union, naming the `trim-until` anchor by identity
# code with no hand-mapped UUID. `trim` mints no tasks — it carries only the anchor
# the server excises up to as it removes the leading run and reconnects `Start` at
# apply. Run via `just dsl-test`.
let { mkTrim, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
[
  # A trim names the anchor by code and emits `{ trim = { anchor } }` — no task
  # body, no UUID, just the bound the server excises up to.
  check (
    mkTrim { anchor = "AB12" }
    == { trim = { anchor = "AB12" } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkTrim { anchor = "AB12", reason = "drop the leading section" }
    == {
      trim = { anchor = "AB12" },
      reason = "drop the leading section",
    }
  ),
]
