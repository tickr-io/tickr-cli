# tickr/dsl/truncate_test.ncl
#
# Positive-path tests for the `mkTruncate` constructor: the emitted document is the
# `truncate` arm of the Patch tagged union, naming the anchor by identity code with
# no hand-mapped UUID. `truncate` mints no tasks — it carries only the anchor the
# server excises after as it removes the tail and reconnects `End` at apply. Run
# via `just dsl-test`.
let { mkTruncate, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
[
  # A truncate names the anchor by code and emits `{ truncate = { anchor } }` — no
  # task body, no UUID, just the anchor the server excises after.
  check (
    mkTruncate { anchor = "AB12" }
    == { truncate = { anchor = "AB12" } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkTruncate { anchor = "AB12", reason = "lop off the tail" }
    == {
      truncate = { anchor = "AB12" },
      reason = "lop off the tail",
    }
  ),
]
