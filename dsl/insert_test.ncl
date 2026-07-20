# tickr/dsl/insert_test.ncl
#
# Positive-path tests for the `mkInsert` constructor: the emitted document is
# the `insert` arm of the Patch tagged union, carrying the anchor code and the
# full inserted-task body, with an optional `reason`. Run via `just dsl-test`
# (or `nickel export insert_test.ncl`); any failing assert fails the run.
#
# Negative cases (a missing anchor, a raw-string task, a typo'd field) are the
# constructor's closed-contract job and would live as fixtures under
# `tests/rejected/`.
let { mkTask, mkInsert, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let enrich =
  mkTask {
    name = "enrich",
    args = [ "run" ],
    nix_expression_path = "flake#enrich",
    outputs = [ "report" ],
  }
in
[
  # The document is the `insert` arm: `{ insert = { anchor, task } }`. The task
  # body is emitted whole (it is built at-patch), and no `reason` is set when
  # omitted.
  check (
    mkInsert { anchor = "AB12", task = enrich }
    == { insert = { anchor = "AB12", task = enrich } }
  ),

  # A `reason` rides the document top-level alongside the operation.
  check (
    mkInsert { anchor = "AB12", task = enrich, reason = "splice enrich after AB12" }
    == {
      insert = { anchor = "AB12", task = enrich },
      reason = "splice enrich after AB12",
    }
  ),

  # A full UUID is a legal anchor (the collision fallback).
  check (
    (mkInsert { anchor = "3f2504e0-4f89-41d3-9a0c-0305e82c3301", task = enrich }).insert.anchor
    == "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
  ),
]
