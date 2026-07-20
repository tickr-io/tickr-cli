# tickr/dsl/routing_test.ncl
#
# Positive-path tests for `mkRoutingVar`. The bad-type reject case lives
# under `tests/rejected/`.
let { mkRoutingVar, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let untyped = mkRoutingVar { name = "amount" } in
let typed = mkRoutingVar { name = "amount", type = "string" } in
[
  check (untyped.name == "amount"),
  check (untyped.kind == "routing-var"),
  # Omitted type is absent, not null.
  check (!std.record.has_field "type" untyped),
  check (typed.type == "string"),
  # The full accepted type set.
  check ((mkRoutingVar { name = "amount", type = "int" }).type == "int"),
  check ((mkRoutingVar { name = "flag", type = "bool" }).type == "bool"),
  check ((mkRoutingVar { name = "blob", type = "bytes" }).type == "bytes"),
  check ((mkRoutingVar { name = "items", type = "array" }).type == "array"),
]
