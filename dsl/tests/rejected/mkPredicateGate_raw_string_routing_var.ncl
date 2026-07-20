# expect: expected an `mkRoutingVar` value
# Passing a raw string instead of an `mkRoutingVar` value fails the
# RoutingVarRef constraint at the call site.
let { mkPredicateGate, .. } = import "lib.ncl" in
mkPredicateGate { routing_var = "amount", op = "Gt", value = 100 }
