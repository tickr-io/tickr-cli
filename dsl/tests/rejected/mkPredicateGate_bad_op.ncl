# expect: invalid operator `==`
# A language-level operator string fails the `Operator` contract, which
# accepts only the PascalCase wire forms.
let { mkRoutingVar, mkPredicateGate, .. } = import "lib.ncl" in
mkPredicateGate { routing_var = mkRoutingVar { name = "amount" }, op = "==", value = 100 }
