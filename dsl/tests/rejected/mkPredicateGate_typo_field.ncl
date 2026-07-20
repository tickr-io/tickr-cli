# expect: extra field `vlaue`
# A misspelled field (`vlaue` for `value`) is caught at the call site by the
# closed `PredicateGateInput` contract.
let { mkRoutingVar, mkPredicateGate, .. } = import "lib.ncl" in
mkPredicateGate { routing_var = mkRoutingVar { name = "amount" }, op = "Gt", vlaue = 100 }
