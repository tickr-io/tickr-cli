# expect: any_of: value didn't match any of the contracts
# A record value has no defined scalar comparison; the `Scalar` contract
# rejects it at the call site.
let { mkRoutingVar, mkPredicateGate, .. } = import "lib.ncl" in
mkPredicateGate { routing_var = mkRoutingVar { name = "amount" }, op = "Eq", value = { nested = "record" } }
