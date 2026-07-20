# expect: invalid routing-variable type `integer`
let { mkRoutingVar, .. } = import "lib.ncl" in
mkRoutingVar { name = "amount", type = "integer" }
