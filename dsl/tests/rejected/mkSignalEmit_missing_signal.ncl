# expect: missing definition for `signal`
# `signal` is required; omitting it fails the closed `SignalEmitInput`
# contract.
let { mkRoutingVar, mkSignalEmit, .. } = import "lib.ncl" in
mkSignalEmit { from_routing_var = mkRoutingVar { name = "amount" } }
