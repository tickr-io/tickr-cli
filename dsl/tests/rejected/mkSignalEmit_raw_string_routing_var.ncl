# expect: expected an `mkRoutingVar` value (a `RoutingVarRef`)
# Passing a raw string instead of an `mkRoutingVar` value fails the
# RoutingVarRef constraint at the call site.
let { mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
mkSignalEmit { signal = mkSignal { name = "user-paid" }, from_routing_var = "amount" }
