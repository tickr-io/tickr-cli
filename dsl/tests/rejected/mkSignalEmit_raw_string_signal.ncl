# expect: expected an `mkSignal` value (a `SignalRef`)
# Passing a raw string instead of an `mkSignal` value fails the SignalRef
# constraint at the call site.
let { mkRoutingVar, mkSignalEmit, .. } = import "lib.ncl" in
mkSignalEmit { signal = "user-paid", from_routing_var = mkRoutingVar { name = "amount" } }
