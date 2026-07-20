# expect: extra field `from_routing_var`
# The failure variant carries no payload reference; `from_routing_var` is an
# extra field that the closed `SignalEmitOnFailureInput` contract rejects at
# the call site.
let { mkSignal, mkRoutingVar, mkSignalEmitOnFailure, .. } = import "lib.ncl" in
mkSignalEmitOnFailure { signal = mkSignal { name = "paid" }, from_routing_var = mkRoutingVar { name = "amount" } }
