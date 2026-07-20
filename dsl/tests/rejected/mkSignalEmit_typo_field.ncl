# expect: extra field `frm_routing_var`
# A misspelled field is caught at the call site by the closed
# `SignalEmitInput` contract.
let { mkSignal, mkRoutingVar, mkSignalEmit, .. } = import "lib.ncl" in
mkSignalEmit { signal = mkSignal { name = "paid" }, from_routing_var = mkRoutingVar { name = "amount" }, frm_routing_var = "x" }
