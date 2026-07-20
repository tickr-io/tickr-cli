# expect: missing definition for `from_routing_var`
# `from_routing_var` is required; omitting it fails the closed
# `SignalEmitInput` contract.
let { mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
mkSignalEmit { signal = mkSignal { name = "user-paid" } }
