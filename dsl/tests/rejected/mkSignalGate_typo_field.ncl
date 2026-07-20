# expect: extra field `predicat`
# A misspelled optional field (`predicat` for `predicate`) is caught at the
# call site by the closed `SignalGateInput` contract.
let { mkSignal, mkSignalGate, .. } = import "lib.ncl" in
mkSignalGate { signal = mkSignal { name = "paid" }, predicat = "$.x" }
