# expect: extra field `signl`
# A misspelled field (`signl` for `signal`) is caught at the call site by
# the closed `SignalEmitOnFailureInput` contract.
let { mkSignal, mkSignalEmitOnFailure, .. } = import "lib.ncl" in
mkSignalEmitOnFailure { signal = mkSignal { name = "paid" }, signl = "x" }
