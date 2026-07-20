# expect: extra field `duraton`
# A misspelled field (`duraton` for `duration`) is caught at the call site
# by the closed `TimerGateInput` contract.
let { mkTimerGate, .. } = import "lib.ncl" in
mkTimerGate { duraton = "30s" }
