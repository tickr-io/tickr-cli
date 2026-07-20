# expect: missing definition for `duration`
# `duration` is required; omitting it fails the closed `TimerGateInput`
# contract.
let { mkTimerGate, .. } = import "lib.ncl" in
mkTimerGate {}
