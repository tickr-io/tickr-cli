# expect: expected an `mkSignal` value (a `SignalRef`)
# `waits-on-signal` requires an `mkSignal` value; a raw string fails the
# `SignalRef` constraint at the call site.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { kind = "waits-on-signal", signal = "some-name" }
