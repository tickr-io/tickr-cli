# expect: expected an `mkSignal` value (a `SignalRef`)
# Passing a raw string instead of an `mkSignal` value fails the SignalRef
# constraint at the call site.
let { mkSignalEmitOnFailure, .. } = import "lib.ncl" in
mkSignalEmitOnFailure { signal = "user-paid" }
