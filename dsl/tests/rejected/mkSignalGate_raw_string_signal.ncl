# expect: expected an `mkSignal` value
# Passing a raw string instead of an `mkSignal` value fails the SignalRef
# constraint at the call site.
let { mkSignalGate, .. } = import "lib.ncl" in
mkSignalGate { signal = "payment-cleared" }
