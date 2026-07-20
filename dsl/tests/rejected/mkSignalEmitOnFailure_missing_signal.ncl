# expect: missing definition for `signal`
# `signal` is required; omitting it fails the closed
# `SignalEmitOnFailureInput` contract.
let { mkSignalEmitOnFailure, .. } = import "lib.ncl" in
mkSignalEmitOnFailure {}
