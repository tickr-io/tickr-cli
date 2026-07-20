# expect: missing definition for `signal`
# `kind = "waits-on-signal"` requires `signal`; omitting it fails the closed sub-contract.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { kind = "waits-on-signal" }
