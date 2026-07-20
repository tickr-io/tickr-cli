# expect: missing definition for `expr`
# `kind = "cron"` requires `expr`; omitting it fails the closed cron sub-contract.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { kind = "cron" }
