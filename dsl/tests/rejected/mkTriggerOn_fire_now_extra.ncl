# expect: extra field `expr`
# `kind = "fire-now"` accepts only `kind`; any extra field is rejected by the
# closed fire-now sub-contract.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { kind = "fire-now", expr = "*/5 * * * *" }
