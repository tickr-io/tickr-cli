# expect: missing definition for `jsonpath`
# `jsonpath` is required; omitting it fails the closed capture input contract.
let { mkTriggerCapture, .. } = import "lib.ncl" in
mkTriggerCapture { name = "x" }
