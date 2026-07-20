# expect: missing definition for `name`
# `name` is required; omitting it fails the closed capture input contract.
let { mkTriggerCapture, .. } = import "lib.ncl" in
mkTriggerCapture { jsonpath = "$.x" }
