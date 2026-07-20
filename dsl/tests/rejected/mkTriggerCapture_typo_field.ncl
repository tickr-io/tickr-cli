# expect: extra field `jsonpth`
# A misspelled field (`jsonpth` for `jsonpath`) is caught by the closed
# capture input contract.
let { mkTriggerCapture, .. } = import "lib.ncl" in
mkTriggerCapture { name = "x", jsonpth = "$.x" }
