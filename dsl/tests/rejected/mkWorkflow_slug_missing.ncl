# expect: missing definition for `slug`
let { mkWorkflow, .. } = import "lib.ncl" in
mkWorkflow { name = "daily-sync", args = [], tasks = [] }
