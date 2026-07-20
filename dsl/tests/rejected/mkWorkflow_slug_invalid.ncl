# expect: invalid slug `Daily_Sync`
let { mkWorkflow, .. } = import "lib.ncl" in
mkWorkflow { slug = "Daily_Sync", name = "daily-sync", args = [], tasks = [] }
