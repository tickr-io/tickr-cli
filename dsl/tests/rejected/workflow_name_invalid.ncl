# expect: invalid identifier `my workflow`
let { mkWorkflow, .. } = import "lib.ncl" in
mkWorkflow { slug = "wf", name = "my workflow", args = [], tasks = [] }
