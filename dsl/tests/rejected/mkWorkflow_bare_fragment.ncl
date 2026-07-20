# expect: wrap it in `mkTaskGroup`
# A bare graph fragment (what `chain` returns) handed straight to
# `mkWorkflow`'s `tasks` list, skipping the group wrapper. The negative
# `WorkflowTaskEntry` guard rejects it and names the fix — wrap it in
# `mkTaskGroup` — so a fragment can never ship as a nameless pseudo-group.
let { mkTask, mkWorkflow, chain, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
mkWorkflow {
  slug = "wf",
  name = "w",
  args = [],
  tasks = [ chain [ a, b ] ],
}
