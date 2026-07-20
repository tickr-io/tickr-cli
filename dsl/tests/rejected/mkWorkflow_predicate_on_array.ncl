# expect: predicate gates are scalar-only
let { mkTask, mkTaskGroup, mkWorkflow, mkRoutingVar, mkPredicateGate, mkEdge, .. } = import "lib.ncl" in
let items = mkRoutingVar { name = "items", type = "array" } in
let t1 = mkTask { name = "a", args = [], nix_expression_path = "p", routing_vars = [ items ] } in
let t2 = mkTask { name = "b", args = [], nix_expression_path = "q" } in
let g = mkTaskGroup {
  name = "grp", args = [], tasks = [ t1, t2 ],
  edges = [ mkEdge { from = t1, to = t2, kind = "data", gate = mkPredicateGate { routing_var = items, op = "Eq", value = 1 } } ],
} in
mkWorkflow { slug = "wf", name = "wf", args = [], tasks = [ g ], triggerOn = { kind = "fire-now" } }
