# expect: expected int, got string
let { mkTask, mkTaskGroup, mkWorkflow, mkRoutingVar, mkPredicateGate, mkEdge, .. } = import "lib.ncl" in
let amount = mkRoutingVar { name = "amount", type = "int" } in
let t1 = mkTask { name = "a", args = [], nix_expression_path = "p", routing_vars = [ amount ] } in
let t2 = mkTask { name = "b", args = [], nix_expression_path = "q" } in
let g = mkTaskGroup {
  name = "grp", args = [], tasks = [ t1, t2 ],
  edges = [ mkEdge { from = t1, to = t2, kind = "data", gate = mkPredicateGate { routing_var = amount, op = "Eq", value = "hello" } } ],
} in
mkWorkflow { slug = "wf", name = "wf", args = [], tasks = [ g ], triggerOn = { kind = "fire-now" } }
