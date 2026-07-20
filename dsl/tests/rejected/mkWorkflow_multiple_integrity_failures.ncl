# expect: declared more than once (single-producer)
# A workflow violating three invariants at once: a duplicated routing-var
# name, a value/type mismatch, and a predicate referencing an undeclared
# var. The integrity walk accumulates one note per violation into a single
# error — all three appear together so the author fixes them in one round.
let { mkTask, mkTaskGroup, mkWorkflow, mkRoutingVar, mkPredicateGate, mkEdge, .. } = import "lib.ncl" in
let dup1 = mkRoutingVar { name = "decision" } in
let dup2 = mkRoutingVar { name = "decision" } in
let amount = mkRoutingVar { name = "amount", type = "int" } in
let ghost = mkRoutingVar { name = "ghost" } in
let t1 = mkTask { name = "a", args = [], nix_expression_path = "p", routing_vars = [ dup1, amount ] } in
let t2 = mkTask { name = "b", args = [], nix_expression_path = "q", routing_vars = [ dup2 ] } in
let g = mkTaskGroup {
  name = "grp", args = [], tasks = [ t1, t2 ],
  edges = [
    mkEdge { from = t1, to = t2, kind = "data", gate = mkPredicateGate { routing_var = amount, op = "Eq", value = "nope" } },
    mkEdge { from = t1, to = t2, kind = "data", gate = mkPredicateGate { routing_var = ghost, op = "Eq", value = 1 } },
  ],
} in
mkWorkflow { slug = "wf", name = "wf", args = [], tasks = [ g ], triggerOn = { kind = "fire-now" } }
