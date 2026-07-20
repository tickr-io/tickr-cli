# expect: routing-var `decision` is declared more than once (single-producer)
let { mkTask, mkTaskGroup, mkWorkflow, mkRoutingVar, .. } = import "lib.ncl" in
let v1 = mkRoutingVar { name = "decision" } in
let v2 = mkRoutingVar { name = "decision" } in
let t1 = mkTask { name = "a", args = [], nix_expression_path = "x", routing_vars = [ v1 ] } in
let t2 = mkTask { name = "b", args = [], nix_expression_path = "y", routing_vars = [ v2 ] } in
let g = mkTaskGroup { name = "grp", args = [], tasks = [ t1, t2 ], edges = [] } in
mkWorkflow { slug = "wf", name = "wf", args = [], tasks = [ g ], triggerOn = { kind = "fire-now" } }
