# expect: `c` is not declared in this group's tasks: [a, b]
let { mkTask, mkTaskGroup, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let c = mkTask { name = "c", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  tasks = [ a, b ],
  edges = [ mkEdge { from = a, to = c } ],
}
