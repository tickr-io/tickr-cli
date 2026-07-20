# expect: `y` is not declared in this group's tasks: [a, b]
let { mkTask, mkTaskGroup, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let x = mkTask { name = "x", args = [], nix_expression_path = "x" } in
let y = mkTask { name = "y", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  tasks = [ a, b ],
  edges = [
    mkEdge { from = a, to = x },
    mkEdge { from = y, to = b },
  ],
}
