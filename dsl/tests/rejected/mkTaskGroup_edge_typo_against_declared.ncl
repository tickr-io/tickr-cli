# expect: `biuld` is not declared in this group's tasks: [build, ship]
let { mkTask, mkTaskGroup, mkEdge, .. } = import "lib.ncl" in
let build = mkTask { name = "build", args = [], nix_expression_path = "x" } in
let ship = mkTask { name = "ship", args = [], nix_expression_path = "x" } in
let typo = mkTask { name = "biuld", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  tasks = [ build, ship ],
  edges = [ mkEdge { from = typo, to = ship } ],
}
