# expect: contract broken by the value of `sources`
# A group's `edges` holds projected `mkEdge` values with `sources`/`targets`
# endpoint-name arrays; a hand-shaped edge with a scalar `sources` blames here.
let { mkTaskGroup, mkTask, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
mkTaskGroup { name = "g", args = [], tasks = [ a ], edges = [ { sources = "a", targets = [ "a" ] } ] }
