# expect: edge has no gate; `kind = "data"` requires a gate
# A `data`-kind edge with no gate is rejected: a data edge is gated by
# definition.
let { mkTask, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge { from = a, to = b, kind = "data" }
