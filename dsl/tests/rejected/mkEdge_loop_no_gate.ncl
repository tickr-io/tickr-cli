# expect: edge has no gate; `kind = "loop"` requires a gate
# A `loop`-kind edge with no gate is rejected: a loop back-edge is gated by
# definition.
let { mkTask, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge { from = a, to = b, kind = "loop" }
