# expect: invalid edge kind `unknown`
# An unrecognised `kind` is rejected by the `EdgeKind` contract naming the
# accepted set.
let { mkTask, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge { from = a, to = b, kind = "unknown" }
