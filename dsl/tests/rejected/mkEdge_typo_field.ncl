# expect: extra field `frm`
# A misspelled field (`frm` for `from`) is caught at the call site by the
# closed `EdgeInput` contract.
let { mkTask, mkEdge, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge { from = a, to = b, frm = a }
