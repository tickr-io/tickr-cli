# expect: identifier must be non-empty
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "", args = [], nix_expression_path = "x" }
