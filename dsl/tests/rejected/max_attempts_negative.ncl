# expect: max_attempts out of range
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", max_attempts = -1 }
