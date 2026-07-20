# expect: max_attempts must be a whole number
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", max_attempts = 3.5 }
