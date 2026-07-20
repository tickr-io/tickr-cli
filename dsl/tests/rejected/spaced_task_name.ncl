# expect: invalid identifier `has spaces`
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "has spaces", args = [], nix_expression_path = "x" }
