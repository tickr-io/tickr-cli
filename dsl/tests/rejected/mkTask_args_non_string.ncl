# expect: contract broken by the value of `args`
# `args` is a string list on the wire; a non-string element is rejected at
# the Nickel contract surface, not passed through to the conductor parser.
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [ 42 ], nix_expression_path = "x" }
