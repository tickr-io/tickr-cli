# expect: input entry must be a bare string or a `{ name, from = .. }` record
# An `inputs` entry is a bare string or a structured `{ name, from }` record;
# a bare number matches neither accepted shape and blames at the call site.
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", inputs = [ 42 ] }
