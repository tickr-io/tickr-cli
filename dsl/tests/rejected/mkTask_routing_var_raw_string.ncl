# expect: expected an `mkRoutingVar` value (a `RoutingVarRef`)
# `routing_vars` holds projected `mkRoutingVar` values; a raw string fails
# the element contract at the call site rather than reaching the parser.
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", routing_vars = [ "amount" ] }
