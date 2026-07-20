# expect: extra field `maxAttempts`
# The retry-budget field is snake_case `max_attempts`; the old camelCase
# `maxAttempts` spelling no longer resolves and blames at the call site as an
# extra field against the closed `TaskInput` contract.
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", maxAttempts = 3 }
