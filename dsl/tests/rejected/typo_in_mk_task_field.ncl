# expect: extra field `naem`
# A misspelled field name (`naem` for `name`) is caught at the call site by
# the closed `TaskInput` contract instead of silently dropping to a missing
# field at registration.
let { mkTask, .. } = import "lib.ncl" in
mkTask { naem = "process", args = [], nix_expression_path = "x" }
