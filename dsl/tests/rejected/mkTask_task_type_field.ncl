# expect: extra field `task_type`
# There is no author-facing path to a non-default task kind: an authored task
# is always `RegularTask`, so a `task_type` field blames at the call site via
# the closed `TaskInput` contract rather than opening an accidental author
# surface for `SensorTask`/`ShadowTask` (both reachable only via system paths).
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "process", args = [], nix_expression_path = "x", task_type = "SensorTask" }
