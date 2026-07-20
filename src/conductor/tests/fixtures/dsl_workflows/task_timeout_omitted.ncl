# No `timeout` kwarg. `Task.timeout_secs` and `Workflow.timeout_secs` both
# stay `None`; no timer arms.
let utils = import "lib.ncl" in
let noop = utils.mkTask {
  name = "noop",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ noop ],
} in
utils.mkWorkflow { slug = "wf",
  name = "timeout-omitted-fixture",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
}
