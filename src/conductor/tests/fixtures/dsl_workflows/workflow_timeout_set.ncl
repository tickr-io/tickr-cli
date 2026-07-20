# Per-workflow timeout pattern. Author writes `mkWorkflow.timeout = "5m"`;
# the parser projects onto `Workflow.timeout_secs = Some(300)`.
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
  name = "workflow-timeout-fixture",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  timeout = "5m",
}
