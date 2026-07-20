# Per-task timeout pattern. Author writes `mkTask.timeout = "30s"`; the
# parser projects onto `Task.timeout_secs = Some(30)`. Also a canary for
# integer rendering: the task's default `max_attempts = 3` must export as
# an integer (not 3.0), or deserialization into `Option<u32>` would fail.
let utils = import "lib.ncl" in
let slow = utils.mkTask {
  name = "slow_task",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  timeout = "30s",
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ slow ],
} in
utils.mkWorkflow { slug = "wf",
  name = "task-timeout-fixture",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
}
