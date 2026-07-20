# Both per-task and per-workflow timeouts on the same workflow. The two
# are independent and both project at registration.
let utils = import "lib.ncl" in
let slow = utils.mkTask {
  name = "slow_task",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  timeout = "1h",
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ slow ],
} in
utils.mkWorkflow { slug = "wf",
  name = "both-timeouts-fixture",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  timeout = "1h",
}
