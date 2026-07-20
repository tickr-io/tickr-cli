# Canonical trigger-input pattern: fire-now triggerOn + workflow-level
# captures + a downstream task declaring `from.trigger`.
let utils = import "lib.ncl" in
let notify = utils.mkTask {
  name = "notify",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  inputs = [
    { name = "user_email", from = { trigger = true } },
  ],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ notify ],
} in
utils.mkWorkflow { slug = "wf",
  name = "fire-now-with-captures",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  captures = [
    utils.mkTriggerCapture { name = "user_email", jsonpath = "$.user.email" },
  ],
}
