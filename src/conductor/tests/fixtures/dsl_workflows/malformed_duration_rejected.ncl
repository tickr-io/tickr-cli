# Malformed duration. Author wrote `timeout = "30q"` (unknown unit); the
# parser rejects at registration — the author learns at deploy, not at
# first dispatch.
let utils = import "lib.ncl" in
let bad = utils.mkTask {
  name = "bad_timeout_task",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  timeout = "30q",
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ bad ],
} in
utils.mkWorkflow { slug = "wf",
  name = "malformed-duration-fixture",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
}
