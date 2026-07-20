# Workflow declares operator-meaningful tags. The conductor copies them
# onto every instance alongside the four runtime-stamped system tags.
let utils = import "lib.ncl" in
let notify = utils.mkTask {
  name = "notify",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ notify ],
} in
utils.mkWorkflow { slug = "wf",
  name = "tags-set",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  tags = {
    env = "prod",
    team = "billing",
  },
}
