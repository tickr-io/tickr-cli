# Negative-path fixture: declaring a `tickr/`-prefixed tag must fail at
# registration. The system stamps four tags under that namespace on every
# instance; a definition key in the same namespace would shadow the
# runtime guarantee.
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
  name = "tags-reserved-clash",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  tags = {
    "tickr/workflow_id" = "spoofed",
  },
}
