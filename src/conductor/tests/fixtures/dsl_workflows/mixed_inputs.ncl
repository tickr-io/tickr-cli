# Mixed bare + structured `mkTask.inputs`, supported position-by-position.
# Bare inputs keep the documentation-only semantic; structured
# `from.trigger` inputs route to the signal-scoped namespace at runtime.
let utils = import "lib.ncl" in
let build = utils.mkTask {
  name = "build",
  args = [],
  nix_expression_path = "x",
  outputs = [ "image_digest" ],
} in
let consume = utils.mkTask {
  name = "consume",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  inputs = [
    "image_digest",
    { name = "user_email", from = { trigger = true } },
  ],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ build, consume ],
} in
utils.mkWorkflow { slug = "wf",
  name = "mixed",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
  captures = [
    utils.mkTriggerCapture { name = "user_email", jsonpath = "$.user.email" },
  ],
}
