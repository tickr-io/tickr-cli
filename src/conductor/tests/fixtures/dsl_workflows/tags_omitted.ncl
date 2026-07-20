# Authors who don't care about tags omit the field entirely. The resulting
# JSON must not carry a `tags` key; the parser loads it via the
# empty-map default.
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
  name = "tags-omitted",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "fire-now" },
}
