let tickr = import "lib.ncl" in

let choose = tickr.mkTask {
  name = "choose-counts",
  args = [],
  nix_expression_path = "path:./examples#choose",
  outputs = [],
  timeout = "30s",
  max_attempts = 1,
} in

let patch = tickr.mkTask {
  name = "patch-two-arms",
  args = [],
  nix_expression_path = "path:./examples#patch",
  outputs = [],
  timeout = "180s",
  max_attempts = 1,
} in

let summary = tickr.mkTask {
  name = "summarize-join",
  args = [],
  nix_expression_path = "path:./examples#summary",
  outputs = [],
  timeout = "30s",
  max_attempts = 1,
} in

tickr.mkWorkflow {
  slug = "runtime-patch",
  name = "runtime-patch",
  args = [],
  outputs = [],
  captures = [
    tickr.mkTriggerCapture { name = "seed", jsonpath = "$.seed" },
  ],
  tasks = [
    tickr.mkTaskGroup {
      name = "seeded-runtime-patch",
      args = [],
      outputs = [],
      tasks = [ tickr.chain [ choose, patch, summary ] ],
    },
  ],
}
