# A two-task sequential loop whose second task is the sole loop controller.
# `inspect` must run first on every lap and remains a non-producer; `decide`
# alone declares/emits `loop_control`. Both ring links remain kind=loop and
# carry mkLoop's continue gate. The default whole-body done exit proves SCC
# teardown, rather than ordinary producer dominance, makes both sources safe.
let utils = import "lib.ncl" in
let inspect = utils.mkTask {
  name = "inspect",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let decide = utils.mkTask {
  name = "decide",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let finalize = utils.mkTask {
  name = "finalize",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
utils.mkWorkflow {
  slug = "non-head-loop-controller",
  name = "non_head_loop_controller",
  args = [],
  outputs = [],
  tasks = [
    utils.mkLoop {
      name = "controlled-body",
      tasks = [ inspect, decide ],
      producer = decide,
      exitTo = finalize,
    },
    utils.mkTaskGroup {
      name = "after-loop",
      args = [],
      outputs = [],
      tasks = [ finalize ],
    },
  ],
}
