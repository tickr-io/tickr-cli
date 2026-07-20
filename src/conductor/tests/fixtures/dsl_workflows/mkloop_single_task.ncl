# A workflow authored with one `mkLoop` call and no hand-wiring. The single
# `poll` task loops on its own `loop_control` output: `mkLoop` auto-wires the
# `loop_control` routing variable, a `kind = loop` self-edge gated
# `loop_control == continue` (the default — keep turning), and a `kind = data`
# exit edge gated `loop_control == done` targeting `End`. The sugar produces
# the same graph the runtime spine is independently proven to run to Completed.
let utils = import "lib.ncl" in
let poll = utils.mkTask {
  name = "poll",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
utils.mkWorkflow {
  slug = "poll-loop",
  name = "poll_loop",
  args = [],
  outputs = [],
  tasks = [ utils.mkLoop { name = "poll-loop-group", task = poll, maxIterations = 5 } ],
}
