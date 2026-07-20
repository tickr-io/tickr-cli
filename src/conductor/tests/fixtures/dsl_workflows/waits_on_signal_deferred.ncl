# `triggerOn.kind = "waits-on-signal"` projects onto the workflow's
# waits-on-signal config (signal reference only, no predicate/captures).
let utils = import "lib.ncl" in
let approval = utils.mkSignal { name = "approval" } in
let noop = utils.mkTask {
  name = "noop",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ noop ],
} in
utils.mkWorkflow { slug = "wf",
  name = "wants-signal",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn { kind = "waits-on-signal", signal = approval },
}
