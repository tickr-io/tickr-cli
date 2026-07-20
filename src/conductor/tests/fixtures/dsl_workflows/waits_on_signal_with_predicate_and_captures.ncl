# A `waits-on-signal` workflow declaring a predicate plus a
# triggerOn-scoped capture. Exercises the full happy path: signal-ref
# resolution, JSONPath predicate parsing, and capture singularity.
let utils = import "lib.ncl" in
let approval = utils.mkSignal { name = "user-paid" } in
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
  name = "wants-user-paid",
  args = [],
  outputs = [],
  tasks = [ tg ],
  triggerOn = utils.mkTriggerOn {
    kind = "waits-on-signal",
    signal = approval,
    predicate = "$[?@.amount > 100]",
    captures = [
      utils.mkTriggerCapture { name = "user_email", jsonpath = "$.user.email" },
    ],
  },
}
