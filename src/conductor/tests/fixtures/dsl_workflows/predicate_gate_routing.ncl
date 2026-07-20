# An authored workflow proving data-driven routing on a produced routing
# variable. `producer` declares a `mkRoutingVar` ("decision") and publishes
# a value into it via tickr-ctx at runtime; two downstream edges each carry
# a `mkPredicateGate` over that one variable — `decision Eq "approve"` to
# `on_approve` and `decision Eq "reject"` to `on_reject`. At runtime the
# edge whose predicate holds fires (its target dispatches) and the other
# stays pending. Both branches are authored over a single produced value;
# the producer dominates both edges (it is their source), so the variable is
# present when each gate evaluates.
let utils = import "lib.ncl" in
let decision = utils.mkRoutingVar { name = "decision", type = "string" } in
let producer = utils.mkTask {
  name = "producer",
  args = [],
  nix_expression_path = "x",
  outputs = [],
  routing_vars = [ decision ],
} in
let on_approve = utils.mkTask {
  name = "on_approve",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let on_reject = utils.mkTask {
  name = "on_reject",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ producer, on_approve, on_reject ],
  edges = [
    utils.mkEdge {
      from = producer,
      to = on_approve,
      kind = "data",
      gate = utils.mkPredicateGate {
        routing_var = decision,
        op = "Eq",
        value = "approve",
      },
    },
    utils.mkEdge {
      from = producer,
      to = on_reject,
      kind = "data",
      gate = utils.mkPredicateGate {
        routing_var = decision,
        op = "Eq",
        value = "reject",
      },
    },
  ],
} in
utils.mkWorkflow { slug = "wf",
  name = "predicate_gate_routing",
  args = [],
  outputs = [],
  tasks = [ tg ],
}
