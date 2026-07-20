# A two-task workflow whose edge declares a `mkSignalGate`. The gate
# carries a JSONPath predicate, a single capture, and a duration timeout —
# exercising every optional field on the gate constructor, and (as a
# canary) confirming that raw JSONPath strings survive Nickel's exporter
# escaping intact. `parse_workflow_from_json` does not consume edges, so
# the assertion target is the DSL → JSON shape.
let utils = import "lib.ncl" in
let paid = utils.mkSignal { name = "payment-cleared" } in
let fetch_order = utils.mkTask {
  name = "fetch_order",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let ship = utils.mkTask {
  name = "ship",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ fetch_order, ship ],
  edges = [
    utils.mkEdge {
      from = fetch_order,
      to = ship,
      kind = "data",
      gate = utils.mkSignalGate {
        signal = paid,
        predicate = "$[?@.amount > 100]",
        captures = [
          utils.mkTriggerCapture { name = "receipt_url", jsonpath = "$.receipt" },
        ],
        timeout = "5m",
      },
    },
  ],
} in
utils.mkWorkflow { slug = "wf",
  name = "gated_workflow",
  args = [],
  outputs = [],
  tasks = [ tg ],
}
