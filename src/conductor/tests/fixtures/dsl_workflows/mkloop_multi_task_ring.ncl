# A two-node loop body authored with one `mkLoop` call and no hand-wiring: a
# `griller` ⇄ `grilly` ring that exits into a downstream `judge`. `mkLoop`
# auto-wires the `loop_control` routing variable on the head (`griller`), the
# ring back-edges (`kind = loop`, gated `loop_control == continue`), the entry
# edge `Start → griller` (the ring is entered at one node), and the whole-body
# exit fan-in (`{ griller, grilly } → judge`, gated `loop_control == done`).
# The `judge` is an ordinary downstream task the author defines; `exitTo` points
# at it. This is the graph the issue-01 runtime tears down to `Completed`.
let utils = import "lib.ncl" in
let griller = utils.mkTask {
  name = "griller",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let grilly = utils.mkTask {
  name = "grilly",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let judge = utils.mkTask {
  name = "judge",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
utils.mkWorkflow {
  slug = "debate",
  name = "debate",
  args = [],
  outputs = [],
  tasks = [
    utils.mkLoop { name = "debate-body", tasks = [ griller, grilly ], exitTo = judge },
    utils.mkTaskGroup { name = "verdict", args = [], outputs = [], tasks = [ judge ] },
  ],
}
