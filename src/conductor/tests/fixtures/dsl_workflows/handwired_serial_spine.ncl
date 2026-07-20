# The hand-wired twin of `chain_serial_spine.ncl`: the same serial spine with
# the task order restated a second time as explicit `mkEdge` control edges
# `prepare → build → verify`. This is a fragment-free group — no `chain` — and
# it must validate and export exactly as before, byte-identically to the
# `chain`-authored spine, so adopting the grammar changes nothing the conductor
# sees.
let utils = import "lib.ncl" in
let prepare = utils.mkTask {
  name = "prepare",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let build = utils.mkTask {
  name = "build",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let verify = utils.mkTask {
  name = "verify",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
utils.mkWorkflow {
  slug = "spine",
  name = "spine",
  args = [],
  outputs = [],
  tasks = [
    utils.mkTaskGroup {
      name = "pipeline",
      args = [],
      outputs = [],
      tasks = [ prepare, build, verify ],
      edges = [
        utils.mkEdge { from = prepare, to = build },
        utils.mkEdge { from = build, to = verify },
      ],
    },
  ],
}
