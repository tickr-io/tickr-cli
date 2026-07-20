# The fully hand-wired twin of `nested_chain_spine.ncl`: the same four-task
# spine with every control edge restated by hand — `prepare → build → test →
# deploy`. This is a fragment-free group (no `chain`); it must deserialise into
# the same parsed workflow the nested-`chain`-authored group produces, so
# building a spine out of nested sub-sequences changes nothing the conductor
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
let test = utils.mkTask {
  name = "test",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let deploy = utils.mkTask {
  name = "deploy",
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
      tasks = [ prepare, build, test, deploy ],
      edges = [
        utils.mkEdge { from = prepare, to = build },
        utils.mkEdge { from = build, to = test },
        utils.mkEdge { from = test, to = deploy },
      ],
    },
  ],
}
