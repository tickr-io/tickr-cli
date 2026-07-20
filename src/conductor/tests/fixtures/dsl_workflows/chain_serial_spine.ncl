# A serial spine authored with the bare-verb `chain` combinator: the author
# declares the task order **once** as an array and the ungated control edges
# `prepare → build → verify` are derived from array position. `mkTaskGroup`
# flattens the returned graph fragment into loose tasks + edges before it
# validates, so this lowers to exactly the wire document the hand-wired twin
# (`handwired_serial_spine.ncl`) produces — proven byte-identical by the test.
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
      tasks = [ utils.chain [ prepare, build, verify ] ],
    },
  ],
}
