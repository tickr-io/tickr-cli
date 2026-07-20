# A serial spine built from reusable sub-sequences: `chain` is closed under
# nesting, so `chain [ chain [prepare, build], chain [test, deploy] ]` threads
# each inner chain and links the one seam between them (last-of-first `build` →
# first-of-second `test`) with a single ungated control edge. Each inner
# chain's own edge (`prepare → build`, `test → deploy`) survives unchanged, so
# the fragment flattens to the full spine `prepare → build → test → deploy` —
# the same parsed workflow the fully hand-wired twin (`handwired_nested_spine.ncl`)
# produces, proven by the test.
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
      tasks = [ utils.chain [ utils.chain [ prepare, build ], utils.chain [ test, deploy ] ] ],
    },
  ],
}
