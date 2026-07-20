# A group where the same task name appears on both a `chain` fragment task and a
# loose task. The flatten folds the fragment's `dup` in beside the loose `dup`;
# group-wide name uniqueness rejects the collision at `nickel export`, blaming the
# duplicated name so two tasks never share the group's name namespace.
let utils = import "lib.ncl" in
let from_fragment = utils.mkTask { name = "dup", args = [], nix_expression_path = "x", outputs = [] } in
let loose = utils.mkTask { name = "dup", args = [], nix_expression_path = "x", outputs = [] } in
utils.mkWorkflow {
  slug = "dupname",
  name = "dupname",
  args = [],
  outputs = [],
  tasks = [
    utils.mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ utils.chain [ from_fragment ], loose ],
    },
  ],
}
