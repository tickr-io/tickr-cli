# expect: `dup` is declared by more than one task in group `g`
# A task name shared across a fragment task and a loose task collides in the
# group's name namespace. The flatten folds the fragment's `dup` in beside the
# loose `dup`; group-wide name uniqueness rejects it, blaming the duplicated name.
let { mkTask, mkTaskGroup, chain, .. } = import "lib.ncl" in
let from_fragment = mkTask { name = "dup", args = [], nix_expression_path = "x" } in
let loose = mkTask { name = "dup", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ chain [ from_fragment ], loose ],
}
