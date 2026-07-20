# A group where a `chain` fragment contributes a control edge whose pair another
# edge in the group also draws. The fragment threads a -> c; a hand-wired barrier
# edge {a, b} -> c draws the pair a -> c among its decomposed pairs. Overlap
# detection compares endpoint pairs, not whole edges, so the shared a -> c is
# caught even though the two edges are not equal — a parallel edge over a
# fragment's pair is a silent gate bypass under per-edge dispatch. Rejected at
# `nickel export`, blaming the offending pair.
let utils = import "lib.ncl" in
let a = utils.mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = utils.mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
let c = utils.mkTask { name = "c", args = [], nix_expression_path = "x", outputs = [] } in
utils.mkWorkflow {
  slug = "overlap",
  name = "overlap",
  args = [],
  outputs = [],
  tasks = [
    utils.mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ utils.chain [ a, c ], b ],
      edges = [ utils.mkEdge { from = [ a, b ], to = c } ],
    },
  ],
}
