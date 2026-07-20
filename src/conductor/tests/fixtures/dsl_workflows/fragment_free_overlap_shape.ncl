# The fragment-free twin of `fragment_overlap_rejected.ncl`: the identical task
# set and edge shape (a barrier edge {a, b} -> c plus a loose a -> c edge drawing
# the same pair), but with **no** `chain` fragment. Because the overlap +
# name-uniqueness detections are fragment-gated, this validates cleanly — proving
# the detection does not regress existing hand-wired files. (The hand-wired
# parallel-edge footgun is a pre-existing, separately-tracked surface.)
let utils = import "lib.ncl" in
let a = utils.mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = utils.mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
let c = utils.mkTask { name = "c", args = [], nix_expression_path = "x", outputs = [] } in
utils.mkWorkflow {
  slug = "ffshape",
  name = "ffshape",
  args = [],
  outputs = [],
  tasks = [
    utils.mkTaskGroup {
      name = "g",
      args = [],
      outputs = [],
      tasks = [ a, b, c ],
      edges = [
        utils.mkEdge { from = a, to = c },
        utils.mkEdge { from = [ a, b ], to = c },
      ],
    },
  ],
}
