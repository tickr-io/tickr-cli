# expect: `a -> b` is drawn by more than one edge in group `g`
# A `chain` fragment contributes the ungated control edge a -> b; a hand-wired
# edge over the same pair is placed beside it. Under per-edge dispatch that
# parallel edge is a silent gate bypass, so the group is rejected — the offending
# pair is blamed and the author is told to split the chain at the link instead.
let { mkTask, mkTaskGroup, mkEdge, chain, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ chain [ a, b ] ],
  edges = [ mkEdge { from = a, to = b } ],
}
