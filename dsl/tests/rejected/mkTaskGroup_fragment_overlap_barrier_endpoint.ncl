# expect: `a -> c` is drawn by more than one edge in group `g`
# The overlap detection decomposes edges to endpoint pairs, not whole edges: the
# `chain` contributes a -> c, and a hand-wired barrier edge {a,b} -> c draws the
# pair a -> c among its two pairs. Whole-edge equality would miss this (the edges
# differ); pair decomposition catches the shared pair.
let { mkTask, mkTaskGroup, mkEdge, chain, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let c = mkTask { name = "c", args = [], nix_expression_path = "x" } in
mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ chain [ a, c ], b ],
  edges = [ mkEdge { from = [ a, b ], to = c } ],
}
