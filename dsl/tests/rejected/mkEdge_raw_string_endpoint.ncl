# expect: edge endpoint must be a `TaskRef`
# Passing a raw string as `from` instead of an `mkTask` value fails the
# `TaskRef` constraint at the call site.
let { mkTask, mkEdge, .. } = import "lib.ncl" in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge { from = "a", to = b }
