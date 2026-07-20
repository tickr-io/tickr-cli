# expect: edge has a gate; `kind` must be explicit
# A gated edge with no explicit `kind` is rejected: the gate/kind rule
# requires an explicit `data` or `loop` kind when a gate is present.
let { mkTask, mkEdge, mkSignal, mkSignalGate, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge {
  from = a,
  to = b,
  gate = mkSignalGate { signal = mkSignal { name = "s" } },
}
