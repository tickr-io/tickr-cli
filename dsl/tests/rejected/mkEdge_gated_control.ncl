# expect: gated edges cannot be `control`-kind
# A gated edge declared `kind = "control"` violates the gate/kind rule: a
# gate implies a `data` or `loop` edge.
let { mkTask, mkEdge, mkSignal, mkSignalGate, .. } = import "lib.ncl" in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
mkEdge {
  from = a,
  to = b,
  kind = "control",
  gate = mkSignalGate { signal = mkSignal { name = "s" } },
}
