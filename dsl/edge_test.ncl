# tickr/dsl/edge_test.ncl
#
# Positive-path tests for the `mkEdge` constructor: the four legal
# gate/kind combinations plus single-task and multi-task `from`/`to`
# endpoints. Each assert checks the projected wire shape. Run via
# `just dsl-test` (or `nickel export edge_test.ncl`); any failing assert
# breaks the constructor and fails the run.
#
# Negative cases (combinations the gate/kind rule must reject) live as
# standalone fixtures under `tests/rejected/`.
let { mkTask, mkEdge, mkSignal, mkSignalGate, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let a = mkTask { name = "a", args = [], nix_expression_path = "x", outputs = [] } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x", outputs = [] } in
let c = mkTask { name = "c", args = [], nix_expression_path = "x", outputs = [] } in
let g = mkSignalGate { signal = mkSignal { name = "s" } } in
[
  # Default kind: no gate, no `kind` — `kind` is omitted on the wire.
  check (mkEdge { from = a, to = b } == { sources = [ "a" ], targets = [ "b" ] }),

  # Explicit `control`: no gate — `kind` is emitted as declared.
  check (
    mkEdge { from = a, to = b, kind = "control" }
    == { sources = [ "a" ], targets = [ "b" ], kind = "control" }
  ),

  # Gated `data`: gate present, explicit `data` kind.
  check (
    mkEdge { from = a, to = b, kind = "data", gate = g }
    == { sources = [ "a" ], targets = [ "b" ], kind = "data", gate = g }
  ),

  # Gated `loop`: gate present, explicit `loop` kind.
  check (
    mkEdge { from = a, to = b, kind = "loop", gate = g }
    == { sources = [ "a" ], targets = [ "b" ], kind = "loop", gate = g }
  ),

  # Single-task `from`/`to` project to one-element `sources`/`targets`.
  check (mkEdge { from = a, to = b } == { sources = [ "a" ], targets = [ "b" ] }),

  # Multi-task `from`/`to` (arrays) project to multi-element arrays.
  check (
    mkEdge { from = [ a, b ], to = [ b, c ] }
    == { sources = [ "a", "b" ], targets = [ "b", "c" ] }
  ),
]
