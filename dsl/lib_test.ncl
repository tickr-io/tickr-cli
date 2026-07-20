# tickr/dsl/lib_test.ncl
#
# Re-export smoke test: destructure every public constructor from `lib.ncl`
# and use each in a no-op shape. Catches a re-export omission in `lib.ncl`
# (a missing or misspelled name) the moment it lands.
let {
  mkTask,
  mkTaskGroup,
  mkLoop,
  mkInsert,
  mkChain,
  chain,
  mkFork,
  mkBranch,
  mkExpand,
  mkSwap,
  mkCut,
  mkPrune,
  mkTrim,
  mkTruncate,
  mkWorkflow,
  mkSignal,
  mkSignalEmit,
  mkSignalEmitOnFailure,
  mkRoutingVar,
  mkSignalGate,
  mkTimerGate,
  mkPredicateGate,
  mkEdge,
  mkTriggerOn,
  mkTriggerCapture,
} = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let sig = mkSignal { name = "s" } in
[
  check (std.is_record a),
  check (std.is_record (mkTaskGroup { name = "g", args = [], tasks = [ a, b ] })),
  check (std.is_record (mkLoop { name = "lp", task = a })),
  check (std.is_record (mkLoop { name = "ring", tasks = [ a, b ] })),
  check (std.is_record (mkInsert { anchor = "AB12", task = a })),
  check (std.is_record (mkChain { anchor = "AB12", steps = [ { handle = "a", task = a } ] })),
  # The bare-verb `chain` authoring combinator, distinct from the patch-verb
  # `mkChain` above — proves it re-exports without colliding with it.
  check (std.is_record (chain [ a, b ])),
  check (std.is_record (mkFork { anchor = "AB12", arms = [ { handle = "a", task = a } ] })),
  check (std.is_record (mkBranch { anchor = "AB12", arms = [ { handle = "a", gate = mkSignalGate { signal = sig }, task = a } ] })),
  check (std.is_record (mkExpand { anchor = "AB12", over = { a = a } })),
  check (std.is_record (mkSwap { target = "AB12", task = a })),
  check (std.is_record (mkCut { target = "AB12" })),
  check (std.is_record (mkPrune { arm = "AB12" })),
  check (std.is_record (mkTrim { anchor = "AB12" })),
  check (std.is_record (mkTruncate { anchor = "AB12" })),
  check (std.is_record (mkWorkflow { slug = "wf", name = "w", args = [], tasks = [] })),
  check (std.is_record sig),
  check (std.is_record (mkSignalEmit { signal = sig, from_routing_var = "v" })),
  check (std.is_record (mkSignalEmitOnFailure { signal = sig })),
  check (std.is_record (mkRoutingVar { name = "r" })),
  check (std.is_record (mkSignalGate { signal = sig })),
  check (std.is_record (mkTimerGate { duration = "5m" })),
  check (std.is_record (mkPredicateGate { routing_var = "r", op = ">", value = 1 })),
  check (std.is_record (mkEdge { from = a, to = b })),
  check (std.is_record (mkTriggerOn { kind = "fire-now" })),
  check (std.is_record (mkTriggerCapture { name = "c", jsonpath = "$.c" })),
]
