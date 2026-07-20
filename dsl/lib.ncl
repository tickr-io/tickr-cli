# tickr/dsl/lib.ncl
#
# The Core DSL: the public entry point. This is the only file authors
# import — `let { mkTask, mkWorkflow, .. } = import "lib.ncl" in …`.
#
# It re-exports the constructor surface from the per-concept internal
# modules (`task.ncl`, `workflow.ncl`, `signal.ncl`, `routing.ncl`,
# `gate.ncl`, `edge.ncl`), which carry the closed-record input contracts
# and pull shared domain contracts from `contracts.ncl`. Those modules are
# internal: do not import them directly.
#
# Validate locally the same way the conductor does:
#   nickel export workflow.ncl --format json -I path/to/tickr/dsl
# or via the `just dsl-check <file>` recipe, which wires the import path
# for you.
let task = import "task.ncl" in
let workflow = import "workflow.ncl" in
let signal = import "signal.ncl" in
let routing = import "routing.ncl" in
let gate = import "gate.ncl" in
let edge = import "edge.ncl" in
let trigger = import "trigger.ncl" in
let loop = import "loop.ncl" in
let insert = import "insert.ncl" in
# The patch-verb `mkChain` module is bound with a `Mod` suffix so the bare-verb
# `chain` authoring field below (a record field, and Nickel record fields are
# recursive) does not shadow it. Same shadowing hazard `loop.ncl` documents.
let chainMod = import "chain.ncl" in
let authoring = import "authoring.ncl" in
let fork = import "fork.ncl" in
let branch = import "branch.ncl" in
let expand = import "expand.ncl" in
let swap = import "swap.ncl" in
let cut = import "cut.ncl" in
let prune = import "prune.ncl" in
let trim = import "trim.ncl" in
let truncate = import "truncate.ncl" in
{
  mkTask = task.mkTask,
  mkTaskGroup = task.mkTaskGroup,
  mkLoop = loop.mkLoop,
  mkInsert = insert.mkInsert,
  mkChain = chainMod.mkChain,
  # Definition-time authoring combinator — bare verb, returns a graph fragment
  # (see `authoring.ncl`). Distinct from the patch-verb `mkChain` above.
  chain = authoring.chain,
  mkFork = fork.mkFork,
  mkBranch = branch.mkBranch,
  mkExpand = expand.mkExpand,
  mkSwap = swap.mkSwap,
  mkCut = cut.mkCut,
  mkPrune = prune.mkPrune,
  mkTrim = trim.mkTrim,
  mkTruncate = truncate.mkTruncate,
  mkWorkflow = workflow.mkWorkflow,
  mkSignal = signal.mkSignal,
  mkSignalEmit = signal.mkSignalEmit,
  mkSignalEmitOnFailure = signal.mkSignalEmitOnFailure,
  mkRoutingVar = routing.mkRoutingVar,
  mkSignalGate = gate.mkSignalGate,
  mkTimerGate = gate.mkTimerGate,
  mkPredicateGate = gate.mkPredicateGate,
  mkEdge = edge.mkEdge,
  mkTriggerOn = trigger.mkTriggerOn,
  mkTriggerCapture = trigger.mkTriggerCapture,
}
