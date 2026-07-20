# tickr/dsl/signal.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# Signal constructors. `mkSignal` is hardened (closed input + identifier
# rule on `name`). The producer constructs `mkSignalEmit` /
# `mkSignalEmitOnFailure` carry closed-record input contracts that accept
# typed references (`SignalRef`, `RoutingVarRef`) and project them down to
# the string shapes the conductor parser expects on the wire (`signal.name`,
# `from_routing_var → .name`); the wire format is unchanged.
let contracts = import "contracts.ncl" in
let Identifier = contracts.Identifier in
let SignalRef = contracts.SignalRef in
let RoutingVarRef = contracts.RoutingVarRef in

let SignalInput = { name | Identifier } in

let SignalEmitInput = {
  signal | SignalRef,
  from_routing_var | RoutingVarRef,
} in

# Closed — the failure variant carries no `from_routing_var`; an extra
# field blames at the call site.
let SignalEmitOnFailureInput = {
  signal | SignalRef,
} in
{
  # Lightweight typed reference for a named signal. Carries only the name
  # today; trigger/gate constructs consume it. Authors name signals in one
  # place and reuse the reference at later sites without magic strings.
  mkSignal = fun cfg =>
    let c = cfg | SignalInput in
    {
      name = c.name,
      kind = "signal",
    },

  # `mkSignalEmit` is a producer construct on `mkTask.emits`: at task-
  # completion-success time the conductor reads the named routing variable
  # from the just-completed task's outputs and publishes a synthesized
  # Wakeup on `tickr.external.signals`.
  mkSignalEmit = fun cfg =>
    let c = cfg | SignalEmitInput in
    {
      kind = "signal-emit",
      signal = { name = c.signal.name },
      from_routing_var = c.from_routing_var.name,
    },

  # `mkSignalEmitOnFailure` is the failure-side producer on `mkTask.emits`:
  # at task-completion-failure time the conductor synthesizes a Wakeup
  # auto-populated with the failing task's lineage. No routing variable —
  # downstream consumers filter by lineage via `mkSignalGate.predicate`.
  mkSignalEmitOnFailure = fun cfg =>
    let c = cfg | SignalEmitOnFailureInput in
    {
      kind = "signal-emit-on-failure",
      signal = { name = c.signal.name },
    },
}
