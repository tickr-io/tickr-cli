# tickr/dsl/gate.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# Gate constructors: `mkSignalGate`, `mkTimerGate`, `mkPredicateGate`.
# Each carries a closed-record input contract (defaults-then-closed-
# contract) so a field-name typo or a raw string where a typed reference
# belongs fails at the author's call site. The constructors project their
# typed-reference fields down to the string shape the conductor parser
# expects on the wire (`.name`); the wire format is unchanged.
let contracts = import "contracts.ncl" in
let SignalRef = contracts.SignalRef in
let RoutingVarRef = contracts.RoutingVarRef in
let Operator = contracts.Operator in
let Scalar = contracts.Scalar in
let Duration = contracts.Duration in
let Nullable = contracts.Nullable in
let CaptureDecl = contracts.CaptureDecl in

# `captures` carries capture declarations (each built by
# `mkTriggerCapture`); `CaptureDecl` checks the projected shape.
let SignalGateInput = {
  signal | SignalRef,
  predicate | Nullable String,
  captures | Array CaptureDecl,
  timeout | Nullable Duration,
} in

let PredicateGateInput = {
  routing_var | RoutingVarRef,
  op | Operator,
  value | Scalar,
  timeout | Nullable Duration,
} in

let TimerGateInput = {
  duration | Duration,
} in
{
  # `mkSignalGate` is an author-declared precondition attached to an
  # `mkEdge` via the `gate` field. The edge fires only when both its
  # sources are `Grounded(Success)` AND the gate is satisfied (an external
  # wakeup matching `signal.name` arrives and the optional JSONPath
  # `predicate` holds). `captures` extracts payload values into tickr-ctx
  # scope; `timeout` bounds the wait (activated by a follow-up slice).
  mkSignalGate = fun cfg =>
    let c = ({ predicate | default = null, captures | default = [], timeout | default = null } & cfg) | SignalGateInput in
    {
      kind = "signal-gate",
      signal = { name = c.signal.name },
    }
    & (if c.predicate != null then { predicate = c.predicate } else {})
    & (if c.captures != [] then { captures = c.captures } else {})
    & (if c.timeout != null then { timeout = c.timeout } else {}),

  # `mkTimerGate` is a pure-timer gate: no signal, no routing variable,
  # just a wall-clock interval. Arms at source-grounding and fires
  # `Satisfied` after `duration` elapses. The only gate variant whose SM
  # never reaches `Rejected`.
  mkTimerGate = fun cfg =>
    let c = cfg | TimerGateInput in
    {
      kind = "timer-gate",
      duration = c.duration,
    },

  # `mkPredicateGate` is an author-declared precondition attached to an
  # `mkEdge` via the `gate` field. The edge fires when its sources are all
  # `Grounded(Success)` AND the named routing variable matches `<op>
  # <value>` against the instance's accumulated `routing_variables`. The
  # evaluator runs server-side. The optional `timeout` arms a `GateExpiry`
  # timer that rejects the gate if the producing task never completes.
  mkPredicateGate = fun cfg =>
    let c = ({ timeout | default = null } & cfg) | PredicateGateInput in
    {
      kind = "predicate-gate",
      routing_var = c.routing_var.name,
      op = c.op,
      value = c.value,
    }
    & (if c.timeout != null then { timeout = c.timeout } else {}),
}
