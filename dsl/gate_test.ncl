# tickr/dsl/gate_test.ncl
#
# Positive-path tests for the gate constructors `mkSignalGate`,
# `mkPredicateGate`, `mkTimerGate`. Each threads a `mkSignal` /
# `mkRoutingVar` value through a let-binding and asserts the projected
# wire shape (`signal = { name }`, `routing_var = <bare string>`) plus
# optional-field omission. Negative cases live under `tests/rejected/`.
let { mkSignal, mkRoutingVar, mkSignalGate, mkPredicateGate, mkTimerGate, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let paid = mkSignal { name = "payment-cleared" } in
let amount = mkRoutingVar { name = "amount", type = "int" } in

let bare_signal_gate = mkSignalGate { signal = paid } in
let full_signal_gate = mkSignalGate {
  signal = paid,
  predicate = "$[?@.amount > 100]",
  captures = [ { name = "receipt_url", from = { trigger = { jsonpath = "$.receipt" } } } ],
  timeout = "5m",
} in
let predicate_gate = mkPredicateGate { routing_var = amount, op = "Gt", value = 100 } in
let predicate_gate_to = mkPredicateGate { routing_var = amount, op = "Eq", value = "done", timeout = "30s" } in
let timer_gate = mkTimerGate { duration = "30s" } in
[
  # mkSignalGate — projects `signal` to `{ name }`, omits unset optionals.
  check (bare_signal_gate.kind == "signal-gate"),
  check (bare_signal_gate.signal == { name = "payment-cleared" }),
  check (!std.record.has_field "predicate" bare_signal_gate),
  check (!std.record.has_field "captures" bare_signal_gate),
  check (!std.record.has_field "timeout" bare_signal_gate),
  check (full_signal_gate.signal == { name = "payment-cleared" }),
  check (full_signal_gate.predicate == "$[?@.amount > 100]"),
  check (std.array.length full_signal_gate.captures == 1),
  check (full_signal_gate.timeout == "5m"),

  # mkPredicateGate — projects `routing_var` to a bare string.
  check (predicate_gate.kind == "predicate-gate"),
  check (predicate_gate.routing_var == "amount"),
  check (predicate_gate.op == "Gt"),
  check (predicate_gate.value == 100),
  check (!std.record.has_field "timeout" predicate_gate),
  check (predicate_gate_to.value == "done"),
  check (predicate_gate_to.timeout == "30s"),

  # mkTimerGate — pure wall-clock interval.
  check (timer_gate.kind == "timer-gate"),
  check (timer_gate.duration == "30s"),
]
