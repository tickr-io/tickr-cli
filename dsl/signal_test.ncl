# tickr/dsl/signal_test.ncl
#
# Positive-path tests for `mkSignal` and the producer constructs
# `mkSignalEmit` / `mkSignalEmitOnFailure`. Typed references (`mkSignal`,
# `mkRoutingVar`) thread through let-bindings and project to the wire shapes
# (`signal = { name }`, `from_routing_var` = bare `.name` string).
let { mkSignal, mkRoutingVar, mkSignalEmit, mkSignalEmitOnFailure, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let sig = mkSignal { name = "user-paid" } in
let amount = mkRoutingVar { name = "amount" } in
let emit = mkSignalEmit { signal = sig, from_routing_var = amount } in
let emit_fail = mkSignalEmitOnFailure { signal = sig } in
[
  check (sig.name == "user-paid"),
  check (sig.kind == "signal"),
  check (emit.kind == "signal-emit"),
  check (emit.signal.name == "user-paid"),
  check (emit.from_routing_var == "amount"),
  check (emit_fail.kind == "signal-emit-on-failure"),
  check (emit_fail.signal.name == "user-paid"),
]
