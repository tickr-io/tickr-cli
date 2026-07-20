# tickr/dsl/contracts_test.ncl
#
# Positive-path tests for the shared domain contracts. Each entry asserts
# that a value the contract should accept evaluates cleanly. Run via
# `just dsl-test` (or `nickel export contracts_test.ncl`); any failing
# assert breaks its contract and fails the run.
#
# Negative cases (values the contracts must reject) live as standalone
# fixtures under `tests/rejected/`.
let c = import "contracts.ncl" in
let check = fun b => (b | std.test.Assert) in
let accepts = fun Contract value => (value | Contract) == value in
[
  # Identifier — letters, digits, underscore, dash; 1–64 chars.
  check (accepts c.Identifier "process"),
  check (accepts c.Identifier "process_1"),
  check (accepts c.Identifier "build-step-2"),
  check (accepts c.Identifier "a"),

  # Duration — the parser/duration.rs grammar: bare number = seconds.
  check (accepts c.Duration "42"),
  check (accepts c.Duration "30s"),
  check (accepts c.Duration "5m"),
  check (accepts c.Duration "1h"),
  # Nullable Duration accepts null.
  check (accepts (c.Nullable c.Duration) null),
  check (accepts (c.Nullable c.Duration) "300"),

  # MaxAttempts — whole number in 1..=100.
  check (accepts c.MaxAttempts 1),
  check (accepts c.MaxAttempts 3),
  check (accepts c.MaxAttempts 100),

  # RoutingVarType — the accepted type tags, plus null via Nullable.
  check (accepts c.RoutingVarType "string"),
  check (accepts c.RoutingVarType "int"),
  check (accepts c.RoutingVarType "bool"),
  check (accepts c.RoutingVarType "bytes"),
  check (accepts c.RoutingVarType "array"),
  check (accepts (c.Nullable c.RoutingVarType) null),

  # Tags — non-`tickr/` string-valued keys.
  check (accepts c.Tags {}),
  check (accepts c.Tags { env = "prod", team = "billing" }),

  # SignalRef — the shape `mkSignal` produces.
  check (accepts c.SignalRef { kind = "signal", name = "payment-cleared" }),

  # RoutingVarRef — the shape `mkRoutingVar` produces, with and without `type`.
  check (accepts c.RoutingVarRef { kind = "routing-var", name = "amount", type = "int" }),
  check (accepts c.RoutingVarRef { kind = "routing-var", name = "amount", type = null }),

  # Operator — the PascalCase wire forms.
  check (accepts c.Operator "Eq"),
  check (accepts c.Operator "NotEq"),
  check (accepts c.Operator "Lt"),
  check (accepts c.Operator "Le"),
  check (accepts c.Operator "Gt"),
  check (accepts c.Operator "Ge"),

  # Scalar — number, string, boolean.
  check (accepts c.Scalar 42),
  check (accepts c.Scalar "done"),
  check (accepts c.Scalar true),

  # AnyGate — any of the three gate-constructor outputs.
  check (accepts c.AnyGate { kind = "signal-gate", signal = { name = "s" } }),
  check (accepts c.AnyGate { kind = "predicate-gate", routing_var = "amount", op = "Gt", value = 100 }),
  check (accepts c.AnyGate { kind = "timer-gate", duration = "30s" }),

  # TaskRef — an open record carrying a valid `name`; the open shape
  # accepts the full `mkTask`/`mkTaskGroup` value, not just `{ name }`.
  check (accepts c.TaskRef { name = "fetch_order" }),
  check (accepts c.TaskRef { name = "g", args = [], outputs = [], tasks = [] }),

  # EdgeKind — the three accepted kinds.
  check (accepts c.EdgeKind "control"),
  check (accepts c.EdgeKind "data"),
  check (accepts c.EdgeKind "loop"),
]
