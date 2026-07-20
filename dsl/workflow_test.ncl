# tickr/dsl/workflow_test.ncl
#
# Positive-path tests for `mkWorkflow`, covering the `triggerOn` accept paths
# (set, and absent → fire-now) and the WorkflowIntegrity invariants.
let { mkTask, mkTaskGroup, mkWorkflow, mkRoutingVar, mkPredicateGate, mkEdge, chain, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let tg = mkTaskGroup {
  name = "g", args = [], tasks = [ mkTask { name = "t", args = [], nix_expression_path = "x" } ],
} in

# A `chain` fragment properly wrapped in `mkTaskGroup` is accepted by
# `mkWorkflow` — the `WorkflowTaskEntry` guard rejects only bare fragments, so
# a wrapped one flows through and flattens byte-identically to its hand-wired
# twin. (The reject path — a bare fragment straight to `mkWorkflow.tasks` — is
# `tests/rejected/mkWorkflow_bare_fragment.ncl`.)
let wc_a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let wc_b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let wrapped_chain = mkWorkflow {
  slug = "wf", name = "w", args = [],
  tasks = [ mkTaskGroup { name = "g", args = [], outputs = [], tasks = [ chain [ wc_a, wc_b ] ] } ],
} in
let fire_now = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ tg ], triggerOn = { kind = "fire-now" } } in
let bare = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ tg ] } in
let tagged = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ tg ], tags = { env = "prod" } } in

# --- WorkflowIntegrity happy paths -----------------------------------------
# A small typed producer→consumer group reused across the invariant tests.
# `amount` is an `int` routing-var declared on the producer; the gated edge
# references it with a matching `int` value.
let amount = mkRoutingVar { name = "amount", type = "int" } in
let mk_group = fun gate_value rv =>
  let producer = mkTask { name = "producer", args = [], nix_expression_path = "p", routing_vars = [ rv ] } in
  let consumer = mkTask { name = "consumer", args = [], nix_expression_path = "q" } in
  mkTaskGroup {
    name = "grp", args = [], tasks = [ producer, consumer ],
    edges = [ mkEdge { from = producer, to = consumer, kind = "data", gate = mkPredicateGate { routing_var = rv, op = "Gt", value = gate_value } } ],
  } in

# Clean workflow exercising all four invariants at once: a single producer
# of `amount`, a predicate gate referencing it, a matching int value, and a
# scalar (non-array) type.
let clean = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ mk_group 5 amount ], triggerOn = { kind = "fire-now" } } in

# Invariant #1 happy path — two distinct routing-var names across two tasks.
let v_a = mkRoutingVar { name = "alpha" } in
let v_b = mkRoutingVar { name = "beta" } in
let unique_names = mkWorkflow { slug = "wf",
  name = "w", args = [],
  tasks = [ mkTaskGroup { name = "grp", args = [],
    tasks = [
      mkTask { name = "a", args = [], nix_expression_path = "p", routing_vars = [ v_a ] },
      mkTask { name = "b", args = [], nix_expression_path = "q", routing_vars = [ v_b ] },
    ], edges = [] } ],
  triggerOn = { kind = "fire-now" },
} in

# Invariant #2 happy path — a predicate gate referencing a declared var.
let ref_declared = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ mk_group 1 (mkRoutingVar { name = "count" }) ], triggerOn = { kind = "fire-now" } } in

# Invariant #3 happy path — bool value against a `bool`-typed routing-var.
let flag = mkRoutingVar { name = "flag", type = "bool" } in
let bool_producer = mkTask { name = "producer", args = [], nix_expression_path = "p", routing_vars = [ flag ] } in
let bool_consumer = mkTask { name = "consumer", args = [], nix_expression_path = "q" } in
let type_match = mkWorkflow { slug = "wf",
  name = "w", args = [],
  tasks = [ mkTaskGroup { name = "grp", args = [], tasks = [ bool_producer, bool_consumer ],
    edges = [ mkEdge { from = bool_producer, to = bool_consumer, kind = "data", gate = mkPredicateGate { routing_var = flag, op = "Eq", value = true } } ] } ],
  triggerOn = { kind = "fire-now" },
} in

# Invariant #4 happy path — a scalar (`string`) typed predicate reference.
let scalar_ref = mkWorkflow { slug = "wf", name = "w", args = [], tasks = [ mk_group "gt" (mkRoutingVar { name = "label", type = "string" }) ], triggerOn = { kind = "fire-now" } } in
[
  # triggerOn alone projects.
  check (fire_now.triggerOn == { kind = "fire-now" }),

  # neither set (fire-now) — triggerOn absent.
  check (!std.record.has_field "triggerOn" bare),

  # tags project; empty tags omitted.
  check (tagged.tags == { env = "prod" }),
  check (!std.record.has_field "tags" bare),
  check (bare.slug == "wf"),

  # WorkflowIntegrity: a workflow passing all four invariants evaluates and
  # projects the gate's routing-var reference down to its bare name.
  check (clean.slug == "wf"),
  check ((std.array.first (std.array.first clean.tasks).edges).gate.routing_var == "amount"),

  # Invariant #1 — distinct routing-var names across tasks evaluate cleanly.
  check (unique_names.slug == "wf"),

  # Invariant #2 — a predicate gate referencing a declared var evaluates.
  check (ref_declared.slug == "wf"),

  # Invariant #3 — a bool value against a `bool`-typed var matches.
  check ((std.array.first (std.array.first type_match.tasks).edges).gate.value == true),

  # Invariant #4 — a scalar (`string`) typed predicate reference is admitted.
  check (scalar_ref.slug == "wf"),

  # A `chain` fragment wrapped in `mkTaskGroup` passes the `WorkflowTaskEntry`
  # guard — the group flattens it and `mkWorkflow` accepts the group. The guard
  # rejects fragments only, so wrapping is the accepted path.
  check (wrapped_chain.slug == "wf"),
  check ((std.array.first wrapped_chain.tasks).name == "g"),
  # A plain task group (no fragment) is still a valid `tasks` element — the
  # guard adds no positive tightening beyond rejecting bare fragments.
  check (bare.slug == "wf" && (std.array.first bare.tasks).name == "g"),
]
