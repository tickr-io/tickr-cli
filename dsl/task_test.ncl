# tickr/dsl/task_test.ncl
#
# Positive-path tests for `mkTask` / `mkTaskGroup`. Asserts the accepted
# shapes evaluate and project onto the expected output fields. Negative
# cases live under `tests/rejected/`.
let { mkTask, mkTaskGroup, mkEdge, mkRoutingVar, mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let minimal = mkTask { name = "build", args = [], nix_expression_path = "x" } in
let full = mkTask {
  name = "ship",
  args = [],
  nix_expression_path = "x",
  outputs = [ "digest" ],
  timeout = "5m",
  max_attempts = 2,
} in
let group = mkTaskGroup { name = "g", args = [], tasks = [ minimal, full ] } in

# A task whose every emit references a routing-var declared on the same task.
let r1 = mkRoutingVar { name = "r1" } in
let r2 = mkRoutingVar { name = "r2" } in
let colocated = mkTask {
  name = "emitter",
  args = [],
  nix_expression_path = "x",
  routing_vars = [ r1, r2 ],
  emits = [
    mkSignalEmit { signal = mkSignal { name = "s1" }, from_routing_var = r1 },
    mkSignalEmit { signal = mkSignal { name = "s2" }, from_routing_var = r2 },
  ],
} in

# A task with a mixed `inputs` list: a bare string (documentation-only) and a
# structured `{ name, from }` binding. Both accepted element shapes project
# through the tightened `inputs` contract verbatim.
let mixed_inputs = mkTask {
  name = "consume",
  args = [],
  nix_expression_path = "x",
  inputs = [ "raw_note", { name = "upstream", from = { task = { name = "produce" } } } ],
} in

# A task group whose every edge endpoint names a task declared in the group.
let a = mkTask { name = "a", args = [], nix_expression_path = "x" } in
let b = mkTask { name = "b", args = [], nix_expression_path = "x" } in
let c = mkTask { name = "c", args = [], nix_expression_path = "x" } in
let wired = mkTaskGroup {
  name = "wired",
  args = [],
  tasks = [ a, b, c ],
  edges = [
    mkEdge { from = a, to = b },
    mkEdge { from = [ a, b ], to = c },
  ],
} in
[
  # Required fields project; defaults fill the rest.
  check (minimal.name == "build"),
  check (minimal.max_attempts == 3),
  # Omitted optional fields carry `optional` metadata: unset is a first-class
  # absence, never a null/empty sentinel baked into the output.
  check (!std.record.has_field "timeout" minimal),
  check (!std.record.has_field "routing_vars" minimal),
  check (!std.record.has_field "emits" minimal),
  # No author-facing task kind: the constructor emits no `task_type`, so the
  # boundary fills the enum default (`RegularTask`). Non-default kinds are
  # reachable only via system paths, never the DSL.
  check (!std.record.has_field "task_type" minimal),

  # Optional fields project when set.
  check (full.timeout == "5m"),
  check (full.max_attempts == 2),
  check (full.outputs == [ "digest" ]),

  # mkTaskGroup.
  check (std.array.length group.tasks == 2),
  check (group.edges == []),

  # Mixed `inputs`: a bare string and a structured `{ name, from }` binding
  # both ride through the tightened element contract unchanged.
  check (std.array.length mixed_inputs.inputs == 2),
  check (std.array.at 0 mixed_inputs.inputs == "raw_note"),
  check ((std.array.at 1 mixed_inputs.inputs).name == "upstream"),

  # Co-located emits pass the local integrity walk and project to their
  # wire shapes (bare-string `from_routing_var`).
  check (std.array.length colocated.emits == 2),
  check ((std.array.at 0 colocated.emits).from_routing_var == "r1"),
  check ((std.array.at 1 colocated.emits).from_routing_var == "r2"),

  # Valid multi-edge group: every endpoint resolves to a declared task.
  check (std.array.length wired.edges == 2),
  check ((std.array.at 0 wired.edges).sources == [ "a" ]),
  check ((std.array.at 1 wired.edges).sources == [ "a", "b" ]),
  check ((std.array.at 1 wired.edges).targets == [ "c" ]),
]
