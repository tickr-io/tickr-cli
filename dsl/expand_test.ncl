# tickr/dsl/expand_test.ncl
#
# Positive-path tests for the `mkExpand` constructor: a definition-time expand
# over a *known* map unrolls to a `fork` document at parse — one arm per map
# element — reusing the fork pipeline verbatim. The emitted document is exactly
# `{ fork = { anchor, arms } }`, so an expand and the equivalent hand-authored
# fork over the same element set are byte-identical. Run via `just dsl-test`.
let { mkTask, mkExpand, mkFork, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in
let arm_task = fun n =>
  mkTask {
    name = n,
    args = [ "run" ],
    nix_expression_path = "flake#%{n}",
    outputs = [ "report" ],
  }
in
let task_l = arm_task "l" in
let task_r = arm_task "r" in
[
  # The default template: `over` is a map of handle => leaf task. Each map entry
  # becomes one arm (key = handle, value = task), and the whole unrolls to a
  # `fork` — expand is sugar over fork.
  check (
    mkExpand { anchor = "AB12", over = { l = task_l, r = task_r } }
    == { fork = { anchor = "AB12", arms = [ { handle = "l", task = task_l }, { handle = "r", task = task_r } ] } }
  ),

  # An expand over a known map is *identical* to the equivalent hand-authored
  # fork over the same element set — the two share one target.
  check (
    mkExpand { anchor = "AB12", over = { l = task_l, r = task_r } }
    == mkFork { anchor = "AB12", arms = [ { handle = "l", task = task_l }, { handle = "r", task = task_r } ] }
  ),

  # A per-element `arm` template parameterises the arm from the map value: here
  # each value carries a task, and the template names the arm by its key. The
  # unroll is still a plain `fork` document.
  check (
    let task_one = arm_task "one" in
    let task_two = arm_task "two" in
    (mkExpand {
      anchor = "AB12",
      over = { one = task_one, two = task_two },
      arm = fun key value => { handle = "arm_%{key}", task = value },
    }).fork.arms
    == [ { handle = "arm_one", task = task_one }, { handle = "arm_two", task = task_two } ]
  ),

  # A template may build a nested (multi-task) arm per element — expand carries
  # the same scope-tree freight fork does.
  check (
    let task_a1 = arm_task "a1" in
    let task_a2 = arm_task "a2" in
    (mkExpand {
      anchor = "AB12",
      over = { a = { first = task_a1, second = task_a2 } },
      arm = fun key value => {
        handle = key,
        steps = [ { handle = "s1", task = value.first }, { handle = "s2", task = value.second } ],
      },
    }).fork.arms
    == [
      { handle = "a", steps = [ { handle = "s1", task = task_a1 }, { handle = "s2", task = task_a2 } ] },
    ]
  ),

  # A `reason` rides the emitted fork document top-level alongside the operation.
  check (
    mkExpand { anchor = "AB12", over = { l = task_l }, reason = "fan out over discovered set" }
    == {
      fork = { anchor = "AB12", arms = [ { handle = "l", task = task_l } ] },
      reason = "fan out over discovered set",
    }
  ),
]
