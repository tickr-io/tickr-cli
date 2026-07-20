# tickr/dsl/workflow.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkWorkflow` constructor and its closed-record input contract. The
# workflow's firing trigger is declared solely through `triggerOn`.
let contracts = import "contracts.ncl" in
let Identifier = contracts.Identifier in
let Slug = contracts.Slug in
let Duration = contracts.Duration in
let Tags = contracts.Tags in
let Nullable = contracts.Nullable in
let CaptureDecl = contracts.CaptureDecl in

# The `triggerOn` value: the projected output of `mkTriggerOn`, a
# `kind`-tagged record. Typing the field against the constructor output
# (rather than freeform) means a hand-shaped trigger record is rejected at
# the call site; authors are pointed at `mkTriggerOn`. Dispatches on the
# wire `kind` and checks each kind's projected shape. Wrapped in `Nullable`
# because the default is `null` (no trigger / fire-now).
let TriggerOnConfig = std.contract.from_validator (fun value =>
  if !std.is_record value || !std.record.has_field "kind" value then
    'Error {
      message = "`triggerOn` must be an `mkTriggerOn` value",
      notes = [ "pass the value produced by `mkTriggerOn { kind = .. }`" ],
    }
  else if std.array.any (fun k => k == value.kind) [ "cron", "fire-now", "waits-on-signal" ] then
    'Ok
  else
    'Error {
      message = "unknown trigger kind `%{std.to_string value.kind}`",
      notes = [ "accepted kinds: `cron`, `fire-now`, `waits-on-signal`" ],
    }) in

# A `mkWorkflow.tasks` element. **Negative** guard: it rejects *only* a graph
# fragment — the tagged `{ kind = "graph-fragment", .. }` value a bare-verb
# combinator like `chain` returns — and blames with the fix: wrap it in
# `mkTaskGroup`. A bare fragment handed straight to `mkWorkflow` would
# otherwise ship verbatim as a nameless pseudo-group, since the entry was an
# untyped `Dyn`; `mkTaskGroup` stays the sole door a fragment may enter
# through. Rejecting fragments only means every element the `tasks` list
# tolerated before (a `mkTaskGroup` value, and anything else) keeps validating
# — this tightens nothing but the fragment ingress.
let WorkflowTaskEntry = std.contract.from_validator (fun value =>
  if std.is_record value
  && std.record.has_field "kind" value
  && value.kind == "graph-fragment" then
    'Error {
      message = "a graph fragment cannot be a `mkWorkflow` task on its own",
      notes = [ "wrap it in `mkTaskGroup`" ],
    }
  else
    'Ok) in

let WorkflowInput = {
  slug | Slug,
  name | Identifier,
  args | Array Dyn,
  tasks | Array WorkflowTaskEntry,
  outputs | Array Dyn,
  triggerOn | Nullable TriggerOnConfig,
  captures | Array CaptureDecl,
  timeout | Nullable Duration,
  tags | Tags,
} in

# Workflow-level integrity: the four cross-task invariants no smaller scope
# can express locally. Chained after the closed-record `WorkflowInput` shape
# so it runs first and this walk sees only well-shaped input. (A custom
# validator cannot record-merge (`&`) with a record contract in Nickel, so
# shape-then-integrity is sequenced through the contract pipe.)
#
# By this point the projected constructor outputs are in hand: a task group
# is `{ name, tasks = [..], edges = [..], .. }`; a task carries
# `routing_vars = [ { name, kind = "routing-var", type? }, .. ]`; a
# predicate gate on an edge is `{ kind = "predicate-gate", routing_var =
# <string>, op, value, .. }` (the `routing_var` is the bare `.name`).
#
# One pass collects every routing-var declaration (name → type → location)
# across all groups and tasks (invariant #1: single-producer uniqueness),
# then iterates every predicate gate to apply invariants #2/#3/#4 against
# that map. All violations accumulate into ONE error, one note each, so an
# author with several problems sees them all in a single round.
let WorkflowIntegrity = std.contract.from_validator (fun r =>
  let groups = r.tasks in

  # Every routing-var declaration, flattened to a record carrying its name,
  # its declared type (or null), and a `group → task` location string.
  let decls =
    groups
    |> std.array.flat_map (fun g =>
      let group_name = std.record.get_or "name" "<group>" g in
      let group_tasks = std.record.get_or "tasks" [] g in
      group_tasks
      |> std.array.flat_map (fun t =>
        let task_name = std.record.get_or "name" "<task>" t in
        (std.record.get_or "routing_vars" [] t)
        |> std.array.map (fun rv => {
          name = rv.name,
          type = std.record.get_or "type" null rv,
          location = "%{group_name} → %{task_name}",
        }))) in

  let declared_names = std.array.map (fun d => d.name) decls in

  # Invariant #1 — single-producer uniqueness. A name appearing in more than
  # one declaration is a duplicate; one note per duplicated name listing all
  # the `group → task` locations it was declared in.
  let dup_names =
    declared_names
    |> std.array.filter (fun n =>
      std.array.length (std.array.filter (fun m => m == n) declared_names) > 1)
    |> std.array.fold_right
      (fun n acc => if std.array.elem n acc then acc else acc @ [ n ])
      [] in
  let dup_notes =
    dup_names
    |> std.array.map (fun n =>
      let locs =
        decls
        |> std.array.filter (fun d => d.name == n)
        |> std.array.map (fun d => d.location)
      in
      "routing-var `%{n}` is declared more than once (single-producer): [%{std.string.join ", " locs}]") in

  # Look a declared routing-var up by name. Returns the first matching
  # declaration record or null.
  let lookup = fun name =>
    let matches = std.array.filter (fun d => d.name == name) decls in
    if std.array.length matches == 0 then null else std.array.first matches in

  # The scalar tag a value carries, mirroring the runtime routing-value
  # semantics: a number is `int`, a string is `string`, a bool is `bool`.
  let value_tag = fun v =>
    if std.is_number v then "int"
    else if std.is_string v then "string"
    else if std.is_bool v then "bool"
    else "non-scalar" in

  # Every predicate gate, flattened with its `group → edge[i]` location.
  let pred_gates =
    groups
    |> std.array.flat_map (fun g =>
      let group_name = std.record.get_or "name" "<group>" g in
      (std.record.get_or "edges" [] g)
      |> std.array.map_with_index (fun i e => { edge = e, location = "%{group_name} → edge[%{std.to_string i}]" })
      |> std.array.filter (fun x =>
        std.record.has_field "gate" x.edge
        && (std.record.get_or "kind" "" x.edge.gate) == "predicate-gate")
      |> std.array.map (fun x => { gate = x.edge.gate, location = x.location })) in

  # Invariants #2/#3/#4 per predicate gate, against the declared set.
  let gate_notes =
    pred_gates
    |> std.array.flat_map (fun pg =>
      let var_name = pg.gate.routing_var in
      let decl = lookup var_name in
      if decl == null then
        # Invariant #2 — the referenced routing-var is not declared anywhere.
        [ "predicate gate at %{pg.location} references routing-var `%{var_name}`, which no task declares" ]
      else if decl.type == null then
        # Type unset: scalar-admit by default; #3 and #4 both skipped.
        []
      else if decl.type == "array" then
        # Invariant #4 — scalar-only. Array (and any future non-scalar) types
        # exist for runtime edge-expansion, not for gating.
        [ "predicate gate at %{pg.location} references routing-var `%{var_name}` of type `%{decl.type}`: predicate gates are scalar-only (`%{decl.type}`-typed routing-vars exist for runtime edge-expansion, not gating)" ]
      else
        # Invariant #3 — value/type matching against the declared scalar type.
        # `bytes` has no DSL-authorable value today, so no scalar value can
        # match it; the mismatch note names the actual tag the value carries.
        let actual = value_tag pg.gate.value in
        if decl.type == actual then
          []
        else
          [ "predicate gate at %{pg.location} on routing-var `%{var_name}`: expected %{decl.type}, got %{actual}" ]) in

  let all_notes = dup_notes @ gate_notes in
  if std.array.length all_notes == 0 then
    'Ok
  else
    'Error {
      message = "workflow integrity check failed",
      notes = all_notes,
    }) in
{
  # `captures` declares JSONPath extractions from the inbound trigger
  # payload. `tags` is an attrset of operator-meaningful labels; keys must
  # not start with `tickr/` (enforced by the `Tags` contract). The workflow's
  # firing trigger is declared through `triggerOn`; absent it, fire-now.
  mkWorkflow = fun cfg =>
    let c = ({
      outputs | default = [],
      triggerOn | default = null,
      captures | default = [],
      timeout | default = null,
      tags | default = {},
    } & cfg) | WorkflowInput | WorkflowIntegrity in
    let base = {
      # `slug` is the immutable, author-owned half of the workflow's
      # `namespace.slug` identity; `name` is the free-to-rename display label.
      slug = c.slug,
      name = c.name,
      args = c.args,
      outputs = c.outputs,
      tasks = c.tasks,
      captures = c.captures,
    } in
    base
    & (if c.triggerOn != null then { triggerOn = c.triggerOn } else {})
    & (if c.timeout != null then { timeout = c.timeout } else {})
    & (if c.tags != {} then { tags = c.tags } else {}),
}
