# tickr/dsl/task.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# Task-level constructors: `mkTask` and `mkTaskGroup`. Each carries the
# closed-record input contract that shapes its `cfg` argument, colocated
# with the constructor that uses it.
#
# Always-present arguments carry defaults via a `| default`-annotated record
# merged under the caller's argument (caller wins). The merged record is
# then checked against the closed input contract: an unexpected field (a
# typo) blames at the call site, and the domain contracts on `name`,
# `timeout`, and `max_attempts` fire with a column pointer to the offending
# value. Genuinely optional fields (`timeout`, `routing_vars`, `emits`) carry
# Nickel `optional` field metadata rather than a null/empty sentinel: an
# omitted field is a first-class absence — it is given no default, so it
# simply does not appear in the constructed output.
let contracts = import "contracts.ncl" in
let Identifier = contracts.Identifier in
let Duration = contracts.Duration in
let MaxAttempts = contracts.MaxAttempts in
let RoutingVarRef = contracts.RoutingVarRef in
let TaskRef = contracts.TaskRef in

# An `inputs` entry mirrors the parser's mixed-shape binding: a bare string
# (documentation-only semantic, preserved verbatim) or a structured
# `{ name, from }` record carrying source information to the parser. The
# `from` source taxonomy (task/trigger/signal) is resolved downstream; this
# contract pins the entry to one of the two accepted shapes so a malformed
# element — a number, or a record missing `name`/`from` — blames at the call
# site instead of reaching the parser.
let InputBinding = std.contract.custom (fun label value =>
  if std.is_string value then
    'Ok value
  else if std.is_record value then
    'Ok (std.contract.apply { name | Identifier, from | Dyn } label value)
  else
    'Error {
      message = "input entry must be a bare string or a `{ name, from = .. }` record",
    }) in

# A projected emit value — the `kind`-tagged output of `mkSignalEmit`
# (`signal-emit`) or `mkSignalEmitOnFailure` (`signal-emit-on-failure`).
# Dispatches on `kind` so a hand-shaped record or a raw string on `emits`
# fails at the call site rather than riding through to the parser. Mirrors
# the `AnyGate` shape-check idiom in `contracts.ncl`.
let EmitRef = std.contract.from_validator (fun value =>
  if !std.is_record value then
    'Error { message = "emit must be an `mkSignalEmit`/`mkSignalEmitOnFailure` value" }
  else if !std.record.has_field "kind" value then
    'Error { message = "emit must be an `mkSignalEmit`/`mkSignalEmitOnFailure` value (missing `kind`)" }
  else if std.array.any (fun k => k == value.kind) [ "signal-emit", "signal-emit-on-failure" ] then
    'Ok
  else
    'Error {
      message = "unknown emit kind `%{std.to_string value.kind}`",
      notes = [ "accepted emits: `mkSignalEmit`, `mkSignalEmitOnFailure`" ],
    }) in

# A projected edge value — the `{ sources, targets, .. }` output of `mkEdge`
# (optional `kind`/`gate` ride along as the open tail). Pins each `edges`
# entry to the endpoint-array shape the group's endpoint-resolution walk
# reads, so a hand-shaped record or raw value blames at the call site.
let EdgeRef = { sources | Array String, targets | Array String, .. } in

# Closed input contract for `mkTask`. Lists every field `mkTask` accepts; an
# extra field is a typo and blames here. `timeout`, `routing_vars`, and
# `emits` carry `optional` metadata — an author simply omits them and they
# stay absent, so "unset" is a first-class absence rather than a sentinel.
# Each array field carries an element-typed contract matching its real wire
# shape (`args`/`outputs`/`secrets` are string lists; `inputs` is a mixed
# bare-string/`{ name, from }` list; `routing_vars`/`emits` hold projected
# `mkRoutingVar`/emit values) so a malformed element blames at the call site
# rather than surfacing as a runtime surprise in the conductor's deserialiser.
#
# No author-facing task-kind field is listed: an authored task is always the
# default `RegularTask`, and a `task_type`/kind field here blames as an extra
# field. Author-facing task-kind selection is deferred to the future
# SensorTask polling feature, which gives a non-default kind runtime meaning.
let TaskInput = {
  name | Identifier,
  nix_expression_path | String,
  args | Array String,
  outputs | Array String,
  inputs | Array InputBinding,
  secrets | Array String,
  routing_vars | Array RoutingVarRef | optional,
  emits | Array EmitRef | optional,
  max_attempts | MaxAttempts,
  timeout | Duration | optional,
} in

let TaskGroupInput = {
  name | Identifier,
  args | Array String,
  tasks | Array TaskRef,
  outputs | Array String,
  edges | Array EdgeRef,
} in

# Local integrity: an emit's source routing-var must live on the same task
# that emits it. Chained after `TaskInput` (shape runs first) so it walks
# only well-shaped input. At this point `emits`/`routing_vars` carry the projected
# constructor outputs: a `mkSignalEmit` is `{ kind = "signal-emit",
# from_routing_var = <string>, .. }` (failure-side emits have no
# `from_routing_var`), and a `mkRoutingVar` is `{ name, kind = "routing-var" }`.
# Multiple violations accumulate into one error, one note each.
let TaskEmitColocation = std.contract.from_validator (fun value =>
  # `routing_vars`/`emits` are `optional` and absent when the author omits
  # them; default to an empty list so the co-location walk stays uniform.
  let declared =
    std.array.map (fun rv => rv.name) (std.record.get_or "routing_vars" [] value) in
  let sources =
    std.record.get_or "emits" [] value
    |> std.array.filter (fun e => std.record.has_field "from_routing_var" e)
    |> std.array.map (fun e => e.from_routing_var)
  in
  let unknown = std.array.filter (fun s => !std.array.elem s declared) sources in
  if std.array.length unknown == 0 then
    'Ok
  else
    'Error {
      message = "emit references undeclared routing-var(s) on this task",
      notes =
        std.array.map
          (fun s =>
            "`from_routing_var = %{s}` is not declared in this task's routing_vars: [%{std.string.join ", " declared}]")
          unknown,
    }) in

# Local integrity: every edge endpoint must name a task declared in this
# group's `tasks` list — edges live on the group, so endpoint resolution is
# locally complete. Chained after `TaskGroupInput` (shape runs first). At this point
# `edges` carry projected `mkEdge` output (`{ sources, targets }` of `.name`
# strings) and `tasks` carry `mkTask`/`mkTaskGroup` output (`{ name, .. }`).
# Multiple violations accumulate into one error, one note each.
let TaskGroupEdgeEndpoints = std.contract.from_validator (fun value =>
  let declared = std.array.map (fun t => t.name) value.tasks in
  let endpoints =
    value.edges
    |> std.array.flat_map (fun e => e.sources @ e.targets)
  in
  let missing = std.array.filter (fun n => !std.array.elem n declared) endpoints in
  if std.array.length missing == 0 then
    'Ok
  else
    'Error {
      message = "edge endpoint names a task not in this group",
      notes =
        std.array.map
          (fun n =>
            "`%{n}` is not declared in this group's tasks: [%{std.string.join ", " declared}]")
          missing,
    }) in

# Fragment-gated structural detection for `mkTaskGroup`. A **contract factory**:
# it closes over `hasFragment` (was a graph fragment — a `chain` — ingested?) and
# `fragEdges` (the edges those fragments contributed, captured before the flatten
# folded them into the group's edge list). It runs its two checks **only when a
# fragment was ingested**, so a fragment-free group evaluates it as a no-op and
# every existing hand-wired file stays byte-identical.
#
#   * **Overlap** — a fragment must not contribute a `(source, target)` pair that
#     another edge in the group already draws. Edges are decomposed to endpoint
#     pairs (`sources × targets`), *not* compared whole-edge, so a barrier /
#     array-endpoint edge that shares a single pair with a fragment's control edge
#     is caught too. This is what gives the split-at-the-link idiom teeth: a
#     *parallel* gated edge placed beside a `chain`'s ungated edge over the same
#     pair is a silent bare-`Success` gate bypass under per-edge dispatch (the
#     entry edge fires while the gated one waits), so it is rejected — the author
#     must split the chain at the gated link instead.
#   * **Name uniqueness** — a task name must not appear more than once across the
#     flattened task set (e.g. a name shared by a fragment task and a loose task),
#     so two tasks never collide in the group's name namespace.
#
# `value` is the already shape-validated group (this contract is chained after
# `TaskGroupInput`), so `.tasks`/`.edges` are well-shaped; `fragEdges` are
# `mkEdge` outputs and equally well-shaped. Multiple violations of a kind
# accumulate into one error, one note each; each note blames the offending pair
# or name.
let TaskGroupFragmentIntegrity = fun hasFragment fragEdges =>
  std.contract.from_validator (fun value =>
    if !hasFragment then
      'Ok
    else
      # Every `(source, target)` pair an edge draws — the cartesian product of
      # its endpoint arrays, so a single barrier edge decomposes to one pair per
      # source.
      let edgePairs = fun e =>
        e.sources
        |> std.array.flat_map (fun s =>
          e.targets |> std.array.map (fun t => { source = s, target = t }))
      in
      # Keep first occurrence only, so a pair/name blamed by several edges/tasks
      # is noted once.
      let dedup =
        std.array.fold_left
          (fun acc x => if std.array.elem x acc then acc else acc @ [ x ])
          []
      in
      # A fragment-contributed pair is an overlap when at least two edges in the
      # group draw it (the fragment's own edge plus another). Comparing the count
      # against every edge's pair-set — not against whole edges — is what catches
      # the barrier-endpoint variant.
      let allPairLists = value.edges |> std.array.map edgePairs in
      let drawnByCount = fun p =>
        allPairLists
        |> std.array.filter (fun ps => std.array.elem p ps)
        |> std.array.length
      in
      let fragPairs = fragEdges |> std.array.flat_map edgePairs in
      let overlaps =
        fragPairs |> std.array.filter (fun p => drawnByCount p >= 2) |> dedup
      in
      let names = std.array.map (fun t => t.name) value.tasks in
      let dupNames =
        names
        |> std.array.filter (fun n =>
          (names |> std.array.filter (fun m => m == n) |> std.array.length) > 1)
        |> dedup
      in
      if std.array.length overlaps > 0 then
        'Error {
          message = "a chain fragment contributes an edge another edge in this group already draws",
          notes =
            std.array.map
              (fun p =>
                "`%{p.source} -> %{p.target}` is drawn by more than one edge in group `%{value.name}`; split the chain at a gated link instead of adding a parallel edge over the same pair")
              overlaps,
        }
      else if std.array.length dupNames > 0 then
        'Error {
          message = "task name declared more than once across this group's flattened tasks",
          notes =
            std.array.map
              (fun n =>
                "`%{n}` is declared by more than one task in group `%{value.name}`")
              dupNames,
        }
      else
        'Ok) in
{
  # `inputs` entries are either bare strings (documentation-only semantic,
  # preserved verbatim) or structured `{ name, from }` records carrying
  # source information through to the parser. The `InputBinding` contract
  # pins each entry to one of those two shapes; the `from` source taxonomy
  # (task/trigger/signal) stays resolved downstream. `mkTask` also validates
  # the field names, identifier, timeout grammar, and retry bounds.
  mkTask = fun cfg =>
    let c = ({
      outputs | default = [],
      inputs | default = [],
      secrets | default = [],
      max_attempts | default = 3,
    } & cfg) | TaskInput | TaskEmitColocation in
    # The author-facing field set already carries its wire names, so the
    # validated input is the wire output verbatim. The `optional` fields
    # (`timeout`, `routing_vars`, `emits`) ride through untouched — absent
    # when omitted, present when set — with no sentinel or merge branch.
    c,

  mkTaskGroup = fun cfg =>
    # A graph fragment (the tagged value a bare-verb authoring combinator like
    # `chain` returns) is flattened into loose tasks + edges *before* the shape
    # and endpoint-locality contracts run, so the group validates exactly the
    # tasks + edges a hand-wirer would have passed and the `graph-fragment` tag
    # never survives export. A fragment-free group is untouched — flatTasks and
    # fragEdges collapse to the original list, so existing files are byte-identical.
    let isFragment = fun x =>
      std.is_record x
      && std.record.has_field "kind" x
      && x.kind == "graph-fragment"
    in
    let rawTasks = std.record.get_or "tasks" [] cfg in
    let hasFragment = std.array.any isFragment rawTasks in
    # The edges each ingested fragment contributes, captured here — before the
    # flatten folds them into the group's edge list — so the fragment-gated
    # overlap detection can tell a fragment's pair from a hand-wired one. Empty
    # (and the detection a no-op) for a fragment-free group.
    let fragEdges =
      rawTasks |> std.array.flat_map (fun el => if isFragment el then el.edges else [])
    in
    let cfg =
      if hasFragment then
        let rawEdges = std.record.get_or "edges" [] cfg in
        let flatTasks =
          rawTasks |> std.array.flat_map (fun el => if isFragment el then el.tasks else [ el ])
        in
        let stripped = std.record.remove "tasks" cfg in
        let stripped =
          if std.record.has_field "edges" stripped then std.record.remove "edges" stripped else stripped
        in
        stripped & { tasks = flatTasks, edges = rawEdges @ fragEdges }
      else
        cfg
    in
    # Shape first (`TaskGroupInput`), then endpoint-locality, then the
    # fragment-gated overlap + name-uniqueness detection (a no-op unless a
    # fragment was ingested) — each sees well-shaped, endpoint-valid input.
    let c =
      ({ outputs | default = [], edges | default = [] } & cfg)
      | TaskGroupInput
      | TaskGroupEdgeEndpoints
      | TaskGroupFragmentIntegrity hasFragment fragEdges
    in
    {
      name = c.name,
      args = c.args,
      tasks = c.tasks,
      outputs = c.outputs,
      edges = c.edges,
    },
}
