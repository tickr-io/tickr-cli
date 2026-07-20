# tickr/dsl/loop.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkLoop` constructor: the author surface for a loop body of one or more
# tasks that cycle until one designated task's output says stop. `mkLoop`
# produces the same `{ name, tasks, edges, .. }` value `mkTaskGroup` does — a
# group-shaped value — so `mkWorkflow`'s existing group-flattening absorbs it
# with no `mkWorkflow` change and no hand-wiring.
#
# `mkLoop` auto-wires:
#   1. a `loop_control` routing variable (a reserved-name string routing
#      variable with the reserved value set `continue`/`done`/`fail`) on the
#      **producer** task only (default: the head). Single-producer holds — the
#      other body tasks never emit it, so they always park.
#   2. the **ring**: every consecutive pair plus last→first, each a
#      `kind = loop` back-edge gated `loop_control == continue`. `continue` is
#      the **default**, so the producer writes `loop_control` only on the turn
#      it decides to stop (an absent value means keep turning). For a single
#      task the ring degenerates to a `loop_control == continue` self-edge.
#   3. the **entry edge** into the head (from `entryFrom`, default the reserved
#      `Start` node) — emitted for a multi-task body so the ring is entered at
#      exactly one node (one node active at a time, a sequential ring). A
#      single-task self-loop omits it and relies on the builder's loop-aware
#      seal, keeping the single-task shape unchanged.
#   4. the **whole-body exit fan-in**: one `kind = data` edge whose sources are
#      every body node (`exitFrom`, default all `tasks`), gated
#      `loop_control == done`, into `exitTo` (default the reserved `End` node).
#      The downstream node runs once the loop body is done — every body node
#      Grounded(Success) AND the gate satisfied.
#
# It does NOT route through `mkTaskGroup`: the entry/exit edges target the
# reserved `Start`/`End` nodes, which `mkTaskGroup`'s edge-endpoint check
# (every endpoint names a task in the group) would reject. The group shape is
# built directly instead.
#
# `mkLoop` produces the **body only** — a judge or any downstream node is an
# ordinary task the author defines, and `exitTo` points at it. The `edge.ncl`
# rule "a gated edge carries an explicit `data`/`loop` kind" is kept; `mkLoop`
# fills the default gates.
#
# Imports are bound with a `Mod` suffix so they do not collide with the record
# field names `mkEdge` projects (`gate`, `kind`, `from`, `to`) — Nickel record
# fields are recursive, so a field named `gate` would shadow a `gate` import
# inside the same literal and recurse infinitely.
let contracts = import "contracts.ncl" in
let routingMod = import "routing.ncl" in
let gateMod = import "gate.ncl" in
let edgeMod = import "edge.ncl" in
let Identifier = contracts.Identifier in
let TaskRef = contracts.TaskRef in
let LoopControl = contracts.LoopControl in
let Nullable = contracts.Nullable in

# Closed input contract for `mkLoop`. The body is given as an ordered `tasks`
# list; `task` is the back-compatible single-task alias (`tasks = [ task ]`).
# `producer` designates the sole `loop_control` declarer (default: the head);
# `entryFrom` overrides the entry source (default `Start`); `exitFrom`
# overrides the exit fan-in sources (default: all body tasks); `exitTo`
# overrides the exit target (default `End`); `maxIterations` is carried but not
# enforced this slice.
let LoopInput = {
  name | Identifier,
  task | Nullable TaskRef,
  tasks | Nullable (Array TaskRef),
  producer | Nullable TaskRef,
  entryFrom | Nullable TaskRef,
  exitFrom | Nullable (Array TaskRef),
  exitTo | Nullable TaskRef,
  maxIterations | Nullable Number,
} in
{
  mkLoop = fun cfg =>
    let c =
      (
        {
          task | default = null,
          tasks | default = null,
          producer | default = null,
          entryFrom | default = null,
          exitFrom | default = null,
          exitTo | default = null,
          maxIterations | default = null,
        }
        & cfg
      ) | LoopInput
    in

    # Normalise the body to an ordered, non-empty list. `tasks` wins; `task` is
    # the single-task alias; neither present is an author error.
    let body =
      if c.tasks != null then
        c.tasks
      else if c.task != null then
        [ c.task ]
      else
        std.fail_with "mkLoop requires `tasks` (an ordered list) or `task` (a single task)"
    in
    let n = std.array.length body in
    let body = if n == 0 then std.fail_with "mkLoop `tasks` must be non-empty" else body in
    let head = std.array.first body in
    let producer = if c.producer != null then c.producer else head in

    # The reserved `loop_control` routing variable, declared on the producer
    # only (single-producer rule). It rides as a validated string — there is no
    # first-class enum routing-var type — and the runtime decodes the reserved
    # values.
    let loopControlVar = routingMod.mkRoutingVar { name = "loop_control", type = "string" } in
    let withLoopControl = fun t =>
      let existingRVs = std.record.get_or "routing_vars" [] t in
      let base =
        if std.record.has_field "routing_vars" t then std.record.remove "routing_vars" t else t
      in
      base & { routing_vars = existingRVs @ [ loopControlVar ] }
    in
    # Stamp `loop_control` on the producer (matched by name); other body tasks
    # are emitted unchanged.
    let bodyTasks =
      std.array.map (fun t => if t.name == producer.name then withLoopControl t else t) body
    in

    # The reserved gate values, pinned to the reserved set by the contract.
    let continueValue = ("continue" | LoopControl) in
    let doneValue = ("done" | LoopControl) in
    let continueGate =
      gateMod.mkPredicateGate { routing_var = loopControlVar, op = "Eq", value = continueValue }
    in
    let doneGate =
      gateMod.mkPredicateGate { routing_var = loopControlVar, op = "Eq", value = doneValue }
    in

    # The ring: every node → its successor (last → first), each a `kind = loop`
    # back-edge gated `continue`. For a single task this is the self-edge.
    let ringEdges =
      body
      |> std.array.map_with_index (fun i t =>
        let nxt = std.array.at (if i + 1 == n then 0 else i + 1) body in
        edgeMod.mkEdge { from = t, to = nxt, kind = "loop", gate = continueGate }
      )
    in

    # Entry edge into the head. Emitted when `entryFrom` is given, or — for a
    # multi-task body — from the reserved `Start` so the ring is entered at
    # exactly one node. A single-task default omits it (the loop-aware seal
    # wires `start → head`), keeping the single-task shape unchanged.
    let entryEdges =
      if c.entryFrom != null then
        [ edgeMod.mkEdge { from = c.entryFrom, to = head } ]
      else if n > 1 then
        [ edgeMod.mkEdge { from = { name = "Start" }, to = head } ]
      else
        []
    in

    # Whole-body exit fan-in: sources = all body nodes (or `exitFrom`), gated
    # `done`, into `exitTo` (default `End`). The fan-in is what makes the
    # downstream node run once the loop body is done.
    let exitSources = if c.exitFrom != null then c.exitFrom else body in
    let exitTarget = if c.exitTo != null then c.exitTo else { name = "End" } in
    let exitEdge =
      edgeMod.mkEdge { from = exitSources, to = exitTarget, kind = "data", gate = doneGate }
    in

    {
      name = c.name,
      args = [],
      tasks = bodyTasks,
      outputs = [],
      edges = entryEdges @ ringEdges @ [ exitEdge ],
    }
    & (if c.maxIterations != null then { max_iterations = c.maxIterations } else {}),
}
