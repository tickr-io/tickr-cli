# tickr/dsl/authoring.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# Definition-time authoring combinators: bare-verb functions over `mkTask`
# values that return a **graph fragment** — a tagged
# `{ kind = "graph-fragment", tasks, edges }` value carrying the tasks plus the
# control edges threading them. Naming convention: `mk*` returns a wire
# document; a bare verb returns a fragment. A fragment never survives export —
# the group that ingests it (`mkTaskGroup`) flattens the tasks into its task
# list and the edges into its edge list, then strips the tag.
#
# `chain [ a, b, c ]` takes a plain array of task values (which already own
# their names) and threads them in array order: the author declares the order
# **once** and the ungated control edges `a → b → c` are derived from array
# position, so a spine's order cannot drift between a `tasks` list and a
# hand-threaded `edges` list.
#
# `chain` is **closed under nesting**: an element may itself be a graph
# fragment (another `chain`), not only a bare task. Each bare-task element
# lifts to a singleton fragment; `chain` concatenates the elements' task lists
# and links each **boundary** — last task of element *i* → first task of
# element *i+1* — with exactly one ungated control edge, so a sequence can be
# built from reusable sub-sequences and the boundaries wire themselves. Each
# inner fragment's own internal edges are preserved unchanged.
let contracts = import "contracts.ncl" in
let edgeMod = import "edge.ncl" in
let TaskRef = contracts.TaskRef in

# A graph fragment is the tagged value a bare-verb combinator returns; an
# element carrying this tag is a nested sub-sequence rather than a bare task.
let isFragment = fun x =>
  std.is_record x
  && std.record.has_field "kind" x
  && x.kind == "graph-fragment"
in
{
  # `chain` threads its elements into a graph fragment. Each seam is one
  # `mkEdge { from = last-of-i, to = first-of-(i+1) }` — the same ungated
  # control edge a hand-wirer would draw, so a `chain`-authored spine lowers to
  # its hand-wired twin's wire shape once `mkTaskGroup` flattens it.
  chain = fun rawElements =>
    # Normalise every element to a `{ tasks, edges }` fragment: a nested
    # fragment rides through as-is (its internal edges preserved); a bare
    # `mkTask` value lifts to a singleton fragment (one task, no edges). A
    # non-task, non-fragment element blames at the `TaskRef` pipe. Names here
    # are distinct from the `tasks`/`edges` record fields below because Nickel
    # record fields are recursive — a binding named `tasks` would recurse.
    let frags =
      rawElements
      |> std.array.map (fun el =>
        if isFragment el then
          { tasks = el.tasks, edges = el.edges }
        else
          { tasks = [ (el | TaskRef) ], edges = [] })
    in
    # The fragment's task list is the concatenation of every element's tasks in
    # array order.
    let allTasks = frags |> std.array.flat_map (fun f => f.tasks) in
    # Every inner fragment's own control edges survive into the flattened edge
    # set unchanged.
    let innerEdges = frags |> std.array.flat_map (fun f => f.edges) in
    # One ungated control edge per seam. Boundary endpoints are **derived** by
    # array order — last/first task of the adjacent fragments — never reached
    # into a nested fragment's task list by index (that would be index-coupled
    # silent rewiring). No boundary is doubled and none is missed.
    let n = std.array.length frags in
    let lastTaskOf = fun f => std.array.at (std.array.length f.tasks - 1) f.tasks in
    let firstTaskOf = fun f => std.array.at 0 f.tasks in
    let boundaryEdges =
      if n <= 1 then
        []
      else
        std.array.range 0 (n - 1)
        |> std.array.map (fun i =>
          edgeMod.mkEdge {
            from = lastTaskOf (std.array.at i frags),
            to = firstTaskOf (std.array.at (i + 1) frags),
          })
    in
    { kind = "graph-fragment", tasks = allTasks, edges = innerEdges @ boundaryEdges },
}
