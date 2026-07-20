# tickr/dsl/edge.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkEdge` constructor, its closed-record input contract, and the
# gate/kind cross-field rule. An edge connects task references (`from` →
# `to`); a `gate` makes the edge conditional. The cross-field rule ties
# `kind` to the presence of a `gate`: a plain control edge has no gate, and
# a gated edge must declare an explicit `data` or `loop` kind.
let contracts = import "contracts.ncl" in
let TaskRef = contracts.TaskRef in
let EdgeKind = contracts.EdgeKind in
let AnyGate = contracts.AnyGate in
let Nullable = contracts.Nullable in

let as_list = fun x => if std.is_array x then x else [ x ] in

# A `from`/`to` endpoint: a single `TaskRef` or an array of them. A raw
# string (a common mistake — passing a task name instead of the `mkTask`
# value) is rejected here naming the `TaskRef` constraint, rather than
# producing a generic any-of blame.
let Endpoint = std.contract.custom (fun label value =>
  if std.is_array value then
    'Ok (std.contract.apply (Array TaskRef) label value)
  else if std.is_record value then
    'Ok (std.contract.apply TaskRef label value)
  else
    'Error {
      message = "edge endpoint must be a `TaskRef` (an `mkTask`/`mkTaskGroup` value) or an array of them, not a raw string",
    }) in

let EdgeInput = {
  from | Endpoint,
  to | Endpoint,
  kind | Nullable EdgeKind,
  gate | Nullable AnyGate,
} in

# The gate/kind cross-field rule. Runs on the merged-but-raw input where
# an omitted `kind` is still `null` (the constructor sets the default after
# this walk), so "omitted" and "explicit `control`" stay distinguishable.
let GateKindIntegrity = std.contract.from_validator (fun value =>
  let has_gate = value.gate != null in
  let kind = value.kind in
  if has_gate then
    if kind == null then
      'Error {
        message = "edge has a gate; `kind` must be explicit",
        notes = [ "set `kind = \"data\"` or `kind = \"loop\"` on a gated edge" ],
      }
    else if kind == "control" then
      'Error {
        message = "gated edges cannot be `control`-kind; `kind` must be one of `data`, `loop`, got `control`",
      }
    else 'Ok
  else
    if kind == "data" then
      'Error { message = "edge has no gate; `kind = \"data\"` requires a gate" }
    else if kind == "loop" then
      'Error { message = "edge has no gate; `kind = \"loop\"` requires a gate" }
    else 'Ok) in
{
  # `mkEdge` connects task references and, when gated, carries a gate plus
  # the `data`/`loop` kind that the gate implies. `from`/`to` accept a
  # single `TaskRef` or an array; both project to the wire `sources`/
  # `targets` arrays of `.name` strings. `kind` is omitted on the wire for
  # plain control edges (preserving the prior wire shape) and emitted
  # explicitly for gated edges.
  mkEdge = fun cfg =>
    let c =
      ({ kind | default = null, gate | default = null } & cfg)
      | EdgeInput
      | GateKindIntegrity
    in
    let srcs = std.array.map (fun t => t.name) (as_list c.from) in
    let tgts = std.array.map (fun t => t.name) (as_list c.to) in
    { sources = srcs, targets = tgts }
    & (if c.kind != null then { kind = c.kind } else {})
    & (if c.gate != null then { gate = c.gate } else {}),
}
