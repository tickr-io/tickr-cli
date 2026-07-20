# tickr/dsl/expand.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkExpand` constructor: **expand** is sugar over **fork** — one arm per
# element of a map. Given a *known* collection (`over`, a map of element key →
# element value) and a per-element `arm` template, `mkExpand` instantiates one
# arm per map entry and emits exactly a `fork` document. The unroll is pure
# Nickel: instantiating a template over a statically-known collection needs no
# live graph, so a definition-time expand is fully desugared to a `fork` at
# parse — it rides the identical fork pipeline (server fans the anchor out to
# each arm's head, threads each arm, and re-seats the anchor's successors onto a
# barrier over every arm's tail).
#
# The map key becomes the arm's scope-local `handle` (so distinct elements yield
# distinct, per-scope-unique handles for free), and the map value is handed to
# the `arm` template. The default template treats the value *as* the arm's leaf
# task (`over` is then a map of handle → `mkTask`); supply an explicit `arm` to
# build a parameterised or nested (multi-task) arm per element.
#
# Because the emitted document is `{ fork = { anchor, arms } }`, the same
# element set applied definition-time (this unroll) and patch-time (the
# server-side `expand` lowering over a runtime map) produce an identical applied
# graph — the two altitudes share one target.
let contracts = import "contracts.ncl" in
let fork = import "fork.ncl" in
let Nullable = contracts.Nullable in

# The anchor reference: an identity code (e.g. "AB12") or a full UUID string.
# It names a *runtime* structure resolved against the live graph at apply.
let AnchorRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "expand `anchor` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# `over` must be a **map** — a record whose fields are the elements to fan over.
# Each field's name becomes an arm handle and its value is passed to `arm`.
let ElementMap =
  std.contract.from_validator (fun value =>
    if std.is_record value then
      'Ok
    else
      'Error { message = "expand `over` must be a map (a record of element key => element value)" }) in

# The default per-element template: the map value *is* the arm's leaf task, and
# the map key is the arm's handle. Override `arm` to parameterise the task per
# element or to build a nested (multi-task) arm.
let default_arm = fun key value => { handle = key, task = value } in

# Closed input contract for `mkExpand`: an unexpected field (a typo) blames here.
let ExpandInput = {
  anchor | AnchorRef,
  over | ElementMap,
  arm | default = default_arm,
  reason | Nullable String | default = null,
} in
{
  mkExpand = fun cfg =>
    let c = cfg | ExpandInput in
    # One arm per map entry, in sorted-key order (deterministic): the key is the
    # arm handle, the value is handed to the template.
    let built_arms =
      std.record.to_array c.over
      |> std.array.map (fun entry => c.arm entry.field entry.value)
    in
    # Desugar to a `fork` — expand adds no wire shape of its own at
    # definition-time; it reuses the fork constructor verbatim.
    fork.mkFork { anchor = c.anchor, arms = built_arms, reason = c.reason },
}
