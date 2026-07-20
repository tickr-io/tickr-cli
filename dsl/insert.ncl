# tickr/dsl/insert.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkInsert` constructor: the first Patch *operation*. An author names a
# live anchor structure by its short identity code (the four-symbol handle read
# off the exposed graph, or a full UUID) and supplies the `mkTask` to splice in
# after it. The server discovers the affected edges and lowers the interpose
# itself — the author composes no edge rewrite.
#
# The emitted document is one arm of the Patch tagged union: `{ insert = {
# anchor, task }, reason? }`. A primitive-op patch uses the sibling `{ ops }`
# arm; a future operation is a new arm, never a wire break. The inserted `task`
# is the full `mkTask` body — it is built through the nix pipeline before apply,
# so it must carry a real, buildable `nix_expression_path` (with a `#fragment`),
# which the `mkTask` contract already requires.
let contracts = import "contracts.ncl" in
let TaskRef = contracts.TaskRef in
let Nullable = contracts.Nullable in

# The anchor reference: an identity code (e.g. "AB12") or a full UUID string.
# It names a *runtime* structure resolved against the live graph at apply, so it
# is a plain string, not an `mkTask` value.
let AnchorRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "insert `anchor` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# Closed input contract for `mkInsert`: an unexpected field (a typo) blames
# here. `task` is a full `mkTask`/`mkTaskGroup` value (its whole body is
# emitted, not just its name); `reason` is an optional why-string.
let InsertInput = {
  anchor | AnchorRef,
  task | TaskRef,
  reason | Nullable String,
} in
{
  mkInsert = fun cfg =>
    let c = ({ reason | default = null } & cfg) | InsertInput in
    { insert = { anchor = c.anchor, task = c.task } }
    & (if c.reason != null then { reason = c.reason } else {}),
}
