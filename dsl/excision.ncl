# tickr/dsl/excision.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The **reusable excision authoring core** every Excision operator (`mkCut`,
# and the follow-on `mkPrune`/`mkTrim`/`mkTruncate`) shares. An excision names
# the region it removes by identity code (the four-symbol handle read off the
# exposed graph, or a full UUID) — never a hand-mapped UUID — and the server
# discovers the affected edges and heals the boundary at apply. This module
# carries the pieces common to all of them: the region-reference contract and
# the tagged-union document emitter.
#
# Each excision operator is one arm of the Patch tagged union — `mkCut` emits
# `{ cut = { target }, reason? }`, and the later operators emit their own
# `{ prune = … }` / `{ trim = … }` / `{ truncate = … }` arms — all built through
# `mkExcision op payload reason` so the arm shape stays uniform.
# A region reference: an identity code (e.g. "AB12") or a full UUID string. It
# names a *runtime* structure resolved against the live graph at apply, so it is
# a plain string, not an `mkTask` value.
let regionRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "an excision region must be an identity-code (or UUID) string read off the exposed graph",
      }) in
{
  RegionRef = regionRef,

  # Emit an excision operation's tagged-union document: `{ <op> = <payload> }`,
  # plus a top-level `reason` when supplied. `op` is the operator's arm key
  # ("cut" / "prune" / "trim" / "truncate"); `payload` is its region record
  # (e.g. `{ target = "AB12" }`); `reason` is the optional why-string (or null).
  # The one place the arm shape is minted, so every excision operator emits an
  # identically-shaped document.
  mkExcision = fun op payload reason =>
    # Bind `reason` to a local so the `{ reason = r }` field does not recursively
    # reference itself (Nickel records are recursive; a bare `reason` on the RHS
    # would resolve to the field, not this parameter).
    let r = reason in
    { "%{op}" = payload }
    & (if r != null then { reason = r } else {}),
}
