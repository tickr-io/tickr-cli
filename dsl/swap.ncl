# tickr/dsl/swap.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkSwap` constructor: substitute a live `Pending` HyperNode with a
# replacement *in place*. An author names the target structure by its short
# identity code (the four-symbol handle read off the exposed graph, or a full
# UUID) and supplies the replacement `mkTask`. The server re-seats the target's
# incident edges onto the replacement and removes the target — a substitution,
# not a raw edge rewrite. Only a Pending, un-minted target is removable; a
# Running or Grounded target is declined structurally at apply (in-flight or
# finished work is cancelled, never silently excised).
#
# The emitted document is one arm of the Patch tagged union: `{ swap = {
# target, task }, reason? }`. A primitive-op patch uses the sibling `{ ops }`
# arm; other operations (`insert`/`chain`/`fork`/`expand`) are their own arms.
# The replacement `task` is the full `mkTask` body — it is built through the nix
# pipeline before apply, so it must carry a real, buildable `nix_expression_path`
# (with a `#fragment`), which the `mkTask` contract already requires.
let contracts = import "contracts.ncl" in
let TaskRef = contracts.TaskRef in
let Nullable = contracts.Nullable in

# The target reference: an identity code (e.g. "AB12") or a full UUID string.
# It names a *runtime* structure resolved against the live graph at apply, so it
# is a plain string, not an `mkTask` value.
let TargetRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "swap `target` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# Closed input contract for `mkSwap`: an unexpected field (a typo) blames here.
# `task` is a full `mkTask`/`mkTaskGroup` value (its whole body is emitted, not
# just its name); `reason` is an optional why-string.
let SwapInput = {
  target | TargetRef,
  task | TaskRef,
  reason | Nullable String,
} in
{
  mkSwap = fun cfg =>
    let c = ({ reason | default = null } & cfg) | SwapInput in
    { swap = { target = c.target, task = c.task } }
    & (if c.reason != null then { reason = c.reason } else {}),
}
