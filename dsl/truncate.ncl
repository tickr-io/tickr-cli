# tickr/dsl/truncate.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkTruncate` constructor: remove the entire tail of live `Pending`
# HyperNodes downstream of an anchor and let the server reconnect `anchor → End`
# so the surviving frontier reaches `End` (no orphan; path-to-End preserved). An
# author names the anchor by its short identity code (the four-symbol handle read
# off the exposed graph, or a full UUID) — never a hand-mapped UUID. The server
# discovers the tail exclusively downstream of the anchor, removes it, and heals
# the boundary at apply; only a Pending, un-run tail is excisable — a Running or
# Grounded node is stopped through cancellation, not excision.
#
# `mkTruncate` is the mirror of `mkTrim` (which removes the leading run upstream of
# its anchor). It emits its document through the shared `excision.mkExcision` core,
# so it stays uniform with the sibling `mkCut`/`mkPrune`/`mkTrim` that reuse the
# same authoring end. The emitted document is one arm of the Patch tagged union:
# `{ truncate = { anchor }, reason? }`. A primitive-op patch uses the sibling
# `{ ops }` arm; other operations
# (`insert`/`chain`/`fork`/`branch`/`swap`/`loop`/`cut`/`prune`/`trim`) are their
# own arms.
let contracts = import "contracts.ncl" in
let excision = import "excision.ncl" in
let RegionRef = excision.RegionRef in
let Nullable = contracts.Nullable in

# Closed input contract for `mkTruncate`: an unexpected field (a typo) blames here.
# `anchor` is the node whose downstream tail is lopped off (by identity code or
# UUID); `reason` is an optional why-string.
let TruncateInput = {
  anchor | RegionRef,
  reason | Nullable String,
} in
{
  mkTruncate = fun cfg =>
    let c = ({ reason | default = null } & cfg) | TruncateInput in
    excision.mkExcision "truncate" { anchor = c.anchor } c.reason,
}
