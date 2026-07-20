# tickr/dsl/trim.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkTrim` constructor: remove a bounded, directional leading run of live
# `Pending` HyperNodes up to an anchor (the `trim-until` bound) and let the server
# reconnect `Start → anchor` so the surviving frontier stays reachable. An author
# names the anchor by its short identity code (the four-symbol handle read off the
# exposed graph, or a full UUID) — never a hand-mapped UUID. The server discovers
# the run exclusively upstream of the anchor, removes it, and heals the boundary at
# apply; only a Pending, un-run run is excisable — a Running or Grounded node is
# stopped through cancellation, not excision.
#
# `mkTrim` is a linear Excision operator. It emits its document through the shared
# `excision.mkExcision` core, so it stays uniform with the sibling `mkCut`/`mkPrune`
# and the mirror `mkTruncate` that reuse the same authoring end. The emitted
# document is one arm of the Patch tagged union: `{ trim = { anchor }, reason? }`.
# A primitive-op patch uses the sibling `{ ops }` arm; other operations
# (`insert`/`chain`/`fork`/`branch`/`swap`/`loop`/`cut`/`prune`/`truncate`) are
# their own arms.
let contracts = import "contracts.ncl" in
let excision = import "excision.ncl" in
let RegionRef = excision.RegionRef in
let Nullable = contracts.Nullable in

# Closed input contract for `mkTrim`: an unexpected field (a typo) blames here.
# `anchor` is the `trim-until` bound (by identity code or UUID); `reason` is an
# optional why-string.
let TrimInput = {
  anchor | RegionRef,
  reason | Nullable String,
} in
{
  mkTrim = fun cfg =>
    let c = ({ reason | default = null } & cfg) | TrimInput in
    excision.mkExcision "trim" { anchor = c.anchor } c.reason,
}
