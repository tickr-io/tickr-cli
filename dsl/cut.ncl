# tickr/dsl/cut.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkCut` constructor: excise a live `Pending` mid-path HyperNode and let the
# server bridge the flow across the gap (`… → pred → target → succ → …` becomes
# `… → pred → succ → …`). An author names the target by its short identity code
# (the four-symbol handle read off the exposed graph, or a full UUID) — never a
# hand-mapped UUID. The server discovers the incident edges, removes the node,
# and mints the bridge at apply; only a Pending, un-run target is excisable — a
# Running or Grounded one is stopped through cancellation, not excision.
#
# `mkCut` is the single-node Excision operator. It emits its document through the
# shared `excision.mkExcision` core, so it stays uniform with the follow-on
# `mkPrune`/`mkTrim`/`mkTruncate` that reuse the same authoring end. The emitted
# document is one arm of the Patch tagged union: `{ cut = { target }, reason? }`.
# A primitive-op patch uses the sibling `{ ops }` arm; other operations
# (`insert`/`chain`/`fork`/`swap`/`loop`) are their own arms.
let contracts = import "contracts.ncl" in
let excision = import "excision.ncl" in
let RegionRef = excision.RegionRef in
let Nullable = contracts.Nullable in

# Closed input contract for `mkCut`: an unexpected field (a typo) blames here.
# `target` is the mid-path node to excise (by identity code or UUID); `reason` is
# an optional why-string.
let CutInput = {
  target | RegionRef,
  reason | Nullable String,
} in
{
  mkCut = fun cfg =>
    let c = ({ reason | default = null } & cfg) | CutInput in
    excision.mkExcision "cut" { target = c.target } c.reason,
}
