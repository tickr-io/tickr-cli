# tickr/dsl/prune.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkPrune` constructor: drop one arm of a live `fork` / `branch` fan-out and
# let the server narrow the join's source set (arity N → N−1). An author names
# the arm by the identity code of its head — the node the fan-out anchor feeds
# (the four-symbol handle read off the exposed graph, or a full UUID) — never a
# hand-mapped UUID. The server discovers the whole arm (the region the head
# dominates), removes it, and narrows the join at apply; only a Pending, un-run
# arm is prunable — an arm holding a Running or Grounded node is stopped through
# cancellation, not excision.
#
# `mkPrune` is a fan-out Excision operator. It emits its document through the
# shared `excision.mkExcision` core, so it stays uniform with the sibling
# `mkCut` and the follow-on `mkTrim`/`mkTruncate` that reuse the same authoring
# end. The emitted document is one arm of the Patch tagged union:
# `{ prune = { arm }, reason? }`. A primitive-op patch uses the sibling `{ ops }`
# arm; other operations (`insert`/`chain`/`fork`/`branch`/`swap`/`loop`/`cut`)
# are their own arms.
let contracts = import "contracts.ncl" in
let excision = import "excision.ncl" in
let RegionRef = excision.RegionRef in
let Nullable = contracts.Nullable in

# Closed input contract for `mkPrune`: an unexpected field (a typo) blames here.
# `arm` is the head of the fan-out arm to drop (by identity code or UUID);
# `reason` is an optional why-string.
let PruneInput = {
  arm | RegionRef,
  reason | Nullable String,
} in
{
  mkPrune = fun cfg =>
    let c = ({ reason | default = null } & cfg) | PruneInput in
    excision.mkExcision "prune" { arm = c.arm } c.reason,
}
