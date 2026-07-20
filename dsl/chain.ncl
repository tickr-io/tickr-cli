# tickr/dsl/chain.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkChain` constructor: interpose a *sequence* of one or more tasks after a
# live anchor (the shipped `mkInsert` is its single-step case). An author names
# the anchor by its short identity code (the four-symbol handle read off the
# exposed graph, or a full UUID) and supplies a **scope-tree** of steps — each a
# scope-local `handle` plus either a leaf `task` (built at-patch) or a nested
# `steps` sub-chain forming a child scope. The server discovers the affected
# edges, threads `anchor → step₁ → … → stepₖ → (anchor's successors)`, and mints
# one node id per leaf task — the author writes only handles and codes, never a
# hand-mapped UUID.
#
# The emitted document is one arm of the Patch tagged union:
# `{ chain = { anchor, steps }, reason? }`. A primitive-op patch uses `{ ops }`;
# a single insert uses `{ insert }`; a future operation is a new arm.
let contracts = import "contracts.ncl" in
let Nullable = contracts.Nullable in

# The anchor reference: an identity code (e.g. "AB12") or a full UUID string.
# It names a *runtime* structure resolved against the live graph at apply.
let AnchorRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "chain `anchor` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# One chain step: a scope-local `handle` plus exactly one of a leaf `task` (a
# full `mkTask` body, built at-patch) or a nested `steps` sub-chain. Validated
# structurally so a typo blames here; the conductor re-checks per-scope handle
# uniqueness and the nested shape recursively.
let ChainStep =
  std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "a chain step must be a record { handle, task | steps }" }
    else if !std.record.has_field "handle" value then
      'Error { message = "a chain step must carry a `handle`" }
    else
      let has_task = std.record.has_field "task" value in
      let has_steps = std.record.has_field "steps" value in
      if has_task && has_steps then
        'Error { message = "a chain step carries either `task` or `steps`, not both" }
      else if !has_task && !has_steps then
        'Error { message = "a chain step must carry a leaf `task` or a nested `steps` sub-chain" }
      else
        'Ok) in

# The optional per-edge **relay** spec, for a chain whose anchor sources a gated
# `kind=Data` edge: `{ edge, var }` names the data edge to relay across (by
# identity code or UUID) and the routing variable its `PredicateHolds` gate keys
# on. The server field-swaps that gate onto the chain's head when the variable is
# not yet produced (the variable-absent arm); its absence is a named reject.
let RelaySpec =
  std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "chain `relay` must be a record { edge, var }" }
    else if !std.record.has_field "edge" value || !std.record.has_field "var" value then
      'Error { message = "chain `relay` must carry both `edge` (the Data edge code) and `var` (its routing variable)" }
    else
      'Ok) in

# Closed input contract for `mkChain`: an unexpected field (a typo) blames here.
let ChainInput = {
  anchor | AnchorRef,
  steps | Array ChainStep,
  relay | Nullable RelaySpec,
  reason | Nullable String,
} in
{
  mkChain = fun cfg =>
    let c = ({ reason | default = null, relay | default = null } & cfg) | ChainInput in
    { chain = { anchor = c.anchor, steps = c.steps } }
    & (if c.relay != null then { chain.relay = c.relay } else {})
    & (if c.reason != null then { reason = c.reason } else {}),
}
