# tickr/dsl/fork.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkFork` constructor: fan a live anchor into N parallel **arms** that all
# run and rejoin at a barrier join (fork-join). An author names the anchor by its
# short identity code (the four-symbol handle read off the exposed graph, or a
# full UUID) and supplies a list of **arms** — each arm is one step (the same
# scope-tree shape a chain step uses): a scope-local `handle` plus either a leaf
# `task` (a single-task arm, built at-patch) or a nested `steps` sub-chain (a
# multi-task arm threaded in declaration order). The server feeds an ungated edge
# from the anchor to each arm's head, threads each arm, and re-seats the anchor's
# successors onto a barrier over every arm's tail — so a successor grounds only
# once all arms ground. One node id is minted per leaf task; the author writes
# only handles and codes, never a hand-mapped UUID.
#
# The emitted document is one arm of the Patch tagged union:
# `{ fork = { anchor, arms }, reason? }`. A primitive-op patch uses `{ ops }`; a
# chain uses `{ chain }`; a single insert uses `{ insert }`.
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
        message = "fork `anchor` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# One fork arm: a scope-local `handle` plus exactly one of a leaf `task` (a full
# `mkTask` body, built at-patch) or a nested `steps` sub-chain (a multi-task arm).
# Structurally identical to a chain step — an arm *is* a chain. Validated
# structurally so a typo blames here; the conductor re-checks per-scope handle
# uniqueness across the arms and the nested shape recursively.
let ForkArm =
  std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "a fork arm must be a record { handle, task | steps }" }
    else if !std.record.has_field "handle" value then
      'Error { message = "a fork arm must carry a `handle`" }
    else
      let has_task = std.record.has_field "task" value in
      let has_steps = std.record.has_field "steps" value in
      if has_task && has_steps then
        'Error { message = "a fork arm carries either `task` or `steps`, not both" }
      else if !has_task && !has_steps then
        'Error { message = "a fork arm must carry a leaf `task` or a nested `steps` sub-chain" }
      else
        'Ok) in

# Closed input contract for `mkFork`: an unexpected field (a typo) blames here.
let ForkInput = {
  anchor | AnchorRef,
  arms | Array ForkArm,
  reason | Nullable String,
} in
{
  mkFork = fun cfg =>
    let c = ({ reason | default = null } & cfg) | ForkInput in
    { fork = { anchor = c.anchor, arms = c.arms } }
    & (if c.reason != null then { reason = c.reason } else {}),
}
