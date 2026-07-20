# tickr/dsl/branch.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkBranch` constructor: fan a live anchor into N **gated arms** whose gates
# select which run, rejoining on the fired arm (a *selection* join, distinct from
# `mkFork`'s barrier). An author names the anchor by its short identity code (the
# four-symbol handle read off the exposed graph, or a full UUID) and supplies a
# list of **arms** — each arm is a chain-step scope-tree (scope-local `handle`
# plus a leaf `task` built at-patch, or a nested `steps` sub-chain) *plus* a
# `gate` (an `mkSignalGate` / `mkPredicateGate` / `mkTimerGate` body). The server
# feeds each arm's head a gated edge carrying that arm's gate, threads the arm,
# and re-seats the anchor's successors as one edge per arm tail — so the successor
# grounds on whichever arm fired. One node id is minted per leaf task; the author
# writes only handles, codes, and gates, never a hand-mapped UUID.
#
# A branch is a fork with two differences: its arm feeders are *gated* (the gate
# selects the arm), and its join is a *selection* (grounds on the fired arm), not
# a barrier (grounds when every arm grounds).
#
# The emitted document is one arm of the Patch tagged union:
# `{ branch = { anchor, arms }, reason? }`. A primitive-op patch uses `{ ops }`;
# a fork uses `{ fork }`; other operations are their own arms.
let contracts = import "contracts.ncl" in
let Nullable = contracts.Nullable in

# The anchor reference: an identity code (e.g. "AB12") or a full UUID string. It
# names a *runtime* structure resolved against the live graph at apply.
let AnchorRef =
  std.contract.from_validator (fun value =>
    if std.is_string value then
      'Ok
    else
      'Error {
        message = "branch `anchor` must be an identity-code (or UUID) string read off the exposed graph",
      }) in

# One branch arm: a scope-local `handle`, a selecting `gate` (the whole point of a
# branch over a fork), and exactly one of a leaf `task` (a full `mkTask` body,
# built at-patch) or a nested `steps` sub-chain. Validated structurally so a typo
# blames here; the conductor re-checks per-scope handle uniqueness, projects the
# gate, and validates the nested shape recursively.
let BranchArm =
  std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "a branch arm must be a record { handle, gate, task | steps }" }
    else if !std.record.has_field "handle" value then
      'Error { message = "a branch arm must carry a `handle`" }
    else if !std.record.has_field "gate" value then
      'Error { message = "a branch arm must carry a selecting `gate` (mkSignalGate/mkPredicateGate/mkTimerGate)" }
    else
      let has_task = std.record.has_field "task" value in
      let has_steps = std.record.has_field "steps" value in
      if has_task && has_steps then
        'Error { message = "a branch arm carries either a `task` or `steps`, not both" }
      else if !has_task && !has_steps then
        'Error { message = "a branch arm must carry a leaf `task` or a nested `steps` sub-chain" }
      else
        'Ok) in

# Closed input contract for `mkBranch`: an unexpected field (a typo) blames here.
let BranchInput = {
  anchor | AnchorRef,
  arms | Array BranchArm,
  reason | Nullable String,
} in
{
  mkBranch = fun cfg =>
    let c = ({ reason | default = null } & cfg) | BranchInput in
    { branch = { anchor = c.anchor, arms = c.arms } }
    & (if c.reason != null then { reason = c.reason } else {}),
}
