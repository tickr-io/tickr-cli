# tickr/dsl/trigger_test.ncl
#
# Positive-path tests for the trigger constructors `mkTriggerOn` and
# `mkTriggerCapture`. Covers each trigger kind (`cron` + `expr`, `fire-now`
# standalone, `waits-on-signal` + signal + optional `predicate`/`captures`)
# and asserts the projected wire shape — `kind`-tagged record, signal
# projected to `{ name }`, optionals omitted when unset — plus the nested
# `from.trigger.jsonpath` shape from `mkTriggerCapture`. Negative cases
# live under `tests/rejected/`.
let { mkSignal, mkTriggerOn, mkTriggerCapture, .. } = import "lib.ncl" in
let check = fun b => (b | std.test.Assert) in

let approval = mkSignal { name = "approval" } in

let cron = mkTriggerOn { kind = "cron", expr = "*/5 * * * *" } in
let fire_now = mkTriggerOn { kind = "fire-now" } in
let waits_bare = mkTriggerOn { kind = "waits-on-signal", signal = approval } in
let waits_full = mkTriggerOn {
  kind = "waits-on-signal",
  signal = approval,
  predicate = "$[?@.amount > 100]",
  captures = [ mkTriggerCapture { name = "user_email", jsonpath = "$.user.email" } ],
} in
let cap = mkTriggerCapture { name = "receipt_url", jsonpath = "$.receipt" } in
[
  # cron — carries only kind + expr.
  check (cron.kind == "cron"),
  check (cron.expr == "*/5 * * * *"),

  # fire-now — carries only kind.
  check (fire_now == { kind = "fire-now" }),

  # waits-on-signal (bare) — signal projected to `{ name }`, optionals omitted.
  check (waits_bare.kind == "waits-on-signal"),
  check (waits_bare.signal == { name = "approval" }),
  check (!std.record.has_field "predicate" waits_bare),
  check (!std.record.has_field "captures" waits_bare),

  # waits-on-signal (full) — predicate + captures present.
  check (waits_full.signal == { name = "approval" }),
  check (waits_full.predicate == "$[?@.amount > 100]"),
  check (std.array.length waits_full.captures == 1),
  check ((std.array.at 0 waits_full.captures).from.trigger.jsonpath == "$.user.email"),

  # mkTriggerCapture — nested `from.trigger.jsonpath` shape.
  check (cap.name == "receipt_url"),
  check (cap.from == { trigger = { jsonpath = "$.receipt" } }),
]
