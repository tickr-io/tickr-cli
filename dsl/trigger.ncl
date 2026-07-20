# tickr/dsl/trigger.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# Trigger constructors: `mkTriggerOn` (the `mkWorkflow.triggerOn` value)
# and `mkTriggerCapture` (a single capture declaration). These are the
# first-class typed entry points for the two nested discriminated-union
# shapes the parser expects — authors call a constructor with flat named
# fields instead of hand-shaping the kebab-cased `kind`-tagged record or
# the nested single-key `from.trigger.jsonpath` record. The wire format is
# unchanged; the constructors only sit above it.
#
# `mkTriggerOn` dispatches on `kind` and applies a per-kind CLOSED sub-
# contract, so a field-name typo or a field that belongs to a different
# kind blames at the call site. An unknown `kind` fails naming the
# accepted set. `mkTriggerCapture` is a flat closed-record input.
let contracts = import "contracts.ncl" in
let SignalRef = contracts.SignalRef in
let Nullable = contracts.Nullable in
let CaptureDecl = contracts.CaptureDecl in

# Per-kind closed input contracts. Each is a closed record literal, so an
# extra/typo field is rejected with blame at the call site.
let CronInput = { kind | String, expr | String } in
let FireNowInput = { kind | String } in
let WaitsOnSignalInput = {
  kind | String,
  signal | SignalRef,
  predicate | Nullable String,
  captures | Array CaptureDecl,
} in

# Reads `kind` first, then applies the matching per-kind closed sub-
# contract. An unknown `kind` (or a missing/non-string one) fails naming
# the accepted set. The custom contract dispatches; the sub-contract
# enforces shape (required fields + no extras).
let TriggerOnInput = std.contract.custom (fun label value =>
  if !std.is_record value || !std.record.has_field "kind" value then
    'Error {
      message = "trigger config must declare a `kind`",
      notes = [ "accepted kinds: `cron`, `fire-now`, `waits-on-signal`" ],
    }
  else if value.kind == "cron" then
    'Ok (std.contract.apply CronInput label value)
  else if value.kind == "fire-now" then
    'Ok (std.contract.apply FireNowInput label value)
  else if value.kind == "waits-on-signal" then
    'Ok (std.contract.apply WaitsOnSignalInput label value)
  else
    'Error {
      message = "unknown trigger kind `%{std.to_string value.kind}`",
      notes = [ "accepted kinds: `cron`, `fire-now`, `waits-on-signal`" ],
    }) in

let CaptureInput = {
  name | contracts.Identifier,
  jsonpath | String,
} in
{
  # `mkTriggerOn` builds the `mkWorkflow.triggerOn` value: the kebab-cased,
  # `kind`-tagged trigger config the parser reads. `cron` carries the cron
  # `expr`; `fire-now` carries nothing else; `waits-on-signal` carries a
  # signal reference plus an optional JSONPath `predicate` and an array of
  # `captures` (each from `mkTriggerCapture`). The `signal` reference is
  # projected to `{ name }` on the wire; optional fields are omitted when
  # unset. `predicate`/`captures` are only meaningful (and only accepted)
  # for `waits-on-signal`, so the per-kind closed sub-contract rejects them
  # on the other kinds.
  mkTriggerOn = fun cfg =>
    let c =
      (
        if std.is_record cfg && (std.record.get_or "kind" "" cfg) == "waits-on-signal" then
          { predicate | default = null, captures | default = [] } & cfg
        else cfg
      ) | TriggerOnInput
    in
    if c.kind == "cron" then
      { kind = "cron", expr = c.expr }
    else if c.kind == "fire-now" then
      { kind = "fire-now" }
    else
      { kind = "waits-on-signal", signal = { name = c.signal.name } }
      & (if c.predicate != null then { predicate = c.predicate } else {})
      & (if c.captures != [] then { captures = c.captures } else {}),

  # `mkTriggerCapture` builds one capture declaration: a JSONPath
  # extraction from the inbound trigger payload, projected into the nested
  # `{ name, from = { trigger = { jsonpath } } }` shape the parser expects.
  # JSONPath syntax is validated by the conductor parser at registration
  # (RFC 9535); duplicating the grammar here would drift.
  mkTriggerCapture = fun cfg =>
    let c = cfg | CaptureInput in
    {
      name = c.name,
      from = { trigger = { jsonpath = c.jsonpath } },
    },
}
