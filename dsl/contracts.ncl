# tickr/dsl/contracts.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The Core DSL's shared domain contracts. Each encodes one author-facing
# domain rule (identifier shape, duration grammar, retry bounds, routing-
# variable type tags, reserved tag namespace) as a Nickel contract. The
# per-concept constructor modules (`task.ncl`, `workflow.ncl`, …) import
# these and attach them to constructor input fields, so a rule lives in one
# place and propagates to every constructor that uses it.
#
# A contract failure here surfaces at `nickel export` time with a column
# pointer to the offending field — the author-time feedback this DSL exists
# to give. The conductor's Rust parser keeps its own equivalents as
# defence-in-depth; these contracts are the primary surface.
{
  # Wrap a contract so `null` passes through unchecked and any non-null
  # value is checked against the inner contract. Used for optional fields
  # whose default is `null` (`timeout`, `mkRoutingVar.type`, …).
  Nullable = fun Contract =>
    std.contract.custom (fun label value =>
      if value == null then 'Ok value
      else 'Ok (std.contract.apply Contract label value)),

  # A tickr identifier: 1–64 chars from `[A-Za-z0-9_-]`, non-empty, no
  # spaces. Shared by every constructor's `name` field so authors learn one
  # naming rule, not five.
  Identifier = std.contract.from_validator (fun value =>
    if !std.is_string value then
      'Error { message = "identifier must be a string" }
    else if std.string.length value == 0 then
      'Error { message = "identifier must be non-empty" }
    else if std.string.is_match "^[A-Za-z0-9_-]{1,64}$" value then
      'Ok
    else
      'Error {
        message = "invalid identifier `%{value}`",
        notes = [
          "allowed characters: letters, digits, `_`, `-`",
          "length must be 1–64 with no spaces",
        ],
      }),

  # A workflow slug: 1–64 chars from `[a-z0-9-]`, lowercase-kebab. Unlike
  # `Identifier`, the slug forbids uppercase and `_` because it is half of
  # the workflow's composite identity (`namespace.slug`) — the immutable
  # handle an author consciously owns. The dot separator that joins it to
  # the namespace is injective only because both segments forbid `.`, which
  # this grammar already excludes. Renaming the display `name` is free;
  # changing the `slug` is an identity boundary.
  Slug = std.contract.from_validator (fun value =>
    if !std.is_string value then
      'Error { message = "slug must be a string" }
    else if std.string.length value == 0 then
      'Error { message = "slug must be non-empty" }
    else if std.string.is_match "^[a-z0-9-]{1,64}$" value then
      'Ok
    else
      'Error {
        message = "invalid slug `%{value}`",
        notes = [
          "allowed characters: lowercase letters, digits, `-`",
          "length must be 1–64 (no spaces, no uppercase, no `_`, no `.`)",
        ],
      }),

  # A duration string. Mirrors the grammar of the conductor's
  # `parser/duration.rs` exactly: `<n>s | <n>m | <n>h | <n>` where a bare
  # number is seconds. The integer must be positive and must not overflow
  # u64 when expanded to seconds. Applied via `Nullable` to optional
  # `timeout` fields.
  #
  # This grammar lives in two places (here and `parser/duration.rs`); a
  # future cleanup collapses the Rust copy. Until then, a new unit must be
  # added in both.
  Duration = std.contract.from_validator (fun value =>
    if !std.is_string value then
      'Error { message = "duration must be a string" }
    else if !std.string.is_match "^[0-9]+(s|m|h)?$" value then
      'Error {
        message = "malformed duration `%{value}`",
        notes = [ "accepted grammar: `<n>s` | `<n>m` | `<n>h` | `<n>` (bare number = seconds)" ],
      }
    else
      let digits = std.string.replace_regex "[smh]$" "" value in
      let n = std.string.to_number digits in
      let factor =
        if std.string.is_match "m$" value then 60
        else if std.string.is_match "h$" value then 3600
        else 1
      in
      if n == 0 then
        'Error { message = "duration must be positive: `%{value}`" }
      else if n * factor > 18446744073709551615 then
        'Error { message = "duration `%{value}` overflows u64 seconds" }
      else 'Ok),

  # A retry count: an integer in `1..=100`. Zero, negative, and fractional
  # values are rejected at the call site. The upper bound guards against a
  # fat-fingered `max_attempts = 1000000` exhausting a node on retries.
  MaxAttempts = std.contract.from_validator (fun value =>
    if !std.is_number value then
      'Error { message = "max_attempts must be a number" }
    else if value % 1 != 0 then
      'Error { message = "max_attempts must be a whole number, got `%{std.to_string value}`" }
    else if value < 1 || value > 100 then
      'Error {
        message = "max_attempts out of range: `%{std.to_string value}`",
        notes = [ "accepted range: 1–100" ],
      }
    else 'Ok),

  # The type tag of a routing variable. Applied via `Nullable` to the
  # optional `mkRoutingVar.type` field.
  RoutingVarType = std.contract.from_validator (fun value =>
    if !std.is_string value then
      'Error { message = "routing-variable type must be a string" }
    else if std.array.any (fun t => t == value) [ "string", "int", "bool", "bytes", "array" ] then
      'Ok
    else
      'Error {
        message = "invalid routing-variable type `%{value}`",
        notes = [ "accepted types: `string`, `int`, `bool`, `bytes`, `array`" ],
      }),

  # A typed reference to a named signal — the shape `mkSignal` produces:
  # `{ kind = "signal", name }`. Constructors that consume a signal (gates,
  # emits, triggers) attach this so an author who passes a raw string
  # instead of an `mkSignal` value fails at the consuming call site rather
  # than producing a wire shape the parser silently mishandles.
  SignalRef = std.contract.from_validator (fun value =>
    if !std.is_record value || (std.record.get_or "kind" "" value) != "signal" then
      'Error {
        message = "expected an `mkSignal` value (a `SignalRef`)",
        notes = [ "pass the value produced by `mkSignal { name = .. }`, not a raw string" ],
      }
    else 'Ok),

  # A typed reference to a routing variable — the shape `mkRoutingVar`
  # produces: `{ kind = "routing-var", name } (& optional { type })`.
  # Consumers (`mkPredicateGate`, `mkSignalEmit`) attach this so a raw
  # string fails at the call site naming the constraint.
  RoutingVarRef = std.contract.from_validator (fun value =>
    if !std.is_record value || (std.record.get_or "kind" "" value) != "routing-var" then
      'Error {
        message = "expected an `mkRoutingVar` value (a `RoutingVarRef`)",
        notes = [ "pass the value produced by `mkRoutingVar { name = .. }`, not a raw string" ],
      }
    else 'Ok),

  # The comparison operator on a predicate gate. PascalCase is the wire
  # form the conductor parser accepts; the DSL surfaces the same vocabulary
  # so authors see DSL operator names (not language-level `==`/`<`) at the
  # authoring layer.
  Operator = std.contract.from_validator (fun value =>
    if std.array.any (fun o => o == value) [ "Eq", "NotEq", "Lt", "Le", "Gt", "Ge" ] then
      'Ok
    else
      'Error {
        message = "invalid operator `%{std.to_string value}`",
        notes = [ "accepted operators: `Eq`, `NotEq`, `Lt`, `Le`, `Gt`, `Ge`" ],
      }),

  # A scalar value: number, string, or boolean. Used on a predicate gate's
  # `value` because the operator set is scalar-only — records and arrays
  # have no defined comparison.
  Scalar = std.contract.any_of [ Number, String, Bool ],

  # Accepts any of the three gate-constructor outputs, dispatching on the
  # `kind` discriminant. The edge constructor's `gate` field attaches this
  # so it accepts an `mkSignalGate` / `mkPredicateGate` / `mkTimerGate`
  # value and rejects anything else.
  AnyGate = std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "gate must be a gate-constructor value" }
    else if !std.record.has_field "kind" value then
      'Error { message = "gate must be a gate-constructor value (missing `kind`)" }
    else if std.array.any (fun k => k == value.kind) [ "signal-gate", "predicate-gate", "timer-gate" ] then
      'Ok
    else
      'Error {
        message = "unknown gate kind `%{std.to_string value.kind}`",
        notes = [ "accepted gates: `mkSignalGate`, `mkPredicateGate`, `mkTimerGate`" ],
      }),

  # A typed reference to a named task — the open shape `mkTask` and
  # `mkTaskGroup` both expose: a record carrying at least a `name`. The
  # edge constructor's `from`/`to` fields attach this so an author who
  # passes a raw string fails at the call site naming the constraint. Open
  # (`..`) because the contract only needs the `name` to project to the
  # wire; it makes no claim about the rest of the task structure.
  TaskRef = { name | Identifier, .. },

  # A capture declaration — the validated output shape `mkTriggerCapture`
  # produces: `{ name, from = { trigger = { jsonpath } } }`. Consumers that
  # hold an array of captures (`mkSignalGate.captures`, the
  # `waits-on-signal` trigger's `captures`, `mkWorkflow.captures`) attach
  # `Array CaptureDecl` so a hand-shaped inline record fails at the call
  # site rather than producing a wire shape the parser mishandles. Closed,
  # so a typo'd inner field blames at the call site.
  CaptureDecl = {
    name | Identifier,
    from | { trigger | { jsonpath | String } },
  },

  # The kind of an edge: `control` (a plain dependency), `data` (a gated
  # data-flow edge), or `loop` (a gated back-edge). `mkEdge.kind` defaults
  # to `control`; the gate/kind cross-field rule in `edge.ncl` governs
  # which kinds are legal with and without a gate.
  EdgeKind = std.contract.from_validator (fun value =>
    if std.array.any (fun k => k == value) [ "control", "data", "loop" ] then
      'Ok
    else
      'Error {
        message = "invalid edge kind `%{std.to_string value}`",
        notes = [ "accepted kinds: `control`, `data`, `loop`" ],
      }),

  # The reserved value set of the `loop_control` routing variable: the
  # strings `continue` (the default — an absent value keeps the loop turning),
  # `done` (exit successfully), or `fail` (exit in failure). `loop_control`
  # rides as a validated string, not a first-class enum routing-variable type
  # (those are `string`/`int`/`bool`/`bytes`/`array`); this contract is what
  # pins it to the reserved set at author time. `mkLoop` applies it to the gate
  # values it auto-wires.
  LoopControl = std.contract.from_validator (fun value =>
    if std.array.any (fun v => v == value) [ "continue", "done", "fail" ] then
      'Ok
    else
      'Error {
        message = "invalid `loop_control` value `%{std.to_string value}`",
        notes = [ "accepted values: `continue` (default), `done`, `fail`" ],
      }),

  # The workflow `tags` map: a record of string values whose keys must not
  # begin with `tickr/`. That namespace is stamped on every materialized
  # instance by the system at runtime (`tickr/workflow_id`,
  # `tickr/trigger_source`, …); an author key in the same namespace would
  # shadow a runtime guarantee, so it is rejected at the call site.
  Tags = std.contract.from_validator (fun value =>
    if !std.is_record value then
      'Error { message = "tags must be a record of string values" }
    else
      let reserved =
        std.array.filter
          (fun k => std.string.is_match "^tickr/" k)
          (std.record.fields value)
      in
      if std.array.length reserved != 0 then
        'Error {
          message = "reserved tag namespace: `%{std.string.join "`, `" reserved}`",
          notes = [ "keys beginning `tickr/` are stamped by the system at runtime and cannot be set by authors" ],
        }
      else
        let non_string =
          std.array.filter
            (fun k => !std.is_string (std.record.get k value))
            (std.record.fields value)
        in
        if std.array.length non_string != 0 then
          'Error { message = "tag values must be strings; non-string value(s) for: `%{std.string.join "`, `" non_string}`" }
        else 'Ok),
}
