# tickr/dsl/routing.ncl
#
# Internal module — do not import directly; see `lib.ncl`.
#
# The `mkRoutingVar` constructor and its closed-record input contract.
let contracts = import "contracts.ncl" in
let Identifier = contracts.Identifier in
let RoutingVarType = contracts.RoutingVarType in
let Nullable = contracts.Nullable in

let RoutingVarInput = {
  name | Identifier,
  type | Nullable RoutingVarType,
} in
{
  # `mkRoutingVar` declares a typed channel through which a task-emitted
  # value crosses from data plane to control plane. Each name is single-
  # producer / completion-only; the parser rejects multi-task ownership at
  # registration. The optional `type` field hints parser-side type checking
  # against `mkPredicateGate.value`.
  mkRoutingVar = fun cfg =>
    let c = ({ type | default = null } & cfg) | RoutingVarInput in
    { name = c.name, kind = "routing-var" }
    & (if c.type != null then { type = c.type } else {}),
}
