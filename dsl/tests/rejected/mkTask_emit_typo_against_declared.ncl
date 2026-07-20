# expect: `from_routing_var = digets` is not declared in this task's routing_vars: [digest]
let { mkTask, mkRoutingVar, mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
mkTask {
  name = "build",
  args = [],
  nix_expression_path = "x",
  routing_vars = [ mkRoutingVar { name = "digest" } ],
  emits = [ mkSignalEmit { signal = mkSignal { name = "s" }, from_routing_var = mkRoutingVar { name = "digets" } } ],
}
