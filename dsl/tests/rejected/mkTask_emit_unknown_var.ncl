# expect: `from_routing_var = r3` is not declared in this task's routing_vars: [r1, r2]
let { mkTask, mkRoutingVar, mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
mkTask {
  name = "build",
  args = [],
  nix_expression_path = "x",
  routing_vars = [ mkRoutingVar { name = "r1" }, mkRoutingVar { name = "r2" } ],
  emits = [ mkSignalEmit { signal = mkSignal { name = "s" }, from_routing_var = mkRoutingVar { name = "r3" } } ],
}
