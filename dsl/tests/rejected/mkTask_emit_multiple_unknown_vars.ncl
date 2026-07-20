# expect: `from_routing_var = nope2` is not declared in this task's routing_vars: [r1]
let { mkTask, mkRoutingVar, mkSignal, mkSignalEmit, .. } = import "lib.ncl" in
mkTask {
  name = "build",
  args = [],
  nix_expression_path = "x",
  routing_vars = [ mkRoutingVar { name = "r1" } ],
  emits = [
    mkSignalEmit { signal = mkSignal { name = "s1" }, from_routing_var = mkRoutingVar { name = "nope1" } },
    mkSignalEmit { signal = mkSignal { name = "s2" }, from_routing_var = mkRoutingVar { name = "nope2" } },
  ],
}
