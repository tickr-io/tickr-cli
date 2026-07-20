# expect: unknown trigger kind `every-thursday`
# An unrecognised `kind` fails at the call site naming the accepted set.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { kind = "every-thursday" }
