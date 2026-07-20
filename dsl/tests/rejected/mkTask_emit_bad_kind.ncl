# expect: unknown emit kind `bogus`
# `emits` holds projected `mkSignalEmit`/`mkSignalEmitOnFailure` values; a
# hand-shaped record with an unknown `kind` is rejected before runtime.
let { mkTask, .. } = import "lib.ncl" in
mkTask { name = "t", args = [], nix_expression_path = "x", emits = [ { kind = "bogus" } ] }
