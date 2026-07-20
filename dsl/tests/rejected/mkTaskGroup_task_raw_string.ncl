# expect: contract broken by the value of `tasks`
# A group's `tasks` holds projected `mkTask`/`mkTaskGroup` values (each a
# `TaskRef` record); a raw string is rejected at the Nickel contract surface.
let { mkTaskGroup, .. } = import "lib.ncl" in
mkTaskGroup { name = "g", args = [], tasks = [ "a" ] }
