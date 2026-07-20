# expect: trigger config must declare a `kind`
# A field-name typo (`knid` for `kind`) fails: with no `kind` present the
# dispatcher cannot pick a sub-contract.
let { mkTriggerOn, .. } = import "lib.ncl" in
mkTriggerOn { knid = "cron", expr = "*/5 * * * *" }
