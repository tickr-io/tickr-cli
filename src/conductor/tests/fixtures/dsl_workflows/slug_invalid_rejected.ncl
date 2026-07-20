# A workflow whose `slug` violates the kebab grammar. The Core DSL's `Slug`
# contract rejects it at `nickel export` — before the JSON ever reaches the
# conductor's parser — naming the slug rule. The display `name` is valid so the
# failure isolates on the slug.
let utils = import "lib.ncl" in
let notify = utils.mkTask {
  name = "notify",
  args = [],
  nix_expression_path = "x",
  outputs = [],
} in
let tg = utils.mkTaskGroup {
  name = "g",
  args = [],
  outputs = [],
  tasks = [ notify ],
} in
utils.mkWorkflow {
  slug = "Bad_Slug",
  name = "slug-invalid",
  args = [],
  outputs = [],
  tasks = [ tg ],
}
