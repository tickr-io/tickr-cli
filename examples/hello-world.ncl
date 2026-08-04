let tickr = import "lib.ncl" in
let hello = tickr.mkTask {
  name = "hello",
  args = ["hello from Tickr"],
  nix_expression_path = "path:./examples#hello",
  outputs = [],
} in
tickr.mkWorkflow {
  slug = "hello-world",
  name = "hello-world",
  args = [],
  outputs = [],
  tasks = [
    tickr.mkTaskGroup {
      name = "hello",
      args = [],
      outputs = [],
      tasks = [ hello ],
    },
  ],
}
