let tickr = import "lib.ncl" in
let hello = tickr.mkTask {
  name = "hello",
  args = ["hello from Tickr"],
  nix_expression_path = "./examples/hello.nix",
  outputs = [],
} in
tickr.mkWorkflow {
  slug = "hello-world",
  name = "hello-world",
  args = [],
  outputs = [],
  tasks = [hello],
}
