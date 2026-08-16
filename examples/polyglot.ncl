let tickr = import "lib.ncl" in

let python = tickr.mkTask {
  name = "python-greeting",
  args = [],
  nix_expression_path = "path:./examples#polyglotPython",
  outputs = [],
  timeout = "60s",
  max_attempts = 1,
} in

let javascript = tickr.mkTask {
  name = "javascript-greeting",
  args = [],
  nix_expression_path = "path:./examples#polyglotJavaScript",
  outputs = [],
  timeout = "60s",
  max_attempts = 1,
} in

let go = tickr.mkTask {
  name = "go-greeting",
  args = [],
  nix_expression_path = "path:./examples#polyglotGo",
  outputs = [],
  timeout = "60s",
  max_attempts = 1,
} in

let rust = tickr.mkTask {
  name = "rust-greeting",
  args = [],
  nix_expression_path = "path:./examples#polyglotRust",
  outputs = [],
  timeout = "60s",
  max_attempts = 1,
} in

tickr.mkWorkflow {
  slug = "polyglot",
  name = "polyglot",
  args = [],
  outputs = [],
  captures = [
    tickr.mkTriggerCapture { name = "greeting", jsonpath = "$.greeting" },
  ],
  tasks = [
    tickr.mkTaskGroup {
      name = "four-languages",
      args = [],
      outputs = [],
      tasks = [ tickr.chain [ python, javascript, go, rust ] ],
    },
  ],
}
