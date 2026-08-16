use serde_json::{json, Value};

#[derive(Clone, Copy, Debug)]
pub(super) struct ExampleSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub source: &'static str,
}

impl ExampleSpec {
    pub fn trigger_body(self) -> Value {
        match self.name {
            "hello-world" => json!({"name": "Hello from Tickr"}),
            "runtime-patch" => json!({
                "name": "Seeded runtime Patch: 42",
                "inputs": {"seed": 42}
            }),
            "polyglot" => json!({
                "name": "Polyglot greeting",
                "inputs": {"greeting": "Hello from Tickr"}
            }),
            _ => unreachable!("every bundled example has a trigger body"),
        }
    }
}

pub(super) const EXAMPLES: [ExampleSpec; 3] = [
    ExampleSpec {
        name: "hello-world",
        description: "One Task prints a greeting",
        source: "examples/hello-world.ncl",
    },
    ExampleSpec {
        name: "runtime-patch",
        description: "A live Run grows two deterministic parallel arms",
        source: "examples/runtime-patch.ncl",
    },
    ExampleSpec {
        name: "polyglot",
        description: "Python, JavaScript, Go, and Rust Tasks share one trigger value",
        source: "examples/polyglot.ncl",
    },
];

pub(super) fn find(name: &str) -> Option<ExampleSpec> {
    EXAMPLES
        .iter()
        .copied()
        .find(|example| example.name == name)
}

pub(super) fn names() -> impl Iterator<Item = &'static str> {
    EXAMPLES.iter().map(|example| example.name)
}
