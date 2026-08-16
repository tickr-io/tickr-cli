use std::process::Command;

fn main() {
    let output = Command::new("tickr-ctx")
        .args([
            "get",
            "greeting",
            "--signal",
            "--default",
            "Hello from Tickr",
        ])
        .output()
        .expect("run tickr-ctx");
    assert!(output.status.success(), "tickr-ctx get failed");
    let greeting = String::from_utf8(output.stdout).expect("tickr-ctx returned UTF-8");
    println!("rust: {}", greeting.trim());
}
