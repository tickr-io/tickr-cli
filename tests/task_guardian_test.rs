#![cfg(unix)]

use std::io;
use std::process::Stdio;
use std::time::Duration;

use tempfile::tempdir;
use tokio::process::Command;
use tokio::time::{sleep, timeout, Instant};

#[tokio::test]
async fn parent_liveness_eof_kills_guarded_task_group() {
    let directory = tempdir().unwrap();
    let pid_file = directory.path().join("descendant.pid");
    let script = format!(
        "trap '' TERM; sleep 60 & echo $! > '{}'; wait",
        pid_file.display()
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_tickr"));
    command
        .arg("__task-guardian")
        .arg("--")
        .arg("/bin/sh")
        .arg("-c")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let mut guardian = command.spawn().unwrap();
    let parent_liveness = guardian.stdin.take().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let descendant = loop {
        if let Ok(pid) = tokio::fs::read_to_string(&pid_file).await {
            break pid.trim().parse::<i32>().unwrap();
        }
        assert!(Instant::now() < deadline, "guarded Task did not start");
        sleep(Duration::from_millis(20)).await;
    };

    drop(parent_liveness);
    let status = timeout(Duration::from_secs(5), guardian.wait())
        .await
        .expect("guardian exceeded its bounded teardown")
        .unwrap();
    assert!(!status.success());

    let deadline = Instant::now() + Duration::from_secs(5);
    while process_exists(descendant) && Instant::now() < deadline {
        sleep(Duration::from_millis(20)).await;
    }
    assert!(
        !process_exists(descendant),
        "guarded Task descendant {descendant} survived parent EOF"
    );
}

fn process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        #[link_name = "kill"]
        fn c_kill(process: i32, signal: i32) -> i32;
    }
    if unsafe { c_kill(pid, 0) } == 0 {
        return true;
    }
    io::Error::last_os_error().raw_os_error() != Some(3)
}
