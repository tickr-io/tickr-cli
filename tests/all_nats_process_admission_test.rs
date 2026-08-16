#![cfg(not(madsim))]

use async_nats::jetstream;
use std::process::Command;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_proto::coord::all_nats as names;

#[tokio::test]
async fn conductor_admits_all_nats_before_opening_postgres() {
    let command = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default()
        .with_tag("2.11.11")
        .with_cmd(&command)
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            eprintln!("skipping: isolated NATS testcontainer unavailable: {error}");
            return;
        }
    };
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("isolated NATS port");
    let url = format!("nats://127.0.0.1:{port}");
    let mut client = None;
    for _ in 0..20 {
        match async_nats::connect(&url).await {
            Ok(connected) => {
                client = Some(connected);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let client = client.expect("isolated NATS accepts connections");

    let output = Command::new(env!("CARGO_BIN_EXE_tickr-cli"))
        .arg("conductor")
        .env("TICKR_NATS_URL", &url)
        .env(
            "TICKR_CONDUCTOR_POSTGRES_URL",
            "postgres://postgres:postgres@127.0.0.1:1/tickr",
        )
        .env_remove("TICKR_SQL_BACKEND")
        .env_remove("TICKR_SQL_TOPOLOGY")
        .output()
        .expect("run all-NATS conductor against unavailable Postgres");
    assert!(
        !output.status.success(),
        "unavailable Postgres must stop the conductor"
    );

    let js = jetstream::new(client);
    js.get_key_value(names::DEFAULT_SCOPE_BUCKET)
        .await
        .expect("ScopeStore admitted before repository startup");
    let identity_store = js
        .get_key_value(names::FORMATION_IDENTITY_BUCKET)
        .await
        .expect("formation identity bucket");
    let identity = identity_store
        .get(names::FORMATION_IDENTITY_KEY)
        .await
        .expect("formation identity read")
        .expect("formation identity value");
    assert_eq!(
        identity.as_ref(),
        format!(
            "{};scope={}",
            names::FORMATION_IDENTITY,
            names::DEFAULT_SCOPE_BUCKET
        )
        .as_bytes()
    );
}
