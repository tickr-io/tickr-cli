//! Real-NATS integration test for replay re-hydration
//! (`replay_rehydration::{plan_rehydration, apply_rehydration_via_nats}`).
//!
//! Asserts the two data-plane contracts a born-Stalled replay depends on:
//!
//!  1. **Verbatim value-copy into a fresh scope.** A carried ctx key lands in
//!     the replay's own run-scoped namespace with its archived envelope bytes
//!     unchanged (`created_at` / `sha256` / `producer` preserved) — never
//!     re-wrapped through `Envelope::new`.
//!  2. **Sentinel as the final act.** `tickr_replay/hydrated` is written after
//!     every carried key, observable as its KV revision being strictly greater
//!     than every carried key's — the ordering the born-Stall release rides on.
//!
//! Requires Docker (testcontainers). Skipped automatically when the NATS
//! container is unavailable — the startup failure is the skip marker.

#![cfg(not(madsim))]

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_nats::jetstream;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_conductor::replay_rehydration::{
    apply_rehydration_via_nats, plan_rehydration, ArchivedCtxEntry, ArchivedRun,
    ArchivedTaskInstanceRow, HYDRATION_SENTINEL_KEY,
};
use tickr_ctx::envelope::{Envelope, Producer};
use tickr_ctx::scope::sanitize_segment;
const CTX_GRAPH_KEY: &str = "tickr_graph";
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: NATS testcontainer unavailable: {}", e);
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{}", port);
    let mut client = None;
    for _ in 0..50 {
        if let Ok(c) = async_nats::connect(&url).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Some((container, client.expect("nats connect")))
}

/// A `start → a → b → end` chain source run whose `a` and `b` each have an
/// archived task-instance row naming their owning node. Returns the run id, the
/// two node ids, and their task-instance ids.
fn chain_source() -> (Uuid, Uuid, Uuid, Uuid, Uuid) {
    let run = Uuid::new_v4();
    let (a_id, b_id) = (Uuid::new_v4(), Uuid::new_v4());
    let (ti_a, ti_b) = (Uuid::new_v4(), Uuid::new_v4());
    (run, a_id, b_id, ti_a, ti_b)
}

fn task_envelope(task_id: Uuid, value: &str) -> serde_json::Value {
    let env = Envelope::new(
        "string",
        serde_json::Value::String(value.to_string()),
        false,
        Producer::Task {
            task_id: task_id.to_string(),
            task_name: "t".to_string(),
        },
    );
    serde_json::to_value(&env).unwrap()
}

fn dump_entry(run: Uuid, name: &str, envelope: serde_json::Value) -> ArchivedCtxEntry {
    ArchivedCtxEntry {
        key: format!("{}/{}", sanitize_segment(&run.to_string()), name),
        envelope,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn carried_keys_land_verbatim_and_sentinel_is_the_final_act() {
    let Some((_nats_c, nats)) = start_nats().await else {
        return;
    };

    let (source_run, a, b, ti_a, ti_b) = chain_source();

    // The source dump: `a`'s and `b`'s outputs, a reserved graph mirror, and a
    // reserved replay key. Only `a` is pre-grounded.
    let out_a = task_envelope(ti_a, "value-a");
    let mirror = {
        let env = Envelope::new(
            "json",
            serde_json::json!({"graph": "parent"}),
            false,
            Producer::System {
                component: "conductor".to_string(),
            },
        );
        serde_json::to_value(&env).unwrap()
    };
    let ctx_dump = vec![
        dump_entry(source_run, "out_a", out_a.clone()),
        dump_entry(source_run, "out_b", task_envelope(ti_b, "value-b")),
        dump_entry(source_run, CTX_GRAPH_KEY, mirror),
    ];
    // Each node has an archived task-instance row used for producer attribution.
    let source = ArchivedRun {
        instance_id: source_run,
        replay_source: None,
        task_instances: vec![
            ArchivedTaskInstanceRow {
                id: ti_a,
                node_id: a,
            },
            ArchivedTaskInstanceRow {
                id: ti_b,
                node_id: b,
            },
        ],
        ctx_dump,
    };
    let pre_grounded: HashSet<Uuid> = [a].into_iter().collect();
    let replay_signal_id = Uuid::new_v4();
    let plan = plan_rehydration(&source, &HashMap::new(), &pre_grounded, replay_signal_id);

    assert_eq!(plan.carried.len(), 1, "only pre-grounded `a` carries");
    assert_eq!(plan.carried[0].name, "out_a");

    // Apply into the replay's fresh scope.
    let replay_run = Uuid::new_v4();
    apply_rehydration_via_nats(&nats, replay_run, &plan)
        .await
        .expect("apply re-hydration");

    let js = jetstream::new(nats.clone());
    let kv = js
        .get_key_value("ctx-default")
        .await
        .expect("ctx bucket exists after apply");
    let prefix = sanitize_segment(&replay_run.to_string());

    // 1. The carried key landed verbatim in the replay's own scope.
    let carried_key = format!("{}/out_a", prefix);
    let carried_entry = kv
        .entry(&carried_key)
        .await
        .expect("kv entry")
        .expect("carried key present in replay scope");
    let carried_env: Envelope = serde_json::from_slice(&carried_entry.value).unwrap();
    let archived_env: Envelope = serde_json::from_value(out_a).unwrap();
    assert_eq!(
        carried_env.created_at, archived_env.created_at,
        "created_at verbatim"
    );
    assert_eq!(carried_env.sha256, archived_env.sha256, "sha256 verbatim");
    assert_eq!(
        carried_env.producer, archived_env.producer,
        "producer verbatim"
    );

    // Reserved and out-of-set keys never made it into the replay scope.
    assert!(
        kv.get(&format!("{}/{}", prefix, CTX_GRAPH_KEY))
            .await
            .expect("kv get")
            .is_none(),
        "the parent's graph mirror never carries — the replay writes its own"
    );
    assert!(
        kv.get(&format!("{}/out_b", prefix))
            .await
            .expect("kv get")
            .is_none(),
        "an out-of-set producer's key is absent, not carried"
    );

    // 2. The sentinel landed as the final act: its revision exceeds the carried
    //    key's, and it carries the expected System-produced payload.
    let sentinel_key = format!("{}/{}", prefix, HYDRATION_SENTINEL_KEY);
    let sentinel_entry = kv
        .entry(&sentinel_key)
        .await
        .expect("kv entry")
        .expect("sentinel present");
    assert!(
        sentinel_entry.revision > carried_entry.revision,
        "sentinel (rev {}) must be written after every carried key (rev {})",
        sentinel_entry.revision,
        carried_entry.revision
    );
    let sentinel_env: Envelope = serde_json::from_slice(&sentinel_entry.value).unwrap();
    assert!(
        matches!(sentinel_env.producer, Producer::System { .. }),
        "the sentinel is System-produced"
    );
    assert_eq!(
        sentinel_env.value["carried_count"], 1,
        "sentinel records the carried count"
    );
    assert_eq!(
        sentinel_env.value["key_list_sha256"],
        serde_json::Value::String(plan.key_list_sha256.clone()),
        "sentinel records the carried-key-list digest"
    );
    assert_eq!(
        sentinel_env.value["signal_id"],
        serde_json::Value::String(replay_signal_id.to_string()),
        "sentinel records the replay signal id"
    );

    let _ = b;
}
