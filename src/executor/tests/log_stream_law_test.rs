#[path = "../../../tests/support/log_stream_laws.rs"]
mod log_stream_laws;

use anyhow::Result;
use async_nats::jetstream;
use std::sync::Arc;
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ImageExt;
use tickr_executor::log_stream::{
    ensure_all_nats_log_stream, AllNatsLogStream, LogStream, LogStreamRoute,
};
use tickr_proto::coord::all_nats;
use tickr_proto::coord::log_stream::{AcceptOutcome, ReplayedLogRecord};
use uuid::Uuid;

async fn start_nats() -> Option<(
    testcontainers_modules::testcontainers::ContainerAsync<Nats>,
    async_nats::Client,
)> {
    let command = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default()
        .with_tag("2.11.8-alpine")
        .with_cmd(&command)
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            eprintln!("skipping: NATS testcontainer unavailable: {error}");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4222).await.ok()?;
    let url = format!("nats://127.0.0.1:{port}");
    for _ in 0..20 {
        if let Ok(client) = async_nats::connect(&url).await {
            return Some((container, client));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[tokio::test]
async fn all_nats_adapter_satisfies_log_stream_laws_and_ambiguous_retry() -> Result<()> {
    let Some((_container, nats)) = start_nats().await else {
        return Ok(());
    };
    ensure_all_nats_log_stream(&nats).await?;
    let js = Arc::new(jetstream::new(nats));
    let workflow_id = Uuid::new_v4();
    let workflow_instance_id = Uuid::new_v4();
    log_stream_laws::assert_log_stream_laws({
        let js = Arc::clone(&js);
        let workflow_id = workflow_id;
        let workflow_instance_id = workflow_instance_id;
        move |identity, timeout| {
            let js = Arc::clone(&js);
            let route = LogStreamRoute {
                workflow_id,
                workflow_instance_id,
                task_instance_id: identity.task_instance_id,
            };
            Box::pin(async move {
                Ok(
                    Box::new(AllNatsLogStream::open(js, route, identity, timeout).await?)
                        as Box<dyn LogStream>,
                )
            })
        }
    })
    .await?;

    let ambiguous = log_stream_laws::identity(Uuid::new_v4(), 1);
    let route = LogStreamRoute {
        workflow_id,
        workflow_instance_id,
        task_instance_id: ambiguous.task_instance_id,
    };
    let mut storage = js.get_stream(all_nats::LOG_STREAM).await?;
    let messages_before_timeout = storage.info().await?.state.messages;
    let mut timed_out = AllNatsLogStream::open(
        Arc::clone(&js),
        route.clone(),
        ambiguous.clone(),
        Duration::ZERO,
    )
    .await?;
    assert!(timed_out
        .accept(log_stream_laws::submission(
            &ambiguous,
            0,
            b"accepted before lost ack",
        ))
        .await
        .is_err());
    let mut accepted_before_lost_ack = false;
    for _ in 0..50 {
        if storage.info().await?.state.messages > messages_before_timeout {
            accepted_before_lost_ack = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        accepted_before_lost_ack,
        "the zero-timeout publish must land before retry"
    );
    drop(timed_out);
    let mut retry =
        AllNatsLogStream::open(js, route, ambiguous.clone(), Duration::from_secs(2)).await?;
    assert_eq!(
        retry
            .accept(log_stream_laws::submission(
                &ambiguous,
                0,
                b"accepted before lost ack",
            ))
            .await?,
        AcceptOutcome::AlreadyAccepted
    );
    assert_eq!(
        retry
            .replay()
            .await?
            .iter()
            .filter(|record| matches!(record, ReplayedLogRecord::Accepted { .. }))
            .count(),
        1
    );
    Ok(())
}
