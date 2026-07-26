#[path = "support/log_stream_laws.rs"]
mod log_stream_laws;

use anyhow::Result;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tickr::data_directory::DataDirectory;
use tickr::local_log_staging::LocalLogStagingStream;
use tickr_executor::log_stream::LogStream;

#[tokio::test]
async fn local_adapter_satisfies_log_stream_laws() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700))?;
    let directory = Arc::new(DataDirectory::admit(temporary.path())?);
    log_stream_laws::assert_log_stream_laws(move |identity, _| {
        let directory = Arc::clone(&directory);
        Box::pin(async move {
            Ok(Box::new(LocalLogStagingStream::open(&directory, identity)?) as Box<dyn LogStream>)
        })
    })
    .await
}
