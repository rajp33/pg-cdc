use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub mod gate;
pub mod lease;
pub mod record;
pub mod standby;

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) async fn unless_cancelled<F: Future>(
    cancel: &CancellationToken,
    future: F,
) -> Option<F::Output> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => None,
        output = future => Some(output),
    }
}
