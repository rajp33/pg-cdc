use std::time::Duration;

pub mod gate;
pub mod lease;
pub mod record;
pub mod standby;

pub const POLL_INTERVAL: Duration = Duration::from_secs(2);
