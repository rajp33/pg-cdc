use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::OnceCell;

use crate::error::TestkitError;

pub const CREATED_LABEL: &str = "pg-cdc-testkit.created";
pub const REAP_AGE: Duration = Duration::from_hours(1);

const STAMP_FILE: &str = "pg-cdc-testkit-reap";
const STAMP_INTERVAL: Duration = Duration::from_mins(10);

static REAPED: OnceCell<()> = OnceCell::const_new();

pub fn stale_containers(listing: &str, now_unix: u64, age: Duration) -> Vec<String> {
    listing
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let id = fields.next()?;
            let created: u64 = fields.next()?.parse().ok()?;
            if fields.next().is_some() {
                return None;
            }
            (created.saturating_add(age.as_secs()) <= now_unix).then(|| id.to_owned())
        })
        .collect()
}

pub async fn reap(age: Duration) -> Result<usize, TestkitError> {
    let format = format!("{{{{.ID}}}} {{{{.Label \"{CREATED_LABEL}\"}}}}");
    let listing = docker(&[
        "ps",
        "-a",
        "--filter",
        &format!("label={CREATED_LABEL}"),
        "--format",
        &format,
    ])
    .await?;
    let stale = stale_containers(&listing, unix_seconds(SystemTime::now()), age);
    if stale.is_empty() {
        return Ok(0);
    }
    let mut args = vec!["rm", "-f"];
    args.extend(stale.iter().map(String::as_str));
    docker(&args).await?;
    Ok(stale.len())
}

pub(crate) fn created_label() -> (String, String) {
    (
        CREATED_LABEL.to_owned(),
        unix_seconds(SystemTime::now()).to_string(),
    )
}

#[derive(Debug)]
pub struct ReaperLock {
    _stamp: File,
}

pub async fn lock_reaper() -> Result<ReaperLock, TestkitError> {
    let stamp = stamp_path();
    tokio::task::spawn_blocking(move || lock_stamp(&stamp))
        .await
        .map_err(std::io::Error::other)?
}

pub(crate) async fn reap_once() {
    REAPED
        .get_or_init(|| async {
            if let Some(_claim) = claim_stamp(&stamp_path(), SystemTime::now()).await {
                let _ = reap(REAP_AGE).await;
            }
        })
        .await;
}

fn stamp_path() -> PathBuf {
    std::env::temp_dir().join(STAMP_FILE)
}

async fn claim_stamp(stamp: &Path, now: SystemTime) -> Option<ReaperLock> {
    let stamp = stamp.to_path_buf();
    tokio::task::spawn_blocking(move || claim_stamp_blocking(&stamp, now))
        .await
        .ok()
        .flatten()
}

fn claim_stamp_blocking(stamp: &Path, now: SystemTime) -> Option<ReaperLock> {
    let mut file = open_stamp(stamp).ok()?;
    file.try_lock().ok()?;
    let metadata = file.metadata().ok()?;
    let fresh = metadata.len() > 0
        && metadata.modified().is_ok_and(|modified| {
            now.duration_since(modified)
                .is_ok_and(|elapsed| elapsed < STAMP_INTERVAL)
        });
    if fresh {
        return None;
    }
    file.set_len(0).ok()?;
    file.write_all(unix_seconds(now).to_string().as_bytes())
        .ok()?;
    file.set_modified(now).ok()?;
    Some(ReaperLock { _stamp: file })
}

fn lock_stamp(stamp: &Path) -> Result<ReaperLock, TestkitError> {
    let file = open_stamp(stamp)?;
    file.lock()?;
    Ok(ReaperLock { _stamp: file })
}

fn open_stamp(stamp: &Path) -> std::io::Result<File> {
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(stamp)
}

async fn docker(args: &[&str]) -> Result<String, TestkitError> {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await?;
    if !output.status.success() {
        return Err(TestkitError::Docker(
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn unix_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn stale_containers_are_older_than_the_age() {
        let now = 100_000;
        let listing = format!(
            "aaa {}\nbbb {}\nnot-a-listing-line\nccc notanumber\nddd {} extra\n",
            now - 7_200,
            now - 600,
            now - 9_000,
        );
        assert_eq!(
            stale_containers(&listing, now, REAP_AGE),
            vec!["aaa".to_owned()]
        );
        assert_eq!(
            stale_containers(&listing, now, Duration::ZERO),
            vec!["aaa".to_owned(), "bbb".to_owned()]
        );
    }

    #[test]
    fn a_container_stamped_in_the_future_is_never_stale() {
        let listing = "future 5000\n";
        assert_eq!(
            stale_containers(listing, 1_000, Duration::ZERO),
            Vec::<String>::new()
        );
    }

    #[test]
    fn the_created_label_carries_the_current_unix_time() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (key, value) = created_label();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(key, CREATED_LABEL);
        let stamped: u64 = value.parse().unwrap();
        assert!((before..=after).contains(&stamped));
    }

    #[tokio::test]
    async fn the_stamp_is_claimed_once_per_ten_minutes() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("reap");
        let now = SystemTime::now();
        assert!(claim_stamp(&stamp, now).await.is_some());
        assert!(
            claim_stamp(&stamp, now + Duration::from_secs(599))
                .await
                .is_none()
        );
        assert!(
            claim_stamp(&stamp, now + Duration::from_secs(601))
                .await
                .is_some()
        );
    }

    async fn concurrent_claims(stamp: &Path, now: SystemTime) -> usize {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
        let claims: Vec<_> = (0..16)
            .map(|_| {
                let barrier = barrier.clone();
                let stamp = stamp.to_path_buf();
                tokio::spawn(async move {
                    barrier.wait().await;
                    claim_stamp(&stamp, now).await.is_some()
                })
            })
            .collect();
        let mut claimed = 0;
        for claim in claims {
            if claim.await.unwrap() {
                claimed += 1;
            }
        }
        claimed
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn exactly_one_concurrent_caller_claims_an_absent_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("reap");
        assert_eq!(concurrent_claims(&stamp, SystemTime::now()).await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn exactly_one_concurrent_caller_claims_an_expired_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("reap");
        let now = SystemTime::now();
        assert!(claim_stamp(&stamp, now).await.is_some());
        let later = now + STAMP_INTERVAL + Duration::from_secs(1);
        assert_eq!(concurrent_claims(&stamp, later).await, 1);
    }

    #[test]
    fn a_held_reaper_lock_refuses_every_claim_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("reap");
        let lock = lock_stamp(&stamp).unwrap();
        assert!(claim_stamp_blocking(&stamp, SystemTime::now()).is_none());
        drop(lock);
        assert!(claim_stamp_blocking(&stamp, SystemTime::now()).is_some());
    }

    #[tokio::test]
    async fn an_unwritable_stamp_location_does_not_claim() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("missing").join("reap");
        assert!(claim_stamp(&stamp, SystemTime::now()).await.is_none());
    }
}
