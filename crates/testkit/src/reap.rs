use std::path::Path;
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

pub(crate) async fn reap_once() {
    REAPED
        .get_or_init(|| async {
            let stamp = std::env::temp_dir().join(STAMP_FILE);
            if claim_stamp(&stamp, SystemTime::now()).await {
                let _ = reap(REAP_AGE).await;
            }
        })
        .await;
}

async fn claim_stamp(stamp: &Path, now: SystemTime) -> bool {
    let fresh = tokio::fs::metadata(stamp)
        .await
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|modified| {
            now.duration_since(modified)
                .is_ok_and(|elapsed| elapsed < STAMP_INTERVAL)
        });
    if fresh {
        return false;
    }
    tokio::fs::write(stamp, unix_seconds(now).to_string())
        .await
        .is_ok()
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
        assert!(claim_stamp(&stamp, now).await);
        assert!(!claim_stamp(&stamp, now + Duration::from_secs(599)).await);
        assert!(claim_stamp(&stamp, now + Duration::from_secs(601)).await);
    }

    #[tokio::test]
    async fn an_unwritable_stamp_location_does_not_claim() {
        let dir = tempfile::tempdir().unwrap();
        let stamp = dir.path().join("missing").join("reap");
        assert!(!claim_stamp(&stamp, SystemTime::now()).await);
    }
}
