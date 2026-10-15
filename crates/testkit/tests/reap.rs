use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use pg_cdc_testkit::{AwsEmulator, CREATED_LABEL, Postgres, REAP_AGE, lock_reaper, reap};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn sleeper(
    created: u64,
) -> Result<ContainerAsync<GenericImage>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(GenericImage::new("postgres", "17-alpine")
        .with_wait_for(WaitFor::Nothing)
        .with_entrypoint("sleep")
        .with_cmd(["600"])
        .with_label(CREATED_LABEL, created.to_string())
        .start()
        .await?)
}

async fn listed(id: &str) -> Result<String, std::io::Error> {
    let output = tokio::process::Command::new("docker")
        .args(["ps", "-aq", "--filter", &format!("id={id}")])
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[tokio::test]
async fn reaping_removes_only_stale_labeled_containers() -> TestResult {
    let _reaper = lock_reaper().await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let old = sleeper(now - 7_200).await?;
    let young = sleeper(now).await?;
    let old_id = old.id().to_owned();
    let young_id = young.id().to_owned();
    assert_ne!(listed(&old_id).await?, "");

    let removed = reap(REAP_AGE).await?;

    assert!(removed >= 1);
    assert_eq!(listed(&old_id).await?, "");
    assert_ne!(listed(&young_id).await?, "");
    Ok(())
}

async fn labeled_on_port(port: u16) -> Result<String, std::io::Error> {
    let output = tokio::process::Command::new("docker")
        .args([
            "ps",
            "-q",
            "--filter",
            &format!("label={CREATED_LABEL}"),
            "--filter",
            &format!("publish={port}"),
        ])
        .output()
        .await?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[tokio::test]
async fn a_started_postgres_carries_the_created_label() -> TestResult {
    let postgres = Postgres::start().await?;
    assert_ne!(labeled_on_port(postgres.port()).await?, "");
    Ok(())
}

#[tokio::test]
async fn a_started_aws_emulator_carries_the_created_label() -> TestResult {
    let emulator = AwsEmulator::start().await?;
    let port: u16 = emulator
        .endpoint()
        .rsplit(':')
        .next()
        .ok_or("endpoint has no port")?
        .parse()?;
    assert_ne!(labeled_on_port(port).await?, "");
    Ok(())
}

const START_ENV: &str = "PG_CDC_TESTKIT_START";
const START_TEST: &str = "start_a_testkit_container_as_a_child_process";
const STAMP_FILE: &str = "pg-cdc-testkit-reap";

#[tokio::test]
#[ignore = "child process entry for the first-start reaping tests"]
async fn start_a_testkit_container_as_a_child_process() -> TestResult {
    match std::env::var(START_ENV).as_deref() {
        Ok("postgres") => drop(Postgres::start().await?),
        Ok("aws") => drop(AwsEmulator::start().await?),
        _ => {}
    }
    Ok(())
}

async fn the_first_start_reaps(kind: &str) -> TestResult {
    let _reaper = lock_reaper().await?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let old = sleeper(now - 7_200).await?;
    let young = sleeper(now).await?;
    let old_id = old.id().to_owned();
    let young_id = young.id().to_owned();
    let temp = tempfile::tempdir()?;

    let child = tokio::process::Command::new(std::env::current_exe()?)
        .args([START_TEST, "--exact", "--ignored"])
        .env(START_ENV, kind)
        .env("TMPDIR", temp.path())
        .stdin(Stdio::null())
        .output()
        .await?;

    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stdout)
    );
    assert_ne!(std::fs::metadata(temp.path().join(STAMP_FILE))?.len(), 0);
    assert_eq!(listed(&old_id).await?, "");
    assert_ne!(listed(&young_id).await?, "");
    Ok(())
}

#[tokio::test]
async fn the_first_postgres_start_reaps_stale_containers() -> TestResult {
    the_first_start_reaps("postgres").await
}

#[tokio::test]
async fn the_first_aws_emulator_start_reaps_stale_containers() -> TestResult {
    the_first_start_reaps("aws").await
}
