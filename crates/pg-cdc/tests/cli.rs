use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use pg_cdc_testkit::{AwsEmulator, Postgres, parse_lsn};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(30);

struct ConfigFile {
    file: tempfile::NamedTempFile,
    state: tempfile::TempDir,
}

impl ConfigFile {
    fn path(&self) -> &Path {
        self.file.path()
    }

    fn parked_files(&self) -> TestResult<Vec<PathBuf>> {
        let parked = self.state.path().join("cli").join("parked");
        let mut files = Vec::new();
        let Ok(keys) = std::fs::read_dir(&parked) else {
            return Ok(files);
        };
        for key in keys {
            for event in std::fs::read_dir(key?.path())? {
                let path = event?.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    files.push(path);
                }
            }
        }
        Ok(files)
    }

    async fn wait_for_parked(&self, count: usize) -> TestResult<Vec<PathBuf>> {
        tokio::time::timeout(WAIT, async {
            loop {
                let files = self.parked_files()?;
                if files.len() >= count {
                    return Ok(files);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?
    }
}

fn write_config(pg: &Postgres, slot: &str, status_interval: &str) -> TestResult<ConfigFile> {
    write_config_with_sink(
        pg,
        slot,
        status_interval,
        "delivery:\n  order_by: pipeline\nsink:\n  type: stdout\n",
    )
}

fn write_config_with_sink(
    pg: &Postgres,
    slot: &str,
    status_interval: &str,
    sections: &str,
) -> TestResult<ConfigFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    let state = tempfile::tempdir()?;
    let state_store = if sections.contains("state_store:") {
        String::new()
    } else {
        format!(
            "state_store:\n  type: filesystem\n  path: {}\n",
            state.path().display()
        )
    };
    write!(
        file,
        "pipeline: cli\nsource:\n  host: {host}\n  port: {port}\n  database: {database}\n  user: {user}\n  credentials:\n    type: password\n  tls:\n    mode: disable\n  slot: {slot}\n  publication: cli_pub\n  status_interval: {status_interval}\n{sections}{state_store}",
        host = pg.host(),
        port = pg.port(),
        database = pg.database(),
        user = pg.user(),
    )?;
    Ok(ConfigFile { file, state })
}

fn spawn(config: &Path, password: &str, stderr: Stdio) -> TestResult<Child> {
    spawn_with_env(config, password, stderr, &[])
}

fn spawn_with_env(
    config: &Path,
    password: &str,
    stderr: Stdio,
    env: &[(&str, &str)],
) -> TestResult<Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
        .arg("run")
        .arg("--config")
        .arg(config)
        .env("PGCDC__SOURCE__CREDENTIALS__PASSWORD", password)
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()?)
}

fn moto_env(aws: &AwsEmulator) -> Vec<(&str, &str)> {
    vec![
        ("AWS_ENDPOINT_URL", aws.endpoint()),
        ("AWS_ACCESS_KEY_ID", "test"),
        ("AWS_SECRET_ACCESS_KEY", "test"),
        ("AWS_REGION", "us-east-1"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ]
}

async fn wait_for_line<R: AsyncBufRead + Unpin>(
    lines: &mut Lines<R>,
    needle: &str,
) -> TestResult<String> {
    tokio::time::timeout(WAIT, async {
        while let Some(line) = lines.next_line().await? {
            if line.contains(needle) {
                return Ok(line);
            }
        }
        Err(format!("output ended before {needle:?}").into())
    })
    .await?
}

async fn next_event(lines: &mut Lines<BufReader<ChildStdout>>) -> TestResult<Value> {
    let line = tokio::time::timeout(WAIT, lines.next_line())
        .await??
        .ok_or("stdout closed")?;
    Ok(serde_json::from_str(&line)?)
}

async fn sigterm(child: &Child) -> TestResult {
    let pid = child.id().ok_or("process already exited")?;
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    if status.success() {
        Ok(())
    } else {
        Err("kill -TERM failed".into())
    }
}

async fn prepare(pg: &Postgres, create_slot: bool) -> TestResult {
    pg.execute("CREATE TABLE orders (id int PRIMARY KEY, status text)")
        .await?;
    pg.create_publication("cli_pub", &["orders"]).await?;
    if create_slot {
        pg.create_slot("cli_slot").await?;
    }
    Ok(())
}

#[tokio::test]
async fn run_streams_to_stdout_acknowledges_and_exits_cleanly_on_sigterm() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let config = write_config(&pg, "cli_slot", "200ms")?;
    let mut child = spawn(config.path(), pg.password(), Stdio::null())?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    pg.execute("INSERT INTO orders VALUES (1, 'paid')").await?;
    let event = next_event(&mut stdout).await?;
    assert_eq!(event["op"], "insert");
    assert_eq!(event["after"]["status"], "paid");
    assert_eq!(event["source"]["pipeline"], "cli");
    let commit = parse_lsn(
        event["source"]["commit_lsn"]
            .as_str()
            .ok_or("no commit_lsn")?,
    )?;
    pg.wait_for_slot("cli_slot", WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn > commit)
    })
    .await?;
    sigterm(&child).await?;
    let status = tokio::time::timeout(WAIT, child.wait()).await??;
    assert!(status.success(), "{status}");
    Ok(())
}

#[tokio::test]
async fn d4_reconnect_rebuilds_relation_cache_and_replays_unacknowledged() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let config = write_config(&pg, "cli_slot", "1h")?;
    let mut child = spawn(config.path(), pg.password(), Stdio::null())?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    pg.execute("INSERT INTO orders VALUES (1, 'before reconnect')")
        .await?;
    let first = next_event(&mut stdout).await?;
    assert_eq!(first["after"]["id"], 1);
    assert!(pg.terminate_walsender("cli_slot").await?);
    pg.execute("ALTER TABLE orders ADD COLUMN note text DEFAULT 'n'")
        .await?;
    pg.execute("INSERT INTO orders VALUES (2, 'after reconnect')")
        .await?;
    let mut after = Vec::new();
    loop {
        let event = next_event(&mut stdout).await?;
        let done = event["after"]["id"] == 2;
        after.push(event);
        if done {
            break;
        }
    }
    let [replayed, second] = after.as_slice() else {
        return Err(format!("expected a replay and insert 2, got {after:?}").into());
    };
    assert_eq!(replayed, &first, "transaction 1 did not replay unchanged");
    assert!(replayed["after"].get("note").is_none(), "{replayed}");
    assert_eq!(
        second["after"],
        serde_json::json!({"id": 2, "status": "after reconnect", "note": "n"})
    );
    sigterm(&child).await?;
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    Ok(())
}

#[tokio::test]
async fn sigterm_while_the_connection_provider_is_built_exits_cleanly() -> TestResult {
    let imds = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", imds.local_addr()?);
    let (contacted, mut first_contact) = tokio::sync::mpsc::channel(1);
    let stalled = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = imds.accept().await {
            held.push(socket);
            let _ = contacted.try_send(());
        }
    });
    let mut file = tempfile::NamedTempFile::new()?;
    write!(
        file,
        "pipeline: cli\nsource:\n  host: 127.0.0.1\n  database: postgres\n  user: pgcdc\n  credentials:\n    type: aws_rds_iam\n  slot: s\n  publication: p\nsink:\n  type: stdout\nstate_store:\n  type: filesystem\n  path: /nonexistent/pg-cdc\n"
    )?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
        .arg("run")
        .arg("--config")
        .arg(file.path())
        .env_remove("AWS_REGION")
        .env_remove("AWS_DEFAULT_REGION")
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_EC2_METADATA_DISABLED")
        .env("AWS_CONFIG_FILE", "/nonexistent/aws/config")
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/aws/credentials",
        )
        .env("AWS_EC2_METADATA_SERVICE_ENDPOINT", &endpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(WAIT, first_contact.recv())
        .await?
        .ok_or("the metadata stub stopped")?;
    sigterm(&child).await?;
    let status = tokio::time::timeout(WAIT, child.wait()).await??;
    stalled.abort();
    assert!(status.success(), "{status}");
    Ok(())
}

#[tokio::test]
async fn s1_missing_slot_is_never_created() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, false).await?;
    let config = write_config(&pg, "missing_slot", "200ms")?;
    let mut child = spawn(config.path(), pg.password(), Stdio::piped())?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        pg.slot_state("missing_slot").await?.is_none(),
        "pg-cdc created a slot"
    );
    assert!(
        child.try_wait()?.is_none(),
        "pg-cdc must keep retrying, not exit"
    );
    child.start_kill()?;
    let output = child.wait_with_output().await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("replication slot missing_slot does not exist"),
        "{stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn password_in_config_file_is_rejected() -> TestResult {
    let mut file = tempfile::NamedTempFile::new()?;
    write!(
        file,
        "pipeline: cli\nsource:\n  host: 127.0.0.1\n  database: postgres\n  user: postgres\n  credentials:\n    type: password\n    password: hunter2\n  slot: s\n  publication: p\nsink:\n  type: stdout\n"
    )?;
    let output = Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
        .arg("run")
        .arg("--config")
        .arg(file.path())
        .output()
        .await?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("PGCDC__SOURCE__CREDENTIALS__PASSWORD"),
        "{stderr}"
    );
    assert!(!stderr.contains("hunter2"), "{stderr}");
    assert_eq!(output.stdout, Vec::<u8>::new());
    Ok(())
}

#[tokio::test]
async fn sigterm_after_stdout_closes_parks_the_undelivered_event_and_exits_cleanly() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let config = write_config(&pg, "cli_slot", "1h")?;
    let mut child = spawn(config.path(), pg.password(), Stdio::piped())?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    let mut stderr = BufReader::new(child.stderr.take().ok_or("no stderr")?).lines();
    pg.execute("INSERT INTO orders VALUES (1, 'delivered')")
        .await?;
    let first = next_event(&mut stdout).await?;
    let first_commit = parse_lsn(
        first["source"]["commit_lsn"]
            .as_str()
            .ok_or("no commit_lsn")?,
    )?;
    drop(stdout);
    pg.execute("INSERT INTO orders VALUES (2, 'undeliverable')")
        .await?;
    let written = pg.current_wal_lsn().await?;
    wait_for_line(&mut stderr, "delivery failed").await?;
    sigterm(&child).await?;
    let status = tokio::time::timeout(WAIT, child.wait()).await??;
    assert!(status.success(), "{status}");
    let state = pg
        .wait_for_slot("cli_slot", WAIT, |state| !state.active)
        .await?;
    let confirmed = state.confirmed_flush_lsn.ok_or("no position")?;
    assert!(
        confirmed > first_commit,
        "the final status update did not confirm the delivered insert"
    );
    assert!(
        confirmed >= written,
        "confirmed {confirmed:X} before the parked insert ending at {written:X}"
    );
    let parked = config.parked_files()?;
    assert_eq!(parked.len(), 1, "{parked:?}");
    let event: Value = serde_json::from_slice(&std::fs::read(&parked[0])?)?;
    assert_eq!(event["envelope"]["after"]["status"], "undeliverable");
    assert_eq!(event["status"], "retrying");
    Ok(())
}

#[tokio::test]
async fn run_publishes_to_sns_with_attributes_and_logs_to_stdout() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("cli", false).await?;
    let sink = format!(
        "sink:\n  type: sns\n  topic_arn: {}\n  attribute_columns: [status]\n",
        route.topic_arn
    );
    let config = write_config_with_sink(&pg, "cli_slot", "200ms", &sink)?;
    let mut child = spawn_with_env(
        config.path(),
        pg.password(),
        Stdio::piped(),
        &moto_env(&aws),
    )?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    wait_for_line(&mut stdout, "pg-cdc starting").await?;
    let ready: Value = serde_json::from_str(&wait_for_line(&mut stdout, "SNS topic ready").await?)?;
    assert_eq!(ready["fields"]["max_message_bytes"], 256 * 1024);
    assert_eq!(ready["fields"]["limit_from_topic"], false);
    assert_eq!(ready["fields"]["fifo"], false);
    pg.execute("INSERT INTO orders VALUES (1, 'paid')").await?;
    let messages = aws.receive(&route.queue_url, 1, WAIT).await?;
    let message = messages.first().ok_or("no message")?;
    let body: Value = serde_json::from_str(message.body().ok_or("no body")?)?;
    assert_eq!(body["op"], "insert");
    assert_eq!(
        body["after"],
        serde_json::json!({"id": 1, "status": "paid"})
    );
    let status = message
        .message_attributes()
        .and_then(|attributes| attributes.get("status"))
        .and_then(|value| value.string_value());
    assert_eq!(status, Some("paid"));
    let commit = parse_lsn(
        body["source"]["commit_lsn"]
            .as_str()
            .ok_or("no commit_lsn")?,
    )?;
    pg.wait_for_slot("cli_slot", WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn > commit)
    })
    .await?;
    sigterm(&child).await?;
    let stopped = wait_for_line(&mut stdout, "pg-cdc stopped cleanly").await?;
    assert_eq!(serde_json::from_str::<Value>(&stopped)?["level"], "INFO");
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    let stderr = child.stderr.take().ok_or("no stderr")?;
    let mut errors = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut BufReader::new(stderr), &mut errors).await?;
    assert_eq!(errors, "");
    Ok(())
}

#[tokio::test]
async fn run_with_the_discard_sink_acknowledges_and_writes_only_logs() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let config = write_config_with_sink(&pg, "cli_slot", "200ms", "sink:\n  type: discard\n")?;
    let mut child = spawn(config.path(), pg.password(), Stdio::null())?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    wait_for_line(&mut stdout, "pg-cdc starting").await?;
    pg.execute("INSERT INTO orders SELECT g, 'x' FROM generate_series(1, 100) g")
        .await?;
    let written = pg.current_wal_lsn().await?;
    pg.wait_for_slot("cli_slot", WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= written)
    })
    .await?;
    sigterm(&child).await?;
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    while let Some(line) = stdout.next_line().await? {
        let log: Value = serde_json::from_str(&line)?;
        assert!(
            log.get("level").is_some() && log.get("op").is_none(),
            "{line}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_missing_sns_topic_keeps_preflight_retrying_without_taking_the_slot() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let aws = AwsEmulator::start().await?;
    let sink = "sink:\n  type: sns\n  topic_arn: arn:aws:sns:us-east-1:123456789012:missing\n";
    let config = write_config_with_sink(&pg, "cli_slot", "200ms", sink)?;
    let mut child = spawn_with_env(config.path(), pg.password(), Stdio::null(), &moto_env(&aws))?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    let failed = wait_for_line(&mut stdout, "sink preflight failed").await?;
    assert!(failed.contains("NotFound"), "{failed}");
    let state = pg.slot_state("cli_slot").await?.ok_or("slot missing")?;
    assert!(
        !state.active,
        "pg-cdc took the slot before its sink passed preflight"
    );
    assert!(
        child.try_wait()?.is_none(),
        "pg-cdc must keep retrying, not exit"
    );
    sigterm(&child).await?;
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    Ok(())
}

#[tokio::test]
async fn a_missing_state_bucket_keeps_the_store_check_retrying_without_taking_the_slot()
-> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let aws = AwsEmulator::start().await?;
    let sections = "sink:\n  type: discard\nstate_store:\n  type: s3\n  bucket: missing-state\n";
    let config = write_config_with_sink(&pg, "cli_slot", "200ms", sections)?;
    let mut child = spawn_with_env(config.path(), pg.password(), Stdio::null(), &moto_env(&aws))?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    let failed = wait_for_line(&mut stdout, "state store check failed").await?;
    assert!(failed.contains("NoSuchBucket"), "{failed}");
    let state = pg.slot_state("cli_slot").await?.ok_or("slot missing")?;
    assert!(
        !state.active,
        "pg-cdc took the slot before its state store answered"
    );
    assert!(
        child.try_wait()?.is_none(),
        "pg-cdc must keep retrying, not exit"
    );
    sigterm(&child).await?;
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    Ok(())
}

#[tokio::test]
async fn a_deleted_topic_parks_events_on_disk_and_the_slot_advances() -> TestResult {
    let pg = Postgres::start().await?;
    prepare(&pg, true).await?;
    let aws = AwsEmulator::start().await?;
    let route = aws.topic_with_queue("cli", false).await?;
    let sink = format!("sink:\n  type: sns\n  topic_arn: {}\n", route.topic_arn);
    let config = write_config_with_sink(&pg, "cli_slot", "200ms", &sink)?;
    let mut env = moto_env(&aws);
    env.push(("PGCDC__DELIVERY__PARK_AFTER", "1s"));
    let mut child = spawn_with_env(config.path(), pg.password(), Stdio::null(), &env)?;
    let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?).lines();
    wait_for_line(&mut stdout, "SNS topic ready").await?;
    aws.sns()
        .await
        .delete_topic()
        .topic_arn(&route.topic_arn)
        .send()
        .await?;
    pg.execute("INSERT INTO orders VALUES (1, 'parked')")
        .await?;
    let written = pg.current_wal_lsn().await?;
    let parked = config.wait_for_parked(1).await?;
    let event: Value = serde_json::from_slice(&std::fs::read(&parked[0])?)?;
    assert_eq!(event["envelope"]["after"]["status"], "parked");
    pg.wait_for_slot("cli_slot", WAIT, |state| {
        state.confirmed_flush_lsn.is_some_and(|lsn| lsn >= written)
    })
    .await?;
    sigterm(&child).await?;
    assert!(tokio::time::timeout(WAIT, child.wait()).await??.success());
    Ok(())
}
