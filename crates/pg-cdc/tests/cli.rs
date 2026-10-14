use std::io::Write as _;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use pg_cdc_testkit::{Postgres, parse_lsn};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(30);

fn write_config(
    pg: &Postgres,
    slot: &str,
    status_interval: &str,
) -> TestResult<tempfile::NamedTempFile> {
    let mut file = tempfile::NamedTempFile::new()?;
    write!(
        file,
        "pipeline: cli\nsource:\n  host: {host}\n  port: {port}\n  database: {database}\n  user: {user}\n  credentials:\n    type: password\n  tls:\n    mode: disable\n  slot: {slot}\n  publication: cli_pub\n  status_interval: {status_interval}\ndelivery:\n  order_by: pipeline\nsink:\n  type: stdout\n",
        host = pg.host(),
        port = pg.port(),
        database = pg.database(),
        user = pg.user(),
    )?;
    Ok(file)
}

fn spawn(config: &Path, password: &str, stderr: Stdio) -> TestResult<Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
        .arg("run")
        .arg("--config")
        .arg(config)
        .env("PGCDC__SOURCE__CREDENTIALS__PASSWORD", password)
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true)
        .spawn()?)
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
        "pipeline: cli\nsource:\n  host: 127.0.0.1\n  database: postgres\n  user: pgcdc\n  credentials:\n    type: aws_rds_iam\n  slot: s\n  publication: p\nsink:\n  type: stdout\n"
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
