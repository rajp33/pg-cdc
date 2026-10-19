use std::collections::{BTreeMap, BTreeSet};
use std::io::{PipeReader, PipeWriter, Write as _};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pg_cdc_testkit::{Postgres, PostgresOptions};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::{sleep, timeout};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Records = Arc<Mutex<Vec<(Instant, Value)>>>;
type Rows = BTreeSet<(i64, String)>;

const WAIT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);
const FENCE_DELAY: Duration = Duration::from_millis(4500);
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(7);
const SHUTDOWN_BOUND: Duration = Duration::from_secs(5);
const EXIT_SLACK: Duration = Duration::from_millis(500);
const TAKEOVER_WINDOW: Duration = Duration::from_secs(8);
const LOSER_QUIET: Duration = Duration::from_secs(3);
const SLOT: &str = "handover_slot";
const TOOK_OVER: &str = "took over the slot";
const STANDBY: &str = "waiting as standby";
const RECONNECTING: &str = "replication session ended; reconnecting";

struct Deployment {
    pg: Postgres,
    config: tempfile::NamedTempFile,
    state: tempfile::TempDir,
}

impl Deployment {
    async fn start() -> TestResult<Self> {
        Self::start_with_sink("stdout").await
    }

    async fn start_with_sink(sink: &str) -> TestResult<Self> {
        Self::start_on(PostgresOptions::default(), sink).await
    }

    async fn start_on(options: PostgresOptions, sink: &str) -> TestResult<Self> {
        let pg = Postgres::start_with(options).await?;
        pg.execute("CREATE TABLE orders (id int PRIMARY KEY, status text)")
            .await?;
        pg.create_publication("handover_pub", &["orders"]).await?;
        pg.create_slot(SLOT).await?;
        let state = tempfile::tempdir()?;
        let mut config = tempfile::NamedTempFile::new()?;
        write!(
            config,
            "pipeline: handover\nsource:\n  host: {host}\n  port: {port}\n  database: {database}\n  user: {user}\n  credentials:\n    type: password\n  tls:\n    mode: disable\n  slot: {SLOT}\n  publication: handover_pub\n  status_interval: 200ms\ndelivery:\n  order_by: key\n  lanes: 4\nsink:\n  type: {sink}\nstate_store:\n  type: filesystem\n  path: {path}\nleadership:\n  lease_timeout: 4s\n  fence_delay: 4500ms\n  drain_timeout: 1s\nadmin:\n  listen: 127.0.0.1:0\n",
            host = pg.host(),
            port = pg.port(),
            database = pg.database(),
            user = pg.user(),
            path = state.path().display(),
        )?;
        Ok(Self { pg, config, state })
    }

    fn spawn(&self) -> TestResult<Task> {
        Task::spawn(self.config.path(), self.pg.password())
    }

    fn spawnunread(&self) -> TestResult<Task> {
        Task::spawnunread(self.config.path(), self.pg.password())
    }

    async fn spawn_with_full_log_pipe(&self, logs: Logs) -> TestResult<(Task, PipeReader)> {
        Task::spawn_with_full_log_pipe(self.config.path(), self.pg.password(), logs).await
    }

    fn leader_record(&self) -> TestResult<Value> {
        leader_record(self.state.path())
    }

    fn config_as(&self, user: &str) -> TestResult<tempfile::NamedTempFile> {
        let text = std::fs::read_to_string(self.config.path())?.replace(
            &format!("\n  user: {}\n", self.pg.user()),
            &format!("\n  user: {user}\n"),
        );
        let mut config = tempfile::NamedTempFile::new()?;
        config.write_all(text.as_bytes())?;
        Ok(config)
    }

    async fn wait_for_record(&self, state: &str, epoch: u64) -> TestResult<bool> {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Ok(record) = self.leader_record()
                && record.get("state") == Some(&Value::from(state))
                && record.get("epoch") == Some(&Value::from(epoch))
            {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            sleep(POLL).await;
        }
    }

    async fn insert(&self, first: i32, last: i32, status: &str) -> TestResult {
        self.pg
            .execute(&format!(
                "INSERT INTO orders SELECT g, '{status}' FROM generate_series({first}, {last}) g"
            ))
            .await?;
        Ok(())
    }

    async fn update(&self, last: i32, status: &str) -> TestResult {
        self.pg
            .execute(&format!(
                "UPDATE orders SET status = '{status}' WHERE id <= {last}"
            ))
            .await?;
        Ok(())
    }

    async fn walsender_pid(&self) -> TestResult<Option<i64>> {
        Ok(self
            .pg
            .slot_state(SLOT)
            .await?
            .and_then(|state| state.active_pid)
            .map(i64::from))
    }
}

#[derive(Clone, Copy)]
enum Logs {
    Stdout,
    Stderr,
}

struct Task {
    child: Child,
    events: Records,
    logs: Records,
    unread: Option<ChildStdout>,
}

impl Task {
    fn spawn(config: &Path, password: &str) -> TestResult<Self> {
        let mut task = Self::spawnunread(config, password)?;
        let stdout = task.unread.take().ok_or("no stdout")?;
        collect(stdout, task.events.clone());
        Ok(task)
    }

    fn spawnunread(config: &Path, password: &str) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
            .arg("run")
            .arg("--config")
            .arg(config)
            .env("PGCDC__SOURCE__CREDENTIALS__PASSWORD", password)
            .env_remove("PGCDC__ADMIN__LISTEN")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let logs = Records::default();
        let unread = child.stdout.take().ok_or("no stdout")?;
        collect(child.stderr.take().ok_or("no stderr")?, logs.clone());
        Ok(Self {
            child,
            events: Records::default(),
            logs,
            unread: Some(unread),
        })
    }

    async fn spawn_with_full_log_pipe(
        config: &Path,
        password: &str,
        logs: Logs,
    ) -> TestResult<(Self, PipeReader)> {
        let (reader, writer) = full_pipe().await?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_pg-cdc"));
        command
            .arg("run")
            .arg("--config")
            .arg(config)
            .env("PGCDC__SOURCE__CREDENTIALS__PASSWORD", password)
            .env_remove("PGCDC__ADMIN__LISTEN")
            .kill_on_drop(true);
        match logs {
            Logs::Stdout => command.stdout(writer).stderr(Stdio::null()),
            Logs::Stderr => command.stdout(Stdio::piped()).stderr(writer),
        };
        let mut child = command.spawn()?;
        let unread = child.stdout.take();
        let task = Self {
            child,
            events: Records::default(),
            logs: Records::default(),
            unread,
        };
        Ok((task, reader))
    }

    fn read_events(&mut self) -> TestResult {
        let stdout = self.unread.take().ok_or("no stdout")?;
        collect(stdout, self.events.clone());
        Ok(())
    }

    fn events(&self) -> TestResult<Vec<(Instant, Value)>> {
        snapshot(&self.events)
    }

    fn find_log(&self, message: &str) -> TestResult<Option<Value>> {
        Ok(snapshot(&self.logs)?
            .into_iter()
            .map(|(_, log)| log)
            .find(|log| log_message(log) == Some(message)))
    }

    async fn wait_for_log_within(
        &self,
        message: &str,
        within: Duration,
    ) -> TestResult<Option<Value>> {
        let deadline = Instant::now() + within;
        loop {
            if let Some(log) = self.find_log(message)? {
                return Ok(Some(log));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            sleep(POLL).await;
        }
    }

    async fn wait_for_logs(&self, message: &str, count: usize) -> TestResult<Vec<Value>> {
        let deadline = Instant::now() + WAIT;
        loop {
            let found: Vec<Value> = snapshot(&self.logs)?
                .into_iter()
                .map(|(_, log)| log)
                .filter(|log| log_message(log) == Some(message))
                .collect();
            if found.len() >= count {
                return Ok(found);
            }
            if Instant::now() >= deadline || self.find_log(STANDBY)?.is_some() {
                return Err(format!("{} {message:?} logs, not {count}", found.len()).into());
            }
            sleep(POLL).await;
        }
    }

    async fn wait_for_log(&self, message: &str) -> TestResult<Value> {
        match self.wait_for_log_within(message, WAIT).await? {
            Some(log) => Ok(log),
            None => Err(format!("no {message:?} log within {WAIT:?}").into()),
        }
    }

    async fn wait_for_event(
        &self,
        wanted: impl Fn(&Value) -> bool,
    ) -> TestResult<(Instant, Value)> {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(found) = self.events()?.into_iter().find(|(_, event)| wanted(event)) {
                return Ok(found);
            }
            if Instant::now() >= deadline {
                return Err(format!("no matching event within {WAIT:?}").into());
            }
            sleep(POLL).await;
        }
    }

    async fn wait_for_row(&self, id: i64, status: &str) -> TestResult<(Instant, Value)> {
        self.wait_for_event(|event| row(event) == Some((id, status)))
            .await
    }

    async fn admin_addr(&self) -> TestResult<SocketAddr> {
        let log = self.wait_for_log("admin listening").await?;
        Ok(log
            .get("fields")
            .and_then(|fields| fields.get("addr"))
            .and_then(Value::as_str)
            .ok_or("no addr in the admin listening line")?
            .parse()?)
    }

    async fn health_ok(&self) -> TestResult<bool> {
        let addr = self.admin_addr().await?;
        let output = timeout(
            WAIT,
            Command::new(env!("CARGO_BIN_EXE_pg-cdc"))
                .arg("health")
                .arg("--addr")
                .arg(addr.to_string())
                .env_remove("PGCDC__ADMIN__LISTEN")
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        Ok(output.status.success())
    }

    async fn readyz_status(&self) -> TestResult<u16> {
        let mut socket = TcpStream::connect(self.admin_addr().await?).await?;
        socket
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: pg-cdc\r\nConnection: close\r\n\r\n")
            .await?;
        let mut response = String::new();
        timeout(WAIT, socket.read_to_string(&mut response)).await??;
        let status = response
            .split_whitespace()
            .nth(1)
            .ok_or("no status line in the readyz response")?;
        Ok(status.parse()?)
    }

    async fn sigterm(&self) -> TestResult {
        let pid = self.child.id().ok_or("process already exited")?;
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

    async fn sigkill(&mut self) -> TestResult {
        self.child.start_kill()?;
        self.child.wait().await?;
        Ok(())
    }

    async fn exit(&mut self, within: Duration) -> TestResult<ExitStatus> {
        Ok(timeout(within, self.child.wait()).await??)
    }

    fn running(&mut self) -> TestResult<bool> {
        Ok(self.child.try_wait()?.is_none())
    }
}

fn collect<R: AsyncRead + Send + Unpin + 'static>(reader: R, records: Records) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && let Ok(mut records) = records.lock()
            {
                records.push((Instant::now(), value));
            }
        }
    });
}

async fn full_pipe() -> TestResult<(PipeReader, PipeWriter)> {
    let (reader, writer) = std::io::pipe()?;
    let mut filler = writer.try_clone()?;
    let written = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&written);
    std::thread::spawn(move || {
        while filler.write_all(b"#").is_ok() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    let mut last = 0;
    loop {
        sleep(Duration::from_millis(200)).await;
        let now = written.load(Ordering::Relaxed);
        if now > 0 && now == last {
            return Ok((reader, writer));
        }
        last = now;
    }
}

fn snapshot(records: &Records) -> TestResult<Vec<(Instant, Value)>> {
    Ok(records
        .lock()
        .map_err(|_| "a reader task panicked")?
        .clone())
}

fn log_message(log: &Value) -> Option<&str> {
    log.get("fields")?.get("message")?.as_str()
}

fn log_field<'a>(log: &'a Value, name: &str) -> Option<&'a Value> {
    log.get("fields")?.get(name)
}

fn row(event: &Value) -> Option<(i64, &str)> {
    let after = event.get("after")?;
    Some((after.get("id")?.as_i64()?, after.get("status")?.as_str()?))
}

fn rows(events: &[(Instant, Value)]) -> Rows {
    events
        .iter()
        .filter_map(|(_, event)| row(event))
        .map(|(id, status)| (id, status.to_owned()))
        .collect()
}

fn expected_rows() -> Rows {
    let statuses = ["one", "two", "three"];
    (1..=5)
        .flat_map(|id| statuses.map(|status| (id, status.to_owned())))
        .chain((6..=10).map(|id| (id, "one".to_owned())))
        .collect()
}

async fn wait_for_union(tasks: &[&Task], expected: &Rows) -> TestResult {
    let deadline = Instant::now() + WAIT;
    loop {
        let mut delivered = Rows::new();
        for task in tasks {
            delivered.extend(rows(&task.events()?));
        }
        if delivered.is_superset(expected) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let missing: Vec<_> = expected.difference(&delivered).collect();
            return Err(format!("events never arrived: {missing:?}").into());
        }
        sleep(POLL).await;
    }
}

fn assert_key_order(events: &[(Instant, Value)]) -> TestResult {
    let mut last: BTreeMap<i64, &str> = BTreeMap::new();
    for (_, event) in events {
        let key = event
            .pointer("/key/id")
            .and_then(Value::as_i64)
            .ok_or("event without a key id")?;
        let id = event
            .get("id")
            .and_then(Value::as_str)
            .ok_or("event without an id")?;
        if let Some(previous) = last.insert(key, id)
            && previous >= id
        {
            return Err(format!("key {key} went from {previous} to {id}").into());
        }
    }
    Ok(())
}

fn leader_record(state_dir: &Path) -> TestResult<Value> {
    let bytes = std::fs::read(state_dir.join("handover").join("leader.json"))?;
    let file: Value = serde_json::from_slice(&bytes)?;
    Ok(file
        .get("record")
        .ok_or("no record in leader.json")?
        .clone())
}

async fn first_takeover(tasks: &[&Task], within: Duration) -> TestResult<usize> {
    let deadline = Instant::now() + within;
    loop {
        for (index, task) in tasks.iter().enumerate() {
            if task.find_log(TOOK_OVER)?.is_some() {
                return Ok(index);
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("no standby took over within {within:?}").into());
        }
        sleep(POLL).await;
    }
}

#[tokio::test]
async fn a_start_first_deploy_hands_over_on_sigterm_without_loss() -> TestResult {
    let deployment = Deployment::start().await?;
    let mut a = deployment.spawn()?;
    let first = a.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&first, "kind"), Some(&Value::from("first")));
    assert_eq!(log_field(&first, "epoch"), Some(&Value::from(1)));
    deployment.insert(1, 5, "one").await?;
    for id in 1..=5 {
        a.wait_for_row(id, "one").await?;
    }
    let mut b = deployment.spawn()?;
    b.wait_for_log(STANDBY).await?;
    assert!(b.health_ok().await?, "a standby must be healthy");
    assert_eq!(b.readyz_status().await?, 503);
    deployment.update(5, "two").await?;
    a.sigterm().await?;
    let t_term = Instant::now();
    deployment.update(5, "three").await?;
    deployment.insert(6, 10, "one").await?;
    let status = a.exit(SHUTDOWN_BUDGET).await?;
    let t_exit = Instant::now();
    assert!(status.success(), "{status}");
    assert!(t_exit.duration_since(t_term) < SHUTDOWN_BUDGET);
    let takeover = b.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&takeover, "kind"), Some(&Value::from("clean")));
    assert_eq!(log_field(&takeover, "epoch"), Some(&Value::from(2)));
    let (first_event, _) = b.wait_for_event(|_| true).await?;
    let gap = first_event.saturating_duration_since(t_exit);
    assert!(gap < FENCE_DELAY, "the handover took {gap:?}");
    wait_for_union(&[&a, &b], &expected_rows()).await?;
    let record = deployment.leader_record()?;
    assert_eq!(record["epoch"], 2);
    assert_eq!(record["state"], "active");
    assert_eq!(Some(&record["holder"]), log_field(&takeover, "holder"));
    b.sigterm().await?;
    let status = b.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    let released = deployment.leader_record()?;
    assert_eq!(released["state"], "released");
    assert_eq!(released["epoch"], 2);
    let from_b = rows(&b.events()?);
    for id in 1..=5 {
        assert!(from_b.contains(&(id, "three".to_owned())), "id {id}");
    }
    assert_key_order(&a.events()?)?;
    assert_key_order(&b.events()?)?;
    Ok(())
}

#[tokio::test]
async fn l1_a_crash_takeover_waits_fence_delay_after_sigkill() -> TestResult {
    let deployment = Deployment::start().await?;
    let mut a = deployment.spawn()?;
    let first = a.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&first, "epoch"), Some(&Value::from(1)));
    let mut b = deployment.spawn()?;
    b.wait_for_log(STANDBY).await?;
    deployment.insert(1, 1, "one").await?;
    a.wait_for_row(1, "one").await?;
    let t_kill = Instant::now();
    a.sigkill().await?;
    deployment.insert(2, 2, "one").await?;
    let takeover = b.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&takeover, "kind"), Some(&Value::from("unclean")));
    assert_eq!(log_field(&takeover, "epoch"), Some(&Value::from(2)));
    let (first_event, _) = b.wait_for_event(|_| true).await?;
    let waited = first_event.saturating_duration_since(t_kill);
    assert!(
        waited >= FENCE_DELAY,
        "B delivered {waited:?} after the kill"
    );
    b.wait_for_row(2, "one").await?;
    b.sigterm().await?;
    let status = b.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    Ok(())
}

#[tokio::test]
async fn a_standby_that_loses_the_race_keeps_polling() -> TestResult {
    let deployment = Deployment::start().await?;
    let mut a = deployment.spawn()?;
    a.wait_for_log(TOOK_OVER).await?;
    let mut standbys = vec![deployment.spawn()?, deployment.spawn()?];
    for standby in &standbys {
        standby.wait_for_log(STANDBY).await?;
        assert!(standby.health_ok().await?, "a standby must be healthy");
    }
    a.sigterm().await?;
    let status = a.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    let refs: Vec<&Task> = standbys.iter().collect();
    let winner = first_takeover(&refs, TAKEOVER_WINDOW).await?;
    let mut winner = standbys.remove(winner);
    let mut loser = standbys.remove(0);
    let takeover = winner.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&takeover, "kind"), Some(&Value::from("clean")));
    assert_eq!(log_field(&takeover, "epoch"), Some(&Value::from(2)));
    sleep(LOSER_QUIET).await;
    assert!(
        loser.find_log(TOOK_OVER)?.is_none(),
        "both standbys took over"
    );
    assert!(loser.running()?, "the loser exited");
    assert!(loser.health_ok().await?, "the loser must stay healthy");
    let acquired = winner.wait_for_log("acquired the slot").await?;
    assert_eq!(
        deployment.walsender_pid().await?,
        log_field(&acquired, "backend_pid").and_then(Value::as_i64)
    );
    winner.sigterm().await?;
    let status = winner.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    let takeover = loser
        .wait_for_log_within(TOOK_OVER, TAKEOVER_WINDOW)
        .await?
        .ok_or("the loser never took over")?;
    assert_eq!(log_field(&takeover, "kind"), Some(&Value::from("clean")));
    assert_eq!(log_field(&takeover, "epoch"), Some(&Value::from(3)));
    deployment.insert(1, 1, "one").await?;
    loser.wait_for_row(1, "one").await?;
    loser.sigterm().await?;
    let status = loser.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    Ok(())
}

#[tokio::test]
async fn a_replacement_without_replication_stays_unhealthy_while_the_holder_streams() -> TestResult
{
    let deployment = Deployment::start().await?;
    let mut a = deployment.spawn()?;
    a.wait_for_log(TOOK_OVER).await?;
    deployment.insert(1, 1, "one").await?;
    a.wait_for_row(1, "one").await?;
    let walsender = deployment.walsender_pid().await?;
    deployment
        .pg
        .execute("CREATE ROLE no_replication LOGIN PASSWORD 'no_replication'")
        .await?;
    let config = deployment.config_as("no_replication")?;
    let mut b = Task::spawn(config.path(), "no_replication")?;
    let failures = b.wait_for_logs(RECONNECTING, 2).await?;
    for failure in &failures {
        let error = log_field(failure, "error")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(error.contains("42501"), "{error}");
    }
    assert!(
        !b.health_ok().await?,
        "a replacement without REPLICATION must stay unhealthy"
    );
    assert!(
        b.find_log(STANDBY)?.is_none(),
        "the replacement went standby"
    );
    assert!(a.running()?, "the holder exited");
    assert_eq!(deployment.walsender_pid().await?, walsender);
    deployment.insert(2, 2, "one").await?;
    a.wait_for_row(2, "one").await?;
    assert_eq!(b.events()?.len(), 0);
    for task in [&mut a, &mut b] {
        task.sigterm().await?;
        let status = task.exit(SHUTDOWN_BUDGET).await?;
        assert!(status.success(), "{status}");
    }
    Ok(())
}

#[tokio::test]
async fn a_replacement_without_a_spare_wal_sender_stays_unhealthy_until_the_holder_stops()
-> TestResult {
    let deployment = Deployment::start_on(
        PostgresOptions {
            max_wal_senders: Some(1),
            ..PostgresOptions::default()
        },
        "stdout",
    )
    .await?;
    let mut a = deployment.spawn()?;
    a.wait_for_log(TOOK_OVER).await?;
    deployment.insert(1, 1, "one").await?;
    a.wait_for_row(1, "one").await?;
    let walsender = deployment.walsender_pid().await?;
    let mut b = deployment.spawn()?;
    let failures = b.wait_for_logs(RECONNECTING, 2).await?;
    for failure in &failures {
        let error = log_field(failure, "error")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            error.contains("53300") && error.contains("max_wal_senders"),
            "{error}"
        );
    }
    assert!(
        !b.health_ok().await?,
        "a replacement that could not check its replication login must stay unhealthy"
    );
    assert!(
        b.find_log(STANDBY)?.is_none(),
        "the replacement went standby"
    );
    assert!(a.running()?, "the holder exited");
    assert_eq!(deployment.walsender_pid().await?, walsender);
    deployment.insert(2, 2, "one").await?;
    a.wait_for_row(2, "one").await?;
    assert_eq!(b.events()?.len(), 0);
    a.sigterm().await?;
    let status = a.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    let takeover = b.wait_for_log(TOOK_OVER).await?;
    assert_eq!(log_field(&takeover, "kind"), Some(&Value::from("clean")));
    assert_eq!(log_field(&takeover, "epoch"), Some(&Value::from(2)));
    deployment.insert(3, 3, "one").await?;
    b.wait_for_row(3, "one").await?;
    assert!(b.health_ok().await?, "the replacement never turned healthy");
    b.sigterm().await?;
    let status = b.exit(SHUTDOWN_BUDGET).await?;
    assert!(status.success(), "{status}");
    Ok(())
}

#[tokio::test]
async fn a_sigterm_with_a_full_stdout_pipe_still_exits_within_the_budget() -> TestResult {
    let deployment = Deployment::start().await?;
    let mut a = deployment.spawnunread()?;
    a.wait_for_log(TOOK_OVER).await?;
    deployment
        .pg
        .execute("INSERT INTO orders SELECT g, repeat('x', 20000) FROM generate_series(1, 40) g")
        .await?;
    sleep(Duration::from_secs(1)).await;

    a.sigterm().await?;
    let t_term = Instant::now();
    let status = a.exit(SHUTDOWN_BUDGET).await?;

    assert!(status.success(), "{status}");
    assert!(t_term.elapsed() < SHUTDOWN_BUDGET, "{:?}", t_term.elapsed());
    assert!(a.find_log("pg-cdc stopped cleanly")?.is_some());
    let record = deployment.leader_record()?;
    assert_eq!(record["state"], "active");
    assert_eq!(record["epoch"], 1);
    Ok(())
}

async fn assert_sigterm_with_a_full_log_pipe_hands_over(logs: Logs) -> TestResult {
    let deployment = Deployment::start_with_sink(match logs {
        Logs::Stdout => "discard",
        Logs::Stderr => "stdout",
    })
    .await?;
    let (mut a, _pipe) = deployment.spawn_with_full_log_pipe(logs).await?;
    if matches!(logs, Logs::Stderr) {
        a.read_events()?;
    }
    let claimed = deployment.wait_for_record("active", 1).await?;
    deployment.insert(1, 5, "one").await?;
    let delivered = match logs {
        Logs::Stdout => true,
        Logs::Stderr => a.wait_for_row(5, "one").await.is_ok(),
    };

    a.sigterm().await?;
    let t_term = Instant::now();
    let status = a.exit(SHUTDOWN_BUDGET).await?;

    assert!(status.success(), "{status}");
    assert!(t_term.elapsed() < SHUTDOWN_BUDGET, "{:?}", t_term.elapsed());
    assert!(claimed, "the leader record was never claimed");
    assert!(delivered, "no event was delivered");
    let record = deployment.leader_record()?;
    assert_eq!(record.get("state"), Some(&Value::from("released")));
    assert_eq!(record.get("epoch"), Some(&Value::from(1)));
    Ok(())
}

#[tokio::test]
async fn a_sigterm_with_a_full_stdout_log_pipe_still_hands_over_within_the_budget() -> TestResult {
    assert_sigterm_with_a_full_log_pipe_hands_over(Logs::Stdout).await
}

#[tokio::test]
async fn a_sigterm_with_a_full_stderr_log_pipe_still_hands_over_within_the_budget() -> TestResult {
    assert_sigterm_with_a_full_log_pipe_hands_over(Logs::Stderr).await
}

#[tokio::test]
async fn a_sigterm_that_exhausts_every_window_with_a_full_log_pipe_exits_within_the_bound()
-> TestResult {
    let deployment = Deployment::start().await?;
    let (mut a, _pipe) = deployment.spawn_with_full_log_pipe(Logs::Stderr).await?;
    assert!(deployment.wait_for_record("active", 1).await?);
    deployment
        .pg
        .execute("INSERT INTO orders SELECT g, repeat('x', 20000) FROM generate_series(1, 40) g")
        .await?;
    sleep(Duration::from_secs(1)).await;
    assert!(deployment.pg.freeze_walsender(SLOT).await?);

    a.sigterm().await?;
    let t_term = Instant::now();
    let status = a.exit(SHUTDOWN_BUDGET).await?;
    let elapsed = t_term.elapsed();

    assert!(status.success(), "{status}");
    assert!(
        elapsed >= SHUTDOWN_BOUND.saturating_sub(EXIT_SLACK),
        "the shutdown windows were not exhausted: {elapsed:?}"
    );
    assert!(elapsed < SHUTDOWN_BOUND + EXIT_SLACK, "{elapsed:?}");
    let record = deployment.leader_record()?;
    assert_eq!(record["state"], "active");
    assert_eq!(record["epoch"], 1);
    Ok(())
}
