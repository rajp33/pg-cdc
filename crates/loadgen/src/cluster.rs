use std::net::SocketAddr;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::Context as _;
use bytes::Bytes;
use http_body_util::{BodyExt as _, Empty};
use hyper::header::HOST;
use hyper::{Method, Request, client};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::time::timeout;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const PARKED_TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_TAIL_LINES: usize = 5;

type LogLines = Arc<Mutex<Vec<String>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedOutput {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug)]
pub struct PgCdc {
    name: String,
    child: Child,
    admin: SocketAddr,
    lines: LogLines,
}

fn admin_address(line: &str) -> Option<SocketAddr> {
    let log: serde_json::Value = serde_json::from_str(line).ok()?;
    let fields = log.get("fields")?;
    if fields.get("message")?.as_str()? != "admin listening" {
        return None;
    }
    fields.get("addr")?.as_str()?.parse().ok()
}

async fn read_lines<R: AsyncRead + Unpin>(
    stream: R,
    lines: LogLines,
    found: mpsc::UnboundedSender<SocketAddr>,
) {
    let mut reader = BufReader::new(stream).lines();
    while let Ok(Some(line)) = reader.next_line().await {
        if let Some(addr) = admin_address(&line) {
            let _ = found.send(addr);
        }
        lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }
}

async fn get(addr: SocketAddr, path: &str) -> anyhow::Result<(u16, Bytes)> {
    let stream = TcpStream::connect(addr).await?;
    let (mut sender, connection) = client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(HOST, addr.to_string())
        .body(Empty::<Bytes>::new())?;
    let outcome = async {
        let response = sender.send_request(request).await?;
        let status = response.status().as_u16();
        let body = response.into_body().collect().await?.to_bytes();
        anyhow::Ok((status, body))
    }
    .await;
    driver.abort();
    outcome
}

impl PgCdc {
    pub async fn spawn(
        binary: &Path,
        config: &Path,
        env: &[(String, String)],
        name: &str,
    ) -> anyhow::Result<Self> {
        let mut child = Command::new(binary)
            .arg("run")
            .arg("--config")
            .arg(config)
            .envs(
                env.iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("{name}: cannot start {}", binary.display()))?;

        let lines = LogLines::default();
        let (found, mut listening) = mpsc::unbounded_channel();
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(read_lines(stdout, Arc::clone(&lines), found.clone()));
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(read_lines(stderr, Arc::clone(&lines), found.clone()));
        }
        drop(found);

        let outcome = timeout(STARTUP_TIMEOUT, listening.recv()).await;
        let admin = match outcome {
            Ok(Some(addr)) => addr,
            Ok(None) => anyhow::bail!(
                "{name}: exited before logging 'admin listening'; last output: {}",
                tail(&lines)
            ),
            Err(_) => anyhow::bail!(
                "{name}: no 'admin listening' line within {STARTUP_TIMEOUT:?}; last output: {}",
                tail(&lines)
            ),
        };
        Ok(Self {
            name: name.to_owned(),
            child,
            admin,
            lines,
        })
    }

    pub fn admin(&self) -> SocketAddr {
        self.admin
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn log_lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub async fn status(&self) -> anyhow::Result<serde_json::Value> {
        let (status, body) = timeout(REQUEST_TIMEOUT, get(self.admin, "/status"))
            .await
            .with_context(|| format!("{}: /status timed out", self.name))??;
        anyhow::ensure!(
            (200..300).contains(&status),
            "{}: /status answered {status}",
            self.name
        );
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn parked(&self, binary: &Path, args: &[&str]) -> anyhow::Result<(i32, String)> {
        let output = self.parked_output(binary, args).await?;
        Ok((output.code, output.stdout))
    }

    pub async fn parked_output(
        &self,
        binary: &Path,
        args: &[&str],
    ) -> anyhow::Result<ParkedOutput> {
        let output = timeout(
            PARKED_TIMEOUT,
            Command::new(binary)
                .arg("parked")
                .args(args)
                .arg("--addr")
                .arg(self.admin.to_string())
                .stdin(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .with_context(|| format!("{}: parked command timed out", self.name))??;
        Ok(ParkedOutput {
            code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    pub async fn sigterm(&self) -> anyhow::Result<()> {
        let pid = self
            .pid()
            .with_context(|| format!("{}: has already exited", self.name))?;
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "{}: kill -TERM {pid} failed", self.name);
        Ok(())
    }

    pub async fn sigkill(&mut self) -> anyhow::Result<()> {
        self.child.start_kill()?;
        self.child.wait().await?;
        Ok(())
    }

    pub async fn wait(&mut self, within: Duration) -> anyhow::Result<Option<ExitStatus>> {
        match timeout(within, self.child.wait()).await {
            Ok(status) => Ok(Some(status?)),
            Err(_) => Ok(None),
        }
    }
}

fn tail(lines: &LogLines) -> String {
    let lines = lines.lock().unwrap_or_else(PoisonError::into_inner);
    let start = lines.len().saturating_sub(STARTUP_TAIL_LINES);
    lines.get(start..).unwrap_or_default().join(" | ")
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::service::service_fn;
    use hyper::{Request, Response, server};
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    use super::*;
    use crate::testing::Scratch;

    #[test]
    fn the_admin_address_is_read_from_the_log_line() {
        let line = r#"{"timestamp":"2026-10-15T10:00:00Z","level":"INFO","fields":{"message":"admin listening","addr":"127.0.0.1:41234"},"target":"pg_cdc"}"#;
        assert_eq!(
            admin_address(line),
            Some("127.0.0.1:41234".parse().unwrap())
        );

        let other = r#"{"level":"INFO","fields":{"message":"streaming","addr":"127.0.0.1:1"}}"#;
        assert_eq!(admin_address(other), None);
        let no_addr = r#"{"fields":{"message":"admin listening"}}"#;
        assert_eq!(admin_address(no_addr), None);
        assert_eq!(admin_address("admin listening"), None);
        assert_eq!(admin_address(""), None);
    }

    async fn status_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service =
                        service_fn(|request: Request<hyper::body::Incoming>| async move {
                            let body = if request.uri().path() == "/status" {
                                r#"{"role":"active"}"#
                            } else {
                                "{}"
                            };
                            Ok::<_, std::convert::Infallible>(Response::new(Full::new(
                                Bytes::from_static(body.as_bytes()),
                            )))
                        });
                    let _ = server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_spawned_process_is_found_queried_signalled_and_reaped() {
        let admin = status_server().await;
        let scratch = Scratch::new();
        let script = scratch.script(
            "fake-pg-cdc",
            &format!(
                "#!/bin/sh\ncase \"$1\" in\n  run)\n    echo 'startup noise'\n    echo '{{\"fields\":{{\"message\":\"admin listening\",\"addr\":\"{admin}\"}}}}'\n    exec sleep 60 ;;\n  parked)\n    echo \"$*\"\n    echo \"err $*\" >&2\n    exit 3 ;;\nesac\n"
            ),
        );

        let env = [("PGCDC_TEST".to_owned(), "1".to_owned())];
        let mut process = PgCdc::spawn(&script, Path::new("config.yaml"), &env, "one")
            .await
            .unwrap();
        assert_eq!(process.admin(), admin);
        assert!(process.pid().is_some());
        assert_eq!(process.status().await.unwrap()["role"], "active");

        let (code, stdout) = process
            .parked(&script, &["list", "--status", "dead"])
            .await
            .unwrap();
        assert_eq!(code, 3);
        assert_eq!(
            stdout.trim(),
            format!("parked list --status dead --addr {admin}")
        );
        let output = process
            .parked_output(&script, &["list", "--status", "dead"])
            .await
            .unwrap();
        assert_eq!(output.code, 3);
        assert_eq!(output.stdout, stdout);
        assert_eq!(
            output.stderr.trim(),
            format!("err parked list --status dead --addr {admin}")
        );

        assert!(
            process
                .wait(Duration::from_millis(100))
                .await
                .unwrap()
                .is_none()
        );
        process.sigterm().await.unwrap();
        let exit = process.wait(Duration::from_secs(10)).await.unwrap();
        assert!(exit.is_some_and(|status| !status.success()));
        assert!(process.pid().is_none());
    }

    #[tokio::test]
    async fn a_process_that_never_reports_its_admin_address_is_an_error() {
        let scratch = Scratch::new();
        let script = scratch.script("silent", "#!/bin/sh\necho 'no listener'\n");

        let error = PgCdc::spawn(&script, Path::new("config.yaml"), &[], "silent")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("silent"), "{error}");
    }

    #[tokio::test]
    async fn sigkill_stops_the_process() {
        let scratch = Scratch::new();
        let script = scratch.script(
            "stubborn",
            "#!/bin/sh\necho '{\"fields\":{\"message\":\"admin listening\",\"addr\":\"127.0.0.1:9\"}}'\ntrap '' TERM\nwhile true; do sleep 1; done\n",
        );

        let mut process = PgCdc::spawn(&script, Path::new("config.yaml"), &[], "stubborn")
            .await
            .unwrap();
        process.sigterm().await.unwrap();
        assert!(
            process
                .wait(Duration::from_millis(300))
                .await
                .unwrap()
                .is_none()
        );
        process.sigkill().await.unwrap();
        assert!(
            process
                .wait(Duration::from_secs(10))
                .await
                .unwrap()
                .is_some()
        );
    }
}
