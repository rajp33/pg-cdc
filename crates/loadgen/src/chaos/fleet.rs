use std::collections::{BTreeMap, BTreeSet};
use std::mem::replace;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context as _;
use tokio::time::Instant;

use crate::cluster::PgCdc;

const TERM_TIMEOUT: Duration = Duration::from_secs(30);
const EXIT_POLL: Duration = Duration::from_millis(1);
const POLL: Duration = Duration::from_millis(50);
const ACTIVE_TIMEOUT: Duration = Duration::from_secs(60);
const NAMES: [&str; 2] = ["a", "b"];
const TAKEOVER: &str = "took over the slot";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub role: String,
    pub epoch: Option<u64>,
}

impl Seen {
    fn holds_a_claim(&self) -> bool {
        self.role == "active" || self.role == "draining"
    }
}

fn parse_status(status: &serde_json::Value) -> Option<Seen> {
    Some(Seen {
        role: status.get("role")?.as_str()?.to_owned(),
        epoch: status.get("epoch").and_then(serde_json::Value::as_u64),
    })
}

#[derive(Debug, Default)]
pub struct Epochs(BTreeSet<u64>);

impl Epochs {
    pub fn observe(&mut self, seen: &[Option<Seen>]) {
        self.0.extend(
            seen.iter()
                .flatten()
                .filter(|seen| seen.holds_a_claim())
                .filter_map(|seen| seen.epoch),
        );
    }

    pub fn takeovers(&self) -> u64 {
        u64::try_from(self.0.len().saturating_sub(1)).unwrap_or(u64::MAX)
    }
}

pub fn takeover_kinds(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            let fields = value.get("fields")?;
            if fields.get("message")?.as_str()? != TAKEOVER {
                return None;
            }
            Some(fields.get("kind")?.as_str()?.to_owned())
        })
        .collect()
}

fn tally(kinds: &mut BTreeMap<String, u64>, process: &PgCdc) {
    for kind in takeover_kinds(&process.log_lines()) {
        *kinds.entry(kind).or_default() += 1;
    }
}

pub fn artifact_path(base: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}-{suffix}", base.display()))
}

#[derive(Debug)]
struct Spawner {
    binary: PathBuf,
    config: PathBuf,
    env: Vec<(String, String)>,
    artifacts: PathBuf,
}

#[derive(Debug)]
enum State {
    Running(PgCdc),
    Terminating { process: PgCdc, since: Instant },
    Down { restart_at: Instant },
}

#[derive(Debug)]
struct Instance {
    name: &'static str,
    generation: u32,
    state: State,
    kinds: BTreeMap<String, u64>,
}

async fn dump_logs(spawner: &Spawner, name: &str, generation: u32, process: &PgCdc) {
    let path = artifact_path(&spawner.artifacts, &format!("{name}-{generation}.log"));
    let mut text = process.log_lines().join("\n");
    text.push('\n');
    let _ = tokio::fs::write(path, text).await;
}

impl Instance {
    async fn spawn(&mut self, spawner: &Spawner) -> anyhow::Result<()> {
        self.generation += 1;
        let process =
            PgCdc::spawn(&spawner.binary, &spawner.config, &spawner.env, self.name).await?;
        self.state = State::Running(process);
        Ok(())
    }

    async fn advance(
        &mut self,
        spawner: &Spawner,
        failures: &mut Vec<String>,
        restart: bool,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        match &mut self.state {
            State::Running(_) => {}
            State::Terminating { process, since } => {
                let failure = match process.wait(EXIT_POLL).await? {
                    Some(status) if status.success() => None,
                    Some(status) => {
                        Some(format!("{}: exited with {status} after SIGTERM", self.name))
                    }
                    None if since.elapsed() >= TERM_TIMEOUT => {
                        process.sigkill().await?;
                        Some(format!(
                            "{}: still running {TERM_TIMEOUT:?} after SIGTERM",
                            self.name
                        ))
                    }
                    None => return Ok(()),
                };
                failures.extend(failure);
                dump_logs(spawner, self.name, self.generation, process).await;
                tally(&mut self.kinds, process);
                self.state = State::Down { restart_at: now };
            }
            State::Down { restart_at } => {
                if restart && now >= *restart_at {
                    self.spawn(spawner).await?;
                }
            }
        }
        Ok(())
    }

    fn take_running(&mut self, placeholder: State) -> Option<PgCdc> {
        if !matches!(self.state, State::Running(_)) {
            return None;
        }
        match replace(&mut self.state, placeholder) {
            State::Running(process) => Some(process),
            State::Terminating { .. } | State::Down { .. } => None,
        }
    }

    async fn kill(&mut self, spawner: &Spawner, restart_delay: Duration) -> anyhow::Result<()> {
        let placeholder = State::Down {
            restart_at: Instant::now() + restart_delay,
        };
        if let Some(mut process) = self.take_running(placeholder) {
            process.sigkill().await?;
            dump_logs(spawner, self.name, self.generation, &process).await;
            tally(&mut self.kinds, &process);
        }
        Ok(())
    }

    async fn term(&mut self) -> anyhow::Result<()> {
        let placeholder = State::Down {
            restart_at: Instant::now(),
        };
        if let Some(process) = self.take_running(placeholder) {
            process.sigterm().await?;
            self.state = State::Terminating {
                process,
                since: Instant::now(),
            };
        }
        Ok(())
    }

    fn process(&self) -> Option<&PgCdc> {
        match &self.state {
            State::Running(process) => Some(process),
            State::Terminating { .. } | State::Down { .. } => None,
        }
    }

    async fn seen(&self) -> Option<Seen> {
        parse_status(&self.process()?.status().await.ok()?)
    }

    fn is_terminating(&self) -> bool {
        matches!(self.state, State::Terminating { .. })
    }
}

#[derive(Debug)]
pub struct Fleet {
    spawner: Spawner,
    instances: Vec<Instance>,
    failures: Vec<String>,
    epochs: Epochs,
}

impl Fleet {
    pub fn new(
        binary: PathBuf,
        config: PathBuf,
        env: Vec<(String, String)>,
        artifacts: PathBuf,
    ) -> Self {
        Self {
            spawner: Spawner {
                binary,
                config,
                env,
                artifacts,
            },
            instances: NAMES
                .iter()
                .map(|name| Instance {
                    name,
                    generation: 0,
                    state: State::Down {
                        restart_at: Instant::now(),
                    },
                    kinds: BTreeMap::new(),
                })
                .collect(),
            failures: Vec::new(),
            epochs: Epochs::default(),
        }
    }

    pub async fn spawn_all(&mut self) -> anyhow::Result<()> {
        for instance in &mut self.instances {
            instance.spawn(&self.spawner).await?;
        }
        Ok(())
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        let (first, rest) = self
            .instances
            .split_first_mut()
            .context("a fleet has instances")?;
        first.spawn(&self.spawner).await?;
        let deadline = Instant::now() + ACTIVE_TIMEOUT;
        loop {
            let seen = first.seen().await;
            if seen.is_some_and(|seen| seen.role == "active" && seen.epoch.is_some()) {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "the first instance did not become active within {ACTIVE_TIMEOUT:?}; last output: {}",
                first
                    .process()
                    .map(|process| process.log_lines().join(" | "))
                    .unwrap_or_default()
            );
            tokio::time::sleep(POLL).await;
        }
        for instance in rest {
            instance.spawn(&self.spawner).await?;
        }
        Ok(())
    }

    pub async fn advance(&mut self, restart: bool) -> anyhow::Result<()> {
        for instance in &mut self.instances {
            instance
                .advance(&self.spawner, &mut self.failures, restart)
                .await?;
        }
        Ok(())
    }

    pub async fn poll(&mut self) -> Vec<Option<Seen>> {
        let mut seen = Vec::with_capacity(self.instances.len());
        for instance in &self.instances {
            seen.push(instance.seen().await);
        }
        self.epochs.observe(&seen);
        seen
    }

    pub fn busy(&self) -> bool {
        self.instances
            .iter()
            .any(|instance| instance.process().is_none())
    }

    pub fn running(&self) -> usize {
        self.instances
            .iter()
            .filter(|instance| instance.process().is_some())
            .count()
    }

    pub fn active(&self, seen: &[Option<Seen>]) -> Option<&PgCdc> {
        self.instances
            .iter()
            .zip(seen)
            .find(|(_, seen)| seen.as_ref().is_some_and(|seen| seen.role == "active"))
            .and_then(|(instance, _)| instance.process())
    }

    fn holder(&self, seen: &[Option<Seen>]) -> Option<usize> {
        if self.busy() {
            return None;
        }
        self.instances
            .iter()
            .zip(seen)
            .position(|(_, seen)| seen.as_ref().is_some_and(Seen::holds_a_claim))
    }

    pub async fn kill_active(
        &mut self,
        seen: &[Option<Seen>],
        restart_delay: Duration,
    ) -> anyhow::Result<bool> {
        let Some(instance) = self
            .holder(seen)
            .and_then(|index| self.instances.get_mut(index))
        else {
            return Ok(false);
        };
        instance.kill(&self.spawner, restart_delay).await?;
        Ok(true)
    }

    pub async fn term_active(&mut self, seen: &[Option<Seen>]) -> anyhow::Result<bool> {
        let Some(instance) = self
            .holder(seen)
            .and_then(|index| self.instances.get_mut(index))
        else {
            return Ok(false);
        };
        instance.term().await?;
        Ok(true)
    }

    pub async fn ensure_running(&mut self) -> anyhow::Result<()> {
        let deadline = Instant::now() + TERM_TIMEOUT + ACTIVE_TIMEOUT;
        for instance in &mut self.instances {
            if let State::Down { restart_at } = &mut instance.state {
                *restart_at = Instant::now();
            }
        }
        while self.busy() {
            anyhow::ensure!(
                Instant::now() < deadline,
                "the instances did not all restart"
            );
            self.advance(true).await?;
            tokio::time::sleep(POLL).await;
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) -> anyhow::Result<()> {
        for instance in &mut self.instances {
            instance.term().await?;
        }
        while self.instances.iter().any(Instance::is_terminating) {
            self.advance(false).await?;
            tokio::time::sleep(POLL).await;
        }
        Ok(())
    }

    pub fn takeovers(&self) -> u64 {
        self.epochs.takeovers()
    }

    pub fn takeover_kinds(&self) -> BTreeMap<String, u64> {
        let mut kinds = BTreeMap::new();
        for instance in &self.instances {
            for (kind, count) in &instance.kinds {
                *kinds.entry(kind.clone()).or_default() += count;
            }
            match &instance.state {
                State::Running(process) | State::Terminating { process, .. } => {
                    tally(&mut kinds, process);
                }
                State::Down { .. } => {}
            }
        }
        kinds
    }

    pub fn failures(&self) -> &[String] {
        &self.failures
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use tokio::time::Instant;

    use std::collections::BTreeMap;

    use super::*;
    use crate::testing::Scratch;

    const CLEAN: &str = "#!/bin/sh\necho '{\"fields\":{\"message\":\"admin listening\",\"addr\":\"127.0.0.1:9\"}}'\ntrap 'exit 0' TERM\nwhile true; do sleep 0.1; done\n";
    const CRASHING: &str = "#!/bin/sh\necho '{\"fields\":{\"message\":\"admin listening\",\"addr\":\"127.0.0.1:9\"}}'\ntrap 'exit 3' TERM\nwhile true; do sleep 0.1; done\n";

    fn fleet(scratch: &Scratch, script: &str) -> Fleet {
        let binary = scratch.script("fake-pg-cdc", script);
        Fleet::new(
            binary,
            scratch.path().join("config.yaml"),
            Vec::new(),
            scratch.path().join("run"),
        )
    }

    fn seen(role: &str, epoch: Option<u64>) -> Seen {
        Seen {
            role: role.to_owned(),
            epoch,
        }
    }

    async fn settled(fleet: &mut Fleet) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while fleet.busy() && Instant::now() < deadline {
            fleet.advance(true).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn a_killed_instance_restarts_after_its_delay_and_the_other_keeps_running() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CLEAN);
        fleet.spawn_all().await.unwrap();
        assert!(!fleet.busy());

        let roles = [Some(seen("standby", None)), Some(seen("active", Some(1)))];
        assert!(
            fleet
                .kill_active(&roles, Duration::from_millis(300))
                .await
                .unwrap()
        );
        assert!(fleet.busy());
        assert_eq!(fleet.running(), 1);
        assert!(
            !fleet.kill_active(&roles, Duration::ZERO).await.unwrap(),
            "a second kill must wait for the first to finish"
        );
        assert!(!fleet.term_active(&roles).await.unwrap());

        fleet.advance(true).await.unwrap();
        assert!(fleet.busy(), "the restart delay has not passed");
        settled(&mut fleet).await;
        assert!(!fleet.busy());
        assert_eq!(fleet.running(), 2);
        assert_eq!(fleet.failures(), [] as [String; 0]);
        fleet.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn nothing_is_killed_when_no_instance_reports_active() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CLEAN);
        fleet.spawn_all().await.unwrap();
        let roles = [Some(seen("standby", None)), None];
        assert!(!fleet.kill_active(&roles, Duration::ZERO).await.unwrap());
        assert!(!fleet.term_active(&roles).await.unwrap());
        assert_eq!(fleet.running(), 2);
        fleet.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn a_draining_instance_counts_as_the_active_one() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CLEAN);
        fleet.spawn_all().await.unwrap();
        let roles = [Some(seen("standby", None)), Some(seen("draining", Some(4)))];
        assert!(
            fleet
                .kill_active(&roles, Duration::from_millis(10))
                .await
                .unwrap()
        );
        settled(&mut fleet).await;
        assert_eq!(fleet.running(), 2);
        fleet.stop_all().await.unwrap();
    }

    #[tokio::test]
    async fn a_terminated_instance_restarts_once_it_exits_and_a_clean_exit_is_no_failure() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CLEAN);
        fleet.spawn_all().await.unwrap();
        let roles = [Some(seen("active", Some(1))), Some(seen("standby", None))];
        assert!(fleet.term_active(&roles).await.unwrap());
        assert!(fleet.busy());
        settled(&mut fleet).await;
        assert_eq!(fleet.running(), 2);
        assert_eq!(fleet.failures(), [] as [String; 0]);
        fleet.stop_all().await.unwrap();
        assert_eq!(fleet.failures(), [] as [String; 0]);
        assert_eq!(fleet.running(), 0);
    }

    #[tokio::test]
    async fn an_instance_that_exits_non_zero_after_sigterm_is_a_failure() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CRASHING);
        fleet.spawn_all().await.unwrap();
        fleet.stop_all().await.unwrap();
        assert_eq!(fleet.failures().len(), 2, "{:?}", fleet.failures());
        assert!(
            fleet
                .failures()
                .iter()
                .all(|failure| failure.contains("exit"))
        );
    }

    #[tokio::test]
    async fn instance_logs_are_written_beside_the_report() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, CLEAN);
        fleet.spawn_all().await.unwrap();
        fleet.stop_all().await.unwrap();
        let log = scratch.path().join("run-a-1.log");
        let text = std::fs::read_to_string(&log).unwrap();
        assert!(text.contains("admin listening"), "{text}");
        assert!(Path::new(&scratch.path().join("run-b-1.log")).exists());
    }

    const TAKES_OVER: &str = "#!/bin/sh\necho '{\"fields\":{\"message\":\"admin listening\",\"addr\":\"127.0.0.1:9\"}}'\necho '{\"fields\":{\"message\":\"took over the slot\",\"epoch\":2,\"kind\":\"clean\",\"holder\":\"h\"}}'\ntrap 'exit 0' TERM\nwhile true; do sleep 0.1; done\n";

    #[test]
    fn takeover_kinds_are_read_from_the_log_lines() {
        let lines = [
            r#"{"fields":{"message":"admin listening","addr":"127.0.0.1:9"}}"#,
            r#"{"level":"INFO","fields":{"message":"took over the slot","epoch":1,"kind":"first","holder":"h"}}"#,
            "not json",
            r#"{"fields":{"message":"took over the slot","epoch":3,"kind":"unclean","holder":"h"}}"#,
            r#"{"fields":{"message":"took over the slot","epoch":4}}"#,
        ]
        .map(str::to_owned);
        assert_eq!(takeover_kinds(&lines), ["first", "unclean"]);
    }

    #[tokio::test]
    async fn takeover_kinds_add_up_over_every_process_generation() {
        let scratch = Scratch::new();
        let mut fleet = fleet(&scratch, TAKES_OVER);
        fleet.spawn_all().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let roles = [Some(seen("active", Some(2))), Some(seen("standby", None))];
        assert!(fleet.kill_active(&roles, Duration::ZERO).await.unwrap());
        settled(&mut fleet).await;
        fleet.stop_all().await.unwrap();
        assert_eq!(
            fleet.takeover_kinds(),
            BTreeMap::from([("clean".to_owned(), 3)])
        );
    }

    #[test]
    fn takeovers_are_the_new_epochs_after_the_first() {
        let mut epochs = Epochs::default();
        assert_eq!(epochs.takeovers(), 0);
        epochs.observe(&[Some(seen("standby", None)), None]);
        assert_eq!(epochs.takeovers(), 0);
        epochs.observe(&[Some(seen("active", Some(1))), Some(seen("standby", None))]);
        epochs.observe(&[Some(seen("active", Some(1))), Some(seen("standby", None))]);
        assert_eq!(epochs.takeovers(), 0);
        epochs.observe(&[None, Some(seen("active", Some(2)))]);
        epochs.observe(&[
            Some(seen("standby", Some(2))),
            Some(seen("draining", Some(2))),
        ]);
        assert_eq!(epochs.takeovers(), 1);
        epochs.observe(&[Some(seen("active", Some(5))), None]);
        assert_eq!(epochs.takeovers(), 2);
    }
}
