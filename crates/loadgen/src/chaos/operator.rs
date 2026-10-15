use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;

use crate::chaos::Unresolved;
use crate::checker::change_from_envelope;
use crate::cluster::{ParkedOutput, PgCdc};

const REFUSALS: [u16; 3] = [404, 409, 503];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardOutcome {
    Discarded,
    Refused,
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadHead {
    pub id: String,
    pub key: i32,
    pub seq: i64,
}

#[derive(Debug)]
pub struct Operator {
    binary: PathBuf,
    redriven: HashSet<String>,
    next_token: u64,
    pub redrives: u64,
    pub refused: u64,
    pub discards: BTreeSet<u64>,
    pub unresolved: Vec<Unresolved>,
}

pub fn classify_discard(output: &ParkedOutput) -> DiscardOutcome {
    if output.code == 0 {
        return DiscardOutcome::Discarded;
    }
    let refused = output.code == 1
        && refusal_status(&output.stderr).is_some_and(|status| REFUSALS.contains(&status));
    if refused {
        DiscardOutcome::Refused
    } else {
        DiscardOutcome::Unresolved
    }
}

fn refusal_status(stderr: &str) -> Option<u16> {
    let detail = stderr.strip_prefix("pg-cdc: ")?;
    let digits: String = detail.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

pub fn dead_queues(listing: &str) -> Vec<String> {
    let Ok(listing) = serde_json::from_str::<serde_json::Value>(listing) else {
        return Vec::new();
    };
    listing
        .get("queues")
        .and_then(serde_json::Value::as_array)
        .map(|queues| {
            queues
                .iter()
                .filter_map(|queue| queue.get("order_key")?.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

pub fn dead_head(shown: &str) -> Option<DeadHead> {
    let shown: serde_json::Value = serde_json::from_str(shown).ok()?;
    let head = shown.get("events")?.as_array()?.first()?;
    if head.get("status")?.as_str()? != "dead" {
        return None;
    }
    let change = change_from_envelope(head.get("envelope")?)?;
    Some(DeadHead {
        id: head.get("id")?.as_str()?.to_owned(),
        key: change.key,
        seq: change.seq,
    })
}

impl Operator {
    pub fn new(binary: PathBuf) -> Self {
        Self {
            binary,
            redriven: HashSet::new(),
            next_token: 1,
            redrives: 0,
            refused: 0,
            discards: BTreeSet::new(),
            unresolved: Vec::new(),
        }
    }

    pub async fn pass(&mut self, active: &PgCdc, record_marker: impl Fn(u64, i32, i64)) {
        let Ok(listing) = active
            .parked_output(&self.binary, &["list", "--status", "dead"])
            .await
        else {
            return;
        };
        if listing.code != 0 {
            return;
        }
        for key in dead_queues(&listing.stdout) {
            if self.redriven.contains(&key) {
                self.discard_head(active, &key, &record_marker).await;
            } else {
                self.redrive(active, &key).await;
            }
        }
    }

    async fn redrive(&mut self, active: &PgCdc, key: &str) {
        let redriven = active.parked_output(&self.binary, &["redrive", key]).await;
        if redriven.is_ok_and(|output| output.code == 0) {
            self.redriven.insert(key.to_owned());
            self.redrives += 1;
        }
    }

    async fn discard_head(
        &mut self,
        active: &PgCdc,
        key: &str,
        record_marker: &impl Fn(u64, i32, i64),
    ) {
        let Ok(shown) = active
            .parked_output(&self.binary, &["show", key, "--limit", "1"])
            .await
        else {
            return;
        };
        let Some(head) = (shown.code == 0)
            .then(|| dead_head(&shown.stdout))
            .flatten()
        else {
            return;
        };
        let token = self.next_token;
        self.next_token += 1;
        record_marker(token, head.key, head.seq);
        let entry = Unresolved {
            token,
            seq: head.seq,
        };
        self.unresolved.push(entry);
        let discarded = active
            .parked_output(&self.binary, &["discard", key, "--id", &head.id])
            .await;
        match discarded
            .as_ref()
            .map_or(DiscardOutcome::Unresolved, classify_discard)
        {
            DiscardOutcome::Discarded => {
                self.unresolved.pop();
                self.discards.insert(token);
            }
            DiscardOutcome::Refused => {
                self.unresolved.pop();
                self.refused += 1;
            }
            DiscardOutcome::Unresolved => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::testing::Scratch;

    fn output(code: i32, stderr: &str) -> ParkedOutput {
        ParkedOutput {
            code,
            stdout: String::new(),
            stderr: stderr.to_owned(),
        }
    }

    #[test]
    fn a_discard_is_refused_only_when_nothing_could_have_changed() {
        assert_eq!(classify_discard(&output(0, "")), DiscardOutcome::Discarded);
        for code in [404, 409, 503] {
            assert_eq!(
                classify_discard(&output(1, &format!("pg-cdc: {code} Reason: text\n"))),
                DiscardOutcome::Refused,
                "{code}"
            );
        }
        for stderr in [
            "pg-cdc: 504 Gateway Timeout: no answer within 30s\n",
            "pg-cdc: parked request failed: connection refused\n",
            "pg-cdc: 500 Internal Server Error: boom\n",
            "",
        ] {
            assert_eq!(
                classify_discard(&output(1, stderr)),
                DiscardOutcome::Unresolved,
                "{stderr:?}"
            );
        }
        assert_eq!(
            classify_discard(&output(-1, "")),
            DiscardOutcome::Unresolved
        );
    }

    #[test]
    fn dead_queues_come_from_the_listing_and_dead_heads_from_the_first_event() {
        let listing = r#"{"queues":[{"order_key":"aa","head":"h1","status":"dead"},{"order_key":"bb","head":"h2","status":"dead"}],"next":null}"#;
        assert_eq!(dead_queues(listing), ["aa", "bb"]);
        assert_eq!(dead_queues("not json"), Vec::<String>::new());
        assert_eq!(dead_queues(r#"{"queues":[]}"#), Vec::<String>::new());

        let shown = |status: &str, envelope: &str| {
            format!(
                r#"{{"order_key":"aa","events":[{{"id":"e1","status":"{status}","envelope":{envelope}}}],"next":null}}"#
            )
        };
        assert_eq!(
            dead_head(&shown("dead", r#"{"key":{"id":7},"after":{"seq":42}}"#)),
            Some(DeadHead {
                id: "e1".to_owned(),
                key: 7,
                seq: 42
            })
        );
        assert_eq!(
            dead_head(&shown("dead", r#"{"key":{"id":7},"before":{"seq":41}}"#)),
            Some(DeadHead {
                id: "e1".to_owned(),
                key: 7,
                seq: 41
            })
        );
        assert_eq!(
            dead_head(&shown("retrying", r#"{"key":{"id":7},"after":{"seq":42}}"#)),
            None
        );
        assert_eq!(dead_head(&shown("dead", r#"{"op":"insert"}"#)), None);
        assert_eq!(dead_head(r#"{"events":[]}"#), None);
        assert_eq!(dead_head("garbage"), None);
    }

    const LIST: &str =
        r#"{"queues":[{"order_key":"KEY1","head":"EVT1","status":"dead"}],"next":null}"#;
    const SHOW: &str = r#"{"order_key":"KEY1","events":[{"id":"EVT1","status":"dead","envelope":{"key":{"id":7},"after":{"seq":42}}}],"next":null}"#;

    struct Cli {
        _scratch: Scratch,
        binary: PathBuf,
        log: PathBuf,
    }

    impl Cli {
        fn new(list: &str, show: &str, discard: &str) -> Self {
            let scratch = Scratch::new();
            let log = scratch.path().join("calls.log");
            let script = format!(
                "#!/bin/sh\ncase \"$1\" in\n  run)\n    echo '{{\"fields\":{{\"message\":\"admin listening\",\"addr\":\"127.0.0.1:9\"}}}}'\n    exec sleep 60 ;;\n  parked)\n    shift\n    case \"$1\" in\n      list)\n        echo \"$*\" >> {log}\n        {list} ;;\n      show)\n        echo \"$*\" >> {log}\n        echo '{show}' ;;\n      redrive)\n        echo \"$*\" >> {log} ;;\n      discard)\n        echo \"$*\" >> {log}\n        {discard} ;;\n    esac ;;\nesac\n",
                log = log.display(),
            );
            let binary = scratch.script("fake-pg-cdc", &script);
            Self {
                _scratch: scratch,
                binary,
                log,
            }
        }

        fn standard(discard: &str) -> Self {
            Self::new(&format!("echo '{LIST}'"), SHOW, discard)
        }

        async fn process(&self) -> PgCdc {
            PgCdc::spawn(&self.binary, Path::new("config.yaml"), &[], "fake")
                .await
                .unwrap()
        }

        fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(&self.log)
                .unwrap_or_default()
                .lines()
                .map(|line| {
                    let words: Vec<&str> = line.split(' ').collect();
                    words
                        .iter()
                        .take_while(|word| !word.starts_with("--addr"))
                        .copied()
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect()
        }
    }

    fn recorder(log: &Path) -> impl Fn(u64, i32, i64) {
        let log = log.to_owned();
        move |token: u64, key: i32, seq: i64| {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log)
                .unwrap();
            writeln!(file, "marker {token} {key} {seq}").unwrap();
        }
    }

    #[tokio::test]
    async fn a_dead_queue_is_redriven_once_then_discarded_after_its_marker() {
        let cli = Cli::standard("exit 0");
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(cli.calls(), ["list --status dead", "redrive KEY1"]);
        assert_eq!(operator.redrives, 1);
        assert_eq!(operator.discards, BTreeSet::new());

        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(
            cli.calls()[2..],
            [
                "list --status dead",
                "show KEY1 --limit 1",
                "marker 1 7 42",
                "discard KEY1 --id EVT1",
            ]
        );
        assert_eq!(operator.redrives, 1);
        assert_eq!(operator.discards, BTreeSet::from([1]));
        assert_eq!(operator.refused, 0);
        assert_eq!(operator.unresolved, []);

        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(operator.discards, BTreeSet::from([1, 2]));
    }

    #[tokio::test]
    async fn a_refused_discard_leaves_its_token_out() {
        let cli = Cli::standard("echo 'pg-cdc: 409 Conflict: the head moved' >&2; exit 1");
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(operator.discards, BTreeSet::new());
        assert_eq!(operator.refused, 1);
        assert_eq!(operator.unresolved, []);
        assert!(cli.calls().contains(&"marker 1 7 42".to_owned()));
    }

    #[tokio::test]
    async fn a_discard_without_an_answer_is_unresolved() {
        let cli = Cli::standard("echo 'pg-cdc: 504 Gateway Timeout: no answer' >&2; exit 1");
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(operator.discards, BTreeSet::new());
        assert_eq!(operator.refused, 0);
        assert_eq!(operator.unresolved, [Unresolved { token: 1, seq: 42 }]);
    }

    #[tokio::test]
    async fn a_pass_cut_short_inside_a_discard_leaves_its_token_unresolved() {
        let cli = Cli::standard("sleep 30");
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        let cut = tokio::time::timeout(
            Duration::from_millis(800),
            operator.pass(&process, recorder(&cli.log)),
        )
        .await;
        assert!(cut.is_err());
        assert_eq!(operator.discards, BTreeSet::new());
        assert_eq!(operator.unresolved, [Unresolved { token: 1, seq: 42 }]);
    }

    #[tokio::test]
    async fn a_failed_listing_skips_the_pass() {
        let cli = Cli::new(
            "echo 'pg-cdc: 409 Conflict: not the holder' >&2; exit 1",
            SHOW,
            "exit 0",
        );
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        assert_eq!(cli.calls(), ["list --status dead"]);
        assert_eq!(operator.redrives, 0);
    }

    #[tokio::test]
    async fn a_head_that_is_not_dead_is_left_alone() {
        let retrying = SHOW.replace("\"dead\"", "\"retrying\"");
        let cli = Cli::new(&format!("echo '{LIST}'"), &retrying, "exit 0");
        let process = cli.process().await;
        let mut operator = Operator::new(cli.binary.clone());
        operator.pass(&process, recorder(&cli.log)).await;
        operator.pass(&process, recorder(&cli.log)).await;
        let calls = cli.calls();
        assert!(calls.iter().any(|call| call.starts_with("show")));
        assert!(
            !calls
                .iter()
                .any(|call| call.starts_with("marker") || call.starts_with("discard"))
        );
    }
}
