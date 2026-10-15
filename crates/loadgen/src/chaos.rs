use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use self::harness::{Harness, workload_config};
use crate::checker::{Observed, Verdict};
use crate::proxy::ProxyCounts;
use crate::workload::{self, Change};

pub mod evidence;
pub mod fleet;
mod harness;
pub mod operator;

#[derive(Debug, Clone)]
pub struct ChaosOptions {
    pub binary: PathBuf,
    pub seed: u64,
    pub duration: Duration,
    pub settle: Duration,
    pub report: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Action {
    KillActive,
    TermActive,
    DropConnections,
    SinkOutage,
    StoreBrownout,
    ErrorBurst,
    Nothing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unresolved {
    pub token: u64,
    pub seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolution {
    pub joined: BTreeSet<u64>,
    pub dropped: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChaosReport {
    pub seed: u64,
    pub duration_s: f64,
    pub actions: BTreeMap<String, u64>,
    pub takeovers: u64,
    pub takeover_kinds: BTreeMap<String, u64>,
    pub redrives: u64,
    pub discards: u64,
    pub discards_refused: u64,
    pub discards_resolved_late: u64,
    pub proxy: [ProxyCounts; 2],
    pub ledger_transactions: u64,
    pub verdict: Verdict,
}

const WEIGHTS: [(Action, u32); 7] = [
    (Action::KillActive, 10),
    (Action::TermActive, 10),
    (Action::DropConnections, 10),
    (Action::SinkOutage, 15),
    (Action::StoreBrownout, 10),
    (Action::ErrorBurst, 10),
    (Action::Nothing, 35),
];

pub fn choose(rng: &mut fastrand::Rng) -> Action {
    let total: u32 = WEIGHTS.iter().map(|(_, weight)| weight).sum();
    let mut draw = rng.u32(0..total);
    for (action, weight) in WEIGHTS {
        if draw < weight {
            return action;
        }
        draw -= weight;
    }
    Action::Nothing
}

pub fn outstanding(committed: &[Change], observed: &[Observed], discards: &BTreeSet<u64>) -> usize {
    let cleared: HashSet<i64> = observed
        .iter()
        .filter_map(|observed| match observed {
            Observed::Delivered(delivery) => Some(delivery.seq),
            Observed::DiscardMarker { token, seq, .. } => discards.contains(token).then_some(*seq),
        })
        .collect();
    committed
        .iter()
        .filter(|change| !cleared.contains(&change.seq))
        .count()
}

pub fn settle_over(pending: usize, elapsed: Duration, limit: Duration) -> bool {
    pending == 0 || elapsed >= limit
}

pub fn resolve_unresolved(
    unresolved: &[Unresolved],
    stored: &HashSet<i64, impl std::hash::BuildHasher>,
    observed: &[Observed],
) -> Resolution {
    let delivered: HashSet<i64> = observed
        .iter()
        .filter_map(|observed| match observed {
            Observed::Delivered(delivery) => Some(delivery.seq),
            Observed::DiscardMarker { .. } => None,
        })
        .collect();
    let mut resolution = Resolution::default();
    for entry in unresolved {
        if stored.contains(&entry.seq) || delivered.contains(&entry.seq) {
            resolution.dropped += 1;
        } else {
            resolution.joined.insert(entry.token);
        }
    }
    resolution
}

pub fn missing_takeovers(kinds: &BTreeMap<String, u64>) -> Vec<String> {
    [
        ("clean", "the run saw no clean takeover"),
        ("unclean", "the run saw no crash (unclean) takeover"),
    ]
    .into_iter()
    .filter(|(kind, _)| kinds.get(*kind).copied().unwrap_or(0) == 0)
    .map(|(_, problem)| problem.to_owned())
    .collect()
}

fn summary(report: &ChaosReport) -> String {
    let verdict = &report.verdict;
    format!(
        "chaos seed={} duration={}s verdict={} committed={} delivered={} duplicates={} stored={} discarded={} takeovers={} clean={} unclean={} redrives={} discards={} missing={} phantoms={} out_of_order={}",
        report.seed,
        report.duration_s,
        if verdict.passed() { "pass" } else { "fail" },
        verdict.committed,
        verdict.delivered,
        verdict.duplicates,
        verdict.stored,
        verdict.discarded,
        report.takeovers,
        report.takeover_kinds.get("clean").copied().unwrap_or(0),
        report.takeover_kinds.get("unclean").copied().unwrap_or(0),
        report.redrives,
        report.discards,
        verdict.missing.len(),
        verdict.phantoms.len(),
        verdict.out_of_order.len(),
    )
}

async fn write_report(path: &std::path::Path, report: &ChaosReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, serde_json::to_vec_pretty(report)?)
        .await
        .with_context(|| format!("cannot write {}", path.display()))
}

pub async fn run(options: &ChaosOptions) -> anyhow::Result<ChaosReport> {
    let mut harness = Harness::start(options).await?;
    let stop = CancellationToken::new();
    let client = harness.writer_client().await?;
    let config = workload_config(options.seed);
    let mut writer = tokio::spawn({
        let stop = stop.clone();
        async move { workload::write(&client, &config, stop).await }
    });
    if let Err(error) = harness.opening(&mut writer).await {
        stop.cancel();
        return Err(error);
    }
    if let Err(error) = harness.drive(options.duration, &mut writer).await {
        stop.cancel();
        return Err(error);
    }
    stop.cancel();
    let ledger = writer.await.context("the workload task panicked")??;
    harness.settle(&ledger, options.settle).await?;
    let outcome = harness.finish(ledger, options).await?;
    write_report(&options.report, &outcome.report).await?;
    writeln!(std::io::stdout(), "{}", summary(&outcome.report))?;
    anyhow::ensure!(
        outcome.problems.is_empty(),
        "{}",
        outcome.problems.join("; ")
    );
    Ok(outcome.report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::Delivery;
    use crate::workload::Change;

    #[test]
    fn the_action_schedule_is_seeded_and_weighted() {
        let mut first = fastrand::Rng::with_seed(7);
        let mut second = fastrand::Rng::with_seed(7);
        let one: Vec<Action> = (0..1_000).map(|_| choose(&mut first)).collect();
        let two: Vec<Action> = (0..1_000).map(|_| choose(&mut second)).collect();
        assert_eq!(one, two);

        let mut rng = fastrand::Rng::with_seed(11);
        let mut counts: BTreeMap<String, u32> = BTreeMap::new();
        for _ in 0..100_000 {
            *counts.entry(format!("{:?}", choose(&mut rng))).or_default() += 1;
        }
        for (action, weight) in WEIGHTS {
            let drawn = counts.get(&format!("{action:?}")).copied().unwrap_or(0);
            let share = f64::from(drawn) / 1_000.0;
            assert!(
                (share - f64::from(weight)).abs() <= 1.0,
                "{action:?} drew {share}% against a weight of {weight}"
            );
        }
    }

    #[test]
    fn settling_waits_until_every_committed_change_is_delivered_or_discarded() {
        let committed: Vec<Change> = (1..=4).map(|seq| Change { seq, key: 1 }).collect();
        let marker = |token, seq| Observed::DiscardMarker { token, key: 1, seq };
        let observed = [delivered(1), delivered(2), marker(5, 3), marker(6, 4)];
        let discards = BTreeSet::from([5]);
        assert_eq!(outstanding(&committed, &observed, &discards), 1);
        assert_eq!(
            outstanding(&committed, &observed, &BTreeSet::from([5, 6])),
            0
        );
        assert_eq!(outstanding(&committed, &[], &BTreeSet::new()), 4);
        assert_eq!(outstanding(&[], &observed, &discards), 0);
    }

    #[test]
    fn settling_ends_at_its_limit_even_while_changes_are_still_draining() {
        let limit = Duration::from_secs(60);
        assert!(!settle_over(5, Duration::from_secs(59), limit));
        assert!(settle_over(5, limit, limit));
        assert!(settle_over(5, Duration::from_secs(600), limit));
        assert!(settle_over(0, Duration::ZERO, limit));
    }

    fn delivered(seq: i64) -> Observed {
        Observed::Delivered(Delivery { seq, key: 1 })
    }

    #[test]
    fn a_run_needs_both_a_clean_and_a_crash_takeover() {
        let kinds = |pairs: &[(&str, u64)]| -> BTreeMap<String, u64> {
            pairs
                .iter()
                .map(|(kind, count)| ((*kind).to_owned(), *count))
                .collect()
        };
        assert_eq!(
            missing_takeovers(&kinds(&[("first", 1), ("clean", 1), ("unclean", 4)])),
            Vec::<String>::new()
        );
        assert_eq!(
            missing_takeovers(&kinds(&[("first", 1), ("unclean", 19)])),
            ["the run saw no clean takeover"]
        );
        assert_eq!(
            missing_takeovers(&kinds(&[("first", 1), ("clean", 2)])),
            ["the run saw no crash (unclean) takeover"]
        );
        assert_eq!(missing_takeovers(&kinds(&[])).len(), 2);
    }

    #[test]
    fn unresolved_discards_count_only_when_the_event_is_gone() {
        let unresolved = [
            Unresolved { token: 1, seq: 10 },
            Unresolved { token: 2, seq: 20 },
            Unresolved { token: 3, seq: 30 },
            Unresolved { token: 4, seq: 40 },
        ];
        let stored: HashSet<i64> = HashSet::from([20]);
        let observed = [
            delivered(30),
            Observed::DiscardMarker {
                token: 1,
                key: 1,
                seq: 10,
            },
        ];
        let resolution = resolve_unresolved(&unresolved, &stored, &observed);
        assert_eq!(resolution.joined, BTreeSet::from([1, 4]));
        assert_eq!(resolution.dropped, 2);

        let nothing = resolve_unresolved(&[], &stored, &observed);
        assert_eq!(nothing, Resolution::default());
    }
}
