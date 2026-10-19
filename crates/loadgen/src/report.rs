use std::fmt::Write as _;
use std::path::Path;

use anyhow::Context as _;
use serde_json::Value;

const NOT_MEASURED: &str = "not measured";
const NOT_RESOLVED: &str = "not resolved";
const UNVERIFIED: &str = "unverified (emulator-bound)";
const RESOLUTION_SPREAD: f64 = 1.01;
const CPU_OVERHEAD: &str = "overhead";
const INSTRUCTION_OVERHEAD: &str = "overhead-instructions";
const BOTH_OVERHEADS: &str = "overhead, overhead-instructions";
const HEADER: [&str; 5] = ["Criterion", "Target", "Measured", "Verdict", "Scenario"];

struct Row {
    criterion: &'static str,
    target: &'static str,
    scenario: &'static str,
    measured: String,
    verdict: String,
}

struct Measurement {
    measured: String,
    verdict: String,
}

fn find<'a>(reports: &'a [Value], scenario: &str) -> Option<&'a Value> {
    reports
        .iter()
        .find(|report| report.get("scenario").and_then(Value::as_str) == Some(scenario))
}

fn number(value: &Value, path: &[&str]) -> Option<f64> {
    path.iter()
        .try_fold(value, |value, key| value.get(key))?
        .as_f64()
}

fn bytes(value: &Value, path: &[&str]) -> Option<u64> {
    path.iter()
        .try_fold(value, |value, key| value.get(key))?
        .as_u64()
}

fn text<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field)?.as_str()
}

fn mib(bytes: u64) -> f64 {
    u32::try_from(bytes >> 10).map_or(f64::MAX, f64::from) / 1024.0
}

fn row(
    reports: &[Value],
    criterion: &'static str,
    target: &'static str,
    scenario: &'static str,
    measure: fn(&Value) -> Option<Measurement>,
) -> Row {
    let measurement = find(reports, scenario).map(measure);
    let (measured, verdict) = match measurement {
        Some(Some(found)) => (found.measured, found.verdict),
        Some(None) | None => (NOT_MEASURED.to_owned(), NOT_MEASURED.to_owned()),
    };
    Row {
        criterion,
        target,
        scenario,
        measured,
        verdict,
    }
}

fn outage(report: &Value) -> Option<Measurement> {
    let peak = bytes(report, &["peak_retained_bytes"])?;
    let expected = bytes(report, &["expected_retained_bytes"])?;
    let parked = number(report, &["parked_stored_per_s"])?;
    let written = number(report, &["write_rate"])?;
    let park_after = number(report, &["park_after_s"])?;
    let verdict = text(report, "criterion_3")?;
    let mut measured = format!(
        "peak retained {:.1} MiB against {:.1} MiB written in park_after ({park_after:.0} s); parking stored {parked:.0} events/s against {written:.0} written",
        mib(peak),
        mib(expected),
    );
    if let Some(drain) = report.get("drain").and_then(drained) {
        measured.push_str("; ");
        measured.push_str(&drain);
    }
    if verdict == UNVERIFIED {
        if let Some(ceiling) = number(report, &["moto_put_ceiling", "puts_per_s"]) {
            let _ = write!(
                measured,
                "; moto's own PutObject ceiling {ceiling:.0} puts/s"
            );
        }
        measured.push_str("; pending the real-S3 check (R128)");
    }
    Some(Measurement {
        measured,
        verdict: verdict.to_owned(),
    })
}

fn grouped(count: u64) -> String {
    let digits = count.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn drained(drain: &Value) -> Option<String> {
    let remaining = bytes(drain, &["remaining_events"])?;
    let watched = number(drain, &["to_s"])? - number(drain, &["from_s"])?;
    let complete = drain
        .get("complete")
        .and_then(Value::as_bool)
        .unwrap_or(remaining == 0);
    if complete {
        return Some(format!(
            "drain complete: no parked keys and lag under 16 MiB {watched:.0} s after writes stopped"
        ));
    }
    let limit = number(drain, &["deadline_s"]).unwrap_or(watched);
    let progress = draining(drain, remaining, watched);
    Some(format!(
        "drain incomplete at the {limit:.0} s limit: {progress}"
    ))
}

fn draining(drain: &Value, remaining: u64, watched: f64) -> String {
    if remaining == 0 {
        return format!(
            "no events parked but lag not yet under 16 MiB {watched:.0} s after writes stopped"
        );
    }
    let parked = format!(
        "{} events still parked {watched:.0} s after writes stopped",
        grouped(remaining)
    );
    let rate = number(drain, &["events_per_s"]);
    if let Some(rising) = rate.filter(|rate| *rate <= 0.0) {
        return format!(
            "{parked} and still rising ({:.1} events/s), so no drain rate was measured",
            -rising
        );
    }
    let rate = rate.map_or_else(
        || "an unmeasured rate".to_owned(),
        |rate| format!("{rate:.1} events/s"),
    );
    let rest = number(drain, &["projected_s"]).map_or_else(
        || "no projected end".to_owned(),
        |projected| format!("about {:.0} min more", projected / 60.0),
    );
    format!("{parked}, draining {rate}, {rest}")
}

fn throughput(report: &Value) -> Option<Measurement> {
    let verdict = text(report, "criterion_4")?;
    let rate = number(report, &["delivered_rate"])?;
    let mut measured = format!("delivered {rate:.0} events/s");
    if let Some(p99) = number(report, &["p99_upper_bound_s"]) {
        let _ = write!(measured, ", p99 upper bound {p99:.2} s");
    }
    if verdict == UNVERIFIED {
        if let Some(ceiling) = number(report, &["moto_ceiling", "entries_per_s"]) {
            let _ = write!(measured, "; moto's own ceiling {ceiling:.0} messages/s");
        }
        measured.push_str("; pending the real-SNS check (R128)");
    }
    Some(Measurement {
        measured,
        verdict: verdict.to_owned(),
    })
}

fn handover(report: &Value) -> Option<Measurement> {
    let count = bytes(report, &["handovers"])?;
    let exit = number(report, &["max_exit_s"])?;
    let ready = number(report, &["max_ready_s"])?;
    let gap = number(report, &["max_gap_s"])?;
    let intact = report.get("checker_passed")?.as_bool()?;
    Some(Measurement {
        measured: format!(
            "{count} handovers: worst SIGTERM to /readyz {ready:.2} s, worst delivery gap {gap:.2} s, worst exit {exit:.2} s; evidence {}",
            if intact { "intact" } else { "broken" }
        ),
        verdict: text(report, "criterion_5")?.to_owned(),
    })
}

fn memory(report: &Value) -> Option<Measurement> {
    let bound = bytes(report, &["max_inflight_bytes"])?;
    let peak = report
        .get("modes")?
        .as_array()?
        .iter()
        .flat_map(|mode| {
            mode.get("runs")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|run| bytes(run, &["peak_rss_bytes"]))
        .max()?;
    Some(Measurement {
        measured: format!(
            "peak RSS {:.1} MiB with max_inflight_bytes {:.0} MiB, flat from 50,000 to 500,000 rows",
            mib(peak),
            mib(bound),
        ),
        verdict: text(report, "criterion_6")?.to_owned(),
    })
}

fn overhead(report: &Value) -> Option<Measurement> {
    let percent = number(report, &["overhead_percent"])?;
    let default_spread = number(report, &["default", "cpu_spread"])?;
    let baseline_spread = number(report, &["baseline", "cpu_spread"])?;
    let spread = default_spread.max(baseline_spread);
    let unresolved = spread > RESOLUTION_SPREAD;
    let verdict = if unresolved {
        NOT_RESOLVED.to_owned()
    } else {
        text(report, "criterion_7")?.to_owned()
    };
    Some(Measurement {
        measured: format!(
            "{percent:+.2}% CPU per event, runs spread {:.1}%{}",
            (spread - 1.0) * 100.0,
            if unresolved {
                ", beyond the 1% to resolve"
            } else {
                ""
            }
        ),
        verdict,
    })
}

fn instructions(report: &Value) -> Option<String> {
    let percent = number(report, &["overhead_percent"])?;
    let noise = number(report, &["noise_percent"])?;
    let rule = text(report, "noise_rule_verdict")?;
    Some(format!(
        "supporting, not a verdict: {percent:+.2}% instructions per event under cachegrind, \
         middle runs spread {noise:.2}%, {rule} by the noise rule"
    ))
}

fn telemetry_overhead(reports: &[Value]) -> Row {
    let mut row = row(
        reports,
        "7. Telemetry overhead",
        "within 1% of a build without the otel feature when unconfigured",
        CPU_OVERHEAD,
        overhead,
    );
    if let Some(supporting) = find(reports, INSTRUCTION_OVERHEAD).and_then(instructions) {
        if row.verdict == NOT_MEASURED {
            "CPU time not measured".clone_into(&mut row.measured);
            NOT_RESOLVED.clone_into(&mut row.verdict);
        }
        row.measured = format!("{}; {supporting}", row.measured);
        row.scenario = BOTH_OVERHEADS;
    }
    row
}

pub fn summary_table(reports: &[Value]) -> String {
    let rows = [
        row(
            reports,
            "3. WAL stays bounded during a sink outage",
            "retained WAL about park_after of write volume",
            "outage",
            outage,
        ),
        row(
            reports,
            "4. Throughput against SNS",
            "at least 5,000 events/s with p99 commit-to-acknowledgement under 1 s",
            "sns",
            throughput,
        ),
        row(
            reports,
            "5. Handover",
            "a deploy handover under 10 s with nothing lost",
            "handover",
            handover,
        ),
        row(
            reports,
            "6. Memory",
            "within max_inflight_bytes plus fixed overhead whatever the transaction size",
            "memory",
            memory,
        ),
        telemetry_overhead(reports),
    ];
    let mut table = format!(
        "| {} |\n|{}\n",
        HEADER.join(" | "),
        "---|".repeat(HEADER.len())
    );
    for row in rows {
        let _ = writeln!(
            table,
            "| {} | {} | {} | {} | {} |",
            row.criterion, row.target, row.measured, row.verdict, row.scenario
        );
    }
    table
}

pub async fn write_summary(report_dir: &Path, out: &Path) -> anyhow::Result<()> {
    let mut paths = Vec::new();
    let mut entries = tokio::fs::read_dir(report_dir)
        .await
        .with_context(|| format!("cannot read {}", report_dir.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            paths.push(path);
        }
    }
    paths.sort();
    let mut reports = Vec::with_capacity(paths.len());
    for path in &paths {
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("cannot read {}", path.display()))?;
        reports.push(
            serde_json::from_slice(&bytes)
                .with_context(|| format!("{} is not JSON", path.display()))?,
        );
    }
    if let Some(parent) = out.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(out, summary_table(&reports))
        .await
        .with_context(|| format!("cannot write {}", out.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn outage() -> Value {
        json!({
            "scenario": "outage",
            "park_after_s": 5.0,
            "write_rate": 1000,
            "parked_stored_per_s": 987.5,
            "expected_retained_bytes": 2_621_440,
            "peak_retained_bytes": 3_145_728,
            "drain": {
                "deadline_s": 180.0,
                "from_s": 150.0,
                "to_s": 330.0,
                "remaining_events": 96_400,
                "remaining_keys": 990,
                "events_per_s": 20.0,
                "projected_s": 4_820.0,
                "complete": false,
            },
            "criterion_3": "pass",
        })
    }

    fn emulator_bound_outage() -> Value {
        let mut report = outage();
        report["criterion_3"] = json!("unverified (emulator-bound)");
        report["moto_put_ceiling"] = json!({ "puts_per_s": 412.5 });
        report
    }

    fn handover(criterion_5: &str) -> Value {
        json!({
            "scenario": "handover",
            "handovers": 5,
            "max_exit_s": 1.25,
            "max_ready_s": 6.5,
            "max_gap_s": 6.75,
            "checker_passed": true,
            "criterion_5": criterion_5,
        })
    }

    fn sns(criterion_4: &str) -> Value {
        json!({
            "scenario": "sns",
            "delivered_rate": 3821.0,
            "p99_upper_bound_s": 60.0,
            "moto_ceiling": { "entries_per_s": 4363.0 },
            "criterion_4": criterion_4,
        })
    }

    fn ceiling() -> Value {
        json!({
            "scenario": "ceiling",
            "sustained": { "achieved_rate": 6000.1, "kept_up": true, "p99_upper_bound_s": 0.01 },
        })
    }

    fn memory() -> Value {
        json!({
            "scenario": "memory",
            "max_inflight_bytes": 67_108_864_u64,
            "modes": [
                { "mode": "discard", "runs": [{ "rows": 50_000, "peak_rss_bytes": 23_085_056_u64 }] },
                { "mode": "stalled", "runs": [
                    { "rows": 50_000, "peak_rss_bytes": 127_746_048_u64 },
                    { "rows": 500_000, "peak_rss_bytes": 128_237_568_u64 },
                ] },
            ],
            "criterion_6": "pass",
        })
    }

    fn overhead(spread: f64, criterion_7: &str) -> Value {
        json!({
            "scenario": "overhead",
            "overhead_percent": -0.73,
            "default": { "cpu_spread": spread },
            "baseline": { "cpu_spread": 1.002 },
            "criterion_7": criterion_7,
        })
    }

    fn cells(table: &str, criterion: &str) -> Vec<String> {
        let row = table
            .lines()
            .find(|line| line.starts_with(&format!("| {criterion}.")))
            .unwrap_or_else(|| panic!("no row for criterion {criterion} in\n{table}"));
        row.trim_matches('|')
            .split('|')
            .map(|cell| cell.trim().to_owned())
            .collect()
    }

    fn every_scenario() -> Vec<Value> {
        vec![
            outage(),
            handover("pass"),
            sns("unverified (emulator-bound)"),
            ceiling(),
            memory(),
            overhead(1.14, "pass"),
        ]
    }

    #[test]
    fn the_summary_has_one_row_per_criterion() {
        let table = summary_table(&every_scenario());
        let header = table.lines().next().unwrap();
        assert_eq!(
            header
                .trim_matches('|')
                .split('|')
                .map(str::trim)
                .collect::<Vec<_>>(),
            ["Criterion", "Target", "Measured", "Verdict", "Scenario"]
        );
        assert_eq!(
            table.lines().nth(1),
            Some("|---|---|---|---|---|"),
            "{table}"
        );
        assert_eq!(table.lines().count(), 7, "{table}");
        for (criterion, scenario) in [
            ("3", "outage"),
            ("4", "sns"),
            ("5", "handover"),
            ("6", "memory"),
            ("7", "overhead"),
        ] {
            let row = cells(&table, criterion);
            assert_eq!(row.len(), 5, "{row:?}");
            assert!(!row[1].is_empty(), "target of {criterion}");
            assert!(!row[2].is_empty(), "measured of {criterion}");
            assert!(!row[3].is_empty(), "verdict of {criterion}");
            assert_eq!(row[4], scenario);
        }
        assert_eq!(cells(&table, "3")[3], "pass");
        assert!(cells(&table, "3")[2].contains("3.0 MiB"), "{table}");
        assert!(cells(&table, "3")[2].contains("2.5 MiB"), "{table}");
        assert!(
            cells(&table, "3")[2].contains(
                "drain incomplete at the 180 s limit: 96,400 events still parked 180 s after writes stopped, draining 20.0 events/s, about 80 min more"
            ),
            "{table}"
        );
        assert_eq!(cells(&table, "5")[3], "pass");
        assert!(cells(&table, "5")[2].contains("6.50 s"), "{table}");
        assert!(cells(&table, "5")[2].contains("6.75 s"), "{table}");
        assert_eq!(cells(&table, "6")[3], "pass");
        assert!(cells(&table, "6")[2].contains("122.3 MiB"), "{table}");
    }

    #[test]
    fn a_missing_scenario_is_not_measured() {
        let reports = [outage(), memory()];
        let table = summary_table(&reports);
        for (criterion, scenario) in [("4", "sns"), ("5", "handover"), ("7", "overhead")] {
            let row = cells(&table, criterion);
            assert_eq!(row[2], "not measured", "{row:?}");
            assert_eq!(row[3], "not measured", "{row:?}");
            assert_eq!(row[4], scenario);
        }
        assert_eq!(cells(&table, "3")[3], "pass");
        assert_eq!(summary_table(&[]).matches("not measured").count(), 10);
    }

    #[test]
    fn criterion_four_takes_its_verdict_from_the_sns_report_only() {
        let reports = [ceiling(), sns("unverified (emulator-bound)")];
        let row = cells(&summary_table(&reports), "4");
        assert_eq!(row[3], "unverified (emulator-bound)");
        assert!(row[2].contains("R128"), "{row:?}");
        assert!(row[2].contains("real"), "{row:?}");
        assert_eq!(row[4], "sns");

        let only_ceiling = cells(&summary_table(&[ceiling()]), "4");
        assert_eq!(only_ceiling[3], "not measured");

        let passed = cells(&summary_table(&[ceiling(), sns("pass")]), "4");
        assert_eq!(passed[3], "pass");
        assert!(!passed[2].contains("R128"), "{passed:?}");

        let failed = cells(&summary_table(&[sns("fail")]), "4");
        assert_eq!(failed[3], "fail");
    }

    #[test]
    fn an_emulator_bound_outage_names_the_pending_real_s3_check() {
        let row = cells(&summary_table(&[emulator_bound_outage()]), "3");
        assert_eq!(row[3], "unverified (emulator-bound)");
        assert!(row[2].contains("R128"), "{row:?}");
        assert!(row[2].contains("412"), "{row:?}");
        let passed = cells(&summary_table(&[outage()]), "3");
        assert!(!passed[2].contains("R128"), "{passed:?}");
    }

    #[test]
    fn a_backlog_still_parking_at_the_drain_deadline_has_no_drain_rate() {
        let mut report = outage();
        report["drain"]["remaining_events"] = json!(74_135);
        report["drain"]["remaining_keys"] = json!(1_000);
        report["drain"]["events_per_s"] = json!(-179.4);
        report["drain"]["projected_s"] = Value::Null;
        let row = cells(&summary_table(&[report]), "3");
        assert!(
            row[2].contains(
                "drain incomplete at the 180 s limit: 74,135 events still parked 180 s after writes stopped and still rising (179.4 events/s), so no drain rate was measured"
            ),
            "{row:?}"
        );

        let mut emptied = outage();
        emptied["drain"]["remaining_events"] = json!(0);
        emptied["drain"]["remaining_keys"] = json!(0);
        emptied["drain"]["to_s"] = json!(212.0);
        emptied["drain"]["projected_s"] = json!(0.0);
        emptied["drain"]["complete"] = json!(true);
        let row = cells(&summary_table(&[emptied.clone()]), "3");
        assert!(
            row[2].contains(
                "drain complete: no parked keys and lag under 16 MiB 62 s after writes stopped"
            ),
            "{row:?}"
        );

        emptied["drain"]["complete"] = json!(false);
        let row = cells(&summary_table(&[emptied]), "3");
        assert!(
            row[2].contains(
                "drain incomplete at the 180 s limit: no events parked but lag not yet under 16 MiB 62 s after writes stopped"
            ),
            "{row:?}"
        );
    }

    #[test]
    fn criterion_seven_is_not_resolved_when_the_runs_spread_beyond_one_percent() {
        let noisy = cells(&summary_table(&[overhead(1.14, "pass")]), "7");
        assert_eq!(noisy[3], "not resolved");
        assert!(noisy[2].contains("-0.73%"), "{noisy:?}");
        let quiet = cells(&summary_table(&[overhead(1.004, "pass")]), "7");
        assert_eq!(quiet[3], "pass");
        let failing = cells(&summary_table(&[overhead(1.004, "fail")]), "7");
        assert_eq!(failing[3], "fail");
    }

    fn instructions(rule: &str) -> Value {
        json!({
            "scenario": "overhead-instructions",
            "overhead_percent": 0.123,
            "noise_percent": 0.456,
            "noise_rule_verdict": rule,
        })
    }

    #[test]
    fn instruction_counts_support_criterion_seven_without_deciding_it() {
        let alone = cells(&summary_table(&[instructions("pass")]), "7");
        assert_eq!(alone[3], "not resolved");
        assert_eq!(
            alone[2],
            "CPU time not measured; supporting, not a verdict: +0.12% instructions per event \
             under cachegrind, middle runs spread 0.46%, pass by the noise rule"
        );
        assert_eq!(alone[4], "overhead, overhead-instructions");

        let noisy_cpu = cells(
            &summary_table(&[overhead(1.14, "pass"), instructions("pass")]),
            "7",
        );
        assert_eq!(noisy_cpu[3], "not resolved");
        assert!(
            noisy_cpu[2].starts_with("-0.73% CPU per event"),
            "{noisy_cpu:?}"
        );
        assert!(
            noisy_cpu[2].contains("pass by the noise rule"),
            "{noisy_cpu:?}"
        );

        let quiet_cpu = cells(
            &summary_table(&[overhead(1.004, "fail"), instructions("pass")]),
            "7",
        );
        assert_eq!(quiet_cpu[3], "fail");
    }

    #[test]
    fn a_failing_handover_or_outage_is_reported_as_failing() {
        let table = summary_table(&[handover("fail")]);
        assert_eq!(cells(&table, "5")[3], "fail");
    }

    #[tokio::test]
    async fn the_summary_is_written_from_every_json_in_the_report_dir() {
        let dir = tempfile::tempdir().unwrap();
        for report in [outage(), handover("pass")] {
            let name = format!("{}.json", report["scenario"].as_str().unwrap());
            std::fs::write(dir.path().join(name), report.to_string()).unwrap();
        }
        std::fs::write(dir.path().join("notes.txt"), "ignored").unwrap();
        std::fs::write(dir.path().join("broken.json"), "{").unwrap();
        let out = dir.path().join("out").join("summary.md");
        let error = write_summary(dir.path(), &out).await.unwrap_err();
        assert!(error.to_string().contains("broken.json"), "{error}");

        std::fs::remove_file(dir.path().join("broken.json")).unwrap();
        write_summary(dir.path(), &out).await.unwrap();
        let written = std::fs::read_to_string(&out).unwrap();
        assert!(written.contains("| 3. "), "{written}");
        assert!(written.contains("| 5. "), "{written}");
        assert_eq!(written.matches("not measured").count(), 6, "{written}");
    }
}
