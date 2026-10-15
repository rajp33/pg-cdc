use std::time::Duration;

use anyhow::Context as _;
use pg_cdc_testkit::{MOTO_TAG, Postgres};
use serde::Serialize;
use tokio::process::Command;

const KIB: u64 = 1024;
const NANOS_DIGITS: usize = 9;
const SECONDS_PER_DAY: u64 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProcessSample {
    pub rss_bytes: u64,
    pub cpu: Duration,
}

#[derive(Debug, Clone, Serialize)]
pub struct Machine {
    pub os: String,
    pub cpu: String,
    pub cores: u32,
    pub memory_bytes: u64,
    pub docker: String,
    pub postgres: String,
    pub moto: String,
    pub rustc: String,
    pub commit: String,
}

pub fn parse_cpu_time(text: &str) -> Option<Duration> {
    let text = text.trim();
    let (days, clock) = match text.split_once('-') {
        Some((days, clock)) => (days.parse::<u64>().ok()?, clock),
        None => (0, text),
    };
    let (whole, fraction) = clock.split_once('.').unwrap_or((clock, ""));
    let fields: Vec<&str> = whole.split(':').collect();
    if !(2..=3).contains(&fields.len()) {
        return None;
    }
    let mut seconds: u64 = 0;
    for field in fields {
        seconds = seconds.checked_mul(60)?.checked_add(field.parse().ok()?)?;
    }
    seconds = seconds.checked_add(days.checked_mul(SECONDS_PER_DAY)?)?;
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let digits: String = fraction.chars().take(NANOS_DIGITS).collect();
    let nanos: u32 = format!("{digits:0<NANOS_DIGITS$}").parse().ok()?;
    Some(Duration::new(seconds, nanos))
}

fn parse_sample(text: &str) -> Option<ProcessSample> {
    let mut fields = text.split_whitespace();
    let rss: u64 = fields.next()?.parse().ok()?;
    let cpu = parse_cpu_time(fields.next()?)?;
    if fields.next().is_some() {
        return None;
    }
    Some(ProcessSample {
        rss_bytes: rss.checked_mul(KIB)?,
        cpu,
    })
}

pub async fn sample(pid: u32) -> anyhow::Result<ProcessSample> {
    let output = Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()
        .await
        .context("cannot run ps")?;
    anyhow::ensure!(output.status.success(), "ps found no process {pid}");
    let text = String::from_utf8_lossy(&output.stdout);
    parse_sample(&text).with_context(|| format!("cannot read ps output {text:?}"))
}

pub fn p99_upper_bound(bounds: &[f64], counts: &[u64]) -> Option<f64> {
    let total: u128 = counts.iter().map(|count| u128::from(*count)).sum();
    if total == 0 {
        return None;
    }
    let mut cumulative: u128 = 0;
    for (index, count) in counts.iter().enumerate() {
        cumulative += u128::from(*count);
        if cumulative * 100 >= total * 99 {
            return bounds.get(index).copied();
        }
    }
    None
}

pub fn median(values: &mut [f64]) -> Option<f64> {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    let high = values.get(middle).copied()?;
    if values.len() % 2 == 1 {
        return Some(high);
    }
    let low = values.get(middle.checked_sub(1)?).copied()?;
    Some(f64::midpoint(low, high))
}

pub fn spread(values: &[f64]) -> Option<f64> {
    let min = values.iter().copied().reduce(f64::min)?;
    let max = values.iter().copied().reduce(f64::max)?;
    (min > 0.0).then_some(max / min)
}

fn linux_cpu(cpuinfo: &str) -> Option<(String, u32)> {
    let model = cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name"))?
        .split_once(':')?
        .1
        .trim()
        .to_owned();
    let cores = cpuinfo
        .lines()
        .filter(|line| line.starts_with("processor"))
        .count();
    Some((model, u32::try_from(cores).ok()?))
}

fn linux_memory(meminfo: &str) -> Option<u64> {
    let line = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))?;
    let kib: u64 = line.trim().strip_suffix("kB")?.trim().parse().ok()?;
    kib.checked_mul(KIB)
}

async fn output(program: &str, args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("cannot run {program}"))?;
    anyhow::ensure!(output.status.success(), "{program} {args:?} failed");
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn hardware() -> anyhow::Result<(String, String, u32, u64)> {
    if cfg!(target_os = "macos") {
        let version = output("sw_vers", &["-productVersion"]).await?;
        let cpu = output("sysctl", &["-n", "machdep.cpu.brand_string"]).await?;
        let cores = output("sysctl", &["-n", "hw.ncpu"]).await?.parse()?;
        let memory = output("sysctl", &["-n", "hw.memsize"]).await?.parse()?;
        Ok((format!("macOS {version}"), cpu, cores, memory))
    } else {
        let os = output("uname", &["-sr"]).await?;
        let cpuinfo = tokio::fs::read_to_string("/proc/cpuinfo").await?;
        let meminfo = tokio::fs::read_to_string("/proc/meminfo").await?;
        let (cpu, cores) = linux_cpu(&cpuinfo).context("cannot read /proc/cpuinfo")?;
        let memory = linux_memory(&meminfo).context("cannot read /proc/meminfo")?;
        Ok((os, cpu, cores, memory))
    }
}

pub async fn machine(pg: &Postgres) -> anyhow::Result<Machine> {
    let (os, cpu, cores, memory_bytes) = hardware().await?;
    let postgres: String = pg
        .client()
        .await?
        .query_one("SHOW server_version", &[])
        .await?
        .get(0);
    Ok(Machine {
        os,
        cpu,
        cores,
        memory_bytes,
        docker: output(
            "docker",
            &[
                "version",
                "--format",
                "{{.Server.Version}} ({{.Server.Os}}/{{.Server.Arch}})",
            ],
        )
        .await?,
        postgres,
        moto: MOTO_TAG.to_owned(),
        rustc: output("rustc", &["--version"]).await?,
        commit: output("git", &["rev-parse", "HEAD"]).await?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_times_parse_in_every_ps_format() {
        assert_eq!(
            parse_cpu_time("0:01.23"),
            Some(Duration::from_millis(1_230))
        );
        assert_eq!(
            parse_cpu_time("12:34.56"),
            Some(Duration::from_millis(754_560))
        );
        assert_eq!(parse_cpu_time("01:02:03"), Some(Duration::from_secs(3_723)));
        assert_eq!(parse_cpu_time("2-01:00:00"), Some(Duration::from_hours(49)));
        assert_eq!(parse_cpu_time("00:07"), Some(Duration::from_secs(7)));
        assert_eq!(
            parse_cpu_time("  0:00.50 "),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_cpu_time("garbage"), None);
        assert_eq!(parse_cpu_time(""), None);
        assert_eq!(parse_cpu_time("1:2:3:4"), None);
        assert_eq!(parse_cpu_time("x-01:00:00"), None);
        assert_eq!(parse_cpu_time("1.5:30"), None);
    }

    #[test]
    fn ps_output_gives_rss_in_bytes_and_cpu_time() {
        assert_eq!(
            parse_sample("  20480   0:01.50\n"),
            Some(ProcessSample {
                rss_bytes: 20_480 * 1024,
                cpu: Duration::from_millis(1_500),
            })
        );
        assert_eq!(parse_sample(""), None);
        assert_eq!(parse_sample("20480"), None);
        assert_eq!(parse_sample("abc 0:01.50"), None);
        assert_eq!(parse_sample("1 0:01.50 extra"), None);
    }

    #[test]
    fn the_p99_is_the_upper_bound_of_its_bucket() {
        let bounds = [0.1, 0.5, 1.0];
        assert_eq!(p99_upper_bound(&bounds, &[90, 9, 1, 0]), Some(0.5));
        assert_eq!(p99_upper_bound(&bounds, &[0, 0, 0, 5]), None);
        assert_eq!(p99_upper_bound(&bounds, &[0, 0, 0, 0]), None);
        assert_eq!(p99_upper_bound(&[], &[]), None);
        assert_eq!(p99_upper_bound(&bounds, &[100, 0, 0, 0]), Some(0.1));
        assert_eq!(p99_upper_bound(&bounds, &[89, 9, 1, 1]), Some(1.0));
        assert_eq!(p99_upper_bound(&bounds, &[88, 9, 1, 2]), None);
    }

    #[test]
    fn medians_and_spreads() {
        assert_eq!(median(&mut [3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&mut [1.0, 2.0, 3.0, 4.0]), Some(2.5));
        assert_eq!(median(&mut []), None);
        assert_eq!(spread(&[3.0, 1.0, 2.0]), Some(3.0));
        assert_eq!(spread(&[]), None);
        assert_eq!(spread(&[0.0, 1.0]), None);
    }

    #[test]
    fn linux_hardware_is_read_from_proc() {
        let cpuinfo = "processor\t: 0\nmodel name\t: Fast CPU @ 3GHz\n\nprocessor\t: 1\nmodel name\t: Fast CPU @ 3GHz\n";
        assert_eq!(linux_cpu(cpuinfo), Some(("Fast CPU @ 3GHz".to_owned(), 2)));
        assert_eq!(linux_cpu("nothing"), None);
        assert_eq!(
            linux_memory("MemFree: 1 kB\nMemTotal:       16384 kB\n"),
            Some(16_384 * 1024)
        );
        assert_eq!(linux_memory("MemTotal: lots"), None);
        assert_eq!(linux_memory(""), None);
    }
}
