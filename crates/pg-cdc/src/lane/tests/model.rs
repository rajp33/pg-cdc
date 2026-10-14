use super::*;

const KEYS: u8 = 4;
const EVENTS: usize = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Flaky(u32),
    Terminal,
    Outage,
}

fn fate(code: u8) -> Fate {
    match code {
        0..=2 => Fate::Flaky(u32::from(code)),
        3 => Fate::Terminal,
        _ => Fate::Outage,
    }
}

fn script(fates: Vec<Fate>, outage_until: Instant) -> Script {
    Box::new(
        move |ordinal, attempt| match fates[usize::try_from(ordinal).unwrap()] {
            Fate::Flaky(failures) if attempt <= failures => {
                Outcome::Retryable(SinkError("flaky".to_owned()))
            }
            Fate::Terminal => Outcome::Terminal(SinkError("rejected".to_owned())),
            Fate::Outage if Instant::now() < outage_until => {
                Outcome::Retryable(SinkError("outage".to_owned()))
            }
            Fate::Flaky(_) | Fate::Outage => Outcome::Delivered,
        },
    )
}

fn check_order(sends: &[Sent], keys: &[u8]) -> Result<(), TestCaseError> {
    for key in 0..KEYS {
        let per_key: Vec<&Sent> = sends
            .iter()
            .filter(|sent| keys[usize::try_from(sent.ordinal).unwrap()] == key)
            .collect();
        for pair in per_key.windows(2) {
            prop_assert!(
                pair[0].ordinal <= pair[1].ordinal,
                "key {key}: {} after {}",
                pair[1].ordinal,
                pair[0].ordinal
            );
            if pair[0].ordinal != pair[1].ordinal {
                prop_assert!(
                    pair[0].delivered,
                    "key {key}: {} sent before {} was delivered",
                    pair[1].ordinal,
                    pair[0].ordinal
                );
            }
        }
    }
    Ok(())
}

async fn run_phase(
    store: TestStore,
    keys: &[u8],
    fates: &[Fate],
    outage_until: Instant,
    lanes: usize,
) -> Result<Harness, TestCaseError> {
    let setup = Setup {
        lanes,
        concurrency: 2,
        park_after: Duration::from_secs(1),
        ..Setup::default()
    };
    let sink = ScriptedSink::new(LIMITS, script(fates.to_vec(), outage_until));
    let mut harness = Harness::start(setup, sink, store).await;
    for (ordinal, key) in keys.iter().enumerate() {
        harness
            .send(u32::try_from(ordinal).unwrap(), &[*key], 10)
            .unwrap();
    }
    let last = Lsn(100 + u64::try_from(keys.len() - 1).unwrap());
    prop_assert_eq!(harness.settle(keys.len()).await, last);
    prop_assert_eq!(harness.ack.inflight_events(), 0);
    tokio::time::sleep(Duration::from_secs(120)).await;
    Ok(harness)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn o2_o3_a1_lanes_park_and_drain_under_random_failures_and_restarts(
        keys in proptest::collection::vec(0u8..KEYS, 1..EVENTS),
        codes in proptest::collection::vec(0u8..6, EVENTS),
        store_failures in 0u32..4,
        lanes in 1usize..3,
    ) {
        let fates: Vec<Fate> = codes.iter().copied().map(fate).collect();
        let mut dead_at: Vec<Option<usize>> = vec![None; usize::from(KEYS)];
        for (ordinal, key) in keys.iter().enumerate() {
            let slot = &mut dead_at[usize::from(*key)];
            if slot.is_none() && fates[ordinal] == Fate::Terminal {
                *slot = Some(ordinal);
            }
        }
        let blocked = |ordinal: usize| {
            dead_at[usize::from(keys[ordinal])].is_some_and(|dead| ordinal >= dead)
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let outage_until = Instant::now() + Duration::from_secs(5);
            let store = TestStore::default();
            store.fail_appends.store(store_failures, Ordering::SeqCst);
            let first = run_phase(store, &keys, &fates, outage_until, lanes).await?;
            let sends = first.sink.sends();
            check_order(&sends, &keys)?;
            let mut delivered: Vec<u32> = sends.iter().filter(|sent| sent.delivered).map(|sent| sent.ordinal).collect();
            delivered.sort_unstable();
            let expected: Vec<u32> = (0..keys.len())
                .filter(|ordinal| !blocked(*ordinal))
                .map(|ordinal| u32::try_from(ordinal).unwrap())
                .collect();
            prop_assert_eq!(&delivered, &expected, "each unblocked event is delivered exactly once");
            for key in 0..KEYS {
                let queue = first.store.queue(&[key]).await;
                let expected: Vec<(u32, ParkStatus)> = (0..keys.len())
                    .filter(|ordinal| keys[*ordinal] == key && blocked(*ordinal))
                    .map(|ordinal| {
                        let status = if Some(ordinal) == dead_at[usize::from(key)] {
                            ParkStatus::Dead
                        } else {
                            ParkStatus::Retrying
                        };
                        (u32::try_from(ordinal).unwrap(), status)
                    })
                    .collect();
                prop_assert_eq!(queue, expected, "key {}", key);
            }

            let restarted = TestStore {
                inner: first.store.inner.clone(),
                ..TestStore::default()
            };
            drop(first);
            let second = run_phase(restarted, &keys, &fates, outage_until, lanes).await?;
            let sends = second.sink.sends();
            check_order(&sends, &keys)?;
            for sent in &sends {
                prop_assert!(!blocked(usize::try_from(sent.ordinal).unwrap()), "blocked event {} was sent after the restart", sent.ordinal);
            }
            let mut delivered: Vec<u32> = sends.iter().filter(|sent| sent.delivered).map(|sent| sent.ordinal).collect();
            delivered.sort_unstable();
            let replayed: Vec<u32> = (0..keys.len())
                .filter(|ordinal| dead_at[usize::from(keys[*ordinal])].is_none())
                .map(|ordinal| u32::try_from(ordinal).unwrap())
                .collect();
            prop_assert_eq!(
                &delivered,
                &replayed,
                "only keys without a parked queue replay to the sink"
            );
            Ok(())
        })?;
    }
}
