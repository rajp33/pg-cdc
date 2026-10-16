use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use serde::Serialize;

use crate::workload::Change;

const DISCARD_FIELD: &str = "loadgen_discard";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    commit_lsn: u64,
    ordinal: u32,
}

impl EventId {
    pub fn new(commit_lsn: u64, ordinal: u32) -> Self {
        Self {
            commit_lsn,
            ordinal,
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        let (lsn, ordinal) = text.split_once('-')?;
        let hex = |digits: &str, width: usize| {
            digits.len() == width && digits.bytes().all(|byte| byte.is_ascii_hexdigit())
        };
        if !hex(lsn, 16) || !hex(ordinal, 8) {
            return None;
        }
        Some(Self::new(
            u64::from_str_radix(lsn, 16).ok()?,
            u32::from_str_radix(ordinal, 16).ok()?,
        ))
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}-{:08x}", self.commit_lsn, self.ordinal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    pub seq: i64,
    pub key: i32,
    pub id: EventId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    Delivered(Delivery),
    DiscardMarker { token: u64, key: i32, seq: i64 },
}

#[derive(Debug, Clone, Default)]
pub struct Evidence {
    pub committed: Vec<Change>,
    pub uncertain: Vec<Vec<Change>>,
    pub observed: Vec<Observed>,
    pub stored: Vec<i64>,
    pub discards: BTreeSet<u64>,
    pub retained_from: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderViolation {
    pub key: i32,
    pub seq: i64,
    pub missing_before: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdViolation {
    pub seq: i64,
    pub id: String,
    pub against_seq: i64,
    pub against_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verdict {
    pub committed: usize,
    pub delivered: usize,
    pub duplicates: usize,
    pub stored: usize,
    pub discarded: usize,
    pub retained: usize,
    pub missing: Vec<i64>,
    pub phantoms: Vec<i64>,
    pub out_of_order: Vec<OrderViolation>,
    pub id_violations: Vec<IdViolation>,
}

impl Verdict {
    pub fn passed(&self) -> bool {
        self.missing.is_empty()
            && self.phantoms.is_empty()
            && self.out_of_order.is_empty()
            && self.id_violations.is_empty()
    }
}

pub fn check(evidence: &Evidence) -> Verdict {
    let written = Written::new(evidence);
    let expected = expected(evidence, &written);
    let committed: HashSet<i64> = expected.iter().map(|change| change.seq).collect();

    let mut deliveries: HashMap<i64, usize> = HashMap::new();
    let mut misrouted = BTreeSet::new();
    let mut discarded_seqs = HashSet::new();
    let mut discarded = 0;
    for observed in &evidence.observed {
        match *observed {
            Observed::Delivered(delivery) if written.matches(delivery.seq, delivery.key) => {
                *deliveries.entry(delivery.seq).or_default() += 1;
            }
            Observed::Delivered(delivery) => {
                misrouted.insert(delivery.seq);
            }
            Observed::DiscardMarker { token, key, seq } => {
                if written.counts_marker(token, key, seq) {
                    discarded += 1;
                    discarded_seqs.insert(seq);
                }
            }
        }
    }

    let stored: HashSet<i64> = evidence.stored.iter().copied().collect();
    let (retained, missing): (BTreeSet<i64>, BTreeSet<i64>) = committed
        .iter()
        .copied()
        .filter(|seq| {
            !deliveries.contains_key(seq) && !stored.contains(seq) && !discarded_seqs.contains(seq)
        })
        .partition(|seq| evidence.retained_from.is_some_and(|from| *seq >= from));

    Verdict {
        committed: evidence.committed.len(),
        delivered: deliveries.len(),
        duplicates: deliveries.values().map(|count| count - 1).sum(),
        stored: stored.len(),
        discarded,
        retained: retained.len(),
        missing: missing.into_iter().collect(),
        phantoms: misrouted.into_iter().collect(),
        out_of_order: order_violations(evidence, &expected, &written),
        id_violations: id_violations(evidence, &written),
    }
}

fn id_violations(evidence: &Evidence, written: &Written<'_>) -> Vec<IdViolation> {
    let violation = |seq: i64, id: EventId, against_seq: i64, against_id: EventId| IdViolation {
        seq,
        id: id.to_string(),
        against_seq,
        against_id: against_id.to_string(),
    };
    let mut first: BTreeMap<i64, EventId> = BTreeMap::new();
    let mut violations = Vec::new();
    for observed in &evidence.observed {
        let Observed::Delivered(delivery) = *observed else {
            continue;
        };
        if !written.matches(delivery.seq, delivery.key) {
            continue;
        }
        let known = *first.entry(delivery.seq).or_insert(delivery.id);
        if known != delivery.id {
            violations.push(violation(delivery.seq, delivery.id, delivery.seq, known));
        }
    }
    let mut previous: Option<(i64, EventId)> = None;
    for (&seq, &id) in &first {
        if let Some((before, before_id)) = previous
            && id <= before_id
        {
            violations.push(violation(seq, id, before, before_id));
        }
        previous = Some((seq, id));
    }
    violations
}

fn expected(evidence: &Evidence, written: &Written<'_>) -> Vec<Change> {
    let seen: HashSet<i64> = evidence
        .observed
        .iter()
        .filter_map(|observed| match *observed {
            Observed::Delivered(delivery) => written
                .matches(delivery.seq, delivery.key)
                .then_some(delivery.seq),
            Observed::DiscardMarker { token, key, seq } => {
                written.counts_marker(token, key, seq).then_some(seq)
            }
        })
        .chain(evidence.stored.iter().copied())
        .collect();
    let mut expected = evidence.committed.clone();
    for transaction in &evidence.uncertain {
        if transaction.iter().any(|change| seen.contains(&change.seq)) {
            expected.extend(transaction);
        }
    }
    expected
}

struct Written<'a> {
    keys: HashMap<i64, i32>,
    discards: &'a BTreeSet<u64>,
}

impl<'a> Written<'a> {
    fn new(evidence: &'a Evidence) -> Self {
        Self {
            keys: evidence
                .committed
                .iter()
                .chain(evidence.uncertain.iter().flatten())
                .map(|change| (change.seq, change.key))
                .collect(),
            discards: &evidence.discards,
        }
    }

    fn matches(&self, seq: i64, key: i32) -> bool {
        self.keys.get(&seq) == Some(&key)
    }

    fn counts_marker(&self, token: u64, key: i32, seq: i64) -> bool {
        self.discards.contains(&token) && self.matches(seq, key)
    }
}

#[derive(Debug, Default)]
struct KeyProgress {
    committed: Vec<i64>,
    cleared: HashSet<i64>,
    frontier: usize,
}

impl KeyProgress {
    fn first_uncleared(&mut self) -> Option<i64> {
        while let Some(seq) = self.committed.get(self.frontier) {
            if !self.cleared.contains(seq) {
                return Some(*seq);
            }
            self.frontier += 1;
        }
        None
    }
}

fn order_violations(
    evidence: &Evidence,
    expected: &[Change],
    written: &Written<'_>,
) -> Vec<OrderViolation> {
    let mut keys: HashMap<i32, KeyProgress> = HashMap::new();
    for change in expected {
        keys.entry(change.key)
            .or_default()
            .committed
            .push(change.seq);
    }
    for progress in keys.values_mut() {
        progress.committed.sort_unstable();
        progress.committed.dedup();
    }

    let mut delivered = HashSet::new();
    let mut violations = Vec::new();
    for observed in &evidence.observed {
        match *observed {
            Observed::Delivered(delivery) if written.matches(delivery.seq, delivery.key) => {
                let progress = keys.entry(delivery.key).or_default();
                if delivered.insert(delivery.seq)
                    && let Some(earlier) = progress
                        .first_uncleared()
                        .filter(|earlier| *earlier < delivery.seq)
                {
                    violations.push(OrderViolation {
                        key: delivery.key,
                        seq: delivery.seq,
                        missing_before: earlier,
                    });
                }
                progress.cleared.insert(delivery.seq);
            }
            Observed::Delivered(_) => {}
            Observed::DiscardMarker { token, key, seq } => {
                if written.counts_marker(token, key, seq) {
                    keys.entry(key).or_default().cleared.insert(seq);
                }
            }
        }
    }
    violations
}

pub fn change_from_envelope(envelope: &serde_json::Value) -> Option<Change> {
    let key = envelope.get("key")?.get("id")?.as_i64()?;
    let seq = row_seq(envelope, "after").or_else(|| row_seq(envelope, "before"))?;
    Some(Change {
        seq,
        key: i32::try_from(key).ok()?,
    })
}

pub fn delivery_from_envelope(envelope: &serde_json::Value) -> Option<Delivery> {
    let change = change_from_envelope(envelope)?;
    Some(Delivery {
        seq: change.seq,
        key: change.key,
        id: EventId::parse(envelope.get("id")?.as_str()?)?,
    })
}

fn row_seq(envelope: &serde_json::Value, side: &str) -> Option<i64> {
    envelope.get(side)?.get("seq")?.as_i64()
}

pub fn discard_marker(token: u64, key: i32, seq: i64) -> serde_json::Value {
    serde_json::json!({ DISCARD_FIELD: { "token": token, "key": key, "seq": seq } })
}

pub fn observed_from_message(body: &serde_json::Value) -> Option<Observed> {
    if let Some(marker) = body.get(DISCARD_FIELD) {
        return Some(Observed::DiscardMarker {
            token: marker.get("token")?.as_u64()?,
            key: i32::try_from(marker.get("key")?.as_i64()?).ok()?,
            seq: marker.get("seq")?.as_i64()?,
        });
    }
    delivery_from_envelope(body).map(Observed::Delivered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn change(seq: i64, key: i32) -> Change {
        Change { seq, key }
    }

    fn id(seq: i64) -> EventId {
        EventId::new(u64::try_from(seq).unwrap() * 0x100, 0)
    }

    fn delivered(seq: i64, key: i32) -> Observed {
        with_id(seq, key, id(seq))
    }

    fn with_id(seq: i64, key: i32, id: EventId) -> Observed {
        Observed::Delivered(Delivery { seq, key, id })
    }

    fn id_violation(seq: i64, id: EventId, against_seq: i64, against_id: EventId) -> IdViolation {
        IdViolation {
            seq,
            id: id.to_string(),
            against_seq,
            against_id: against_id.to_string(),
        }
    }

    fn marker(token: u64, seq: i64, key: i32) -> Observed {
        Observed::DiscardMarker { token, key, seq }
    }

    fn violation(key: i32, seq: i64, missing_before: i64) -> OrderViolation {
        OrderViolation {
            key,
            seq,
            missing_before,
        }
    }

    fn evidence(committed: &[(i64, i32)], observed: Vec<Observed>) -> Evidence {
        Evidence {
            committed: committed
                .iter()
                .map(|&(seq, key)| change(seq, key))
                .collect(),
            observed,
            ..Evidence::default()
        }
    }

    #[test]
    fn complete_ordered_evidence_passes() {
        let verdict = check(&evidence(
            &[(1, 1), (2, 2), (3, 1), (4, 2)],
            vec![
                delivered(1, 1),
                delivered(2, 2),
                delivered(3, 1),
                delivered(4, 2),
            ],
        ));
        assert!(verdict.passed(), "{verdict:?}");
        assert_eq!(verdict.committed, 4);
        assert_eq!(verdict.delivered, 4);
        assert_eq!(verdict.duplicates, 0);
    }

    #[test]
    fn a_lost_change_is_missing_unless_it_is_stored_or_discarded() {
        let committed = [(1, 1), (2, 2), (3, 1)];
        let observed = vec![delivered(1, 1), delivered(2, 2)];

        let lost = check(&evidence(&committed, observed.clone()));
        assert_eq!(lost.missing, vec![3]);
        assert!(!lost.passed());

        let parked = check(&Evidence {
            stored: vec![3],
            ..evidence(&committed, observed.clone())
        });
        assert!(parked.passed(), "{parked:?}");
        assert_eq!(parked.stored, 1);

        let mut with_marker = observed;
        with_marker.insert(0, marker(9, 3, 1));
        let discarded = check(&Evidence {
            discards: BTreeSet::from([9]),
            ..evidence(&committed, with_marker)
        });
        assert!(discarded.passed(), "{discarded:?}");
        assert_eq!(discarded.discarded, 1);
    }

    #[test]
    fn a_change_the_slot_still_retains_is_not_missing() {
        let committed = [(1, 1), (2, 2), (3, 1), (4, 2)];
        let observed = vec![delivered(1, 1)];
        let retained = check(&Evidence {
            stored: vec![2],
            retained_from: Some(3),
            ..evidence(&committed, observed.clone())
        });
        assert!(retained.passed(), "{retained:?}");
        assert_eq!((retained.stored, retained.retained), (1, 2));

        let confirmed = check(&Evidence {
            retained_from: Some(4),
            ..evidence(&committed, observed)
        });
        assert_eq!(confirmed.missing, vec![2, 3]);
        assert_eq!(confirmed.retained, 1);

        let overtaken = check(&Evidence {
            retained_from: Some(3),
            ..evidence(
                &committed,
                vec![
                    delivered(1, 1),
                    delivered(2, 2),
                    delivered(4, 2),
                    delivered(3, 1),
                ],
            )
        });
        assert!(overtaken.passed(), "{overtaken:?}");
        assert_eq!(overtaken.retained, 0);
        let skipped = check(&Evidence {
            retained_from: Some(3),
            ..evidence(
                &[(1, 1), (3, 1), (5, 1)],
                vec![delivered(1, 1), delivered(5, 1)],
            )
        });
        assert_eq!(skipped.out_of_order.len(), 1, "{skipped:?}");
    }

    #[test]
    fn a_later_change_delivered_first_breaks_order() {
        let committed = [(1, 1), (3, 1)];
        let reordered = check(&evidence(
            &committed,
            vec![delivered(3, 1), delivered(1, 1)],
        ));
        assert_eq!(reordered.out_of_order, vec![violation(1, 3, 1)]);
        assert_eq!(reordered.missing, Vec::<i64>::new());
        assert!(!reordered.passed());

        let discarded_first = check(&Evidence {
            discards: BTreeSet::from([4]),
            ..evidence(&committed, vec![marker(4, 1, 1), delivered(3, 1)])
        });
        assert!(discarded_first.passed(), "{discarded_first:?}");
    }

    #[test]
    fn a_delivery_before_its_predecessors_discard_breaks_order() {
        let committed = [(1, 1), (2, 1)];
        let discards = BTreeSet::from([5]);

        let late = check(&Evidence {
            discards: discards.clone(),
            ..evidence(&committed, vec![delivered(2, 1), marker(5, 1, 1)])
        });
        assert_eq!(late.out_of_order, vec![violation(1, 2, 1)]);
        assert_eq!(late.missing, Vec::<i64>::new());

        let early = check(&Evidence {
            discards,
            ..evidence(&committed, vec![marker(5, 1, 1), delivered(2, 1)])
        });
        assert!(early.passed(), "{early:?}");

        let failed_discard = check(&evidence(
            &committed,
            vec![marker(5, 1, 1), delivered(2, 1)],
        ));
        assert_eq!(failed_discard.out_of_order, vec![violation(1, 2, 1)]);
        assert_eq!(failed_discard.missing, vec![1]);
        assert_eq!(failed_discard.discarded, 0);
    }

    #[test]
    fn replays_after_a_takeover_are_duplicates_not_violations() {
        let verdict = check(&evidence(
            &[(1, 1), (2, 1), (3, 1)],
            vec![
                delivered(1, 1),
                delivered(2, 1),
                delivered(1, 1),
                delivered(2, 1),
                delivered(3, 1),
            ],
        ));
        assert!(verdict.passed(), "{verdict:?}");
        assert_eq!(verdict.delivered, 3);
        assert_eq!(verdict.duplicates, 2);
    }

    #[test]
    fn uncertain_changes_may_appear_and_unknown_ones_may_not() {
        let base = Evidence {
            uncertain: vec![vec![change(2, 1)]],
            ..evidence(&[(1, 1)], vec![delivered(1, 1)])
        };
        assert!(check(&base).passed());

        let appeared = check(&Evidence {
            observed: vec![delivered(1, 1), delivered(2, 1)],
            ..base.clone()
        });
        assert!(appeared.passed(), "{appeared:?}");

        let phantom = check(&Evidence {
            observed: vec![delivered(1, 1), delivered(7, 1)],
            ..base
        });
        assert_eq!(phantom.phantoms, vec![7]);
        assert!(!phantom.passed());
    }

    #[test]
    fn an_uncertain_transaction_seen_committed_is_held_to_order_and_presence() {
        let within = check(&Evidence {
            uncertain: vec![vec![change(1, 1), change(2, 1)]],
            ..evidence(&[], vec![delivered(2, 1), delivered(1, 1)])
        });
        assert_eq!(within.out_of_order, vec![violation(1, 2, 1)]);

        let across = check(&Evidence {
            uncertain: vec![vec![change(1, 1)]],
            ..evidence(&[(2, 1)], vec![delivered(2, 1), delivered(1, 1)])
        });
        assert_eq!(across.out_of_order, vec![violation(1, 2, 1)]);

        let partly_seen = check(&Evidence {
            uncertain: vec![vec![change(1, 1), change(2, 2)]],
            ..evidence(&[(3, 1)], vec![delivered(2, 2), delivered(3, 1)])
        });
        assert_eq!(partly_seen.missing, vec![1]);
        assert_eq!(partly_seen.out_of_order, vec![violation(1, 3, 1)]);

        let parked = check(&Evidence {
            uncertain: vec![vec![change(1, 1)]],
            stored: vec![1],
            ..evidence(&[(2, 1)], vec![delivered(2, 1)])
        });
        assert_eq!(parked.out_of_order, vec![violation(1, 2, 1)]);

        let absent = check(&Evidence {
            uncertain: vec![vec![change(1, 1), change(2, 2)]],
            ..evidence(&[(3, 1)], vec![delivered(3, 1)])
        });
        assert!(absent.passed(), "{absent:?}");

        let misrouted = check(&Evidence {
            uncertain: vec![vec![change(1, 1)]],
            ..evidence(&[(2, 1)], vec![delivered(1, 5), delivered(2, 1)])
        });
        assert_eq!(misrouted.phantoms, vec![1]);
        assert_eq!(misrouted.out_of_order, Vec::<OrderViolation>::new());
    }

    #[test]
    fn a_delivery_under_the_wrong_key_counts_for_nothing() {
        let committed = [(1, 1), (2, 1)];
        let swapped = check(&evidence(
            &committed,
            vec![delivered(2, 2), delivered(1, 2)],
        ));
        assert_eq!(swapped.phantoms, vec![1, 2]);
        assert_eq!(swapped.missing, vec![1, 2]);
        assert_eq!(swapped.delivered, 0);
        assert!(!swapped.passed());

        let one_misrouted = check(&evidence(
            &committed,
            vec![delivered(2, 2), delivered(1, 1)],
        ));
        assert_eq!(one_misrouted.phantoms, vec![2]);
        assert_eq!(one_misrouted.missing, vec![2]);
        assert!(!one_misrouted.passed());

        let uncertain_elsewhere = check(&Evidence {
            uncertain: vec![vec![change(3, 1)]],
            ..evidence(
                &committed,
                vec![delivered(1, 1), delivered(2, 1), delivered(3, 4)],
            )
        });
        assert_eq!(uncertain_elsewhere.phantoms, vec![3]);

        let misplaced_marker = check(&Evidence {
            discards: BTreeSet::from([8]),
            ..evidence(&committed, vec![marker(8, 1, 3), delivered(2, 1)])
        });
        assert_eq!(misplaced_marker.missing, vec![1]);
        assert_eq!(misplaced_marker.out_of_order, vec![violation(1, 2, 1)]);
        assert_eq!(misplaced_marker.discarded, 0);
    }

    #[test]
    fn event_ids_stay_the_same_across_replays_and_increase_with_seq() {
        let committed = [(1, 1), (2, 2), (3, 1)];
        let replayed = check(&evidence(
            &committed,
            vec![
                delivered(1, 1),
                delivered(2, 2),
                delivered(1, 1),
                delivered(3, 1),
                delivered(2, 2),
            ],
        ));
        assert!(replayed.passed(), "{replayed:?}");

        let renamed = check(&evidence(
            &committed,
            vec![
                delivered(1, 1),
                delivered(2, 2),
                with_id(1, 1, EventId::new(0x100, 1)),
                delivered(3, 1),
            ],
        ));
        assert_eq!(
            renamed.id_violations,
            vec![id_violation(1, EventId::new(0x100, 1), 1, id(1))]
        );
        assert!(!renamed.passed());

        let shared = check(&evidence(
            &committed,
            vec![delivered(1, 1), with_id(2, 2, id(1)), delivered(3, 1)],
        ));
        assert_eq!(shared.id_violations, vec![id_violation(2, id(1), 1, id(1))]);

        let decreasing = check(&evidence(
            &committed,
            vec![with_id(2, 2, id(1)), with_id(1, 1, id(2)), delivered(3, 1)],
        ));
        assert_eq!(
            decreasing.id_violations,
            vec![id_violation(2, id(1), 1, id(2))]
        );

        let phantom = check(&evidence(
            &committed,
            vec![
                delivered(1, 1),
                delivered(2, 2),
                delivered(3, 1),
                with_id(9, 1, id(1)),
            ],
        ));
        assert_eq!(phantom.id_violations, Vec::<IdViolation>::new());
    }

    #[test]
    fn event_ids_print_and_parse_as_pg_cdc_writes_them() {
        let parsed = EventId::parse("000000001a2b3c4d-00000003").unwrap();
        assert_eq!(parsed, EventId::new(0x1a2b_3c4d, 3));
        assert_eq!(parsed.to_string(), "000000001a2b3c4d-00000003");
        assert!(EventId::new(1, 9) < EventId::new(2, 0));
        assert!(EventId::new(2, 0) < EventId::new(2, 1));
        for bad in [
            "",
            "e1",
            "000000001a2b3c4d",
            "000000001a2b3c4d-0000003",
            "00000001a2b3c4d-00000003",
            "000000001a2b3c4g-00000003",
            "+00000001a2b3c4d-00000003",
        ] {
            assert_eq!(EventId::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn deliveries_are_read_from_after_or_before() {
        let upsert = json!({"id": "0000000000000100-00000002", "op": "update", "key": {"id": 4}, "before": null, "after": {"id": 4, "seq": 11}});
        assert_eq!(
            delivery_from_envelope(&upsert),
            Some(Delivery {
                seq: 11,
                key: 4,
                id: EventId::new(0x100, 2)
            })
        );

        let removal = json!({"id": "0000000000000100-00000003", "op": "delete", "key": {"id": 4}, "before": {"id": 4, "seq": 9}, "after": null});
        assert_eq!(
            delivery_from_envelope(&removal),
            Some(Delivery {
                seq: 9,
                key: 4,
                id: EventId::new(0x100, 3)
            })
        );

        let pointer = json!({"id": "0000000000000100-00000002", "op": "insert", "key": {"id": 4}, "payload_ref": "s3://bucket/key"});
        assert_eq!(delivery_from_envelope(&pointer), None);

        let keyless = json!({"id": "0000000000000100-00000002", "after": {"seq": 1}});
        assert_eq!(delivery_from_envelope(&keyless), None);

        for id in [json!(null), json!("e1"), json!(7)] {
            let unnamed = json!({"id": id, "key": {"id": 4}, "after": {"seq": 11}});
            assert_eq!(delivery_from_envelope(&unnamed), None, "{id}");
            assert_eq!(
                change_from_envelope(&unnamed),
                Some(Change { seq: 11, key: 4 })
            );
        }

        assert_eq!(
            observed_from_message(&discard_marker(7, 1, 3)),
            Some(Observed::DiscardMarker {
                token: 7,
                key: 1,
                seq: 3
            })
        );
        assert_eq!(
            observed_from_message(&upsert),
            Some(with_id(11, 4, EventId::new(0x100, 2)))
        );
        assert_eq!(observed_from_message(&json!({"unrelated": true})), None);
    }
}
