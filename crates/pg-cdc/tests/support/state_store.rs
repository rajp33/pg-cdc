use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use pg_cdc::event::EventId;
use pg_cdc::order::OrderKeyId;
use pg_cdc::sink::AttributeValue;
use pg_cdc::state::{
    LeaderRecord, LeaderState, ParkStatus, ParkedEvent, ParkedHead, StateStore, StoreError,
    SwapOutcome, Version, Versioned,
};
use pg_cdc_decoder::Lsn;

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) async fn conforms<F, Fut>(open: F) -> TestResult
where
    F: Fn() -> Fut,
    Fut: Future<Output = TestResult<Arc<dyn StateStore>>>,
{
    let store = open().await?;
    empty_calls_succeed(store.as_ref()).await?;
    queues_list_in_key_order_from_a_cursor_with_capped_lengths(store.as_ref()).await?;
    reads_return_ascending_ids_and_respect_after_and_limit(store.as_ref()).await?;
    appends_are_create_only(store.as_ref()).await?;
    an_append_with_a_malformed_event_is_all_or_nothing(store.as_ref()).await?;
    heads_are_the_lowest_id_per_queue_in_their_partition(store.as_ref()).await?;
    writes_are_durable_across_instances(&open).await?;
    status_changes_rewrite_one_event(store.as_ref(), &open).await?;
    removal_is_idempotent_and_reads_reflect_it(store.as_ref()).await?;
    the_leader_record_is_a_compare_and_swap(store.as_ref(), &open).await?;
    concurrent_calls_are_safe(&store).await?;
    Ok(())
}

pub(crate) fn key(partition: u8, n: u8) -> TestResult<OrderKeyId> {
    Ok(format!("{partition:02x}{}{n:02x}", "0".repeat(28)).parse()?)
}

pub(crate) fn id(lsn: u64, ordinal: u32) -> EventId {
    EventId::new(Lsn(lsn), ordinal)
}

pub(crate) fn event(key: OrderKeyId, id: EventId, status: ParkStatus) -> ParkedEvent {
    ParkedEvent {
        order_key: key,
        id,
        status,
        attempts: 2,
        first_failure_ms: 1_760_443_200_123,
        last_error: "Throttling: rate exceeded \u{2014} \"slow down\"".to_owned(),
        attributes: vec![
            (
                "schema".to_owned(),
                AttributeValue::String("public".to_owned()),
            ),
            (
                "table".to_owned(),
                AttributeValue::String("orders".to_owned()),
            ),
            ("op".to_owned(), AttributeValue::String("insert".to_owned())),
            (
                "amount".to_owned(),
                AttributeValue::Number("42.50".to_owned()),
            ),
        ],
        body: Bytes::from(format!(
            r#"{{"id":"{id}","after":{{"note":"café 😀 \"x\"","n":12345678901234567890.10}}}}"#
        )),
    }
}

fn leader(epoch: u64, holder: &str, state: LeaderState) -> LeaderRecord {
    LeaderRecord {
        epoch,
        holder: holder.to_owned(),
        state,
        confirmed_lsn: Lsn(0x1_0000_0000 + epoch),
        updated_ms: 1_760_443_200_000 + epoch,
    }
}

async fn empty_calls_succeed(store: &dyn StateStore) -> TestResult {
    store.append_parked(&[]).await?;
    store.remove_parked(&[]).await?;
    assert_eq!(store.read_parked(&key(0x10, 1)?, None, 10).await?, vec![]);
    assert_eq!(store.load_parked_heads(0x10).await?, vec![]);
    Ok(())
}

async fn queues_list_in_key_order_from_a_cursor_with_capped_lengths(
    store: &dyn StateStore,
) -> TestResult {
    assert_eq!(store.list_parked_queues(None, 10, 10).await?, vec![]);
    let (k1, k2, k3, k4) = (key(0x10, 1)?, key(0x10, 2)?, key(0x11, 1)?, key(0x12, 1)?);
    let mut events = vec![
        event(k1, id(4, 0), ParkStatus::Retrying),
        event(k1, id(2, 1), ParkStatus::Dead),
        event(k1, id(3, 0), ParkStatus::Retrying),
        event(k3, id(7, 0), ParkStatus::Retrying),
        event(k4, id(5, 0), ParkStatus::Retrying),
        event(k4, id(6, 0), ParkStatus::Retrying),
    ];
    events.extend((0..25).map(|n| event(k2, id(1, n), ParkStatus::Retrying)));
    store.append_parked(&events).await?;
    let head = |order_key, head, length| ParkedHead {
        order_key,
        head,
        length,
    };
    assert_eq!(
        store.list_parked_queues(None, 2, 10).await?,
        vec![head(k1, id(2, 1), 3), head(k2, id(1, 0), 10)]
    );
    assert_eq!(
        store.list_parked_queues(Some(&k2), 10, 10).await?,
        vec![head(k3, id(7, 0), 1), head(k4, id(5, 0), 2)]
    );
    assert_eq!(
        store.list_parked_queues(Some(&k1), 1, 100).await?,
        vec![head(k2, id(1, 0), 25)]
    );
    assert_eq!(store.list_parked_queues(Some(&k4), 10, 10).await?, vec![]);
    assert_eq!(store.list_parked_queues(None, 0, 10).await?, vec![]);
    store.remove_parked(&[(k3, id(7, 0))]).await?;
    let listed: Vec<OrderKeyId> = store
        .list_parked_queues(None, 10, 10)
        .await?
        .iter()
        .map(|queue| queue.order_key)
        .collect();
    assert_eq!(listed, vec![k1, k2, k4]);
    let all: Vec<_> = events
        .iter()
        .map(|event| (event.order_key, event.id))
        .collect();
    store.remove_parked(&all).await?;
    assert_eq!(store.list_parked_queues(None, 10, 10).await?, vec![]);
    Ok(())
}

async fn reads_return_ascending_ids_and_respect_after_and_limit(
    store: &dyn StateStore,
) -> TestResult {
    let queue = key(0x20, 1)?;
    let events = [
        event(queue, id(5, 1), ParkStatus::Retrying),
        event(queue, id(3, 0), ParkStatus::Dead),
        event(queue, id(5, 0), ParkStatus::Retrying),
    ];
    store.append_parked(&events).await?;
    let sorted = [events[1].clone(), events[2].clone(), events[0].clone()];
    assert_eq!(store.read_parked(&queue, None, 10).await?, sorted);
    assert_eq!(store.read_parked(&queue, None, 2).await?, sorted[..2]);
    assert_eq!(
        store.read_parked(&queue, Some(&id(3, 0)), 10).await?,
        sorted[1..]
    );
    assert_eq!(
        store.read_parked(&queue, Some(&id(4, 0)), 1).await?,
        sorted[1..2]
    );
    assert_eq!(
        store.read_parked(&queue, Some(&id(5, 1)), 10).await?,
        vec![]
    );
    assert_eq!(store.read_parked(&key(0x20, 2)?, None, 10).await?, vec![]);
    Ok(())
}

async fn appends_are_create_only(store: &dyn StateStore) -> TestResult {
    let queue = key(0x30, 1)?;
    let original = event(queue, id(7, 0), ParkStatus::Retrying);
    store.append_parked(std::slice::from_ref(&original)).await?;
    let mut changed = event(queue, id(7, 0), ParkStatus::Dead);
    changed.attempts = 99;
    "changed".clone_into(&mut changed.last_error);
    let added = event(queue, id(8, 0), ParkStatus::Retrying);
    store.append_parked(&[changed, added.clone()]).await?;
    assert_eq!(
        store.read_parked(&queue, None, 10).await?,
        vec![original, added]
    );
    Ok(())
}

async fn an_append_with_a_malformed_event_is_all_or_nothing(store: &dyn StateStore) -> TestResult {
    let queue = key(0x31, 1)?;
    let mut events: Vec<ParkedEvent> = (0..64)
        .map(|n| event(queue, id(4, n), ParkStatus::Retrying))
        .collect();
    events.push(ParkedEvent {
        body: Bytes::from_static(b"not json"),
        ..event(queue, id(5, 0), ParkStatus::Retrying)
    });
    let stored = match store.append_parked(&events).await {
        Ok(()) => events.len(),
        Err(StoreError::Malformed { .. }) => 0,
        Err(error) => return Err(error.into()),
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(store.read_parked(&queue, None, 100).await?.len(), stored);
    store
        .remove_parked(
            &events
                .iter()
                .map(|event| (queue, event.id))
                .collect::<Vec<_>>(),
        )
        .await?;
    Ok(())
}

async fn heads_are_the_lowest_id_per_queue_in_their_partition(
    store: &dyn StateStore,
) -> TestResult {
    let first = key(0x40, 1)?;
    let second = key(0x40, 2)?;
    let elsewhere = key(0x41, 1)?;
    store
        .append_parked(&[
            event(second, id(9, 2), ParkStatus::Retrying),
            event(first, id(6, 0), ParkStatus::Retrying),
            event(first, id(2, 5), ParkStatus::Dead),
            event(elsewhere, id(1, 0), ParkStatus::Retrying),
            event(second, id(9, 1), ParkStatus::Retrying),
            event(first, id(4, 0), ParkStatus::Retrying),
        ])
        .await?;
    let mut heads = store.load_parked_heads(0x40).await?;
    heads.sort_by_key(|head| head.order_key);
    assert_eq!(
        heads,
        vec![
            ParkedHead {
                order_key: first,
                head: id(2, 5),
                length: 3
            },
            ParkedHead {
                order_key: second,
                head: id(9, 1),
                length: 2
            },
        ]
    );
    assert_eq!(
        store.load_parked_heads(0x41).await?,
        vec![ParkedHead {
            order_key: elsewhere,
            head: id(1, 0),
            length: 1
        }]
    );
    assert_eq!(store.load_parked_heads(0x42).await?, vec![]);
    assert_eq!(store.load_parked_heads(0x04).await?, vec![]);
    Ok(())
}

async fn writes_are_durable_across_instances<F, Fut>(open: &F) -> TestResult
where
    F: Fn() -> Fut,
    Fut: Future<Output = TestResult<Arc<dyn StateStore>>>,
{
    let reopened = open().await?;
    let queue = key(0x20, 1)?;
    let ids: Vec<EventId> = reopened
        .read_parked(&queue, None, 10)
        .await?
        .iter()
        .map(|event| event.id)
        .collect();
    assert_eq!(ids, vec![id(3, 0), id(5, 0), id(5, 1)]);
    let written = event(key(0x50, 1)?, id(1, 1), ParkStatus::Retrying);
    reopened
        .append_parked(std::slice::from_ref(&written))
        .await?;
    let again = open().await?;
    assert_eq!(
        again.read_parked(&key(0x50, 1)?, None, 10).await?,
        vec![written]
    );
    Ok(())
}

async fn status_changes_rewrite_one_event<F, Fut>(store: &dyn StateStore, open: &F) -> TestResult
where
    F: Fn() -> Fut,
    Fut: Future<Output = TestResult<Arc<dyn StateStore>>>,
{
    let queue = key(0x60, 1)?;
    let head = event(queue, id(1, 0), ParkStatus::Retrying);
    let next = event(queue, id(2, 0), ParkStatus::Retrying);
    store.append_parked(&[head.clone(), next.clone()]).await?;
    store
        .set_parked_status(&queue, &id(1, 0), ParkStatus::Dead)
        .await?;
    let dead = ParkedEvent {
        status: ParkStatus::Dead,
        ..head.clone()
    };
    assert_eq!(
        open().await?.read_parked(&queue, None, 10).await?,
        vec![dead, next.clone()]
    );
    store
        .set_parked_status(&queue, &id(1, 0), ParkStatus::Retrying)
        .await?;
    assert_eq!(store.read_parked(&queue, None, 10).await?, vec![head, next]);
    let missing = store
        .set_parked_status(&queue, &id(9, 9), ParkStatus::Dead)
        .await;
    assert!(
        matches!(missing, Err(StoreError::NotFound(_))),
        "{missing:?}"
    );
    let missing = store
        .set_parked_status(&key(0x60, 2)?, &id(1, 0), ParkStatus::Dead)
        .await;
    assert!(
        matches!(missing, Err(StoreError::NotFound(_))),
        "{missing:?}"
    );
    Ok(())
}

async fn removal_is_idempotent_and_reads_reflect_it(store: &dyn StateStore) -> TestResult {
    let queue = key(0x70, 1)?;
    let other = key(0x70, 2)?;
    store
        .append_parked(&[
            event(queue, id(1, 0), ParkStatus::Dead),
            event(queue, id(2, 0), ParkStatus::Retrying),
            event(queue, id(3, 0), ParkStatus::Retrying),
            event(other, id(1, 0), ParkStatus::Retrying),
        ])
        .await?;
    store
        .remove_parked(&[
            (queue, id(1, 0)),
            (queue, id(9, 9)),
            (key(0x70, 3)?, id(1, 0)),
        ])
        .await?;
    store.remove_parked(&[(queue, id(1, 0))]).await?;
    let left: Vec<EventId> = store
        .read_parked(&queue, None, 10)
        .await?
        .iter()
        .map(|event| event.id)
        .collect();
    assert_eq!(left, vec![id(2, 0), id(3, 0)]);
    let mut heads = store.load_parked_heads(0x70).await?;
    heads.sort_by_key(|head| head.order_key);
    assert_eq!(
        heads,
        vec![
            ParkedHead {
                order_key: queue,
                head: id(2, 0),
                length: 2
            },
            ParkedHead {
                order_key: other,
                head: id(1, 0),
                length: 1
            },
        ]
    );
    store
        .remove_parked(&[(queue, id(2, 0)), (queue, id(3, 0))])
        .await?;
    assert_eq!(store.read_parked(&queue, None, 10).await?, vec![]);
    assert_eq!(
        store.load_parked_heads(0x70).await?,
        vec![ParkedHead {
            order_key: other,
            head: id(1, 0),
            length: 1
        }]
    );
    store
        .append_parked(&[event(queue, id(4, 0), ParkStatus::Retrying)])
        .await?;
    let ids: Vec<EventId> = store
        .read_parked(&queue, None, 10)
        .await?
        .iter()
        .map(|event| event.id)
        .collect();
    assert_eq!(ids, vec![id(4, 0)]);
    Ok(())
}

async fn the_leader_record_is_a_compare_and_swap<F, Fut>(
    store: &dyn StateStore,
    open: &F,
) -> TestResult
where
    F: Fn() -> Fut,
    Fut: Future<Output = TestResult<Arc<dyn StateStore>>>,
{
    assert_eq!(store.read_leader().await?, None);
    let stale = Version("not-a-version".to_owned());
    let first = leader(1, "task-a", LeaderState::Active);
    assert_eq!(
        store.swap_leader(&first, Some(&stale)).await?,
        SwapOutcome::Conflict
    );
    let SwapOutcome::Swapped(v1) = store.swap_leader(&first, None).await? else {
        return Err("creating the leader record failed".into());
    };
    assert_eq!(
        store.read_leader().await?,
        Some(Versioned {
            value: first.clone(),
            version: v1.clone()
        })
    );
    let second = leader(2, "task-b", LeaderState::Active);
    assert_eq!(
        store.swap_leader(&second, None).await?,
        SwapOutcome::Conflict
    );
    assert_eq!(
        store.swap_leader(&second, Some(&stale)).await?,
        SwapOutcome::Conflict
    );
    let SwapOutcome::Swapped(v2) = store.swap_leader(&second, Some(&v1)).await? else {
        return Err("swapping with the current version failed".into());
    };
    assert_ne!(v1, v2);
    let third = leader(3, "task-c", LeaderState::Released);
    assert_eq!(
        store.swap_leader(&third, Some(&v1)).await?,
        SwapOutcome::Conflict
    );
    assert_eq!(
        open().await?.read_leader().await?,
        Some(Versioned {
            value: second,
            version: v2
        })
    );
    Ok(())
}

async fn concurrent_calls_are_safe(store: &Arc<dyn StateStore>) -> TestResult {
    let queue = key(0x80, 1)?;
    let mut appends = tokio::task::JoinSet::new();
    for task in 0..16_u32 {
        let store = Arc::clone(store);
        appends.spawn(async move {
            let mut events: Vec<ParkedEvent> = (0..24)
                .map(|n| event(queue, id(100, n), ParkStatus::Retrying))
                .collect();
            events.rotate_left(usize::try_from(task)?);
            store.append_parked(&events).await?;
            TestResult::Ok(())
        });
    }
    while let Some(joined) = appends.join_next().await {
        joined??;
    }
    let ids: Vec<EventId> = store
        .read_parked(&queue, None, 100)
        .await?
        .iter()
        .map(|event| event.id)
        .collect();
    assert_eq!(ids, (0..24).map(|n| id(100, n)).collect::<Vec<_>>());

    let Some(current) = store.read_leader().await? else {
        return Err("the leader record is missing".into());
    };
    let mut swaps = tokio::task::JoinSet::new();
    for task in 0..8_u64 {
        let store = Arc::clone(store);
        let expected = current.version.clone();
        swaps.spawn(async move {
            let next = leader(10 + task, "racer", LeaderState::Active);
            store.swap_leader(&next, Some(&expected)).await
        });
    }
    let mut swapped = 0;
    while let Some(joined) = swaps.join_next().await {
        if matches!(joined??, SwapOutcome::Swapped(_)) {
            swapped += 1;
        }
    }
    assert_eq!(swapped, 1, "exactly one concurrent swap must win");
    Ok(())
}
