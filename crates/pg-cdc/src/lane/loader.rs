use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tokio::task::{Id, JoinSet};

use super::{Inbound, RETRY_BASE, RETRY_CAP};
use crate::state::{ParkedHead, StateStore};
use crate::supervisor::Backoff;

pub(super) async fn load(
    store: Arc<dyn StateStore>,
    inboxes: Vec<mpsc::UnboundedSender<Inbound>>,
    concurrency: usize,
    parked: Arc<AtomicU64>,
) {
    let mut partitions = 0..=u8::MAX;
    let mut running = JoinSet::new();
    let mut started: HashMap<Id, u8> = HashMap::new();
    loop {
        while running.len() < concurrency.max(1) {
            let Some(partition) = partitions.next() else {
                break;
            };
            let handle = running.spawn(load_partition(Arc::clone(&store), partition));
            started.insert(handle.id(), partition);
        }
        let Some(joined) = running.join_next_with_id().await else {
            return;
        };
        match joined {
            Ok((id, (partition, heads))) => {
                started.remove(&id);
                let length: u64 = heads.iter().map(|head| head.length).sum();
                parked.fetch_add(length, Ordering::SeqCst);
                deliver(&inboxes, partition, heads);
            }
            Err(error) => {
                if let Some(partition) = started.remove(&error.id()) {
                    let handle = running.spawn(load_partition(Arc::clone(&store), partition));
                    started.insert(handle.id(), partition);
                }
            }
        }
    }
}

async fn load_partition(store: Arc<dyn StateStore>, partition: u8) -> (u8, Vec<ParkedHead>) {
    let mut backoff = Backoff::new(RETRY_BASE, RETRY_CAP);
    loop {
        match store.load_parked_heads(partition).await {
            Ok(heads) => return (partition, heads),
            Err(error) => {
                let delay = backoff.next_delay();
                tracing::warn!(partition, %error, ?delay, "cannot load parked queue heads; retrying");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn deliver(inboxes: &[mpsc::UnboundedSender<Inbound>], partition: u8, heads: Vec<ParkedHead>) {
    for head in heads {
        if let Some(inbox) = inboxes.get(head.order_key.lane(inboxes.len())) {
            let _ = inbox.send(Inbound::Head(head));
        }
    }
    for inbox in inboxes {
        let _ = inbox.send(Inbound::Loaded(partition));
    }
}
