use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::thread;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, oneshot};

use super::{
    LEADER_OBJECT, LeaderRecord, PARKED_DIR, ParkStatus, ParkedEvent, ParkedHead, StateStore,
    StoreError, SwapOutcome, Version, Versioned, decode_parked, encode_parked,
};
use crate::event::EventId;
use crate::order::OrderKeyId;

pub const LEADER_LOCK: &str = "leader.lock";
const CREATE_ATTEMPTS: usize = 3;

type Cancelled<'a> = &'a dyn Fn() -> bool;
type LeaderJob = Box<dyn FnOnce(&Path) + Send>;

struct CancelOnDrop {
    flag: Arc<AtomicBool>,
    completed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.completed {
            self.flag.store(true, Ordering::Release);
        }
    }
}

fn check(cancelled: Cancelled<'_>) -> Result<(), StoreError> {
    if cancelled() {
        return Err(StoreError::Unavailable("cancelled".to_owned()));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct LeaderWorker {
    jobs: Mutex<Option<mpsc::Sender<LeaderJob>>>,
}

impl LeaderWorker {
    fn submit(&self, root: &Path, job: LeaderJob) -> Result<(), StoreError> {
        let mut jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        let job = match jobs.as_ref() {
            Some(sender) => match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(mpsc::SendError(job)) => job,
            },
            None => job,
        };
        let (sender, receiver) = mpsc::channel::<LeaderJob>();
        let root = root.to_path_buf();
        thread::Builder::new()
            .name("pg-cdc-leader".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job(&root);
                }
            })
            .map_err(|error| StoreError::Unavailable(format!("leader worker: {error}")))?;
        sender
            .send(job)
            .map_err(|_| StoreError::Unavailable("the leader worker stopped".to_owned()))?;
        *jobs = Some(sender);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemStateStore {
    root: PathBuf,
    permits: Arc<Semaphore>,
    leader: Arc<LeaderWorker>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaderFile {
    version: u64,
    record: LeaderRecord,
}

impl FilesystemStateStore {
    pub fn new(path: &Path, pipeline: &str, max_concurrency: usize) -> Self {
        Self {
            root: path.join(pipeline),
            permits: Arc::new(Semaphore::new(
                max_concurrency.clamp(1, Semaphore::MAX_PERMITS),
            )),
            leader: Arc::default(),
        }
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Path, Cancelled<'_>) -> Result<T, StoreError> + Send + 'static,
    {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable("the store is closed".to_owned()))?;
        let root = self.root.clone();
        let flag = Arc::new(AtomicBool::new(false));
        let mut guard = CancelOnDrop {
            flag: Arc::clone(&flag),
            completed: false,
        };
        let joined =
            tokio::task::spawn_blocking(move || operation(&root, &|| flag.load(Ordering::Acquire)))
                .await;
        guard.completed = true;
        joined.map_err(|error| StoreError::Unavailable(error.to_string()))?
    }

    async fn leader<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<T, StoreError> + Send + 'static,
    {
        let (reply, answer) = oneshot::channel();
        self.leader.submit(
            &self.root,
            Box::new(move |root| {
                let _ = reply.send(operation(root));
            }),
        )?;
        answer
            .await
            .map_err(|_| StoreError::Unavailable("the leader worker stopped".to_owned()))?
    }
}

fn unavailable(path: &Path, error: &io::Error) -> StoreError {
    StoreError::Unavailable(format!("{}: {error}", path.display()))
}

fn sync_dir(dir: &Path) -> Result<(), StoreError> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|error| unavailable(dir, &error))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| unavailable(path, &error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| unavailable(path, &error))
}

fn temp_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!(".tmp-{name}-{:016x}", fastrand::u64(..)))
}

fn ensure_dir(root: &Path, dir: &Path) -> Result<(), StoreError> {
    fs::create_dir_all(dir).map_err(|error| unavailable(dir, &error))?;
    for ancestor in dir.ancestors().skip(1) {
        sync_dir(ancestor)?;
        if !ancestor.starts_with(root) {
            break;
        }
    }
    Ok(())
}

fn create_once(root: &Path, dir: &Path, name: &str, bytes: &[u8]) -> Result<(), StoreError> {
    let mut last = None;
    for _ in 0..CREATE_ATTEMPTS {
        ensure_dir(root, dir)?;
        let temp = temp_path(dir, name);
        match write_synced(&temp, bytes) {
            Ok(()) => {}
            Err(error) if !dir.is_dir() => {
                last = Some(error);
                continue;
            }
            Err(error) => return Err(error),
        }
        let target = dir.join(name);
        let linked = fs::hard_link(&temp, &target);
        let _ = fs::remove_file(&temp);
        return match linked {
            Ok(()) => sync_dir(dir),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => sync_dir(dir),
            Err(error) => Err(unavailable(&target, &error)),
        };
    }
    Err(last.unwrap_or_else(|| StoreError::Unavailable(format!("{} vanished", dir.display()))))
}

fn replace(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), StoreError> {
    let temp = temp_path(dir, name);
    write_synced(&temp, bytes)?;
    let target = dir.join(name);
    if let Err(error) = fs::rename(&temp, &target) {
        let _ = fs::remove_file(&temp);
        return Err(unavailable(&target, &error));
    }
    sync_dir(dir)
}

fn event_ids(dir: &Path) -> Result<Vec<EventId>, StoreError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(unavailable(dir, &error)),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| unavailable(dir, &error))?;
        let name = entry.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|stem| stem.parse::<EventId>().ok())
        else {
            continue;
        };
        ids.push(id);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn key_dir(root: &Path, key: &OrderKeyId) -> PathBuf {
    root.join(PARKED_DIR).join(key.to_string())
}

fn append(
    root: &Path,
    events: &[(OrderKeyId, String, Vec<u8>)],
    cancelled: Cancelled<'_>,
) -> Result<(), StoreError> {
    for (key, name, bytes) in events {
        check(cancelled)?;
        create_once(root, &key_dir(root, key), name, bytes)?;
    }
    Ok(())
}

fn load_heads(root: &Path, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
    let parked = root.join(PARKED_DIR);
    let entries = match fs::read_dir(&parked) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(unavailable(&parked, &error)),
    };
    let mut heads = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| unavailable(&parked, &error))?;
        let Some(order_key) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<OrderKeyId>().ok())
        else {
            continue;
        };
        if order_key.partition() != partition {
            continue;
        }
        let ids = event_ids(&entry.path())?;
        if let Some(head) = ids.first() {
            heads.push(ParkedHead {
                order_key,
                head: *head,
                length: u64::try_from(ids.len()).unwrap_or(u64::MAX),
            });
        }
    }
    Ok(heads)
}

fn list_queues(
    root: &Path,
    after: Option<OrderKeyId>,
    limit: usize,
    length_cap: usize,
    cancelled: Cancelled<'_>,
) -> Result<Vec<ParkedHead>, StoreError> {
    let parked = root.join(PARKED_DIR);
    let entries = match fs::read_dir(&parked) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(unavailable(&parked, &error)),
    };
    let mut keys = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| unavailable(&parked, &error))?;
        let Some(order_key) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<OrderKeyId>().ok())
        else {
            continue;
        };
        if after.is_none_or(|after| order_key > after) {
            keys.push(order_key);
        }
    }
    keys.sort_unstable();
    let mut queues = Vec::new();
    for order_key in keys {
        if queues.len() >= limit {
            break;
        }
        check(cancelled)?;
        let ids = event_ids(&key_dir(root, &order_key))?;
        if let Some(head) = ids.first() {
            queues.push(ParkedHead {
                order_key,
                head: *head,
                length: u64::try_from(ids.len().min(length_cap.max(1))).unwrap_or(u64::MAX),
            });
        }
    }
    Ok(queues)
}

fn read(
    root: &Path,
    key: OrderKeyId,
    after: Option<EventId>,
    limit: usize,
) -> Result<Vec<ParkedEvent>, StoreError> {
    let dir = key_dir(root, &key);
    let mut events = Vec::new();
    for id in event_ids(&dir)? {
        if events.len() >= limit {
            break;
        }
        if after.is_some_and(|after| id <= after) {
            continue;
        }
        let name = format!("{id}.json");
        let path = dir.join(&name);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        };
        events.push(decode_parked(&super::parked_name(&key, &id), &bytes)?);
    }
    Ok(events)
}

fn set_status(
    root: &Path,
    key: OrderKeyId,
    id: EventId,
    status: ParkStatus,
    cancelled: Cancelled<'_>,
) -> Result<(), StoreError> {
    check(cancelled)?;
    let dir = key_dir(root, &key);
    let name = format!("{id}.json");
    let path = dir.join(&name);
    let object = super::parked_name(&key, &id);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(StoreError::NotFound(object));
        }
        Err(error) => return Err(unavailable(&path, &error)),
    };
    let mut event = decode_parked(&object, &bytes)?;
    event.status = status;
    replace(&dir, &name, &encode_parked(&event)?)
}

fn remove(
    root: &Path,
    events: &[(OrderKeyId, EventId)],
    cancelled: Cancelled<'_>,
) -> Result<(), StoreError> {
    let parked = root.join(PARKED_DIR);
    for (key, id) in events {
        check(cancelled)?;
        let dir = key_dir(root, key);
        let path = dir.join(format!("{id}.json"));
        match fs::remove_file(&path) {
            Ok(()) => sync_dir(&dir)?,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        }
        if fs::remove_dir(&dir).is_ok() {
            sync_dir(&parked)?;
        }
    }
    Ok(())
}

fn read_leader_file(root: &Path) -> Result<Option<LeaderFile>, StoreError> {
    let path = root.join(LEADER_OBJECT);
    match fs::read(&path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| StoreError::Malformed {
                    key: LEADER_OBJECT.to_owned(),
                    reason: error.to_string(),
                })
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(unavailable(&path, &error)),
    }
}

fn swap(
    root: &Path,
    next: LeaderRecord,
    expected: Option<&Version>,
) -> Result<SwapOutcome, StoreError> {
    ensure_dir(root, root)?;
    let lock_path = root.join(LEADER_LOCK);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|error| unavailable(&lock_path, &error))?;
    lock.lock()
        .map_err(|error| unavailable(&lock_path, &error))?;
    let current = read_leader_file(root)?.map(|file| file.version);
    let matches = match (current, expected) {
        (None, None) => true,
        (Some(current), Some(expected)) => expected.0 == current.to_string(),
        _ => false,
    };
    if !matches {
        return Ok(SwapOutcome::Conflict);
    }
    let version = current.map_or(1, |current| current.saturating_add(1));
    let file = LeaderFile {
        version,
        record: next,
    };
    let bytes = serde_json::to_vec(&file).map_err(|error| StoreError::Malformed {
        key: LEADER_OBJECT.to_owned(),
        reason: error.to_string(),
    })?;
    replace(root, LEADER_OBJECT, &bytes)?;
    Ok(SwapOutcome::Swapped(Version(version.to_string())))
}

#[async_trait]
impl StateStore for FilesystemStateStore {
    async fn append_parked(&self, events: &[ParkedEvent]) -> Result<(), StoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut sorted: Vec<&ParkedEvent> = events.iter().collect();
        sorted.sort_by_key(|event| (event.order_key, event.id));
        let mut encoded = Vec::with_capacity(sorted.len());
        for event in sorted {
            encoded.push((
                event.order_key,
                format!("{}.json", event.id),
                encode_parked(event)?,
            ));
        }
        self.blocking(move |root, cancelled| append(root, &encoded, cancelled))
            .await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.blocking(move |root, _| load_heads(root, partition))
            .await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<Vec<ParkedHead>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after.copied();
        self.blocking(move |root, cancelled| list_queues(root, after, limit, length_cap, cancelled))
            .await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        let (key, after) = (*key, after.copied());
        self.blocking(move |root, _| read(root, key, after, limit))
            .await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        let (key, id) = (*key, *id);
        self.blocking(move |root, cancelled| set_status(root, key, id, status, cancelled))
            .await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let events = events.to_vec();
        self.blocking(move |root, cancelled| remove(root, &events, cancelled))
            .await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        self.leader(|root| {
            Ok(read_leader_file(root)?.map(|file| Versioned {
                value: file.record,
                version: Version(file.version.to_string()),
            }))
        })
        .await
    }

    async fn swap_leader(
        &self,
        next: &LeaderRecord,
        expected: Option<&Version>,
    ) -> Result<SwapOutcome, StoreError> {
        let (next, expected) = (next.clone(), expected.cloned());
        self.leader(move |root| swap(root, next, expected.as_ref()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use pg_cdc_decoder::Lsn;
    use tokio::time::timeout;

    use super::*;
    use crate::state::LeaderState;

    fn key(n: u8) -> OrderKeyId {
        format!("ab{}{n:02x}", "0".repeat(28)).parse().unwrap()
    }

    fn event_id(ordinal: u32) -> EventId {
        EventId::new(Lsn(7), ordinal)
    }

    fn parked(key: OrderKeyId, ordinal: u32) -> ParkedEvent {
        ParkedEvent {
            order_key: key,
            id: event_id(ordinal),
            status: ParkStatus::Retrying,
            attempts: 1,
            first_failure_ms: 1,
            last_error: "x".to_owned(),
            attributes: Vec::new(),
            body: Bytes::from_static(b"{}"),
        }
    }

    fn encoded(key: OrderKeyId, count: u32) -> Vec<(OrderKeyId, String, Vec<u8>)> {
        (0..count)
            .map(|n| {
                let event = parked(key, n);
                (
                    key,
                    format!("{}.json", event.id),
                    encode_parked(&event).unwrap(),
                )
            })
            .collect()
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = match fs::read_dir(dir) {
            Ok(entries) => entries
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect(),
            Err(_) => Vec::new(),
        };
        names.sort_unstable();
        names
    }

    fn cancelled_after(allowed: usize) -> impl Fn() -> bool {
        let calls = AtomicUsize::new(0);
        move || calls.fetch_add(1, Ordering::SeqCst) >= allowed
    }

    fn is_cancelled<T: std::fmt::Debug>(result: &Result<T, StoreError>) -> bool {
        matches!(result, Err(StoreError::Unavailable(message)) if message == "cancelled")
    }

    fn record() -> LeaderRecord {
        LeaderRecord {
            epoch: 1,
            holder: "task-a".to_owned(),
            state: LeaderState::Active,
            confirmed_lsn: Lsn(0x10),
            updated_ms: 1,
        }
    }

    #[tokio::test]
    async fn leader_calls_bypass_the_permits_parking_holds() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStateStore::new(dir.path(), "orders", 2);
        let _held = store.permits.acquire_many(2).await.unwrap();
        let swapped = timeout(Duration::from_secs(1), store.swap_leader(&record(), None)).await;
        assert!(
            matches!(swapped, Ok(Ok(SwapOutcome::Swapped(_)))),
            "{swapped:?}"
        );
        let read = timeout(Duration::from_secs(1), store.read_leader())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(read.value, record());
        let parked_read = timeout(
            Duration::from_millis(100),
            store.read_parked(&key(1), None, 1),
        )
        .await;
        assert!(parked_read.is_err());
    }

    #[test]
    fn leader_calls_finish_while_parking_occupies_every_blocking_thread() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(2)
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let store = FilesystemStateStore::new(dir.path(), "orders", 4);
            let (started, mut running) = tokio::sync::mpsc::unbounded_channel();
            let mut releases = Vec::new();
            let mut parking = Vec::new();
            for _ in 0..2 {
                let (release, wait) = std::sync::mpsc::channel::<()>();
                releases.push(release);
                let (store, started) = (store.clone(), started.clone());
                parking.push(tokio::spawn(async move {
                    store
                        .blocking(move |_, _| {
                            started.send(()).unwrap();
                            let _ = wait.recv();
                            Ok(())
                        })
                        .await
                }));
            }
            for _ in 0..2 {
                timeout(Duration::from_secs(5), running.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }

            let swapped = timeout(Duration::from_secs(2), store.swap_leader(&record(), None)).await;
            assert!(
                matches!(swapped, Ok(Ok(SwapOutcome::Swapped(_)))),
                "{swapped:?}"
            );
            let read = timeout(Duration::from_secs(2), store.read_leader())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(read.value, record());

            drop(releases);
            for call in parking {
                call.await.unwrap().unwrap();
            }
        });
    }

    #[tokio::test]
    async fn a_stopped_leader_worker_is_replaced_on_the_next_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStateStore::new(dir.path(), "orders", 2);
        assert!(store.read_leader().await.unwrap().is_none());
        *store.leader.jobs.lock().unwrap() = Some(mpsc::channel().0);
        let swapped = store.swap_leader(&record(), None).await;
        assert!(
            matches!(swapped, Ok(SwapOutcome::Swapped(_))),
            "{swapped:?}"
        );
        let read = store.read_leader().await.unwrap().unwrap();
        assert_eq!(read.value, record());
    }

    #[tokio::test]
    async fn a_dropped_call_signals_its_blocking_operation() {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStateStore::new(dir.path(), "orders", 2);
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (report, reported) = std::sync::mpsc::channel::<bool>();
        let mut call = Box::pin(store.blocking(move |_, cancelled| {
            wait.recv().unwrap();
            report.send(cancelled()).unwrap();
            Ok(())
        }));
        assert!(timeout(Duration::from_millis(50), &mut call).await.is_err());
        drop(call);
        release.send(()).unwrap();
        assert!(reported.recv_timeout(Duration::from_secs(5)).unwrap());

        let completed = store
            .blocking(|_, cancelled| Ok(cancelled()))
            .await
            .unwrap();
        assert!(!completed);
    }

    #[test]
    fn a_cancelled_append_stops_before_its_next_event() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        let events = encoded(queue, 3);
        let result = append(&root, &events, &cancelled_after(1));
        assert!(is_cancelled(&result), "{result:?}");
        assert_eq!(
            names(&key_dir(&root, &queue)),
            vec![format!("{}.json", event_id(0))]
        );

        let untouched = dir.path().join("untouched");
        let result = append(&untouched, &events, &cancelled_after(0));
        assert!(is_cancelled(&result), "{result:?}");
        assert!(!untouched.exists());
    }

    #[test]
    fn a_cancelled_remove_stops_before_its_next_event() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        append(&root, &encoded(queue, 3), &|| false).unwrap();
        let ids: Vec<_> = (0..3).map(|n| (queue, event_id(n))).collect();
        let result = remove(&root, &ids, &cancelled_after(0));
        assert!(is_cancelled(&result), "{result:?}");
        assert_eq!(names(&key_dir(&root, &queue)).len(), 3);

        let result = remove(&root, &ids, &cancelled_after(1));
        assert!(is_cancelled(&result), "{result:?}");
        assert_eq!(
            names(&key_dir(&root, &queue)),
            vec![
                format!("{}.json", event_id(1)),
                format!("{}.json", event_id(2))
            ]
        );
    }

    #[test]
    fn a_cancelled_status_change_leaves_the_event_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        append(&root, &encoded(queue, 1), &|| false).unwrap();
        let result = set_status(&root, queue, event_id(0), ParkStatus::Dead, &|| true);
        assert!(is_cancelled(&result), "{result:?}");
        let stored = read(&root, queue, None, 1).unwrap();
        assert_eq!(stored[0].status, ParkStatus::Retrying);
        assert_eq!(names(&key_dir(&root, &queue)).len(), 1);
    }
}
