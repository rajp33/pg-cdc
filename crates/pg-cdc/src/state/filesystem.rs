use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, mpsc};
use std::thread;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, oneshot};

use super::{
    Admission, ByteBudget, LEADER_OBJECT, LeaderRecord, PARKED_DIR, PARKED_SUMMARY_BYTES,
    ParkStatus, ParkedEntry, ParkedEvent, ParkedHead, ParkedQueues, ParkedSlice, ParkedSummary,
    StateStore, StoreError, SwapOutcome, Version, Versioned, decode_parked, decode_parked_summary,
    encode_parked,
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

fn nonempty(dir: &Path) -> &Path {
    if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    }
}

#[derive(Debug, Default)]
struct RootPath {
    existing: OnceLock<PathBuf>,
    durable: AtomicBool,
}

impl RootPath {
    fn establish(&self, root: &Path) -> Result<(), StoreError> {
        if self.durable.load(Ordering::Acquire) {
            return Ok(());
        }
        let existing = self.existing.get_or_init(|| {
            root.ancestors()
                .map(nonempty)
                .find(|dir| dir.is_dir())
                .unwrap_or(Path::new("."))
                .to_path_buf()
        });
        fs::create_dir_all(root).map_err(|error| unavailable(root, &error))?;
        if existing != root {
            for dir in root.ancestors().skip(1).map(nonempty) {
                sync_dir(dir)?;
                if dir == existing {
                    break;
                }
            }
        }
        self.durable.store(true, Ordering::Release);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FilesystemStateStore {
    root: PathBuf,
    path: Arc<RootPath>,
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
            path: Arc::default(),
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
    faults::before_sync(dir)
        .and_then(|()| File::open(dir))
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
        .and_then(|()| faults::before_sync(path))
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

fn queue_entries(dir: &Path) -> Result<Option<fs::ReadDir>, StoreError> {
    match fs::read_dir(dir) {
        Ok(entries) => Ok(Some(entries)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(unavailable(dir, &error)),
    }
}

fn each_event_id(
    dir: &Path,
    cancelled: Cancelled<'_>,
    mut visit: impl FnMut(EventId),
) -> Result<(), StoreError> {
    let Some(entries) = queue_entries(dir)? else {
        return Ok(());
    };
    for entry in entries {
        check(cancelled)?;
        let entry = entry.map_err(|error| unavailable(dir, &error))?;
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|stem| stem.parse::<EventId>().ok())
        {
            visit(id);
        }
    }
    Ok(())
}

fn keep_smallest<T: Ord>(kept: &mut BinaryHeap<T>, item: T, keep: usize) {
    if kept.len() < keep {
        kept.push(item);
    } else if kept.peek().is_some_and(|largest| item < *largest) {
        kept.pop();
        kept.push(item);
    }
}

fn smallest_ids(
    dir: &Path,
    after: Option<EventId>,
    limit: usize,
    cancelled: Cancelled<'_>,
) -> Result<Vec<EventId>, StoreError> {
    let mut kept = BinaryHeap::new();
    each_event_id(dir, cancelled, |id| {
        if after.is_none_or(|after| id > after) {
            keep_smallest(&mut kept, id, limit);
        }
    })?;
    Ok(kept.into_sorted_vec())
}

fn queue_head(
    dir: &Path,
    length_cap: usize,
    cancelled: Cancelled<'_>,
) -> Result<Option<(EventId, usize)>, StoreError> {
    let mut head: Option<EventId> = None;
    let mut length = 0_usize;
    each_event_id(dir, cancelled, |id| {
        head = Some(head.map_or(id, |head| head.min(id)));
        length = length.saturating_add(1).min(length_cap);
    })?;
    Ok(head.map(|head| (head, length)))
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

fn load_heads(
    root: &Path,
    partition: u8,
    cancelled: Cancelled<'_>,
) -> Result<Vec<ParkedHead>, StoreError> {
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
        if let Some((head, length)) = queue_head(&entry.path(), usize::MAX, cancelled)? {
            heads.push(ParkedHead {
                order_key,
                head,
                length: u64::try_from(length).unwrap_or(u64::MAX),
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
) -> Result<ParkedQueues, StoreError> {
    let parked = root.join(PARKED_DIR);
    let entries = match fs::read_dir(&parked) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(ParkedQueues::default()),
        Err(error) => return Err(unavailable(&parked, &error)),
    };
    let mut kept = BinaryHeap::new();
    for entry in entries {
        check(cancelled)?;
        let entry = entry.map_err(|error| unavailable(&parked, &error))?;
        let Some(order_key) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<OrderKeyId>().ok())
        else {
            continue;
        };
        if after.is_none_or(|after| order_key > after) {
            keep_smallest(&mut kept, order_key, limit.saturating_add(1));
        }
    }
    let mut keys = kept.into_sorted_vec();
    let more = keys.len() > limit;
    keys.truncate(limit);
    let mut queues = Vec::new();
    for order_key in &keys {
        let dir = key_dir(root, order_key);
        if let Some((head, length)) = queue_head(&dir, length_cap.max(1), cancelled)? {
            queues.push(ParkedHead {
                order_key: *order_key,
                head,
                length: u64::try_from(length).unwrap_or(u64::MAX),
            });
        }
    }
    Ok(ParkedQueues {
        queues,
        next: keys.last().copied().filter(|_| more),
    })
}

fn read(
    root: &Path,
    key: OrderKeyId,
    after: Option<EventId>,
    limit: usize,
    cancelled: Cancelled<'_>,
) -> Result<Vec<ParkedEvent>, StoreError> {
    let dir = key_dir(root, &key);
    let mut events = Vec::new();
    for id in smallest_ids(&dir, after, limit, cancelled)? {
        check(cancelled)?;
        let path = dir.join(format!("{id}.json"));
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        };
        events.push(decode_parked(&super::parked_name(&key, &id), &bytes)?);
    }
    Ok(events)
}

fn read_within(
    root: &Path,
    key: OrderKeyId,
    after: Option<EventId>,
    limit: usize,
    max_bytes: u64,
    cancelled: Cancelled<'_>,
) -> Result<ParkedSlice, StoreError> {
    let dir = key_dir(root, &key);
    let mut budget = ByteBudget::new(limit, max_bytes);
    let mut slice = ParkedSlice::default();
    for id in smallest_ids(&dir, after, limit, cancelled)? {
        check(cancelled)?;
        let path = dir.join(format!("{id}.json"));
        let bytes = match fs::metadata(&path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        };
        match budget.admit(bytes) {
            Admission::Load => {}
            Admission::Withhold => {
                slice.entries.push(ParkedEntry::Withheld { id, bytes });
                continue;
            }
            Admission::Full => {
                slice.full = true;
                break;
            }
            Admission::Done => break,
        }
        let stored = match fs::read(&path) {
            Ok(stored) => stored,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        };
        slice.entries.push(ParkedEntry::Loaded(decode_parked(
            &super::parked_name(&key, &id),
            &stored,
        )?));
    }
    Ok(slice)
}

fn read_summary(
    root: &Path,
    key: OrderKeyId,
    cancelled: Cancelled<'_>,
) -> Result<Option<ParkedSummary>, StoreError> {
    let dir = key_dir(root, &key);
    loop {
        let Some((head, _)) = queue_head(&dir, 1, cancelled)? else {
            return Ok(None);
        };
        check(cancelled)?;
        let path = dir.join(format!("{head}.json"));
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(unavailable(&path, &error)),
        };
        let mut prefix = Vec::with_capacity(PARKED_SUMMARY_BYTES);
        file.take(u64::try_from(PARKED_SUMMARY_BYTES).unwrap_or(u64::MAX))
            .read_to_end(&mut prefix)
            .map_err(|error| unavailable(&path, &error))?;
        return decode_parked_summary(&super::parked_name(&key, &head), &prefix).map(Some);
    }
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

fn sync_absence(dir: &Path, parked: &Path) -> Result<(), StoreError> {
    if dir.is_dir() {
        sync_dir(dir)
    } else if parked.is_dir() {
        sync_dir(parked)
    } else {
        Ok(())
    }
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
            Err(error) if error.kind() == ErrorKind::NotFound => {
                sync_absence(&dir, &parked)?;
            }
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
        let path = Arc::clone(&self.path);
        self.blocking(move |root, cancelled| {
            check(cancelled)?;
            path.establish(root)?;
            append(root, &encoded, cancelled)
        })
        .await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.blocking(move |root, cancelled| load_heads(root, partition, cancelled))
            .await
    }

    async fn list_parked_queues(
        &self,
        after: Option<&OrderKeyId>,
        limit: usize,
        length_cap: usize,
    ) -> Result<ParkedQueues, StoreError> {
        if limit == 0 {
            return Ok(ParkedQueues::default());
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
        self.blocking(move |root, cancelled| read(root, key, after, limit, cancelled))
            .await
    }

    async fn read_parked_within(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
        max_bytes: u64,
    ) -> Result<ParkedSlice, StoreError> {
        let (key, after) = (*key, after.copied());
        self.blocking(move |root, cancelled| {
            read_within(root, key, after, limit, max_bytes, cancelled)
        })
        .await
    }

    async fn read_parked_summary(
        &self,
        key: &OrderKeyId,
    ) -> Result<Option<ParkedSummary>, StoreError> {
        let key = *key;
        self.blocking(move |root, cancelled| read_summary(root, key, cancelled))
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
        let path = Arc::clone(&self.path);
        self.leader(move |root| {
            path.establish(root)?;
            swap(root, next, expected.as_ref())
        })
        .await
    }
}

#[cfg(not(test))]
mod faults {
    #[expect(
        clippy::unnecessary_wraps,
        reason = "matches the test build, which injects fsync failures"
    )]
    pub(super) fn before_sync(_: &std::path::Path) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod faults {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, PoisonError};

    type Fault = Box<dyn FnMut(&Path) -> io::Result<()> + Send>;

    static FAULTS: Mutex<Vec<(PathBuf, Fault)>> = Mutex::new(Vec::new());

    pub(super) fn before_sync(path: &Path) -> io::Result<()> {
        let mut faults = FAULTS.lock().unwrap_or_else(PoisonError::into_inner);
        faults
            .iter_mut()
            .find(|(scope, _)| path.starts_with(scope))
            .map_or(Ok(()), |(_, fault)| fault(path))
    }

    pub(super) struct Injected(PathBuf);

    impl Drop for Injected {
        fn drop(&mut self) {
            FAULTS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|(scope, _)| *scope != self.0);
        }
    }

    pub(super) fn inject(
        scope: &Path,
        fault: impl FnMut(&Path) -> io::Result<()> + Send + 'static,
    ) -> Injected {
        FAULTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((scope.to_path_buf(), Box::new(fault)));
        Injected(scope.to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
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

    fn backlog(root: &Path, queue: OrderKeyId, count: u32) -> Vec<EventId> {
        let dir = key_dir(root, &queue);
        fs::create_dir_all(&dir).unwrap();
        let mut ordinals: Vec<u32> = (0..count).collect();
        fastrand::shuffle(&mut ordinals);
        for ordinal in ordinals {
            let bytes = encode_parked(&parked(queue, ordinal)).unwrap();
            fs::write(dir.join(format!("{}.json", event_id(ordinal))), bytes).unwrap();
        }
        (0..count).map(event_id).collect()
    }

    #[test]
    fn a_listing_stops_inside_a_large_queue_once_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        let ids = backlog(&root, queue, 3000);

        let listed = list_queues(&root, None, 1, 10, &|| false).unwrap();
        assert_eq!(
            listed.queues,
            vec![ParkedHead {
                order_key: queue,
                head: ids[0],
                length: 10,
            }]
        );
        let cancelled = list_queues(&root, None, 1, 10, &cancelled_after(1));
        assert!(is_cancelled(&cancelled), "{cancelled:?}");
    }

    #[test]
    fn reads_from_a_large_queue_keep_only_the_page_and_stop_once_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        let ids = backlog(&root, queue, 3000);

        let read_ids = |after, limit| -> Vec<EventId> {
            read(&root, queue, after, limit, &|| false)
                .unwrap()
                .into_iter()
                .map(|event| event.id)
                .collect()
        };
        assert_eq!(read_ids(None, 3), ids[..3]);
        assert_eq!(read_ids(Some(ids[1499]), 2), ids[1500..1502]);
        assert_eq!(read_ids(Some(ids[2998]), 5), ids[2999..]);
        let cancelled = read(&root, queue, None, 1, &cancelled_after(1));
        assert!(is_cancelled(&cancelled), "{cancelled:?}");

        let heads = load_heads(&root, queue.partition(), &|| false).unwrap();
        assert_eq!(
            heads,
            vec![ParkedHead {
                order_key: queue,
                head: ids[0],
                length: 3000,
            }]
        );
        let cancelled = load_heads(&root, queue.partition(), &cancelled_after(1));
        assert!(is_cancelled(&cancelled), "{cancelled:?}");
    }

    type Synced = Arc<Mutex<Vec<PathBuf>>>;

    fn record_syncs(
        scope: &Path,
        mut fail: impl FnMut(&Path) -> bool + Send + 'static,
    ) -> (faults::Injected, Synced) {
        let synced = Synced::default();
        let log = Arc::clone(&synced);
        let injected = faults::inject(scope, move |path| {
            log.lock().unwrap().push(path.to_path_buf());
            if fail(path) {
                Err(io::Error::other("injected fsync failure"))
            } else {
                Ok(())
            }
        });
        (injected, synced)
    }

    fn fail_once(target: PathBuf) -> impl FnMut(&Path) -> bool + Send + 'static {
        let mut failed = false;
        move |path| {
            let fail = !failed && path == target;
            failed |= fail;
            fail
        }
    }

    #[tokio::test]
    async fn a_store_path_with_missing_ancestors_is_synced_up_to_the_first_existing_one() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        let (_injected, synced) = record_syncs(base, |_| false);

        let parking = base.join("new").join("deeper");
        let store = FilesystemStateStore::new(&parking, "orders", 2);
        store.append_parked(&[parked(key(1), 0)]).await.unwrap();
        let seen = std::mem::take(&mut *synced.lock().unwrap());
        for created_in in [base.to_path_buf(), base.join("new"), parking.clone()] {
            assert!(seen.contains(&created_in), "{created_in:?} not in {seen:?}");
        }

        let leading = base.join("other").join("deeper");
        let store = FilesystemStateStore::new(&leading, "orders", 2);
        store.swap_leader(&record(), None).await.unwrap();
        let seen = std::mem::take(&mut *synced.lock().unwrap());
        for created_in in [base.to_path_buf(), base.join("other"), leading.clone()] {
            assert!(seen.contains(&created_in), "{created_in:?} not in {seen:?}");
        }
    }

    #[tokio::test]
    async fn ancestors_created_before_a_failed_sync_are_synced_by_the_retry() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        let (_injected, synced) = record_syncs(base, fail_once(base.join("new")));
        let store = FilesystemStateStore::new(&base.join("new").join("deeper"), "orders", 2);

        assert!(store.append_parked(&[parked(key(1), 0)]).await.is_err());
        synced.lock().unwrap().clear();
        store.append_parked(&[parked(key(1), 0)]).await.unwrap();
        let seen = synced.lock().unwrap().clone();
        for created_in in [base.to_path_buf(), base.join("new")] {
            assert!(seen.contains(&created_in), "{created_in:?} not in {seen:?}");
        }
    }

    #[test]
    fn a_removal_retried_after_a_failed_directory_sync_syncs_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        append(&root, &encoded(queue, 2), &|| false).unwrap();
        let queue_dir = key_dir(&root, &queue);
        let (_injected, synced) = record_syncs(dir.path(), fail_once(queue_dir.clone()));

        let removed = [(queue, event_id(0))];
        assert!(remove(&root, &removed, &|| false).is_err());
        assert_eq!(names(&queue_dir), vec![format!("{}.json", event_id(1))]);
        synced.lock().unwrap().clear();
        remove(&root, &removed, &|| false).unwrap();
        assert_eq!(*synced.lock().unwrap(), vec![queue_dir]);
    }

    #[test]
    fn a_removal_retried_after_its_emptied_directory_went_unsynced_syncs_the_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        append(&root, &encoded(queue, 1), &|| false).unwrap();
        let parked_dir = root.join(PARKED_DIR);
        let (_injected, synced) = record_syncs(dir.path(), fail_once(parked_dir.clone()));

        let removed = [(queue, event_id(0))];
        assert!(remove(&root, &removed, &|| false).is_err());
        assert!(!key_dir(&root, &queue).exists());
        synced.lock().unwrap().clear();
        remove(&root, &removed, &|| false).unwrap();
        assert_eq!(*synced.lock().unwrap(), vec![parked_dir]);
    }

    #[test]
    fn a_cancelled_status_change_leaves_the_event_alone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let queue = key(1);
        append(&root, &encoded(queue, 1), &|| false).unwrap();
        let result = set_status(&root, queue, event_id(0), ParkStatus::Dead, &|| true);
        assert!(is_cancelled(&result), "{result:?}");
        let stored = read(&root, queue, None, 1, &|| false).unwrap();
        assert_eq!(stored[0].status, ParkStatus::Retrying);
        assert_eq!(names(&key_dir(&root, &queue)).len(), 1);
    }
}
