use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::{
    LEADER_OBJECT, LeaderRecord, PARKED_DIR, ParkStatus, ParkedEvent, ParkedHead, StateStore,
    StoreError, SwapOutcome, Version, Versioned, decode_parked, encode_parked,
};
use crate::event::EventId;
use crate::order::OrderKeyId;

pub const LEADER_LOCK: &str = "leader.lock";
const CREATE_ATTEMPTS: usize = 3;

#[derive(Debug, Clone)]
pub struct FilesystemStateStore {
    root: PathBuf,
    permits: Arc<Semaphore>,
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
            permits: Arc::new(Semaphore::new(max_concurrency.max(1))),
        }
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<T, StoreError> + Send + 'static,
    {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable("the store is closed".to_owned()))?;
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || operation(&root))
            .await
            .map_err(|error| StoreError::Unavailable(error.to_string()))?
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

fn append(root: &Path, events: &[(OrderKeyId, String, Vec<u8>)]) -> Result<(), StoreError> {
    for (key, name, bytes) in events {
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
) -> Result<(), StoreError> {
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

fn remove(root: &Path, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
    let parked = root.join(PARKED_DIR);
    for (key, id) in events {
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
        let mut encoded = Vec::with_capacity(events.len());
        for event in events {
            encoded.push((
                event.order_key,
                format!("{}.json", event.id),
                encode_parked(event)?,
            ));
        }
        self.blocking(move |root| append(root, &encoded)).await
    }

    async fn load_parked_heads(&self, partition: u8) -> Result<Vec<ParkedHead>, StoreError> {
        self.blocking(move |root| load_heads(root, partition)).await
    }

    async fn read_parked(
        &self,
        key: &OrderKeyId,
        after: Option<&EventId>,
        limit: usize,
    ) -> Result<Vec<ParkedEvent>, StoreError> {
        let (key, after) = (*key, after.copied());
        self.blocking(move |root| read(root, key, after, limit))
            .await
    }

    async fn set_parked_status(
        &self,
        key: &OrderKeyId,
        id: &EventId,
        status: ParkStatus,
    ) -> Result<(), StoreError> {
        let (key, id) = (*key, *id);
        self.blocking(move |root| set_status(root, key, id, status))
            .await
    }

    async fn remove_parked(&self, events: &[(OrderKeyId, EventId)]) -> Result<(), StoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let events = events.to_vec();
        self.blocking(move |root| remove(root, &events)).await
    }

    async fn read_leader(&self) -> Result<Option<Versioned<LeaderRecord>>, StoreError> {
        self.blocking(|root| {
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
        self.blocking(move |root| swap(root, next, expected.as_ref()))
            .await
    }
}
