#[path = "support/state_store.rs"]
mod state_store;

use std::sync::Arc;

use pg_cdc::state::filesystem::FilesystemStateStore;
use pg_cdc::state::{ParkStatus, StateStore, StoreError, decode_parked};

use state_store::{TestResult, event, id, key};

#[tokio::test]
async fn the_filesystem_store_conforms() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_owned();
    state_store::conforms(|| {
        let store: Arc<dyn StateStore> = Arc::new(FilesystemStateStore::new(&path, "orders", 4));
        async move { Ok(store) }
    })
    .await
}

#[tokio::test]
async fn parked_events_use_the_spec_layout_and_stray_files_are_ignored() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = FilesystemStateStore::new(dir.path(), "orders", 4);
    let queue = key(0xab, 1)?;
    let parked = event(queue, id(0x1A2B_3C4D, 3), ParkStatus::Retrying);
    store.append_parked(std::slice::from_ref(&parked)).await?;
    let queue_dir = dir
        .path()
        .join("orders")
        .join("parked")
        .join(queue.to_string());
    let object = queue_dir.join("000000001a2b3c4d-00000003.json");
    let name = format!("{queue}/000000001a2b3c4d-00000003.json");
    assert_eq!(decode_parked(&name, &std::fs::read(&object)?)?, parked);
    std::fs::write(
        queue_dir.join(".tmp-000000001a2b3c4d-00000004.json-1"),
        b"partial",
    )?;
    std::fs::write(queue_dir.join("notes.txt"), b"x")?;
    std::fs::create_dir_all(
        dir.path()
            .join("orders")
            .join("parked")
            .join("ab-not-a-key"),
    )?;
    let heads = store.load_parked_heads(0xab).await?;
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].length, 1);
    assert_eq!(
        store.read_parked(&queue, None, 10).await?,
        vec![parked.clone()]
    );
    std::fs::remove_file(queue_dir.join(".tmp-000000001a2b3c4d-00000004.json-1"))?;
    std::fs::remove_file(queue_dir.join("notes.txt"))?;
    store.remove_parked(&[(queue, parked.id)]).await?;
    assert!(!queue_dir.exists(), "an emptied queue leaves no directory");
    let leader_dir = dir.path().join("orders");
    assert!(!leader_dir.join("leader.json").exists());
    Ok(())
}

#[tokio::test]
async fn an_unwritable_root_reports_the_store_unavailable() -> TestResult {
    let dir = tempfile::tempdir()?;
    let blocker = dir.path().join("file");
    std::fs::write(&blocker, b"not a directory")?;
    let store = FilesystemStateStore::new(&blocker, "orders", 4);
    let queue = key(0x01, 1)?;
    let result = store
        .append_parked(&[event(queue, id(1, 0), ParkStatus::Retrying)])
        .await;
    assert!(
        matches!(result, Err(StoreError::Unavailable(_))),
        "{result:?}"
    );
    assert!(matches!(
        store.read_leader().await,
        Err(StoreError::Unavailable(_))
    ));
    Ok(())
}
