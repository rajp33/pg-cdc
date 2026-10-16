#[path = "support/state_store.rs"]
mod state_store;

use std::sync::Arc;

use pg_cdc::state::StateStore;
use pg_cdc::state::memory::MemoryStateStore;

#[tokio::test]
async fn the_memory_store_conforms() -> state_store::TestResult {
    let store = MemoryStateStore::new();
    state_store::conforms(|| {
        let store: Arc<dyn StateStore> = Arc::new(store.clone());
        async move { Ok(store) }
    })
    .await
}
