use super::backend::{BlockStore, PeerResolver, SelfPositionSource};
use super::engine::{SpatialPolicy, StorageEngine};
use crate::error::Result;
use crate::layers::l2::L2Storage;
use crate::types::Vector3;
use async_trait::async_trait;
use std::sync::Arc;

pub struct P2PStorage<B: BlockStore, P: PeerResolver> {
    engine: StorageEngine<B, P>,
}

impl<B: BlockStore, P: PeerResolver> P2PStorage<B, P> {
    pub fn new(
        store: B,
        resolver: P,
        max_capacity_bytes: u64,
        position_source: Option<Arc<dyn SelfPositionSource>>,
        spatial: SpatialPolicy,
    ) -> Self {
        Self {
            engine: StorageEngine::new(
                store,
                resolver,
                max_capacity_bytes,
                position_source,
                spatial,
            ),
        }
    }

    pub async fn get_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        self.engine.get_block(cid).await
    }

    /// Explicit-position variant of `add` (see `StorageEngine::add_at`).
    pub async fn add_at(
        &self,
        name: &str,
        data: &[u8],
        position: Option<Vector3>,
    ) -> Result<String> {
        self.engine.add_at(name, data, position).await
    }

    /// Runs one spatial-decay sweep (see `StorageEngine::run_decay_sweep`).
    /// Native/wasm are responsible for calling this on a timer.
    pub async fn run_decay_sweep(&self) -> usize {
        self.engine.run_decay_sweep().await
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
impl<B: BlockStore, P: PeerResolver> L2Storage for P2PStorage<B, P> {
    async fn add(&self, name: &str, data: &[u8]) -> Result<String> {
        self.engine.add(name, data).await
    }
    async fn get(&self, root_cid: &str) -> Result<Vec<u8>> {
        self.engine.get(root_cid).await
    }
    async fn enforce_capacity_limit(&self) -> Result<()> {
        self.engine.enforce_capacity_limit().await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<B: BlockStore + Send + Sync, P: PeerResolver + Send + Sync> L2Storage for P2PStorage<B, P> {
    async fn add(&self, name: &str, data: &[u8]) -> Result<String> {
        self.engine.add(name, data).await
    }
    async fn get(&self, root_cid: &str) -> Result<Vec<u8>> {
        self.engine.get(root_cid).await
    }
    async fn enforce_capacity_limit(&self) -> Result<()> {
        self.engine.enforce_capacity_limit().await
    }
}
