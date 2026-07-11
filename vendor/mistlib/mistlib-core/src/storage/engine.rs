use super::backend::{BlockStore, PeerResolver, SelfPositionSource};
use super::cid::{compute_cid, verify_cid, MULTICODEC_DAG_CBOR, MULTICODEC_RAW};
use super::types::{FileManifest, StorageManager, CHUNK_SIZE};
use crate::config::StorageConfig;
use crate::error::{MistError, Result};
use crate::types::Vector3;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// Spatial-eviction knobs threaded from `StorageConfig` down to
/// `StorageManager`. The decay sweep *interval* is deliberately not part of
/// this policy: driving the periodic sweep is native/wasm's job (core only
/// exposes `StorageEngine::run_decay_sweep` to be called on a timer).
#[derive(Debug, Clone, Copy)]
pub struct SpatialPolicy {
    pub retention_radius: f32,
    pub decay_max_probability: f32,
}

impl From<&StorageConfig> for SpatialPolicy {
    fn from(cfg: &StorageConfig) -> Self {
        // Clamp misconfigured values: a negative radius would flip the sign
        // of the decay formula and silently disable decay (the opposite of
        // the documented "0 = no protection"), and a probability outside
        // [0, 1] is meaningless.
        Self {
            retention_radius: cfg.spatial_retention_radius.max(0.0),
            decay_max_probability: cfg.spatial_decay_max_probability.clamp(0.0, 1.0),
        }
    }
}

impl Default for SpatialPolicy {
    fn default() -> Self {
        Self::from(&StorageConfig::default())
    }
}

pub struct StorageEngine<B: BlockStore, P: PeerResolver> {
    store: B,
    resolver: P,
    manager: Mutex<StorageManager>,
    position_source: Option<Arc<dyn SelfPositionSource>>,
    spatial: SpatialPolicy,
    sweep_counter: AtomicU64,
}

impl<B: BlockStore, P: PeerResolver> StorageEngine<B, P> {
    pub fn new(
        store: B,
        resolver: P,
        max_capacity_bytes: u64,
        position_source: Option<Arc<dyn SelfPositionSource>>,
        spatial: SpatialPolicy,
    ) -> Self {
        Self {
            store,
            resolver,
            manager: Mutex::new(StorageManager::new(max_capacity_bytes)),
            position_source,
            spatial,
            sweep_counter: AtomicU64::new(0),
        }
    }

    pub async fn get_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        let block = self.store.load_block(cid).await?;
        if block.is_some() {
            self.manager.lock().unwrap().touch(cid);
        }
        Ok(block)
    }

    /// Resolves the position to auto-tag a freshly tracked block with when no
    /// explicit position was given: the first (join-order) session's current
    /// position, or `None` if there's no source or no session has reported
    /// one yet.
    async fn resolve_auto_position(&self) -> Option<Vector3> {
        match &self.position_source {
            Some(source) => source.self_positions().await.into_iter().next(),
            None => None,
        }
    }

    pub async fn add(&self, name: &str, data: &[u8]) -> Result<String> {
        self.add_at(name, data, None).await
    }

    /// Like `add`, but with an explicit spatial tag for every chunk and the
    /// manifest. `position: None` falls back to auto-tagging via the
    /// configured `SelfPositionSource` (see `resolve_auto_position`).
    pub async fn add_at(
        &self,
        name: &str,
        data: &[u8],
        position: Option<Vector3>,
    ) -> Result<String> {
        debug!("StorageEngine::add: name={}, size={}", name, data.len());
        let position = match position {
            Some(p) => Some(p),
            None => self.resolve_auto_position().await,
        };

        use futures_util::stream::{FuturesUnordered, StreamExt};

        // Mirrors get()'s bounded-concurrency pattern: at most 4 chunk writes
        // in flight at once. enforce_capacity_limit() runs after each drain so
        // a large add() can't blow past max_capacity_bytes until the whole
        // file has been written.
        //
        // A block only becomes tracked (= an eviction candidate) once its
        // write has actually completed: FuturesUnordered resolves in
        // completion order, so tracking at submit time could let the LRU
        // evict a cid whose write is still in flight — the delete would hit
        // nothing and the late write would then resurrect the block untracked.
        // The cost is that usage accounting lags the in-flight writes by up
        // to `concurrency` chunks, which is bounded and acceptable.
        let concurrency = 4;
        let mut chunk_cids = Vec::new();
        let mut writes = FuturesUnordered::new();

        for chunk in data.chunks(CHUNK_SIZE) {
            let cid = compute_cid(chunk, MULTICODEC_RAW);
            chunk_cids.push(cid.clone());

            writes.push(async move {
                let len = chunk.len() as u64;
                self.store
                    .store_block(&cid, chunk)
                    .await
                    .map(|()| (cid, len))
            });

            if writes.len() >= concurrency {
                if let Some(res) = writes.next().await {
                    let (done_cid, len) = res?;
                    self.manager
                        .lock()
                        .unwrap()
                        .track_block(&done_cid, len, position);
                }
                self.enforce_capacity_limit().await?;
            }
        }

        while let Some(res) = writes.next().await {
            let (done_cid, len) = res?;
            self.manager
                .lock()
                .unwrap()
                .track_block(&done_cid, len, position);
            self.enforce_capacity_limit().await?;
        }

        let manifest = FileManifest {
            name: name.to_string(),
            size: data.len() as u64,
            chunks: chunk_cids,
        };
        let manifest_bytes =
            serde_cbor::to_vec(&manifest).map_err(|e| MistError::Serialization(e.to_string()))?;
        let root_cid = compute_cid(&manifest_bytes, MULTICODEC_DAG_CBOR);

        self.store.store_block(&root_cid, &manifest_bytes).await?;
        {
            let mut mgr = self.manager.lock().unwrap();
            mgr.track_block(&root_cid, manifest_bytes.len() as u64, position);
        }
        self.enforce_capacity_limit().await?;
        Ok(root_cid)
    }

    pub async fn get(&self, root_cid: &str) -> Result<Vec<u8>> {
        info!("StorageEngine::get: cid={}", root_cid);
        let manifest_bytes = self.resolve_or_fetch(root_cid, MULTICODEC_DAG_CBOR).await?;
        let manifest: FileManifest = serde_cbor::from_slice(&manifest_bytes)
            .map_err(|e| MistError::Serialization(e.to_string()))?;

        info!(
            "StorageEngine: downloading '{}' ({} bytes, {} chunks)",
            manifest.name,
            manifest.size,
            manifest.chunks.len()
        );

        use futures_util::stream::{FuturesUnordered, StreamExt};
        use std::collections::BTreeMap;

        let concurrency = 4;
        let mut pending: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        let mut futures = FuturesUnordered::new();
        let mut next_index = 0usize;
        let mut result = Vec::with_capacity(manifest.size as usize);

        let drain =
            |result: &mut Vec<u8>, pending: &mut BTreeMap<usize, Vec<u8>>, next: &mut usize| {
                while let Some(chunk) = pending.remove(next) {
                    result.extend_from_slice(&chunk);
                    *next += 1;
                }
            };

        for (i, cid) in manifest.chunks.iter().enumerate() {
            let cid = cid.clone();
            futures.push(async move {
                let data = self.resolve_or_fetch(&cid, MULTICODEC_RAW).await?;
                Ok::<(usize, Vec<u8>), MistError>((i, data))
            });

            if futures.len() >= concurrency {
                if let Some(res) = futures.next().await {
                    let (idx, data) = res?;
                    if idx == next_index {
                        result.extend_from_slice(&data);
                        next_index += 1;
                        drain(&mut result, &mut pending, &mut next_index);
                    } else {
                        pending.insert(idx, data);
                    }
                }
            }
        }

        while let Some(res) = futures.next().await {
            let (idx, data) = res?;
            if idx == next_index {
                result.extend_from_slice(&data);
                next_index += 1;
                drain(&mut result, &mut pending, &mut next_index);
            } else {
                pending.insert(idx, data);
            }
        }

        info!("StorageEngine: successfully retrieved '{}'", manifest.name);
        Ok(result)
    }

    pub async fn enforce_capacity_limit(&self) -> Result<()> {
        // Fetched before locking `manager`: `self_positions()` is async and
        // `manager` is a plain `std::sync::Mutex`, which must never be held
        // across an `.await` point.
        let positions = self.resolve_positions_for_eviction().await;

        let victims = {
            let mgr = self.manager.lock().unwrap();
            if positions.is_empty() {
                mgr.eviction_candidates()
            } else {
                mgr.spatial_eviction_candidates(&positions, self.spatial.retention_radius)
            }
        };
        for cid in victims {
            info!("StorageEngine: Evicting {}", cid);
            self.store.delete_block(&cid).await?;
            let mut mgr = self.manager.lock().unwrap();
            mgr.untrack_block(&cid);
        }
        Ok(())
    }

    /// Runs one decay sweep: probabilistically deletes spatially-tagged
    /// blocks far from every current self-position (see
    /// `StorageManager::decay_candidates`). Driving this on a timer is
    /// native/wasm's responsibility; core just does the one sweep and
    /// reports how many blocks it removed.
    pub async fn run_decay_sweep(&self) -> usize {
        let positions = self.resolve_positions_for_eviction().await;
        if positions.is_empty() {
            return 0;
        }

        let sweep = self.sweep_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let victims = {
            let mgr = self.manager.lock().unwrap();
            mgr.decay_candidates(
                &positions,
                self.spatial.retention_radius,
                self.spatial.decay_max_probability,
                sweep,
            )
        };

        let mut evicted = 0;
        for cid in victims {
            match self.store.delete_block(&cid).await {
                Ok(()) => {
                    self.manager.lock().unwrap().untrack_block(&cid);
                    evicted += 1;
                }
                Err(e) => {
                    warn!("StorageEngine: decay sweep failed to delete {}: {}", cid, e);
                }
            }
        }
        evicted
    }

    async fn resolve_positions_for_eviction(&self) -> Vec<Vector3> {
        match &self.position_source {
            Some(source) => source.self_positions().await,
            None => Vec::new(),
        }
    }

    async fn resolve_or_fetch(&self, cid: &str, codec: u64) -> Result<Vec<u8>> {
        if let Some(data) = self.store.load_block(cid).await? {
            self.manager.lock().unwrap().touch(cid);
            return Ok(data);
        }
        let data = self.resolver.resolve_block(cid).await.ok_or_else(|| {
            MistError::Network(format!("Block not found locally or on peers: {}", cid))
        })?;
        if !verify_cid(cid, &data, codec) {
            warn!("StorageEngine: Hash mismatch for {}", cid);
            return Err(MistError::Internal("Hash mismatch".into()));
        }
        let position = self.resolve_auto_position().await;
        {
            let mut mgr = self.manager.lock().unwrap();
            mgr.track_block(cid, data.len() as u64, position);
        }
        self.enforce_capacity_limit().await?;
        self.store.store_block(cid, &data).await?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests;
