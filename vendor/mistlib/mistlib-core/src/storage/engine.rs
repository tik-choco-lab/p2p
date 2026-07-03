use super::backend::{BlockStore, PeerResolver};
use super::cid::{compute_cid, verify_cid, MULTICODEC_DAG_CBOR, MULTICODEC_RAW};
use super::types::{FileManifest, StorageManager, CHUNK_SIZE};
use crate::error::{MistError, Result};
use std::sync::Mutex;
use tracing::{debug, info, warn};

pub struct StorageEngine<B: BlockStore, P: PeerResolver> {
    store: B,
    resolver: P,
    manager: Mutex<StorageManager>,
}

impl<B: BlockStore, P: PeerResolver> StorageEngine<B, P> {
    pub fn new(store: B, resolver: P, max_capacity_bytes: u64) -> Self {
        Self {
            store,
            resolver,
            manager: Mutex::new(StorageManager::new(max_capacity_bytes)),
        }
    }

    pub async fn get_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        self.store.load_block(cid).await
    }

    pub async fn add(&self, name: &str, data: &[u8]) -> Result<String> {
        debug!("StorageEngine::add: name={}, size={}", name, data.len());
        let mut chunk_cids = Vec::new();
        for chunk in data.chunks(CHUNK_SIZE) {
            let cid = compute_cid(chunk, MULTICODEC_RAW);
            {
                let mut mgr = self.manager.lock().unwrap();
                mgr.track_block(&cid, chunk.len() as u64);
            }
            self.store.store_block(&cid, chunk).await?;
            chunk_cids.push(cid);
        }

        let manifest = FileManifest {
            name: name.to_string(),
            size: data.len() as u64,
            chunks: chunk_cids,
        };
        let manifest_bytes =
            serde_cbor::to_vec(&manifest).map_err(|e| MistError::Serialization(e.to_string()))?;
        let root_cid = compute_cid(&manifest_bytes, MULTICODEC_DAG_CBOR);

        {
            let mut mgr = self.manager.lock().unwrap();
            mgr.track_block(&root_cid, manifest_bytes.len() as u64);
        }
        self.enforce_capacity_limit().await?;
        self.store.store_block(&root_cid, &manifest_bytes).await?;
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
        let victims = {
            let mgr = self.manager.lock().unwrap();
            mgr.eviction_candidates()
        };
        for cid in victims {
            info!("StorageEngine: Evicting {}", cid);
            self.store.delete_block(&cid).await?;
            let mut mgr = self.manager.lock().unwrap();
            mgr.untrack_block(&cid);
        }
        Ok(())
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
        {
            let mut mgr = self.manager.lock().unwrap();
            mgr.track_block(cid, data.len() as u64);
        }
        self.enforce_capacity_limit().await?;
        self.store.store_block(cid, &data).await?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests;
