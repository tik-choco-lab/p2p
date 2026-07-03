pub mod fs;
pub mod resolver;

use crate::storage::fs::NativeBlockStore;
use crate::storage::resolver::{NativePeerResolver, WantRegistry};
use mistlib_core::storage::P2PStorage;
use std::sync::Arc;
use tokio::sync::OnceCell;

pub type NativeStorageInstance = P2PStorage<NativeBlockStore, NativePeerResolver>;

pub static STORAGE: OnceCell<Arc<NativeStorageInstance>> = OnceCell::const_new();
pub static WANT_REGISTRY: OnceCell<WantRegistry> = OnceCell::const_new();

pub async fn init_storage(
    transport: Arc<dyn mistlib_core::transport::Transport>,
    max_capacity_bytes: u64,
    cache_dir: Option<std::path::PathBuf>,
) {
    let registry = WantRegistry::new();
    WANT_REGISTRY.set(registry.clone()).unwrap_or(());

    let base_dir = cache_dir.unwrap_or_else(|| std::env::temp_dir().join("mistlib_blocks"));
    let store = NativeBlockStore::new(base_dir)
        .await
        .expect("Failed to init block store");

    let resolver = NativePeerResolver::new(transport, registry, 5000);
    let storage = P2PStorage::new(store, resolver, max_capacity_bytes);

    STORAGE.set(Arc::new(storage)).unwrap_or(());
}

fn have_chunk_count(data_len: usize) -> Option<u16> {
    let chunk_count = data_len.div_ceil(resolver::HAVE_CHUNK_SIZE);
    if chunk_count > u16::MAX as usize {
        None
    } else {
        Some(chunk_count as u16)
    }
}

fn build_have_payload(cid: &str, data: &[u8], chunk_index: u16, total_chunks: u16) -> Vec<u8> {
    if total_chunks <= 1 {
        return resolver::build_have_message(cid, data);
    }

    let start = (chunk_index as usize) * resolver::HAVE_CHUNK_SIZE;
    let end = ((chunk_index as usize + 1) * resolver::HAVE_CHUNK_SIZE).min(data.len());
    resolver::build_have_chunk_message(cid, chunk_index, total_chunks, &data[start..end])
}

pub async fn handle_want(from: mistlib_core::types::NodeId, cid: String) {
    if let Some(storage) = STORAGE.get() {
        use mistlib_core::types::DeliveryMethod;

        let block = storage.get_block(&cid).await.ok().flatten();

        if let Some(data) = block {
            if let Some(ctx) = crate::engine::ENGINE.get_context().await {
                let Some(total_chunks) = have_chunk_count(data.len()) else {
                    tracing::warn!(
                        "Storage: refusing to serve oversized block {} ({} bytes, {} chunks)",
                        cid,
                        data.len(),
                        data.len().div_ceil(resolver::HAVE_CHUNK_SIZE)
                    );
                    return;
                };

                if total_chunks <= 1 {
                    let msg = build_have_payload(&cid, &data, 0, total_chunks);
                    let _ = ctx
                        .transport
                        .send(
                            &from,
                            bytes::Bytes::from(msg),
                            DeliveryMethod::ReliableOrdered,
                        )
                        .await;
                } else {
                    for chunk_index in 0..total_chunks {
                        let msg = build_have_payload(&cid, &data, chunk_index, total_chunks);

                        let _ = ctx
                            .transport
                            .send(
                                &from,
                                bytes::Bytes::from(msg),
                                DeliveryMethod::ReliableOrdered,
                            )
                            .await;

                        if chunk_index % 8 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                }

                tracing::debug!(
                    "Storage: served `have` for {} to {} ({} bytes, {} chunks)",
                    cid,
                    from.0,
                    data.len(),
                    total_chunks.max(1)
                );
            }
        }
    }
}

pub async fn handle_query(from: mistlib_core::types::NodeId, cid: String) {
    if let Some(storage) = STORAGE.get() {
        use mistlib_core::types::DeliveryMethod;

        let block_exists = storage.get_block(&cid).await.ok().flatten().is_some();

        if block_exists {
            let msg = resolver::build_have_status_message(&cid);
            if let Some(ctx) = crate::engine::ENGINE.get_context().await {
                let _ = ctx
                    .transport
                    .send(
                        &from,
                        bytes::Bytes::from(msg),
                        DeliveryMethod::ReliableOrdered,
                    )
                    .await;
            }
        }
    }
}

pub fn handle_have_status(from: mistlib_core::types::NodeId, cid: String) {
    if let Some(registry) = WANT_REGISTRY.get() {
        registry.register_peer(&cid, from);
    }
}

pub fn handle_have(cid: String, data: Vec<u8>) {
    if let Some(registry) = WANT_REGISTRY.get() {
        registry.fulfill(&cid, data);
    }
}

pub fn handle_have_chunk(cid: String, chunk_index: u16, chunk_total: u16, data: Vec<u8>) {
    if let Some(registry) = WANT_REGISTRY.get() {
        registry.fulfill_chunk(&cid, chunk_index, chunk_total, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_have_payload_uses_single_message() {
        let data = b"small";
        let total = have_chunk_count(data.len()).expect("small block should fit");
        let msg = build_have_payload("cid-small", data, 0, total);
        let parsed = resolver::parse_have_message(&msg).expect("single HAVE should parse");

        assert_eq!(total, 1);
        assert_eq!(parsed.0, "cid-small");
        assert_eq!(parsed.1, data);
    }

    #[test]
    fn one_mib_have_payload_is_split_into_datachannel_safe_chunks() {
        let data = vec![7u8; 1024 * 1024];
        let total = have_chunk_count(data.len()).expect("1MiB block should fit");

        assert_eq!(
            total as usize,
            data.len().div_ceil(resolver::HAVE_CHUNK_SIZE)
        );
        assert!(total > 1, "1MiB block must not be sent as one HAVE frame");

        let mut reassembled = Vec::with_capacity(data.len());
        for chunk_index in 0..total {
            let msg = build_have_payload("cid-large", &data, chunk_index, total);
            assert_ne!(msg[0], resolver::MSG_HAVE);

            let (cid, parsed_index, parsed_total, payload) =
                resolver::parse_have_chunk_message(&msg).expect("chunk HAVE should parse");
            assert_eq!(cid, "cid-large");
            assert_eq!(parsed_index, chunk_index);
            assert_eq!(parsed_total, total);
            assert!(payload.len() <= resolver::HAVE_CHUNK_SIZE);
            reassembled.extend_from_slice(&payload);
        }

        assert_eq!(reassembled, data);
    }
}
