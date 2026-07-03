use async_trait::async_trait;
use mistlib_core::storage::PeerResolver;
use mistlib_core::transport::Transport;
use mistlib_core::types::DeliveryMethod;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub const MSG_WANT: u8 = 0x01;
pub const MSG_HAVE: u8 = 0x02;
pub const MSG_QUERY: u8 = 0x03;
pub const MSG_HAVE_STATUS: u8 = 0x04;
pub const MSG_HAVE_CHUNK: u8 = 0x05;
pub const HAVE_CHUNK_SIZE: usize = 16 * 1024;

type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<Vec<u8>>>>>;
type PeerCache = Arc<Mutex<HashMap<String, Vec<mistlib_core::types::NodeId>>>>;
type PeerNotifiers = Arc<Mutex<HashMap<String, Vec<oneshot::Sender<()>>>>>;
type ChunkMap = Arc<Mutex<HashMap<String, ChunkAssembly>>>;

#[derive(Debug, Clone)]
struct ChunkAssembly {
    total_chunks: u16,
    received_chunks: u16,
    chunks: Vec<Option<Vec<u8>>>,
}

#[derive(Clone)]
pub struct WantRegistry {
    pending: PendingMap,
    peer_cache: PeerCache,
    peer_notifiers: PeerNotifiers,
    chunk_assemblies: ChunkMap,
}

impl Default for WantRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WantRegistry {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(HashMap::new())),
            peer_cache: Arc::new(Mutex::new(HashMap::new())),
            peer_notifiers: Arc::new(Mutex::new(HashMap::new())),
            chunk_assemblies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(&self, cid: &str) -> oneshot::Receiver<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(cid.to_string(), tx);
        rx
    }

    pub fn fulfill(&self, cid: &str, data: Vec<u8>) {
        self.chunk_assemblies.lock().unwrap().remove(cid);
        if let Some(tx) = self.pending.lock().unwrap().remove(cid) {
            let _ = tx.send(data);
        }
    }

    pub fn fulfill_chunk(&self, cid: &str, chunk_index: u16, chunk_total: u16, data: Vec<u8>) {
        if chunk_total == 0 || chunk_index >= chunk_total {
            return;
        }

        let mut assembled_payload: Option<Vec<u8>> = None;

        {
            let mut assemblies = self.chunk_assemblies.lock().unwrap();
            let entry = assemblies
                .entry(cid.to_string())
                .or_insert_with(|| ChunkAssembly {
                    total_chunks: chunk_total,
                    received_chunks: 0,
                    chunks: vec![None; chunk_total as usize],
                });

            if entry.total_chunks != chunk_total {
                *entry = ChunkAssembly {
                    total_chunks: chunk_total,
                    received_chunks: 0,
                    chunks: vec![None; chunk_total as usize],
                };
            }

            let slot = &mut entry.chunks[chunk_index as usize];
            if slot.is_none() {
                *slot = Some(data);
                entry.received_chunks += 1;
            }

            if entry.received_chunks == entry.total_chunks {
                let mut full = Vec::new();
                for chunk in &mut entry.chunks {
                    if let Some(part) = chunk.take() {
                        full.extend_from_slice(&part);
                    }
                }
                assembled_payload = Some(full);
                assemblies.remove(cid);
            }
        }

        if let Some(full) = assembled_payload {
            self.fulfill(cid, full);
        }
    }

    pub fn cancel(&self, cid: &str) {
        self.pending.lock().unwrap().remove(cid);
        self.peer_notifiers.lock().unwrap().remove(cid);
        self.chunk_assemblies.lock().unwrap().remove(cid);
    }

    pub fn register_peer_notifier(&self, cid: &str) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.peer_notifiers
            .lock()
            .unwrap()
            .entry(cid.to_string())
            .or_default()
            .push(tx);
        rx
    }

    pub fn register_peer(&self, cid: &str, peer_id: mistlib_core::types::NodeId) {
        {
            let mut cache = self.peer_cache.lock().unwrap();
            let peers = cache.entry(cid.to_string()).or_default();
            if !peers.contains(&peer_id) {
                peers.push(peer_id);
            }
        }

        if let Some(notifiers) = self.peer_notifiers.lock().unwrap().remove(cid) {
            for tx in notifiers {
                let _ = tx.send(());
            }
        }
    }

    pub fn get_peers(&self, cid: &str) -> Vec<mistlib_core::types::NodeId> {
        self.peer_cache
            .lock()
            .unwrap()
            .get(cid)
            .cloned()
            .unwrap_or_default()
    }
}

pub struct NativePeerResolver {
    transport: Arc<dyn Transport>,
    registry: WantRegistry,
    timeout_ms: u64,
    /// Round-robin cursor so successive chunk requests are spread across the
    /// known peers instead of repeatedly hitting one (client/server style).
    next_peer: std::sync::atomic::AtomicUsize,
}

impl NativePeerResolver {
    pub fn new(transport: Arc<dyn Transport>, registry: WantRegistry, timeout_ms: u64) -> Self {
        Self {
            transport,
            registry,
            timeout_ms,
            next_peer: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl PeerResolver for NativePeerResolver {
    async fn resolve_block(&self, cid: &str) -> Option<Vec<u8>> {
        let mut known_peers = self.registry.get_peers(cid);
        if known_peers.is_empty() {
            tracing::debug!("PeerResolver: Discovery phase for {}", cid);
            let rx_peer = self.registry.register_peer_notifier(cid);

            let query_msg = build_query_message(cid);
            let _ = self
                .transport
                .broadcast(
                    bytes::Bytes::from(query_msg),
                    DeliveryMethod::ReliableOrdered,
                )
                .await;

            let _ = tokio::time::timeout(std::time::Duration::from_millis(500), rx_peer).await;
            known_peers = self.registry.get_peers(cid);
        }

        let per_attempt = std::time::Duration::from_millis(self.timeout_ms);

        // Try each known peer in round-robin order, failing over to the next one
        // if the chosen peer does not deliver the block within the deadline. The
        // starting offset is shared across calls so concurrent chunk requests
        // spread their load across the swarm instead of hammering one node.
        if !known_peers.is_empty() {
            let start = self
                .next_peer
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            for offset in 0..known_peers.len() {
                let target = &known_peers[(start + offset) % known_peers.len()];
                let rx_data = self.registry.register(cid);

                tracing::debug!("PeerResolver: targeted WANT for {} to {}", cid, target.0);
                let mut want_msg = vec![MSG_WANT];
                want_msg.extend_from_slice(cid.as_bytes());
                let _ = self
                    .transport
                    .send(
                        target,
                        bytes::Bytes::from(want_msg),
                        DeliveryMethod::ReliableOrdered,
                    )
                    .await;

                if let Ok(Ok(data)) = tokio::time::timeout(per_attempt, rx_data).await {
                    return Some(data);
                }
                tracing::debug!(
                    "PeerResolver: peer {} did not deliver {}, failing over",
                    target.0,
                    cid
                );
            }
        }

        // No known peers, or every known peer failed: broadcast a last-resort WANT.
        tracing::debug!("PeerResolver: broadcasting fallback WANT for {}", cid);
        let rx_data = self.registry.register(cid);
        let mut want_msg = vec![MSG_WANT];
        want_msg.extend_from_slice(cid.as_bytes());
        let _ = self
            .transport
            .broadcast(
                bytes::Bytes::from(want_msg),
                DeliveryMethod::ReliableOrdered,
            )
            .await;

        match tokio::time::timeout(per_attempt, rx_data).await {
            Ok(Ok(data)) => Some(data),
            _ => {
                self.registry.cancel(cid);
                tracing::debug!("PeerResolver: failed to receive data for CID {}", cid);
                None
            }
        }
    }
}

pub fn parse_have_message(raw: &[u8]) -> Option<(String, Vec<u8>)> {
    if raw.len() < 2 || raw[0] != MSG_HAVE {
        return None;
    }
    let cid_len = raw[1] as usize;
    if raw.len() < 2 + cid_len {
        return None;
    }
    let cid = std::str::from_utf8(&raw[2..2 + cid_len]).ok()?.to_string();
    Some((cid, raw[2 + cid_len..].to_vec()))
}

pub fn build_have_message(cid: &str, data: &[u8]) -> Vec<u8> {
    let cb = cid.as_bytes();
    let mut msg = Vec::with_capacity(2 + cb.len() + data.len());
    msg.push(MSG_HAVE);
    msg.push(cb.len() as u8);
    msg.extend_from_slice(cb);
    msg.extend_from_slice(data);
    msg
}

pub fn build_have_chunk_message(
    cid: &str,
    chunk_index: u16,
    chunk_total: u16,
    data: &[u8],
) -> Vec<u8> {
    let cb = cid.as_bytes();
    let mut msg = Vec::with_capacity(6 + cb.len() + data.len());
    msg.push(MSG_HAVE_CHUNK);
    msg.push(cb.len() as u8);
    msg.extend_from_slice(&chunk_index.to_be_bytes());
    msg.extend_from_slice(&chunk_total.to_be_bytes());
    msg.extend_from_slice(cb);
    msg.extend_from_slice(data);
    msg
}

pub fn parse_have_chunk_message(raw: &[u8]) -> Option<(String, u16, u16, Vec<u8>)> {
    if raw.len() < 6 || raw[0] != MSG_HAVE_CHUNK {
        return None;
    }

    let cid_len = raw[1] as usize;
    let header_end = 6 + cid_len;
    if raw.len() < header_end {
        return None;
    }

    let chunk_index = u16::from_be_bytes([raw[2], raw[3]]);
    let chunk_total = u16::from_be_bytes([raw[4], raw[5]]);
    if chunk_total == 0 || chunk_index >= chunk_total {
        return None;
    }

    let cid = std::str::from_utf8(&raw[6..header_end]).ok()?.to_string();
    let payload = raw[header_end..].to_vec();
    Some((cid, chunk_index, chunk_total, payload))
}

pub fn parse_want_message(raw: &[u8]) -> Option<String> {
    if raw.is_empty() || raw[0] != MSG_WANT {
        return None;
    }
    std::str::from_utf8(&raw[1..]).ok().map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn have_chunk_message_roundtrips() {
        let msg = build_have_chunk_message("cid-1", 1, 3, b"payload");
        let parsed = parse_have_chunk_message(&msg).expect("chunk message should parse");
        assert_eq!(parsed.0, "cid-1");
        assert_eq!(parsed.1, 1);
        assert_eq!(parsed.2, 3);
        assert_eq!(parsed.3, b"payload");
    }

    #[tokio::test]
    async fn registry_reassembles_have_chunks() {
        let registry = WantRegistry::new();
        let rx = registry.register("cid-1");

        registry.fulfill_chunk("cid-1", 1, 3, b"bb".to_vec());
        registry.fulfill_chunk("cid-1", 0, 3, b"aa".to_vec());
        registry.fulfill_chunk("cid-1", 2, 3, b"cc".to_vec());

        let data = rx.await.expect("assembled payload should be delivered");
        assert_eq!(data, b"aabbcc");
    }

    /// Transport stub that records every targeted `send` so a test can inspect
    /// which peers received WANT requests.
    struct RecordingTransport {
        sends: Arc<Mutex<Vec<(mistlib_core::types::NodeId, Vec<u8>)>>>,
    }

    #[async_trait]
    impl Transport for RecordingTransport {
        async fn start(
            &self,
            _handler: Arc<dyn mistlib_core::transport::NetworkEventHandler>,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        async fn send(
            &self,
            node: &mistlib_core::types::NodeId,
            data: bytes::Bytes,
            _method: DeliveryMethod,
        ) -> mistlib_core::error::Result<()> {
            self.sends.lock().unwrap().push((node.clone(), data.to_vec()));
            Ok(())
        }
        async fn broadcast(
            &self,
            _data: bytes::Bytes,
            _method: DeliveryMethod,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        fn get_connection_state(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::types::ConnectionState {
            mistlib_core::types::ConnectionState::Connected
        }
        async fn connect(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        async fn disconnect(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        fn get_connected_nodes(&self) -> Vec<mistlib_core::types::NodeId> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn chunk_wants_are_distributed_across_multiple_peers() {
        use mistlib_core::storage::PeerResolver;
        use mistlib_core::types::NodeId;

        let sends = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(RecordingTransport {
            sends: sends.clone(),
        });
        let registry = WantRegistry::new();

        // Three peers all advertise the same set of chunks.
        let peers = [NodeId("peer-a".into()), NodeId("peer-b".into()), NodeId("peer-c".into())];
        let chunk_cids = ["chunk-0", "chunk-1", "chunk-2", "chunk-3", "chunk-4", "chunk-5"];
        for cid in chunk_cids {
            for peer in &peers {
                registry.register_peer(cid, peer.clone());
            }
        }

        // Short timeout: we never fulfill, so each resolve sends its WANT then
        // times out. We only care about the recorded targets.
        let resolver = NativePeerResolver::new(transport, registry, 30);

        // Resolve all chunks concurrently, mirroring the engine's parallel fan-out.
        let mut tasks = Vec::new();
        for cid in chunk_cids {
            let r = &resolver;
            tasks.push(async move { r.resolve_block(cid).await });
        }
        let _ = futures_util::future::join_all(tasks).await;

        // We never fulfill, so each chunk fails over through every known peer.
        // The load-distribution property lives in the *first* WANT each chunk
        // issues: round-robin must spread those across the swarm rather than
        // starting every request at the same node (client/server style).
        let first_target_per_cid: std::collections::BTreeMap<String, String> = {
            let mut map = std::collections::BTreeMap::new();
            for (node, data) in sends.lock().unwrap().iter() {
                if let Some(cid) = parse_want_message(data) {
                    map.entry(cid).or_insert_with(|| node.0.clone());
                }
            }
            map
        };

        assert_eq!(
            first_target_per_cid.len(),
            chunk_cids.len(),
            "every chunk issues a WANT"
        );

        let distinct_starts: std::collections::BTreeSet<_> =
            first_target_per_cid.values().cloned().collect();
        assert!(
            distinct_starts.len() > 1,
            "chunk requests all started at one peer (client/server style): {distinct_starts:?}"
        );
        // Round-robin over 3 peers for 6 chunks must start across all three.
        assert_eq!(
            distinct_starts.len(),
            peers.len(),
            "round-robin should spread initial WANTs across every known peer: {distinct_starts:?}"
        );
    }

    /// Transport that only the designated `good_peer` answers: a WANT sent to it
    /// is fulfilled through the registry, while WANTs to any other peer are
    /// silently dropped (simulating an unresponsive/dead peer).
    struct FailoverTransport {
        registry: WantRegistry,
        good_peer: mistlib_core::types::NodeId,
        block: Vec<u8>,
        sends: Arc<Mutex<Vec<mistlib_core::types::NodeId>>>,
    }

    #[async_trait]
    impl Transport for FailoverTransport {
        async fn start(
            &self,
            _handler: Arc<dyn mistlib_core::transport::NetworkEventHandler>,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        async fn send(
            &self,
            node: &mistlib_core::types::NodeId,
            data: bytes::Bytes,
            _method: DeliveryMethod,
        ) -> mistlib_core::error::Result<()> {
            self.sends.lock().unwrap().push(node.clone());
            if let Some(cid) = parse_want_message(&data) {
                if *node == self.good_peer {
                    self.registry.fulfill(&cid, self.block.clone());
                }
            }
            Ok(())
        }
        async fn broadcast(
            &self,
            _data: bytes::Bytes,
            _method: DeliveryMethod,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        fn get_connection_state(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::types::ConnectionState {
            mistlib_core::types::ConnectionState::Connected
        }
        async fn connect(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        async fn disconnect(
            &self,
            _node: &mistlib_core::types::NodeId,
        ) -> mistlib_core::error::Result<()> {
            Ok(())
        }
        fn get_connected_nodes(&self) -> Vec<mistlib_core::types::NodeId> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn unresponsive_peer_fails_over_to_a_live_peer() {
        use mistlib_core::storage::PeerResolver;
        use mistlib_core::types::NodeId;

        let registry = WantRegistry::new();
        let dead = NodeId("dead-peer".into());
        let good = NodeId("good-peer".into());
        // Order matters: round-robin starts at the dead peer, forcing a failover.
        registry.register_peer("cid-x", dead.clone());
        registry.register_peer("cid-x", good.clone());

        let sends = Arc::new(Mutex::new(Vec::new()));
        let transport = Arc::new(FailoverTransport {
            registry: registry.clone(),
            good_peer: good.clone(),
            block: b"payload-from-good-peer".to_vec(),
            sends: sends.clone(),
        });

        // Short per-attempt timeout so the dead peer's attempt fails quickly.
        let resolver = NativePeerResolver::new(transport, registry, 40);
        let data = resolver.resolve_block("cid-x").await;

        assert_eq!(
            data.as_deref(),
            Some(b"payload-from-good-peer".as_slice()),
            "download should succeed by failing over to the live peer"
        );

        let targets = sends.lock().unwrap().clone();
        assert_eq!(
            targets,
            vec![dead, good],
            "resolver should try the dead peer first, then fail over to the live one"
        );
    }
}

pub fn build_query_message(cid: &str) -> Vec<u8> {
    let mut msg = vec![MSG_QUERY];
    msg.extend_from_slice(cid.as_bytes());
    msg
}

pub fn parse_query_message(raw: &[u8]) -> Option<String> {
    if raw.is_empty() || raw[0] != MSG_QUERY {
        return None;
    }
    std::str::from_utf8(&raw[1..]).ok().map(|s| s.to_string())
}

pub fn build_have_status_message(cid: &str) -> Vec<u8> {
    let mut msg = vec![MSG_HAVE_STATUS];
    msg.extend_from_slice(cid.as_bytes());
    msg
}

pub fn parse_have_status_message(raw: &[u8]) -> Option<String> {
    if raw.is_empty() || raw[0] != MSG_HAVE_STATUS {
        return None;
    }
    std::str::from_utf8(&raw[1..]).ok().map(|s| s.to_string())
}
