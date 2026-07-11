use super::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;

struct MemBlockStore {
    blocks: StdMutex<HashMap<String, Vec<u8>>>,
}

impl MemBlockStore {
    fn new() -> Self {
        Self {
            blocks: StdMutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BlockStore for MemBlockStore {
    async fn store_block(&self, cid: &str, data: &[u8]) -> Result<()> {
        self.blocks
            .lock()
            .unwrap()
            .insert(cid.to_string(), data.to_vec());
        Ok(())
    }
    async fn load_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.lock().unwrap().get(cid).cloned())
    }
    async fn delete_block(&self, cid: &str) -> Result<()> {
        self.blocks.lock().unwrap().remove(cid);
        Ok(())
    }
}

/// Like `MemBlockStore`, but appends a `"store:<cid>"`/`"delete:<cid>"` entry
/// to a shared log on every call, so tests can assert about *when* an
/// eviction happened relative to other block writes (not just the end state).
struct LoggingBlockStore {
    blocks: StdMutex<HashMap<String, Vec<u8>>>,
    log: StdMutex<Vec<String>>,
}

impl LoggingBlockStore {
    fn new() -> Self {
        Self {
            blocks: StdMutex::new(HashMap::new()),
            log: StdMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl BlockStore for LoggingBlockStore {
    async fn store_block(&self, cid: &str, data: &[u8]) -> Result<()> {
        self.log.lock().unwrap().push(format!("store:{cid}"));
        self.blocks
            .lock()
            .unwrap()
            .insert(cid.to_string(), data.to_vec());
        Ok(())
    }
    async fn load_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.blocks.lock().unwrap().get(cid).cloned())
    }
    async fn delete_block(&self, cid: &str) -> Result<()> {
        self.log.lock().unwrap().push(format!("delete:{cid}"));
        self.blocks.lock().unwrap().remove(cid);
        Ok(())
    }
}

struct NullResolver;

#[async_trait]
impl PeerResolver for NullResolver {
    async fn resolve_block(&self, _cid: &str) -> Option<Vec<u8>> {
        None
    }
}

/// A future that returns `Pending` exactly once, allowing sibling futures
/// driven by the same `FuturesUnordered` to make progress before this one
/// resumes. Used to deterministically expose in-flight overlap without
/// relying on wall-clock timers (which are unavailable under the busy-loop
/// `block_on` and would be flaky).
struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            std::task::Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

/// A `PeerResolver` that serves blocks from a seeded map while recording the
/// maximum number of resolves that were ever in flight simultaneously.
///
/// In a client/server model each block is fetched in a single serialized
/// request stream, so concurrency would stay at 1. The P2P engine fans the
/// chunk requests out across peers in parallel, so the observed peak should
/// rise to the engine's concurrency window.
struct CountingResolver {
    blocks: HashMap<String, Vec<u8>>,
    active: StdMutex<usize>,
    max_active: StdMutex<usize>,
}

impl CountingResolver {
    fn new(blocks: HashMap<String, Vec<u8>>) -> Self {
        Self {
            blocks,
            active: StdMutex::new(0),
            max_active: StdMutex::new(0),
        }
    }
}

#[async_trait]
impl PeerResolver for CountingResolver {
    async fn resolve_block(&self, cid: &str) -> Option<Vec<u8>> {
        {
            let mut active = self.active.lock().unwrap();
            *active += 1;
            let mut max = self.max_active.lock().unwrap();
            if *active > *max {
                *max = *active;
            }
        }
        // Yield repeatedly so any concurrently-issued sibling resolves get a
        // chance to enter this critical section before we finish.
        for _ in 0..16 {
            YieldOnce(false).await;
        }
        let data = self.blocks.get(cid).cloned();
        *self.active.lock().unwrap() -= 1;
        data
    }
}

fn block_on<F: std::future::Future<Output = T>, T>(f: F) -> T {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    let mut f = Box::pin(f);
    fn raw_waker_clone() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(|_| raw_waker_clone(), |_| {}, |_| {}, |_| {}),
        )
    }
    let waker = unsafe { Waker::from_raw(raw_waker_clone()) };
    let mut cx = Context::from_waker(&waker);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn test_add_get_cycle() {
    block_on(async {
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let data = b"Hello modular storage!";
        let root = engine.add("test.txt", data).await.unwrap();
        let retrieved = engine.get(&root).await.unwrap();
        assert_eq!(data.to_vec(), retrieved);
    });
}

#[test]
fn test_multichunk_deduplication() {
    block_on(async {
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let chunk_size = 1024 * 1024;
        let data = vec![0u8; chunk_size * 2];
        let root = engine.add("zeros.bin", &data).await.unwrap();
        let retrieved = engine.get(&root).await.unwrap();
        assert_eq!(data, retrieved);

        assert_eq!(engine.manager.lock().unwrap().block_count(), 2);
    });
}

#[test]
fn test_blocks_are_fetched_from_peers_in_parallel() {
    block_on(async {
        // Seed an engine and capture every block it produced (manifest + chunks).
        let seeder = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            64 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let chunk_count = 8usize;
        let data = vec![7u8; CHUNK_SIZE * chunk_count];
        let root = seeder.add("parallel.bin", &data).await.unwrap();
        let seeded: HashMap<String, Vec<u8>> = seeder.store.blocks.lock().unwrap().clone();

        // A fresh engine with an empty store must pull everything from peers.
        let resolver = CountingResolver::new(seeded);
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            resolver,
            64 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let retrieved = engine.get(&root).await.unwrap();

        // Correctness: parallel fetch + reordering still yields the exact bytes.
        assert_eq!(data, retrieved);

        // Efficiency: chunk fetches overlapped instead of running one-at-a-time
        // (the client/server baseline). The engine caps concurrency at 4, and
        // with 8 chunks the window should fill completely.
        let peak = *engine.resolver.max_active.lock().unwrap();
        assert!(
            peak >= 4,
            "expected parallel peer fetches (peak >= 4), observed peak {peak}"
        );
        assert!(
            peak > 1,
            "fetches ran sequentially like client/server, peak {peak}"
        );
    });
}

/// A resolver simulating several seeder nodes that each hold the *full* content.
/// Incoming block requests are spread across the seeders round-robin, and each
/// seeder serves the block from its own copy while recording how many it served.
struct MultiSeederResolver {
    /// One block map per seeder node — all identical (every node has everything).
    seeders: Vec<HashMap<String, Vec<u8>>>,
    /// Number of blocks each seeder has served so far.
    served: StdMutex<Vec<usize>>,
    /// Round-robin cursor over the seeders.
    next: StdMutex<usize>,
}

impl MultiSeederResolver {
    fn new(seeder_count: usize, blocks: HashMap<String, Vec<u8>>) -> Self {
        Self {
            seeders: vec![blocks; seeder_count],
            served: StdMutex::new(vec![0; seeder_count]),
            next: StdMutex::new(0),
        }
    }
}

#[async_trait]
impl PeerResolver for MultiSeederResolver {
    async fn resolve_block(&self, cid: &str) -> Option<Vec<u8>> {
        let idx = {
            let mut next = self.next.lock().unwrap();
            let idx = *next % self.seeders.len();
            *next += 1;
            idx
        };
        // Serve from the chosen seeder's own copy and record the hit.
        let data = self.seeders[idx].get(cid).cloned();
        if data.is_some() {
            self.served.lock().unwrap()[idx] += 1;
        }
        data
    }
}

#[test]
fn test_chunks_are_served_distributed_across_seeders() {
    block_on(async {
        // Produce a multi-chunk file and capture all of its blocks.
        let origin = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            64 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let chunk_count = 9usize;
        // Distinct content per chunk so each maps to a unique CID (no dedup).
        let mut data = Vec::with_capacity(CHUNK_SIZE * chunk_count);
        for i in 0..chunk_count {
            data.extend(std::iter::repeat_n(i as u8, CHUNK_SIZE));
        }
        let root = origin.add("shared.bin", &data).await.unwrap();
        let content: HashMap<String, Vec<u8>> = origin.store.blocks.lock().unwrap().clone();

        // Three seeder nodes each hold the complete content.
        let seeder_count = 3;
        let resolver = MultiSeederResolver::new(seeder_count, content);
        let downloader = StorageEngine::new(
            MemBlockStore::new(),
            resolver,
            64 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );

        // The downloading node retrieves the file purely from the seeders.
        let retrieved = downloader.get(&root).await.unwrap();
        assert_eq!(
            data, retrieved,
            "reassembled content must match the original"
        );

        // Each seeder must have contributed its share — no single node served
        // everything (which would be the centralized client/server case).
        let served = downloader.resolver.served.lock().unwrap().clone();
        let total: usize = served.iter().sum();
        assert_eq!(
            total,
            chunk_count + 1,
            "every block (chunks + manifest) is served exactly once: {served:?}"
        );
        assert!(
            served.iter().all(|&n| n > 0),
            "load was not distributed; some seeder served nothing: {served:?}"
        );
    });
}

#[test]
fn test_get_block_touches_lru_entry() {
    block_on(async {
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            15,
            None,
            SpatialPolicy::default(),
        );
        engine.store.store_block("a", b"0123456789").await.unwrap();
        engine.store.store_block("b", b"0123456789").await.unwrap();
        {
            let mut mgr = engine.manager.lock().unwrap();
            mgr.track_block("a", 10, None);
            mgr.track_block("b", 10, None);
        }

        // "a" is the oldest entry; reading it through get_block() must count
        // as a fresh access (bug: it used to bypass StorageManager entirely),
        // or it would be picked as the eviction victim below instead of "b".
        assert!(engine.get_block("a").await.unwrap().is_some());

        let victims = engine.manager.lock().unwrap().eviction_candidates();
        assert_eq!(
            victims,
            vec!["b".to_string()],
            "get_block() must touch the LRU entry it reads"
        );
    });
}

#[test]
fn test_add_enforces_capacity_before_finishing_large_file() {
    block_on(async {
        // Capacity for ~5 chunks. Seed one old chunk, then add a 12-chunk file:
        // capacity is exceeded partway through, long before the last chunk is
        // written. The old block must be evicted while later chunks of the new
        // file are still being stored, not deferred until the whole 12MB file
        // has landed (the pre-fix behavior, which only enforced once at the end).
        let cap = 5 * CHUNK_SIZE as u64;
        let engine = StorageEngine::new(
            LoggingBlockStore::new(),
            NullResolver,
            cap,
            None,
            SpatialPolicy::default(),
        );

        let old_data = vec![9u8; CHUNK_SIZE];
        let old_cid = compute_cid(&old_data, MULTICODEC_RAW);
        engine.add("old.bin", &old_data).await.unwrap();

        let chunk_count = 12usize;
        let mut new_data = Vec::with_capacity(CHUNK_SIZE * chunk_count);
        for i in 0..chunk_count {
            new_data.extend(std::iter::repeat_n(i as u8, CHUNK_SIZE));
        }
        let last_chunk = &new_data[(chunk_count - 1) * CHUNK_SIZE..];
        let last_chunk_cid = compute_cid(last_chunk, MULTICODEC_RAW);

        engine.add("new.bin", &new_data).await.unwrap();

        let log = engine.store.log.lock().unwrap();
        let evict_pos = log.iter().position(|e| e == &format!("delete:{old_cid}"));
        let last_store_pos = log
            .iter()
            .position(|e| e == &format!("store:{last_chunk_cid}"));

        assert!(evict_pos.is_some(), "old block was never evicted: {log:?}");
        assert!(
            last_store_pos.is_some(),
            "last chunk was never stored: {log:?}"
        );
        assert!(
            evict_pos.unwrap() < last_store_pos.unwrap(),
            "capacity enforcement was deferred until after the whole file was \
             written instead of running mid-loop: {log:?}"
        );
    });
}

/// A `SelfPositionSource` that always reports the same fixed list of
/// positions, standing in for whatever native/wasm derives from its session
/// registry (SPEC-15).
struct FixedPositionSource {
    positions: Vec<Vector3>,
}

impl FixedPositionSource {
    fn new(positions: Vec<Vector3>) -> Self {
        Self { positions }
    }
}

#[async_trait]
impl SelfPositionSource for FixedPositionSource {
    async fn self_positions(&self) -> Vec<Vector3> {
        self.positions.clone()
    }
}

#[test]
fn test_add_auto_tags_blocks_with_first_self_position() {
    block_on(async {
        let source: Arc<dyn SelfPositionSource> = Arc::new(FixedPositionSource::new(vec![
            Vector3::new(1.0, 2.0, 3.0),
            Vector3::new(9.0, 9.0, 9.0),
        ]));
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            Some(source),
            SpatialPolicy::default(),
        );

        let data = b"auto-tag me";
        let root_cid = engine.add("auto.bin", data).await.unwrap();
        let chunk_cid = compute_cid(data, MULTICODEC_RAW);

        let mgr = engine.manager.lock().unwrap();
        assert_eq!(
            mgr.peek_position(&chunk_cid),
            Some(Vector3::new(1.0, 2.0, 3.0)),
            "chunk must be auto-tagged with the *first* self position, not the second"
        );
        assert_eq!(
            mgr.peek_position(&root_cid),
            Some(Vector3::new(1.0, 2.0, 3.0)),
            "manifest must be auto-tagged too"
        );
    });
}

#[test]
fn test_add_at_explicit_position_overrides_auto_tag() {
    block_on(async {
        let source: Arc<dyn SelfPositionSource> =
            Arc::new(FixedPositionSource::new(vec![Vector3::new(1.0, 2.0, 3.0)]));
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            Some(source),
            SpatialPolicy::default(),
        );

        let data = b"explicit position";
        let explicit = Vector3::new(42.0, 0.0, 0.0);
        let root_cid = engine
            .add_at("explicit.bin", data, Some(explicit))
            .await
            .unwrap();

        let mgr = engine.manager.lock().unwrap();
        assert_eq!(mgr.peek_position(&root_cid), Some(explicit));
    });
}

#[test]
fn test_resolve_or_fetch_auto_tags_with_self_position() {
    block_on(async {
        let seeder = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            64 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        let data = b"peer content";
        let root = seeder.add("peer.bin", data).await.unwrap();
        let seeded: HashMap<String, Vec<u8>> = seeder.store.blocks.lock().unwrap().clone();

        let resolver = CountingResolver::new(seeded);
        let source: Arc<dyn SelfPositionSource> =
            Arc::new(FixedPositionSource::new(vec![Vector3::new(5.0, 5.0, 5.0)]));
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            resolver,
            64 * 1024 * 1024,
            Some(source),
            SpatialPolicy::default(),
        );

        let retrieved = engine.get(&root).await.unwrap();
        assert_eq!(retrieved, data.to_vec());

        let chunk_cid = compute_cid(data, MULTICODEC_RAW);
        let mgr = engine.manager.lock().unwrap();
        assert_eq!(
            mgr.peek_position(&chunk_cid),
            Some(Vector3::new(5.0, 5.0, 5.0)),
            "block fetched from a peer must be auto-tagged with the self position"
        );
        assert_eq!(mgr.peek_position(&root), Some(Vector3::new(5.0, 5.0, 5.0)));
    });
}

#[test]
fn test_enforce_capacity_limit_prefers_spatial_eviction_when_source_present() {
    block_on(async {
        // Comfortably fits "near" + "far" (chunk + tiny manifest each); the
        // third block pushes usage past the cap and forces an eviction.
        let cap = 3 * CHUNK_SIZE as u64;
        let source: Arc<dyn SelfPositionSource> =
            Arc::new(FixedPositionSource::new(vec![Vector3::new(0.0, 0.0, 0.0)]));
        let engine = StorageEngine::new(
            LoggingBlockStore::new(),
            NullResolver,
            cap,
            Some(source),
            SpatialPolicy::default(), // retention_radius = 100.0
        );

        // "near" is added first (older, so plain LRU would pick it first) but
        // tagged right next to self (distance coefficient ~1.0). "far" is
        // younger but tagged far beyond the retention radius (huge distance
        // coefficient), so spatial eviction must pick "far" despite it being
        // the newer block.
        let near_data = vec![1u8; CHUNK_SIZE];
        let near_chunk_cid = compute_cid(&near_data, MULTICODEC_RAW);
        engine
            .add_at("near.bin", &near_data, Some(Vector3::new(1.0, 0.0, 0.0)))
            .await
            .unwrap();

        let far_data = vec![2u8; CHUNK_SIZE];
        let far_chunk_cid = compute_cid(&far_data, MULTICODEC_RAW);
        engine
            .add_at("far.bin", &far_data, Some(Vector3::new(10_000.0, 0.0, 0.0)))
            .await
            .unwrap();

        let trigger_data = vec![3u8; CHUNK_SIZE];
        engine.add("trigger.bin", &trigger_data).await.unwrap();

        let log = engine.store.log.lock().unwrap();
        assert!(
            log.iter().any(|e| e == &format!("delete:{far_chunk_cid}")),
            "the block tagged far beyond the retention radius must be evicted: {log:?}"
        );
        assert!(
            !log.iter().any(|e| e == &format!("delete:{near_chunk_cid}")),
            "the block tagged near self must be protected from eviction: {log:?}"
        );
    });
}

#[test]
fn test_run_decay_sweep_deletes_and_untracks_far_blocks() {
    block_on(async {
        let source: Arc<dyn SelfPositionSource> =
            Arc::new(FixedPositionSource::new(vec![Vector3::new(0.0, 0.0, 0.0)]));
        let spatial = SpatialPolicy {
            retention_radius: 10.0,
            decay_max_probability: 1.0,
        };
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            Some(source),
            spatial,
        );

        let data = b"far away block";
        let root_cid = engine
            .add_at("far.bin", data, Some(Vector3::new(1000.0, 0.0, 0.0)))
            .await
            .unwrap();

        // Both the chunk and the manifest are tagged at (1000,0,0), well
        // beyond 4*retention_radius (40.0), so with max_probability = 1.0
        // both must decay.
        let removed = engine.run_decay_sweep().await;
        assert_eq!(removed, 2, "chunk + manifest must both decay");
        assert_eq!(engine.manager.lock().unwrap().block_count(), 0);
        assert!(engine.store.load_block(&root_cid).await.unwrap().is_none());
    });
}

#[test]
fn test_run_decay_sweep_returns_zero_without_position_source() {
    block_on(async {
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            None,
            SpatialPolicy::default(),
        );
        engine
            .add_at("x.bin", b"data", Some(Vector3::new(999.0, 0.0, 0.0)))
            .await
            .unwrap();
        assert_eq!(engine.run_decay_sweep().await, 0);
    });
}

#[test]
fn test_run_decay_sweep_returns_zero_when_source_reports_no_positions() {
    block_on(async {
        let source: Arc<dyn SelfPositionSource> = Arc::new(FixedPositionSource::new(vec![]));
        let engine = StorageEngine::new(
            MemBlockStore::new(),
            NullResolver,
            10 * 1024 * 1024,
            Some(source),
            SpatialPolicy::default(),
        );
        engine
            .add_at("x.bin", b"data", Some(Vector3::new(999.0, 0.0, 0.0)))
            .await
            .unwrap();
        assert_eq!(engine.run_decay_sweep().await, 0);
    });
}
