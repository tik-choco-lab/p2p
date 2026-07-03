
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
        let engine = StorageEngine::new(MemBlockStore::new(), NullResolver, 10 * 1024 * 1024);
        let data = b"Hello modular storage!";
        let root = engine.add("test.txt", data).await.unwrap();
        let retrieved = engine.get(&root).await.unwrap();
        assert_eq!(data.to_vec(), retrieved);
    });
}

#[test]
fn test_multichunk_deduplication() {
    block_on(async {
        let engine = StorageEngine::new(MemBlockStore::new(), NullResolver, 10 * 1024 * 1024);
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
        let seeder = StorageEngine::new(MemBlockStore::new(), NullResolver, 64 * 1024 * 1024);
        let chunk_count = 8usize;
        let data = vec![7u8; CHUNK_SIZE * chunk_count];
        let root = seeder.add("parallel.bin", &data).await.unwrap();
        let seeded: HashMap<String, Vec<u8>> = seeder.store.blocks.lock().unwrap().clone();

        // A fresh engine with an empty store must pull everything from peers.
        let resolver = CountingResolver::new(seeded);
        let engine = StorageEngine::new(MemBlockStore::new(), resolver, 64 * 1024 * 1024);
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
        let origin = StorageEngine::new(MemBlockStore::new(), NullResolver, 64 * 1024 * 1024);
        let chunk_count = 9usize;
        // Distinct content per chunk so each maps to a unique CID (no dedup).
        let mut data = Vec::with_capacity(CHUNK_SIZE * chunk_count);
        for i in 0..chunk_count {
            data.extend(std::iter::repeat(i as u8).take(CHUNK_SIZE));
        }
        let root = origin.add("shared.bin", &data).await.unwrap();
        let content: HashMap<String, Vec<u8>> = origin.store.blocks.lock().unwrap().clone();

        // Three seeder nodes each hold the complete content.
        let seeder_count = 3;
        let resolver = MultiSeederResolver::new(seeder_count, content);
        let downloader =
            StorageEngine::new(MemBlockStore::new(), resolver, 64 * 1024 * 1024);

        // The downloading node retrieves the file purely from the seeders.
        let retrieved = downloader.get(&root).await.unwrap();
        assert_eq!(data, retrieved, "reassembled content must match the original");

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
