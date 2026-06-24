use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardMetrics {
    pub active_conns: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Debug, Default)]
struct ForwardCounters {
    active_conns: AtomicUsize,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct ForwardRuntime {
    counters: Arc<ForwardCounters>,
    shutdown_tx: watch::Sender<bool>,
}

impl ForwardRuntime {
    pub fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            counters: Arc::new(ForwardCounters::default()),
            shutdown_tx,
        }
    }

    pub fn metrics(&self) -> ForwardMetrics {
        ForwardMetrics {
            active_conns: self.counters.active_conns.load(Ordering::Relaxed),
            bytes_in: self.counters.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.counters.bytes_out.load(Ordering::Relaxed),
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub fn cancel(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn record_conn_open(&self) {
        self.counters.active_conns.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_conn_close(&self) {
        let _ = self.counters.active_conns.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| current.checked_sub(1),
        );
    }

    pub fn record_bytes_in(&self, bytes: usize) {
        self.counters
            .bytes_in
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_bytes_out(&self, bytes: usize) {
        self.counters
            .bytes_out
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

impl Default for ForwardRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_start_at_zero() {
        let runtime = ForwardRuntime::new();

        assert_eq!(
            runtime.metrics(),
            ForwardMetrics {
                active_conns: 0,
                bytes_in: 0,
                bytes_out: 0
            }
        );
    }

    #[test]
    fn records_connections_and_bytes() {
        let runtime = ForwardRuntime::new();

        runtime.record_conn_open();
        runtime.record_conn_open();
        runtime.record_conn_close();
        runtime.record_bytes_in(10);
        runtime.record_bytes_in(5);
        runtime.record_bytes_out(7);

        assert_eq!(
            runtime.metrics(),
            ForwardMetrics {
                active_conns: 1,
                bytes_in: 15,
                bytes_out: 7
            }
        );
    }

    #[test]
    fn close_connection_saturates_at_zero() {
        let runtime = ForwardRuntime::new();

        runtime.record_conn_close();

        assert_eq!(runtime.metrics().active_conns, 0);
    }

    #[tokio::test]
    async fn cancel_notifies_subscribers() {
        let runtime = ForwardRuntime::new();
        let mut shutdown = runtime.subscribe();

        runtime.cancel();
        shutdown.changed().await.unwrap();

        assert!(*shutdown.borrow());
    }
}
