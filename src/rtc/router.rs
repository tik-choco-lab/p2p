use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::signal::SignalMessage;

const CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
const CACHE_EXPIRY: Duration = Duration::from_secs(300);

pub struct SignalingRouter {
    self_id: String,
    cache: Arc<Mutex<HashMap<String, Instant>>>,
    on_relay: Arc<dyn Fn(SignalMessage) + Send + Sync>,
    on_target: Arc<dyn Fn(SignalMessage) + Send + Sync>,
    quit: tokio::sync::Notify,
}

impl SignalingRouter {
    pub fn new(
        self_id: String,
        on_relay: Arc<dyn Fn(SignalMessage) + Send + Sync>,
        on_target: Arc<dyn Fn(SignalMessage) + Send + Sync>,
    ) -> Arc<Self> {
        let router = Arc::new(Self {
            self_id,
            cache: Arc::new(Mutex::new(HashMap::new())),
            on_relay,
            on_target,
            quit: tokio::sync::Notify::new(),
        });

        let r = router.clone();
        tokio::spawn(async move { r.cleanup_loop().await });

        router
    }

    pub async fn receive(&self, msg: SignalMessage) {
        if let Some(ref msg_id) = msg.msg_id {
            if !msg_id.is_empty() {
                let mut cache = self.cache.lock().await;
                if cache.contains_key(msg_id) {
                    return;
                }
                cache.insert(msg_id.clone(), Instant::now());
            }
        }

        let receiver_id = msg.receiver_id.as_deref().unwrap_or("");

        if !receiver_id.is_empty() && receiver_id != self.self_id {
            (self.on_relay)(msg);
            return;
        }

        if receiver_id.is_empty() {
            (self.on_relay)(msg.clone());
        }
        (self.on_target)(msg);
    }

    async fn cleanup_loop(&self) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(CACHE_CLEANUP_INTERVAL) => {
                    let mut cache = self.cache.lock().await;
                    let now = Instant::now();
                    cache.retain(|_, t| now.duration_since(*t) < CACHE_EXPIRY);
                }
                _ = self.quit.notified() => { return; }
            }
        }
    }

    pub fn stop(&self) {
        self.quit.notify_one();
    }
}
