use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::auth::{AuthDecision, AuthRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthEventSource {
    Policy,
    TrustStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthEvent {
    pub sequence: u64,
    pub timestamp_ms: u128,
    pub peer_id: String,
    pub forward_key: String,
    pub target_addr: String,
    pub proto: String,
    pub decision: AuthDecision,
    pub source: AuthEventSource,
}

#[derive(Debug, Clone)]
pub struct AuthAuditLog {
    inner: Arc<RwLock<Inner>>,
}

#[derive(Debug)]
struct Inner {
    next_sequence: u64,
    capacity: usize,
    events: VecDeque<AuthEvent>,
}

impl Default for AuthAuditLog {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

impl AuthAuditLog {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                next_sequence: 1,
                capacity,
                events: VecDeque::new(),
            })),
        }
    }

    pub async fn record(&self, req: &AuthRequest, decision: AuthDecision, source: AuthEventSource) {
        let mut inner = self.inner.write().await;
        if inner.capacity == 0 {
            return;
        }
        let event = AuthEvent {
            sequence: inner.next_sequence,
            timestamp_ms: now_ms(),
            peer_id: req.peer_id.clone(),
            forward_key: req.forward_key.clone(),
            target_addr: req.target_addr.clone(),
            proto: req.proto.clone(),
            decision,
            source,
        };
        inner.next_sequence += 1;
        inner.events.push_back(event);
        while inner.events.len() > inner.capacity {
            inner.events.pop_front();
        }
    }

    pub async fn list(&self) -> Vec<AuthEvent> {
        self.inner.read().await.events.iter().cloned().collect()
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
