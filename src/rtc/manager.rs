use std::sync::Arc;

use anyhow::{anyhow, Result};

mod event;
mod handlers;
mod payload;
mod state;

use event::dispatch_event;
use payload::P2pPayload;
use state::{PeerRole, RTCManagerInner};

#[cfg(test)]
mod tests;

const MISTLIB_CONFIG_ENV: &str = "P2P_MISTLIB_CONFIG_JSON";

#[derive(Clone)]
pub struct RTCManagerHandle {
    inner: Arc<RTCManagerInner>,
}

#[allow(dead_code)]
impl RTCManagerHandle {
    pub async fn new(self_id: String, room_id: String, is_server: bool) -> Self {
        let role = if is_server {
            PeerRole::Server
        } else {
            PeerRole::Client
        };

        let handle = Self {
            inner: Arc::new(RTCManagerInner::new(self_id.clone(), role)),
        };

        let weak = Arc::downgrade(&handle.inner);
        let runtime = tokio::runtime::Handle::current();
        mistlib::register_raw_handler(move |message_type, from, data| {
            dispatch_event(&runtime, &weak, message_type, from, data);
        });
        let config = mistlib_config();
        let initialized = tokio::task::spawn_blocking(move || match config {
            Some(config) => mistlib::init_with_config(self_id, config.as_slice()),
            None => {
                mistlib::init(self_id, String::new());
                true
            }
        })
        .await
        .unwrap_or(false);
        if !initialized {
            tracing::warn!("mistlib config was rejected");
        }
        mistlib::join_room(room_id);
        handle.send_role_to_all().await;

        handle
    }

    pub fn self_id(&self) -> &str {
        &self.inner.self_id
    }

    pub async fn get_server_peers(&self) -> Vec<String> {
        self.get_server_peers_for("").await
    }

    pub async fn get_server_peers_for(&self, target: &str) -> Vec<String> {
        if !target.is_empty() {
            let keys = self.inner.peer_forward_keys.read().await;
            let matched = keys
                .iter()
                .filter_map(|(id, keys)| {
                    if keys.contains(target) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if !matched.is_empty() {
                return matched;
            }
        }

        let roles = self.inner.peer_roles.read().await;
        roles
            .iter()
            .filter_map(|(id, role)| {
                if *role == PeerRole::Server {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Selects a single server peer for the given target, load-balancing across
    /// all peers that advertise the target key using a per-target round-robin
    /// cursor. Peers that disconnect drop out of the advertised key set, so this
    /// also provides failover. Returns `None` when no peer is available yet.
    pub async fn select_server_peer_for(&self, target: &str) -> Option<String> {
        let mut peers = self.get_server_peers_for(target).await;
        if peers.is_empty() {
            return None;
        }
        // Deterministic ordering so the round-robin cursor is stable regardless
        // of the underlying map iteration order.
        peers.sort();
        let mut cursors = self.inner.peer_rr_cursor.write().await;
        let cursor = cursors.entry(target.to_string()).or_insert(0);
        let idx = *cursor % peers.len();
        *cursor = cursor.wrapping_add(1);
        Some(peers[idx].clone())
    }

    pub async fn publish_tunnel_target(&self, target: &str) {
        self.inner
            .self_forward_keys
            .write()
            .await
            .insert(target.to_string());
        self.send_capabilities_to_all().await;
    }

    pub async fn unpublish_tunnel_target(&self, target: &str) {
        self.inner.self_forward_keys.write().await.remove(target);
        self.send_capabilities_to_all().await;
    }

    pub async fn send_chat_to_all(&self, msg: &str) {
        let _ = self
            .send_payload("", P2pPayload::Chat { text: msg.into() })
            .await;
    }

    pub async fn send_tunnel_to(&self, peer_id: &str, data: Vec<u8>) -> Result<()> {
        self.send_payload(peer_id, P2pPayload::Tunnel { data })
            .await
    }

    pub async fn send_stdio_to(&self, peer_id: &str, data: Vec<u8>) -> Result<()> {
        self.send_payload(peer_id, P2pPayload::Stdio { data }).await
    }

    async fn send_role_to_all(&self) {
        let _ = self
            .send_payload(
                "",
                P2pPayload::Role {
                    role: self.inner.self_role.as_str().to_string(),
                },
            )
            .await;
    }

    async fn send_capabilities_to_all(&self) {
        let forwards = self
            .inner
            .self_forward_keys
            .read()
            .await
            .iter()
            .cloned()
            .collect();
        let _ = self
            .send_payload("", P2pPayload::Capabilities { forwards })
            .await;
    }

    async fn send_payload(&self, peer_id: &str, payload: P2pPayload) -> Result<()> {
        let data = serde_json::to_vec(&payload)?;
        mistlib::send_message_direct(peer_id.to_string(), data, mistlib::DELIVERY_RELIABLE)
            .await
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub async fn close(&self) {
        mistlib::clear_raw_handler();
        mistlib::leave_room();
    }
}

pub type RTCManager = RTCManagerHandle;

fn mistlib_config() -> Option<Vec<u8>> {
    std::env::var(MISTLIB_CONFIG_ENV)
        .ok()
        .map(|config| config.into_bytes())
}
