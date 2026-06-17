use std::sync::Arc;

use anyhow::{anyhow, Result};

mod event;
mod payload;
mod state;

use event::dispatch_event;
use payload::P2pPayload;
use state::{PeerRole, RTCManagerInner};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct RTCManagerHandle {
    inner: Arc<RTCManagerInner>,
}

#[allow(dead_code)]
impl RTCManagerHandle {
    pub async fn new(url: &str, self_id: String, room_id: String, is_server: bool) -> Self {
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
        mistlib::init_and_join(self_id, url.to_string(), room_id);
        handle.send_role_to_all().await;

        handle
    }

    pub fn self_id(&self) -> &str {
        &self.inner.self_id
    }

    pub async fn on_chat_message<F: Fn(String, String) + Send + Sync + 'static>(&self, f: F) {
        self.inner.chat_handlers.write().await.push(Arc::new(f));
    }

    pub async fn on_tunnel_message<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .tunnel_msg_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_stdio_message<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_msg_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_open<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .tunnel_open_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_stdio_open<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_open_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_close<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .tunnel_close_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_stdio_close<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .stdio_close_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_peer_connected<F: Fn(String) + Send + Sync + 'static>(&self, f: F) {
        self.inner
            .peer_conn_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn get_server_peers(&self) -> Vec<String> {
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

    pub async fn send_chat_to_all(&self, msg: &str) {
        let _ = self
            .send_payload("", P2pPayload::Chat { text: msg.into() })
            .await;
    }

    pub async fn send_tunnel_to(&self, peer_id: &str, data: Vec<u8>) -> Result<()> {
        self.send_payload(peer_id, P2pPayload::Tunnel { data }).await
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
