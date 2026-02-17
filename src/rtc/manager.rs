#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::signal::{SignalClient, SignalMessage};

use super::remote_peer::{next_msg_id, RemotePeer};
use super::router::SignalingRouter;

#[derive(Debug, Clone, PartialEq)]
pub enum PeerRole {
    Client,
    Server,
}

impl PeerRole {
    pub fn as_str(&self) -> &str {
        match self {
            PeerRole::Client => "client",
            PeerRole::Server => "server",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "server" => PeerRole::Server,
            _ => PeerRole::Client,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PeerListMessage {
    #[serde(rename = "type")]
    msg_type: String,
    peer_ids: Vec<String>,
}

#[derive(Clone)]
pub struct RTCManagerHandle {
    inner: Arc<RTCManagerInner>,
}

struct RTCManagerInner {
    sig: Arc<SignalClient>,
    self_id: String,
    room_id: String,
    self_role: PeerRole,
    peers: RwLock<HashMap<String, Arc<RemotePeer>>>,
    router: RwLock<Option<Arc<SignalingRouter>>>,

    chat_handlers: RwLock<Vec<Arc<dyn Fn(String, String) + Send + Sync>>>,
    tunnel_msg_handlers: RwLock<Vec<Arc<dyn Fn(String, Vec<u8>) + Send + Sync>>>,
    stdio_msg_handlers: RwLock<Vec<Arc<dyn Fn(String, Vec<u8>) + Send + Sync>>>,
    tunnel_open_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    stdio_open_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    tunnel_close_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    stdio_close_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    peer_conn_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
}

impl RTCManagerHandle {
    pub async fn new(
        sig: Arc<SignalClient>,
        self_id: String,
        room_id: String,
        is_server: bool,
    ) -> Self {
        let role = if is_server {
            PeerRole::Server
        } else {
            PeerRole::Client
        };

        let handle = Self {
            inner: Arc::new(RTCManagerInner {
                sig,
                self_id: self_id.clone(),
                room_id: room_id.clone(),
                self_role: role,
                peers: RwLock::new(HashMap::new()),
                router: RwLock::new(None),
                chat_handlers: RwLock::new(Vec::new()),
                tunnel_msg_handlers: RwLock::new(Vec::new()),
                stdio_msg_handlers: RwLock::new(Vec::new()),
                tunnel_open_handlers: RwLock::new(Vec::new()),
                stdio_open_handlers: RwLock::new(Vec::new()),
                tunnel_close_handlers: RwLock::new(Vec::new()),
                stdio_close_handlers: RwLock::new(Vec::new()),
                peer_conn_handlers: RwLock::new(Vec::new()),
            }),
        };

        let handle_relay = handle.clone();
        let handle_target = handle.clone();
        let router = SignalingRouter::new(
            self_id.clone(),
            Arc::new(move |msg| {
                let h = handle_relay.clone();
                tokio::spawn(async move { h.relay_signal(msg).await });
            }),
            Arc::new(move |msg| {
                let h = handle_target.clone();
                tokio::spawn(async move { h.process_signal(msg).await });
            }),
        );
        *handle.inner.router.write().await = Some(router);

        let h = handle.clone();
        handle
            .inner
            .sig
            .on_message(move |msg| {
                let h = h.clone();
                tokio::spawn(async move {
                    if let Some(router) = h.inner.router.read().await.as_ref() {
                        router.receive(msg).await;
                    }
                });
            })
            .await;

        let h = handle.clone();
        handle
            .inner
            .sig
            .on_reconnect_handler(move || {
                let h = h.clone();
                tokio::spawn(async move { h.request_all_peers().await });
            })
            .await;

        handle
    }

    pub fn self_id(&self) -> &str {
        &self.inner.self_id
    }

    pub fn room_id(&self) -> &str {
        &self.inner.room_id
    }

    pub async fn self_role(&self) -> String {
        self.inner.self_role.as_str().to_string()
    }

    async fn get_or_create_peer(&self, peer_id: &str) -> Arc<RemotePeer> {
        let mut peers = self.inner.peers.write().await;
        if let Some(p) = peers.get(peer_id) {
            return p.clone();
        }
        let p = RemotePeer::new(peer_id.to_string());
        peers.insert(peer_id.to_string(), p.clone());
        p
    }

    pub async fn process_signal(&self, msg: SignalMessage) {
        let peer = self.get_or_create_peer(&msg.sender_id).await;

        if let Some(ref role) = msg.role {
            if !role.is_empty() {
                peer.set_role(PeerRole::from_str(role)).await;
            }
        }

        let signaling_stable = peer.signaling_stable().await;
        let conn_active = peer.conn_active().await;
        let has_pc = peer.has_pc().await;

        match msg.msg_type.as_str() {
            "Request" => {
                if signaling_stable || conn_active {
                    return;
                }
                if self.inner.self_id < msg.sender_id {
                    if let Err(e) = peer.start_offer(self).await {
                        error!("[{}] Failed to start offer: {}", msg.sender_id, e);
                    }
                }
            }
            "offer" => {
                if signaling_stable && conn_active {
                    return;
                }
                if !signaling_stable && has_pc {
                    if self.inner.self_id < msg.sender_id {
                        debug!(
                            "[{}] Glare: I am winner (Smaller ID), ignoring incoming offer",
                            msg.sender_id
                        );
                        return;
                    }
                    debug!(
                        "[{}] Glare: I am polite (Larger ID), yielding to their offer",
                        msg.sender_id
                    );
                }
                if let Some(ref data) = msg.data {
                    if let Err(e) = peer.handle_offer(self, data).await {
                        error!("[{}] Failed to handle offer: {}", msg.sender_id, e);
                    }
                }
            }
            "answer" => {
                if let Some(ref data) = msg.data {
                    if let Err(e) = peer.handle_answer(data).await {
                        error!("[{}] Failed to handle answer: {}", msg.sender_id, e);
                    }
                }
            }
            "candidate" => {
                if let Some(ref data) = msg.data {
                    if let Err(e) = peer.handle_candidate(data).await {
                        error!("[{}] Failed to handle candidate: {}", msg.sender_id, e);
                    }
                }
            }
            "peer_list" => {
                if let Some(ref data) = msg.data {
                    if let Ok(pl) = serde_json::from_str::<PeerListMessage>(data) {
                        self.handle_peer_list(&pl.peer_ids).await;
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn relay_signal(&self, mut msg: SignalMessage) {
        if let Some(ref mut hops) = msg.hops {
            *hops -= 1;
            if *hops < 0 {
                return;
            }
        } else {
            return;
        }

        let data = match serde_json::to_vec(&msg) {
            Ok(d) => d,
            Err(_) => return,
        };

        let peers = self.inner.peers.read().await;

        if let Some(ref receiver_id) = msg.receiver_id {
            if !receiver_id.is_empty() {
                if let Some(p) = peers.get(receiver_id.as_str()) {
                    if p.send_signal_relay(&data).await.is_ok() {
                        return;
                    }
                }
            }
        }

        for (id, p) in peers.iter() {
            let sender = &msg.sender_id;
            let receiver = msg.receiver_id.as_deref().unwrap_or("");
            if id != sender && id != receiver {
                let _ = p.send_signal_relay(&data).await;
            }
        }
    }

    pub async fn send_signal(&self, mut msg: SignalMessage) {
        if msg.msg_id.is_none() || msg.msg_id.as_deref() == Some("") {
            msg.msg_id = Some(next_msg_id(&self.inner.self_id));
        }
        if msg.hops.is_none() || msg.hops == Some(0) {
            msg.hops = Some(5);
        }

        self.relay_signal(msg.clone()).await;

        if msg.sender_id == self.inner.self_id {
            let _ = self.inner.sig.send(msg).await;
        }
    }

    pub async fn broadcast_peer_list(&self, target_peer_id: &str) {
        let peers = self.inner.peers.read().await;
        let ids: Vec<String> = peers
            .keys()
            .filter(|id| id.as_str() != target_peer_id)
            .cloned()
            .collect();
        drop(peers);

        if ids.is_empty() {
            return;
        }

        let pl = PeerListMessage {
            msg_type: "peer_list".to_string(),
            peer_ids: ids,
        };
        let pl_data = serde_json::to_string(&pl).unwrap_or_default();

        self.send_signal(SignalMessage {
            msg_type: "peer_list".into(),
            data: Some(pl_data),
            sender_id: self.inner.self_id.clone(),
            receiver_id: Some(target_peer_id.to_string()),
            ..Default::default()
        })
        .await;
    }

    async fn handle_peer_list(&self, ids: &[String]) {
        for id in ids {
            if id == &self.inner.self_id {
                continue;
            }
            let exists = self.inner.peers.read().await.contains_key(id);
            if !exists {
                self.request_peer_connection(id).await;
            }
        }
    }

    pub async fn request_peer_connection(&self, peer_id: &str) {
        self.send_signal(SignalMessage {
            msg_type: "Request".into(),
            sender_id: self.inner.self_id.clone(),
            receiver_id: Some(peer_id.to_string()),
            room_id: Some(self.inner.room_id.clone()),
            role: Some(self.inner.self_role.as_str().to_string()),
            ..Default::default()
        })
        .await;
    }

    async fn request_all_peers(&self) {
        let _ = self
            .inner
            .sig
            .send(SignalMessage {
                msg_type: "Request".into(),
                sender_id: self.inner.self_id.clone(),
                room_id: Some(self.inner.room_id.clone()),
                role: Some(self.inner.self_role.as_str().to_string()),
                ..Default::default()
            })
            .await;
    }

    pub async fn handle_raw_relay(&self, data: &[u8]) {
        if let Ok(msg) = serde_json::from_slice::<SignalMessage>(data) {
            if let Some(router) = self.inner.router.read().await.as_ref() {
                router.receive(msg).await;
            }
        }
    }

    pub async fn notify_chat(&self, peer_id: &str, msg: &str) {
        let handlers = self.inner.chat_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string(), msg.to_string());
        }
    }

    pub async fn notify_tunnel_msg(&self, peer_id: &str, data: &[u8]) {
        let handlers = self.inner.tunnel_msg_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string(), data.to_vec());
        }
    }

    pub async fn notify_stdio_msg(&self, peer_id: &str, data: &[u8]) {
        let handlers = self.inner.stdio_msg_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string(), data.to_vec());
        }
    }

    pub async fn notify_tunnel_open(&self, peer_id: &str) {
        let handlers = self.inner.tunnel_open_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string());
        }
    }

    pub async fn notify_stdio_open(&self, peer_id: &str) {
        let handlers = self.inner.stdio_open_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string());
        }
    }

    pub async fn notify_tunnel_close(&self, peer_id: &str) {
        let handlers = self.inner.tunnel_close_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string());
        }
    }

    pub async fn notify_stdio_close(&self, peer_id: &str) {
        let handlers = self.inner.stdio_close_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string());
        }
    }

    pub async fn notify_peer_connected(&self, peer_id: &str) {
        let handlers = self.inner.peer_conn_handlers.read().await;
        for h in handlers.iter() {
            h(peer_id.to_string());
        }
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

    pub async fn get_peer(&self, id: &str) -> Option<Arc<RemotePeer>> {
        self.inner.peers.read().await.get(id).cloned()
    }

    pub async fn get_all_peers(&self) -> Vec<Arc<RemotePeer>> {
        self.inner.peers.read().await.values().cloned().collect()
    }

    pub async fn get_server_peers(&self) -> Vec<Arc<RemotePeer>> {
        let peers = self.inner.peers.read().await;
        let mut result = Vec::new();
        for p in peers.values() {
            if p.is_server().await {
                result.push(p.clone());
            }
        }
        result
    }

    pub async fn send_chat_to_all(&self, msg: &str) {
        let peers = self.inner.peers.read().await;
        for p in peers.values() {
            let _ = p.send_chat(msg).await;
        }
    }

    pub async fn close(&self) {
        if let Some(router) = self.inner.router.read().await.as_ref() {
            router.stop();
        }
        let peers: Vec<Arc<RemotePeer>> = {
            let mut w = self.inner.peers.write().await;
            let ps: Vec<_> = w.values().cloned().collect();
            w.clear();
            ps
        };
        for p in peers {
            p.close().await;
        }
        self.inner.sig.close().await;
    }
}

pub type RTCManager = RTCManagerHandle;
