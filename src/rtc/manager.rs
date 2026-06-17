use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use anyhow::{anyhow, Result};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub enum PeerRole {
    Client,
    Server,
}

impl PeerRole {
    fn as_str(&self) -> &'static str {
        match self {
            PeerRole::Client => "client",
            PeerRole::Server => "server",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "server" => PeerRole::Server,
            _ => PeerRole::Client,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum P2pPayload {
    Role { role: String },
    Chat { text: String },
    Tunnel { data: Vec<u8> },
    Stdio { data: Vec<u8> },
}

#[derive(Clone)]
pub struct RTCManagerHandle {
    inner: Arc<RTCManagerInner>,
}

struct RTCManagerInner {
    self_id: String,
    self_role: PeerRole,
    peers: RwLock<HashSet<String>>,
    peer_roles: RwLock<HashMap<String, PeerRole>>,

    chat_handlers: RwLock<Vec<Arc<dyn Fn(String, String) + Send + Sync>>>,
    tunnel_msg_handlers: RwLock<Vec<Arc<dyn Fn(String, Vec<u8>) + Send + Sync>>>,
    stdio_msg_handlers: RwLock<Vec<Arc<dyn Fn(String, Vec<u8>) + Send + Sync>>>,
    tunnel_open_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    stdio_open_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    tunnel_close_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    stdio_close_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    peer_conn_handlers: RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
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
            inner: Arc::new(RTCManagerInner {
                self_id: self_id.clone(),
                self_role: role,
                peers: RwLock::new(HashSet::new()),
                peer_roles: RwLock::new(HashMap::new()),
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
        mistlib::send_message_direct(
            peer_id.to_string(),
            data,
            mistlib::DELIVERY_RELIABLE,
        )
        .await
        .map_err(|e| anyhow!(e.to_string()))
    }

    pub async fn close(&self) {
        mistlib::clear_raw_handler();
        mistlib::leave_room();
    }
}

fn dispatch_event(
    runtime: &tokio::runtime::Handle,
    weak: &Weak<RTCManagerInner>,
    message_type: u32,
    from: String,
    data: Vec<u8>,
) {
    let Some(inner) = weak.upgrade() else {
        return;
    };

    runtime.spawn(async move {
        match message_type {
            mistlib::EVENT_JOIN => handle_join(inner, from).await,
            mistlib::EVENT_LEAVE => handle_leave(inner, from).await,
            mistlib::EVENT_RAW | mistlib::EVENT_OVERLAY => handle_payload(inner, from, data).await,
            _ => {}
        }
    });
}

async fn handle_join(inner: Arc<RTCManagerInner>, peer_id: String) {
    if peer_id == inner.self_id {
        return;
    }

    inner.peers.write().await.insert(peer_id.clone());
    notify(&inner.peer_conn_handlers, peer_id.clone()).await;
    notify(&inner.tunnel_open_handlers, peer_id.clone()).await;
    notify(&inner.stdio_open_handlers, peer_id.clone()).await;

    let data = match serde_json::to_vec(&P2pPayload::Role {
        role: inner.self_role.as_str().to_string(),
    }) {
        Ok(data) => data,
        Err(_) => return,
    };
    let _ = mistlib::send_message_direct(peer_id, data, mistlib::DELIVERY_RELIABLE).await;
}

async fn handle_leave(inner: Arc<RTCManagerInner>, peer_id: String) {
    inner.peers.write().await.remove(&peer_id);
    inner.peer_roles.write().await.remove(&peer_id);
    notify(&inner.tunnel_close_handlers, peer_id.clone()).await;
    notify(&inner.stdio_close_handlers, peer_id).await;
}

async fn handle_payload(inner: Arc<RTCManagerInner>, peer_id: String, data: Vec<u8>) {
    if peer_id == inner.self_id {
        return;
    }

    let Ok(payload) = serde_json::from_slice::<P2pPayload>(&data) else {
        return;
    };

    match payload {
        P2pPayload::Role { role } => {
            inner
                .peer_roles
                .write()
                .await
                .insert(peer_id, PeerRole::from_str(&role));
        }
        P2pPayload::Chat { text } => {
            let handlers = inner.chat_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), text.clone());
            }
        }
        P2pPayload::Tunnel { data } => {
            let handlers = inner.tunnel_msg_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), data.clone());
            }
        }
        P2pPayload::Stdio { data } => {
            let handlers = inner.stdio_msg_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), data.clone());
            }
        }
    }
}

async fn notify(
    handlers: &RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>,
    peer_id: String,
) {
    let handlers = handlers.read().await;
    for h in handlers.iter() {
        h(peer_id.clone());
    }
}

pub type RTCManager = RTCManagerHandle;
