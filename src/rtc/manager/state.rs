use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

type ChatHandler = Arc<dyn Fn(String, String) + Send + Sync>;
type DataHandler = Arc<dyn Fn(String, Vec<u8>) + Send + Sync>;
type PeerHandler = Arc<dyn Fn(String) + Send + Sync>;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PeerRole {
    Client,
    Server,
}

impl PeerRole {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            PeerRole::Client => "client",
            PeerRole::Server => "server",
        }
    }

    pub(super) fn from_str(s: &str) -> Self {
        match s {
            "server" => PeerRole::Server,
            _ => PeerRole::Client,
        }
    }
}

pub(super) struct RTCManagerInner {
    pub(super) self_id: String,
    pub(super) self_role: PeerRole,
    pub(super) peers: RwLock<HashSet<String>>,
    pub(super) peer_roles: RwLock<HashMap<String, PeerRole>>,

    pub(super) chat_handlers: RwLock<Vec<ChatHandler>>,
    pub(super) tunnel_msg_handlers: RwLock<Vec<DataHandler>>,
    pub(super) stdio_msg_handlers: RwLock<Vec<DataHandler>>,
    pub(super) tunnel_open_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) stdio_open_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) tunnel_close_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) stdio_close_handlers: RwLock<Vec<PeerHandler>>,
    pub(super) peer_conn_handlers: RwLock<Vec<PeerHandler>>,
}

impl RTCManagerInner {
    pub(super) fn new(self_id: String, self_role: PeerRole) -> Self {
        Self {
            self_id,
            self_role,
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
        }
    }
}
