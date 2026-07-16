use std::sync::Arc;

use anyhow::{anyhow, Result};

mod event;
mod handlers;
mod payload;
mod state;

use crate::forward_args::{node_scoped_target, split_node_scope};
use event::dispatch_event;
use payload::P2pPayload;
use state::{PeerRole, RTCManagerInner};

#[cfg(test)]
mod tests;

const MISTLIB_CONFIG_ENV: &str = "P2P_MISTLIB_CONFIG_JSON";

/// A peer's proposal to establish a forward: the peer wants to reach
/// `remote_addr` on this node, multiplexed under `target`.
#[derive(Debug, Clone)]
pub struct ForwardRequestEvent {
    pub req_id: String,
    pub proto: String,
    pub remote_addr: String,
    pub target: String,
}

/// A peer's answer to a previously sent [`ForwardRequestEvent`].
#[derive(Debug, Clone)]
pub struct ForwardResponseEvent {
    pub req_id: String,
    pub target: String,
    pub accepted: bool,
}

#[derive(Clone)]
pub struct RTCManagerHandle {
    inner: Arc<RTCManagerInner>,
}

#[allow(dead_code)]
impl RTCManagerHandle {
    /// Builds a handle backed by fresh in-memory state, without touching
    /// mistlib's global native singleton (no `init`/`join_room`). Lets other
    /// modules' tests (e.g. `crate::tcp`) register/fire handlers and drive
    /// join/leave notifications deterministically.
    #[cfg(test)]
    pub(crate) fn for_test(self_id: &str) -> Self {
        Self {
            inner: Arc::new(RTCManagerInner::new(self_id.to_string(), PeerRole::Client)),
        }
    }

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
        // Single FIFO worker for EVENT_JOIN/EVENT_LEAVE/EVENT_RAW/EVENT_OVERLAY:
        // mistlib delivers events to `dispatch_event` in strict order from
        // one dispatch thread, and this worker processes each to completion
        // before the next, preserving that order end to end (see
        // `event::dispatch_event` and `event::run_payload_worker`).
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        runtime.spawn(event::run_payload_worker(weak.clone(), rx));
        mistlib::register_raw_handler(move |message_type, from, data| {
            dispatch_event(&weak, &tx, message_type, from, data);
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

    /// Roles and forward keys are retained across a peer's leave (see
    /// `event::handle_leave`) so a transient disconnect doesn't lose routing
    /// information, so this filters both branches down to peers currently
    /// present in `peers` -- otherwise a departed peer whose capabilities we
    /// still remember would keep being routed to.
    ///
    /// A node-scoped `target` (`"{base}@{peer_id}"`, see
    /// [`crate::forward_args::split_node_scope`]) pins routing to that one
    /// peer: this returns `vec![peer_id]` only if the peer is currently live
    /// AND has advertised the exact scoped target string, and returns an
    /// empty vec otherwise. A scoped target never falls back to the
    /// server-role-peer list below -- that fallback would route pinned
    /// traffic to a node the caller didn't ask for. Unscoped targets keep
    /// their original behavior: exact advertiser match, falling back to
    /// every live server-role peer when nobody advertises the target.
    pub async fn get_server_peers_for(&self, target: &str) -> Vec<String> {
        let live = self.inner.peers.read().await;

        if !target.is_empty() {
            let (_, scope) = split_node_scope(target);
            if let Some(scope) = scope {
                let keys = self.inner.peer_forward_keys.read().await;
                return if live.contains(scope)
                    && keys.get(scope).is_some_and(|k| k.contains(target))
                {
                    vec![scope.to_string()]
                } else {
                    Vec::new()
                };
            }

            let keys = self.inner.peer_forward_keys.read().await;
            let matched = keys
                .iter()
                .filter_map(|(id, keys)| {
                    if keys.contains(target) && live.contains(id) {
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
                if *role == PeerRole::Server && live.contains(id) {
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
    ///
    /// A node-scoped target (see [`get_server_peers_for`](Self::get_server_peers_for))
    /// always resolves to the same single pinned peer (or `None`) -- the
    /// round-robin cursor is a no-op in that case since there is at most one
    /// candidate.
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

    /// The peer's current session epoch: incremented on each `EVENT_JOIN`
    /// for `peer_id`, `0` if the peer has never joined. Lets callers detect
    /// a fresh session for a peer_id (e.g. to purge state scoped to the
    /// prior session) versus a peer that never disconnected.
    pub async fn peer_epoch(&self, peer_id: &str) -> u64 {
        self.inner
            .peer_epochs
            .read()
            .await
            .get(peer_id)
            .copied()
            .unwrap_or(0)
    }

    pub async fn connected_peers(&self) -> Vec<String> {
        let mut peers = self
            .inner
            .peers
            .read()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        peers.sort();
        peers
    }

    pub async fn send_forward_request(&self, peer_id: &str, ev: ForwardRequestEvent) -> Result<()> {
        self.send_payload(
            peer_id,
            P2pPayload::ForwardRequest {
                req_id: ev.req_id,
                proto: ev.proto,
                remote_addr: ev.remote_addr,
                target: ev.target,
            },
        )
        .await
    }

    pub async fn send_forward_response(
        &self,
        peer_id: &str,
        ev: ForwardResponseEvent,
    ) -> Result<()> {
        self.send_payload(
            peer_id,
            P2pPayload::ForwardResponse {
                req_id: ev.req_id,
                target: ev.target,
                accepted: ev.accepted,
            },
        )
        .await
    }

    /// Advertises `target` as servable by this node. When `target` is
    /// unscoped, this also advertises the self-scoped variant
    /// (`"{target}@{self_id}"`, see [`node_scoped_target`]) so a connecting
    /// client can pin to this specific node even though it was started with
    /// a plain, unscoped target. An already-scoped `target` is published
    /// as-is only.
    pub async fn publish_tunnel_target(&self, target: &str) {
        let mut keys = self.inner.self_forward_keys.write().await;
        keys.insert(target.to_string());
        if split_node_scope(target).1.is_none() {
            keys.insert(node_scoped_target(target, &self.inner.self_id));
        }
        drop(keys);
        self.send_capabilities_to_all().await;
    }

    /// Reverses [`publish_tunnel_target`](Self::publish_tunnel_target),
    /// removing the self-scoped variant alongside an unscoped `target` too.
    pub async fn unpublish_tunnel_target(&self, target: &str) {
        let mut keys = self.inner.self_forward_keys.write().await;
        keys.remove(target);
        if split_node_scope(target).1.is_none() {
            keys.remove(&node_scoped_target(target, &self.inner.self_id));
        }
        drop(keys);
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

    /// Switches the underlying mistlib session to a different room without
    /// tearing down this manager. Rebuilding a fresh `RTCManagerHandle`
    /// would call `mistlib::init` again, but that initializes mistlib's
    /// process-wide singleton engine and is not meant to be called more
    /// than once per process; `leave_room`/`join_room` are the pair mistlib
    /// exposes for changing rooms mid-session (its engine even has a
    /// dedicated race guard -- `MistEngine::run`'s `await_previous_cleanup`
    /// -- specifically so a `leave_room()` immediately followed by a new
    /// join is safe).
    ///
    /// Every handler registered via `on_*` stays wired (they live on this
    /// same `inner`, untouched here). Only per-room membership state is
    /// reset: `leave_room()` tears the transport down directly rather than
    /// emitting a real `EVENT_LEAVE` per peer (see `event::handle_leave`),
    /// so `peers`/`peer_roles`/`peer_forward_keys`/`peer_epochs` are cleared
    /// by hand here instead. Callers that need the equivalent of a per-peer
    /// leave notification (e.g. `SessionContext::switch_room`, which purges
    /// pending auth/forward state scoped to peers about to become
    /// unreachable) must snapshot `connected_peers()` before calling this.
    pub async fn switch_room(&self, room_id: String) {
        mistlib::leave_room();
        self.inner.peers.write().await.clear();
        self.inner.peer_roles.write().await.clear();
        self.inner.peer_forward_keys.write().await.clear();
        self.inner.peer_epochs.write().await.clear();
        mistlib::join_room(room_id);
        self.send_role_to_all().await;
    }
}

pub type RTCManager = RTCManagerHandle;

fn mistlib_config() -> Option<Vec<u8>> {
    std::env::var(MISTLIB_CONFIG_ENV)
        .ok()
        .map(|config| config.into_bytes())
}
