use std::sync::{Arc, Weak};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;

use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use crate::rtc::TunnelMessage;

/// A membership or data-plane event, queued for in-order processing by
/// [`run_payload_worker`].
pub(super) enum RawEvent {
    Join { peer_id: String },
    Leave { peer_id: String },
    Payload { from: String, data: Vec<u8> },
}

pub(super) type RawEventSender = UnboundedSender<RawEvent>;
pub(super) type RawEventReceiver = UnboundedReceiver<RawEvent>;

/// The mistlib raw-event callback: invoked synchronously, in strict FIFO
/// order, from mistlib's single dispatch thread (see
/// `mistlib-native/src/engine.rs::spawn_event_dispatch_thread`).
///
/// `EVENT_JOIN`, `EVENT_LEAVE`, and `EVENT_RAW`/`EVENT_OVERLAY` (tunnel/stdio
/// payloads) all go through `tx`, an unbounded channel drained by a single
/// FIFO worker (`run_payload_worker`) that awaits each event to completion
/// before starting the next. `UnboundedSender::send` is synchronous and
/// order-preserving, so the enqueue order here matches mistlib's delivery
/// order, and the worker's sequential processing preserves it end to end.
///
/// Join/leave used to be dispatched via independent `tokio::spawn` calls,
/// which let a rapid leave->rejoin for the same peer execute out of order
/// (task scheduling order on a multi-thread runtime is not guaranteed to
/// match spawn order), sometimes leaving a live peer absent from manager
/// state, or a late leave wiping capabilities a since-completed rejoin had
/// just restored. Routing everything through one FIFO worker removes that
/// race: this function itself never blocks/awaits, preserving the
/// non-blocking contract mistlib's dispatch thread requires.
pub(super) fn dispatch_event(
    weak: &Weak<RTCManagerInner>,
    tx: &RawEventSender,
    message_type: u32,
    from: String,
    data: Vec<u8>,
) {
    // Cheap liveness check so we don't queue work for a manager that was
    // dropped without calling `close` (which would otherwise leave the
    // channel filling up with no worker draining it, since the worker also
    // exits once `weak` fails to upgrade).
    if weak.upgrade().is_none() {
        return;
    }

    let event = match message_type {
        mistlib::EVENT_JOIN => RawEvent::Join { peer_id: from },
        mistlib::EVENT_LEAVE => RawEvent::Leave { peer_id: from },
        mistlib::EVENT_RAW | mistlib::EVENT_OVERLAY => RawEvent::Payload { from, data },
        _ => return,
    };
    let _ = tx.send(event);
}

/// Drains `rx` and processes one event to completion (`.await`) before
/// starting the next, preserving mistlib's original delivery order across
/// join/leave/payload events alike. Exits once the channel is closed (the
/// sender lives inside the closure registered via
/// `mistlib::register_raw_handler`; `mistlib::clear_raw_handler` -- called
/// from `RTCManagerHandle::close` -- drops it) or once `weak` can no longer
/// be upgraded (the manager was dropped without calling `close`).
pub(super) async fn run_payload_worker(weak: Weak<RTCManagerInner>, mut rx: RawEventReceiver) {
    while let Some(event) = rx.recv().await {
        let Some(inner) = weak.upgrade() else {
            break;
        };
        match event {
            RawEvent::Join { peer_id } => handle_join(inner, peer_id).await,
            RawEvent::Leave { peer_id } => handle_leave(inner, peer_id).await,
            RawEvent::Payload { from, data } => handle_payload(inner, from, data).await,
        }
    }
}

pub(super) async fn handle_join(inner: Arc<RTCManagerInner>, peer_id: String) {
    if peer_id == inner.self_id {
        return;
    }

    let epoch = {
        let mut epochs = inner.peer_epochs.write().await;
        let epoch = epochs.entry(peer_id.clone()).or_insert(0);
        *epoch += 1;
        *epoch
    };

    // A fresh JOIN starts a new session for this peer_id: drop whatever
    // role/capabilities we cached for it so a stale prior session's
    // advertisements can't linger. The peer (re-)broadcasts Role/Capabilities
    // once joined, which repopulates these via `handle_payload`.
    inner.peer_roles.write().await.remove(&peer_id);
    inner.peer_forward_keys.write().await.remove(&peer_id);

    inner.peers.write().await.insert(peer_id.clone());
    notify(&inner.peer_conn_handlers, peer_id.clone()).await;
    notify(&inner.tunnel_open_handlers, peer_id.clone()).await;
    notify(&inner.stdio_open_handlers, peer_id.clone()).await;
    notify_epoch(&inner.peer_join_handlers, peer_id.clone(), epoch).await;

    let data = match serde_json::to_vec(&P2pPayload::Role {
        role: inner.self_role.as_str().to_string(),
    }) {
        Ok(data) => data,
        Err(_) => return,
    };
    let _ = mistlib::send_message_direct(peer_id.clone(), data, mistlib::DELIVERY_RELIABLE).await;
    send_capabilities_to_peer(&inner, peer_id).await;
}

pub(super) async fn handle_leave(inner: Arc<RTCManagerInner>, peer_id: String) {
    // `peer_roles`/`peer_forward_keys` are intentionally retained across a
    // leave: mistlib can recover a session for the same peer_id without the
    // remote process restarting, and wiping these here would permanently
    // blind `select_server_peer_for` to a peer that never really left. Only
    // `peers` (current-presence) is cleared; routing filters on that, and a
    // fresh JOIN resets the retained state (see `handle_join`).
    inner.peers.write().await.remove(&peer_id);
    let epoch = inner
        .peer_epochs
        .read()
        .await
        .get(&peer_id)
        .copied()
        .unwrap_or(0);
    notify(&inner.tunnel_close_handlers, peer_id.clone()).await;
    notify(&inner.stdio_close_handlers, peer_id.clone()).await;
    notify_epoch(&inner.peer_leave_handlers, peer_id, epoch).await;
}

pub(super) async fn handle_payload(inner: Arc<RTCManagerInner>, peer_id: String, data: Vec<u8>) {
    if peer_id == inner.self_id {
        return;
    }

    // Receiving any message proves the peer is connected. `EVENT_JOIN` only
    // fires for peers that join after us, so the later-joining side would
    // otherwise never register peers that were already in the room.
    if inner.peers.write().await.insert(peer_id.clone()) {
        notify(&inner.peer_conn_handlers, peer_id.clone()).await;
        notify(&inner.tunnel_open_handlers, peer_id.clone()).await;
        notify(&inner.stdio_open_handlers, peer_id.clone()).await;
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
        P2pPayload::Capabilities { forwards } => {
            inner
                .peer_forward_keys
                .write()
                .await
                .insert(peer_id, forwards.into_iter().collect());
        }
        P2pPayload::Chat { text } => {
            let handlers = inner.chat_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), text.clone());
            }
        }
        P2pPayload::Tunnel { data } => {
            let tunnel_msg = serde_json::from_slice::<TunnelMessage>(&data).ok();
            let default_target = if tunnel_msg.as_ref().is_some_and(|msg| msg.target.is_empty()) {
                inner.default_tunnel_target.read().await.clone()
            } else {
                None
            };
            let handlers = inner.tunnel_msg_handlers.read().await;
            for entry in handlers.iter() {
                if !tunnel_handler_matches(
                    entry.target.as_deref(),
                    tunnel_msg.as_ref(),
                    &default_target,
                ) {
                    continue;
                }
                (entry.handler)(peer_id.clone(), data.clone());
            }
        }
        P2pPayload::Stdio { data } => {
            let handlers = inner.stdio_msg_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), data.clone());
            }
        }
        P2pPayload::ForwardRequest {
            req_id,
            proto,
            remote_addr,
            target,
        } => {
            let ev = super::ForwardRequestEvent {
                req_id,
                proto,
                remote_addr,
                target,
            };
            let handlers = inner.forward_request_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), ev.clone());
            }
        }
        P2pPayload::ForwardResponse {
            req_id,
            target,
            accepted,
        } => {
            let ev = super::ForwardResponseEvent {
                req_id,
                target,
                accepted,
            };
            let handlers = inner.forward_response_handlers.read().await;
            for h in handlers.iter() {
                h(peer_id.clone(), ev.clone());
            }
        }
    }
}

fn tunnel_handler_matches(
    handler_target: Option<&str>,
    msg: Option<&TunnelMessage>,
    default_target: &Option<String>,
) -> bool {
    match handler_target {
        None => true,
        Some(target) => msg.is_some_and(|msg| {
            msg.target == target
                || (msg.target.is_empty() && default_target.as_deref() == Some(target))
        }),
    }
}

async fn send_capabilities_to_peer(inner: &Arc<RTCManagerInner>, peer_id: String) {
    let forwards = inner
        .self_forward_keys
        .read()
        .await
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let data = match serde_json::to_vec(&P2pPayload::Capabilities { forwards }) {
        Ok(data) => data,
        Err(_) => return,
    };
    let _ = mistlib::send_message_direct(peer_id, data, mistlib::DELIVERY_RELIABLE).await;
}

async fn notify(handlers: &RwLock<Vec<Arc<dyn Fn(String) + Send + Sync>>>, peer_id: String) {
    let handlers = handlers.read().await;
    for h in handlers.iter() {
        h(peer_id.clone());
    }
}

async fn notify_epoch(
    handlers: &RwLock<Vec<super::state::PeerEpochHandler>>,
    peer_id: String,
    epoch: u64,
) {
    let handlers = handlers.read().await;
    for h in handlers.iter() {
        h(peer_id.clone(), epoch);
    }
}
