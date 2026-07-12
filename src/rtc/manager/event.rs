use std::sync::{Arc, Weak};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;

use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use crate::rtc::TunnelMessage;

/// A data-plane event (`EVENT_RAW`/`EVENT_OVERLAY`) queued for in-order
/// processing by [`run_payload_worker`].
pub(super) struct RawEvent {
    from: String,
    data: Vec<u8>,
}

pub(super) type RawEventSender = UnboundedSender<RawEvent>;
pub(super) type RawEventReceiver = UnboundedReceiver<RawEvent>;

/// The mistlib raw-event callback: invoked synchronously, in strict FIFO
/// order, from mistlib's single dispatch thread (see
/// `mistlib-native/src/engine.rs::spawn_event_dispatch_thread`).
///
/// `EVENT_JOIN`/`EVENT_LEAVE` are still dispatched via `tokio::spawn`: they
/// have no ordering contract with tunnel data, and `handle_join` awaits a
/// `send_message_direct` call (with the transport's own retry/backoff), so
/// serializing it in front of data processing would introduce head-of-line
/// blocking for no benefit.
///
/// `EVENT_RAW`/`EVENT_OVERLAY` (tunnel/stdio payloads) instead go through
/// `tx`, an unbounded channel drained by a single FIFO worker
/// (`run_payload_worker`) that awaits each event to completion before
/// starting the next. `UnboundedSender::send` is synchronous and
/// order-preserving, so the enqueue order here matches mistlib's delivery
/// order, and the worker's sequential processing preserves it end to end --
/// unlike the previous per-event `tokio::spawn`, whose task order on a
/// multi-thread runtime is not guaranteed to match spawn order.
pub(super) fn dispatch_event(
    runtime: &tokio::runtime::Handle,
    weak: &Weak<RTCManagerInner>,
    tx: &RawEventSender,
    message_type: u32,
    from: String,
    data: Vec<u8>,
) {
    let Some(inner) = weak.upgrade() else {
        return;
    };

    match message_type {
        mistlib::EVENT_JOIN => {
            runtime.spawn(async move { handle_join(inner, from).await });
        }
        mistlib::EVENT_LEAVE => {
            runtime.spawn(async move { handle_leave(inner, from).await });
        }
        mistlib::EVENT_RAW | mistlib::EVENT_OVERLAY => {
            // `inner` was only needed for the liveness check above; drop it
            // so it doesn't outlive this call. The worker re-upgrades `weak`
            // itself before processing each event.
            drop(inner);
            let _ = tx.send(RawEvent { from, data });
        }
        _ => {}
    }
}

/// Drains `rx` and processes one data-plane event to completion (`.await`)
/// before starting the next, preserving mistlib's original delivery order.
/// Exits once the channel is closed (the sender lives inside the closure
/// registered via `mistlib::register_raw_handler`; `mistlib::clear_raw_handler`
/// -- called from `RTCManagerHandle::close` -- drops it) or once `weak` can
/// no longer be upgraded (the manager was dropped without calling `close`).
pub(super) async fn run_payload_worker(weak: Weak<RTCManagerInner>, mut rx: RawEventReceiver) {
    while let Some(RawEvent { from, data }) = rx.recv().await {
        let Some(inner) = weak.upgrade() else {
            break;
        };
        handle_payload(inner, from, data).await;
    }
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
    let _ = mistlib::send_message_direct(peer_id.clone(), data, mistlib::DELIVERY_RELIABLE).await;
    send_capabilities_to_peer(&inner, peer_id).await;
}

pub(super) async fn handle_leave(inner: Arc<RTCManagerInner>, peer_id: String) {
    inner.peers.write().await.remove(&peer_id);
    inner.peer_roles.write().await.remove(&peer_id);
    inner.peer_forward_keys.write().await.remove(&peer_id);
    notify(&inner.tunnel_close_handlers, peer_id.clone()).await;
    notify(&inner.stdio_close_handlers, peer_id).await;
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
