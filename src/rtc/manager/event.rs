use std::sync::{Arc, Weak};

use tokio::sync::RwLock;

use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use crate::rtc::TunnelMessage;

pub(super) fn dispatch_event(
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
