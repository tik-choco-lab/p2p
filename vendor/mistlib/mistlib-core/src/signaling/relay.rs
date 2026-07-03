use crate::overlay::OverlayTransport;
use crate::signaling::{MessageContent, Signaler, SignalingHandler};
use crate::types::NodeId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalingRoute {
    WebSocket,
    Overlay,
}

pub struct RoutedSignaler {
    bootstrap: Arc<dyn Signaler>,
    overlay: Arc<OverlayTransport>,
    peer_routes: Mutex<HashMap<NodeId, SignalingRoute>>,
}

impl RoutedSignaler {
    pub fn new(bootstrap: Arc<dyn Signaler>, overlay: Arc<OverlayTransport>) -> Self {
        Self {
            bootstrap,
            overlay,
            peer_routes: Mutex::new(HashMap::new()),
        }
    }

    pub fn remember_route(&self, peer: &NodeId, route: SignalingRoute) {
        if peer.is_server() || peer.is_broadcast() {
            return;
        }
        self.peer_routes
            .lock()
            .expect("signaling route lock poisoned")
            .insert(peer.clone(), route);
    }

    pub fn route_for(&self, peer: &NodeId) -> Option<SignalingRoute> {
        self.peer_routes
            .lock()
            .expect("signaling route lock poisoned")
            .get(peer)
            .copied()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl Signaler for RoutedSignaler {
    async fn send_signaling(&self, to: &NodeId, msg: MessageContent) -> crate::error::Result<()> {
        if to.is_server() || to.is_broadcast() {
            return self.bootstrap.send_signaling(to, msg).await;
        }

        if self.overlay.has_signaling_route(to) {
            return self.overlay.send_signaling(to, msg).await;
        }

        match self.route_for(to) {
            Some(SignalingRoute::WebSocket) => self.bootstrap.send_signaling(to, msg).await,
            Some(SignalingRoute::Overlay) | None => self.overlay.send_signaling(to, msg).await,
        }
    }

    async fn reset_session(&self) -> crate::error::Result<()> {
        self.peer_routes
            .lock()
            .expect("signaling route lock poisoned")
            .clear();
        self.bootstrap.reset_session().await
    }

    async fn close(&self) -> crate::error::Result<()> {
        self.bootstrap.close().await
    }
}

pub struct RoutedSignalingHandler {
    routes: Arc<RoutedSignaler>,
    inner: Arc<dyn SignalingHandler>,
    ingress: SignalingRoute,
}

impl RoutedSignalingHandler {
    pub fn new(
        routes: Arc<RoutedSignaler>,
        inner: Arc<dyn SignalingHandler>,
        ingress: SignalingRoute,
    ) -> Self {
        Self {
            routes,
            inner,
            ingress,
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SignalingHandler for RoutedSignalingHandler {
    async fn handle_message(&self, msg: MessageContent) -> crate::error::Result<()> {
        if let MessageContent::Data(data) = &msg {
            self.routes.remember_route(&data.sender_id, self.ingress);
        }
        self.inner.handle_message(msg).await
    }
}

#[cfg(test)]
mod tests;
