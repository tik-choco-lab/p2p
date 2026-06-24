use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::state::DataHandlerEntry;
use super::RTCManagerHandle;

#[allow(dead_code)]
impl RTCManagerHandle {
    pub async fn on_chat_message<F: Fn(String, String) + Send + Sync + 'static>(&self, f: F) {
        self.inner.chat_handlers.write().await.push(Arc::new(f));
    }

    pub async fn on_forward_request<
        F: Fn(String, super::ForwardRequestEvent) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) {
        self.inner
            .forward_request_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_forward_response<
        F: Fn(String, super::ForwardResponseEvent) + Send + Sync + 'static,
    >(
        &self,
        f: F,
    ) {
        self.inner
            .forward_response_handlers
            .write()
            .await
            .push(Arc::new(f));
    }

    pub async fn on_tunnel_message<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(
        &self,
        f: F,
    ) -> u64 {
        let id = self
            .inner
            .next_tunnel_handler_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .tunnel_msg_handlers
            .write()
            .await
            .push(DataHandlerEntry {
                id,
                target: None,
                handler: Arc::new(f),
            });
        id
    }

    pub async fn on_tunnel_message_for<F: Fn(String, Vec<u8>) + Send + Sync + 'static>(
        &self,
        target: String,
        f: F,
    ) -> u64 {
        {
            let mut default = self.inner.default_tunnel_target.write().await;
            if default.is_none() {
                *default = Some(target.clone());
            }
        }

        let id = self
            .inner
            .next_tunnel_handler_id
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .tunnel_msg_handlers
            .write()
            .await
            .push(DataHandlerEntry {
                id,
                target: Some(target),
                handler: Arc::new(f),
            });
        id
    }

    pub async fn remove_tunnel_message_handler(&self, id: u64) -> bool {
        let mut handlers = self.inner.tunnel_msg_handlers.write().await;
        let removed_target = handlers
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.target.clone());
        let old_len = handlers.len();
        handlers.retain(|entry| entry.id != id);
        let removed = handlers.len() != old_len;
        drop(handlers);

        if removed_target.is_some() {
            self.refresh_default_tunnel_target().await;
        }

        removed
    }

    async fn refresh_default_tunnel_target(&self) {
        let handlers = self.inner.tunnel_msg_handlers.read().await;
        let next_default = handlers.iter().find_map(|entry| entry.target.clone());
        drop(handlers);

        *self.inner.default_tunnel_target.write().await = next_default;
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
}
