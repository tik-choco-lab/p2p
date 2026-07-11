use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, error};

use crate::auth::AuthRequest;
use crate::rtc::TunnelMessage;

use super::{
    log_tcp_io_error, TcpManager, MSG_TYPE_CLOSE, MSG_TYPE_CONNECT, MSG_TYPE_DATA, MSG_TYPE_PING,
};

impl TcpManager {
    pub(super) async fn on_tunnel_message(&self, peer_id: &str, data: &[u8]) {
        if data.is_empty() || data[0] != b'{' {
            return;
        }
        let tm: TunnelMessage = match serde_json::from_slice(data) {
            Ok(m) => m,
            Err(_) => return,
        };
        match tm.msg_type.as_str() {
            MSG_TYPE_CONNECT => self.handle_remote_connect(peer_id, &tm).await,
            MSG_TYPE_DATA => self.handle_remote_data(&tm).await,
            MSG_TYPE_CLOSE => self.close_conn(&tm.conn_id, false).await,
            // Keepalive: purely to keep the channel/NAT mapping warm, no
            // action needed on receipt.
            MSG_TYPE_PING => {}
            // Unknown/unrecognized types (older/newer peer versions) are
            // ignored for cross-version compat.
            _ => {}
        }
    }

    async fn handle_remote_connect(&self, peer_id: &str, tm: &TunnelMessage) {
        let req = AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: self.target.clone(),
            target_addr: self.remote_addr.clone(),
            proto: "tcp".to_string(),
        };
        let decision = self.authorizer.authorize(&req).await;
        if !decision.is_allowed() {
            debug!(
                "denied tcp tunnel connection from {} to {}",
                peer_id, self.target
            );
            self.send_close(peer_id, &tm.conn_id).await;
            return;
        }

        let addr = &self.remote_addr;
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let (read_half, write_half) = stream.into_split();
                self.track_conn(&tm.conn_id, write_half, peer_id, true)
                    .await;
                let mgr = Arc::new(self.clone_inner());
                let cid = tm.conn_id.clone();
                let pid = peer_id.to_string();
                tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
            }
            Err(e) => {
                error!("failed to connect to remote ({}): {}", addr, e);
                self.send_close(peer_id, &tm.conn_id).await;
            }
        }
    }

    async fn send_close(&self, peer_id: &str, conn_id: &str) {
        let close_msg = TunnelMessage {
            msg_type: MSG_TYPE_CLOSE.into(),
            conn_id: conn_id.to_string(),
            target: self.target.clone(),
            payload: None,
        };
        let _ = self.send_to(peer_id, &close_msg).await;
    }

    async fn handle_remote_data(&self, tm: &TunnelMessage) {
        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p,
            _ => return,
        };
        let Some((writer, metrics)) = ({
            let conns = self.conns.read().await;
            match conns.get(&tm.conn_id) {
                Some(tc) => {
                    let tc = tc.read().await;
                    Some((tc.writer.clone(), tc.metrics.clone()))
                }
                None => None,
            }
        }) else {
            return;
        };

        let mut writer = writer.lock().await;
        if let Err(e) = writer.write_all(payload).await {
            log_tcp_io_error("failed to write to tcp", &e);
            drop(writer);
            self.close_conn(&tm.conn_id, true).await;
        } else {
            metrics.record_bytes_out(payload.len());
        }
    }
}
