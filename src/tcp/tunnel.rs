use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::error;

use crate::rtc::TunnelMessage;

use super::TcpManager;

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
            "connect" => self.handle_remote_connect(peer_id, &tm).await,
            "data" => self.handle_remote_data(&tm).await,
            "close" => self.close_conn(&tm.conn_id, false).await,
            _ => {}
        }
    }

    async fn handle_remote_connect(&self, peer_id: &str, tm: &TunnelMessage) {
        let addr = &self.remote_addr;
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let (read_half, write_half) = stream.into_split();
                self.track_conn(&tm.conn_id, write_half, peer_id, true).await;
                let mgr = Arc::new(self.clone_inner());
                let cid = tm.conn_id.clone();
                let pid = peer_id.to_string();
                tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
            }
            Err(e) => {
                error!("failed to connect to remote ({}): {}", addr, e);
                let close_msg = TunnelMessage {
                    msg_type: "close".into(),
                    conn_id: tm.conn_id.clone(),
                    target: self.target.clone(),
                    payload: None,
                };
                let _ = self.send_to(peer_id, &close_msg).await;
            }
        }
    }

    async fn handle_remote_data(&self, tm: &TunnelMessage) {
        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p,
            _ => return,
        };
        let conns = self.conns.read().await;
        if let Some(tc) = conns.get(&tm.conn_id) {
            let tc = tc.read().await;
            let mut writer = tc.writer.lock().await;
            if let Err(e) = writer.write_all(payload).await {
                error!("failed to write to tcp: {}", e);
                drop(writer);
                drop(tc);
                drop(conns);
                self.close_conn(&tm.conn_id, true).await;
            }
        }
    }
}
