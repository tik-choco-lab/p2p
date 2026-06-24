use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, error};

use crate::rtc::TunnelMessage;

use super::{TcpManager, RETRY_INTERVAL, TCP_BUFFER_SIZE, TUNNEL_READY_TIMEOUT};

impl TcpManager {
    pub(super) async fn handle_local_connection(self: &Arc<Self>, stream: TcpStream) {
        let conn_id = uuid::Uuid::new_v4().to_string();

        let peer_id = match self.wait_for_tunnel_ready(TUNNEL_READY_TIMEOUT).await {
            Ok(id) => id,
            Err(e) => {
                error!("tunnel not ready: {}", e);
                return;
            }
        };

        let (read_half, write_half) = stream.into_split();
        self.track_conn(&conn_id, write_half, &peer_id, true).await;

        let connect_msg = TunnelMessage {
            msg_type: "connect".into(),
            conn_id: conn_id.clone(),
            target: self.target.clone(),
            payload: None,
        };
        if let Err(e) = self.send_to(&peer_id, &connect_msg).await {
            error!("failed to send tunnel connect: {}", e);
            self.close_conn(&conn_id, false).await;
            return;
        }

        let mgr = self.clone();
        let cid = conn_id.clone();
        let pid = peer_id.clone();
        tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
    }

    async fn wait_for_tunnel_ready(&self, timeout: std::time::Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let peers = self.rtc_manager.get_server_peers_for(&self.target).await;
            if let Some(peer_id) = peers.first() {
                debug!("Selected server peer: {}", peer_id);
                return Ok(peer_id.clone());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("server peer not connected");
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    pub(super) async fn forward_tcp_to_dc(
        self: Arc<Self>,
        conn_id: String,
        peer_id: String,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
    ) {
        let mut buf = vec![0u8; TCP_BUFFER_SIZE];
        let mut shutdown = self.runtime.subscribe();
        loop {
            tokio::select! {
                read = read_half.read(&mut buf) => {
                    match read {
                        Ok(0) => {
                            self.close_conn(&conn_id, true).await;
                            return;
                        }
                        Ok(n) => {
                            let msg = TunnelMessage {
                                msg_type: "data".into(),
                                conn_id: conn_id.clone(),
                                target: self.target.clone(),
                                payload: Some(buf[..n].to_vec()),
                            };
                            if let Err(e) = self.send_to(&peer_id, &msg).await {
                                error!("failed to send tunnel data: {}", e);
                                self.close_conn(&conn_id, false).await;
                                return;
                            }
                            self.runtime.record_bytes_in(n);
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                error!("tcp read error: {}", e);
                            }
                            self.close_conn(&conn_id, true).await;
                            return;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.close_conn(&conn_id, false).await;
                        return;
                    }
                }
            }
        }
    }
}
