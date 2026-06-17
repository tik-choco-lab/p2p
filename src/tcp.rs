use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::rtc::{RTCManager, TunnelMessage};

const TCP_BUFFER_SIZE: usize = 4096;
const TUNNEL_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

struct TunnelConn {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    peer_id: String,
    notify_remote: bool,
}

pub struct TcpManager {
    rtc_manager: RTCManager,
    conns: Arc<RwLock<HashMap<String, Arc<RwLock<TunnelConn>>>>>,
    remote_addr: String,
}

impl TcpManager {
    pub async fn listen_and_serve(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
    ) -> Result<()> {
        let resolved = if !remote_addr.is_empty() {
            if remote_addr.contains(':') {
                remote_addr.clone()
            } else {
                format!("127.0.0.1:{}", remote_addr)
            }
        } else {
            remote_addr.clone()
        };

        let mgr = Arc::new(Self {
            rtc_manager: rtc_manager.clone(),
            conns: Arc::new(RwLock::new(HashMap::new())),
            remote_addr: resolved,
        });

        let mgr_msg = mgr.clone();
        rtc_manager
            .on_tunnel_message(move |peer_id, data| {
                let mgr = mgr_msg.clone();
                tokio::spawn(async move { mgr.on_tunnel_message(&peer_id, &data).await });
            })
            .await;

        let mgr_close = mgr.clone();
        rtc_manager
            .on_tunnel_close(move |peer_id| {
                let mgr = mgr_close.clone();
                tokio::spawn(async move {
                    debug!("tunnel DC closed for peer {}; closing TCP conns", peer_id);
                    mgr.close_all_for_peer(&peer_id).await;
                });
            })
            .await;

        if listen_port == -1 {
            debug!("No local listen port; skipping TCP server");
            return Ok(());
        }

        let addr = format!("0.0.0.0:{}", listen_port);
        let listener = TcpListener::bind(&addr).await?;
        debug!("TCP server listening on port {}", listen_port);

        loop {
            let (stream, _) = listener.accept().await?;
            let mgr = mgr.clone();
            tokio::spawn(async move {
                mgr.handle_local_connection(stream).await;
            });
        }
    }

    async fn handle_local_connection(self: &Arc<Self>, stream: TcpStream) {
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
            let peers = self.rtc_manager.get_server_peers().await;
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

    async fn forward_tcp_to_dc(
        self: Arc<Self>,
        conn_id: String,
        peer_id: String,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
    ) {
        let mut buf = vec![0u8; TCP_BUFFER_SIZE];
        loop {
            match read_half.read(&mut buf).await {
                Ok(0) => {
                    self.close_conn(&conn_id, true).await;
                    return;
                }
                Ok(n) => {
                    let msg = TunnelMessage {
                        msg_type: "data".into(),
                        conn_id: conn_id.clone(),
                        payload: Some(buf[..n].to_vec()),
                    };
                    if let Err(e) = self.send_to(&peer_id, &msg).await {
                        error!("failed to send tunnel data: {}", e);
                        self.close_conn(&conn_id, false).await;
                        return;
                    }
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
    }

    async fn on_tunnel_message(&self, peer_id: &str, data: &[u8]) {
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
                self.track_conn(&tm.conn_id, write_half, peer_id, true)
                    .await;
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

    async fn track_conn(
        &self,
        conn_id: &str,
        write_half: tokio::net::tcp::OwnedWriteHalf,
        peer_id: &str,
        notify_remote: bool,
    ) {
        let tc = TunnelConn {
            writer: Arc::new(tokio::sync::Mutex::new(write_half)),
            peer_id: peer_id.to_string(),
            notify_remote,
        };
        self.conns
            .write()
            .await
            .insert(conn_id.to_string(), Arc::new(RwLock::new(tc)));
    }

    async fn close_conn(&self, conn_id: &str, notify_remote: bool) {
        let tc = self.conns.write().await.remove(conn_id);
        if let Some(tc) = tc {
            let tc = tc.read().await;
            if notify_remote && tc.notify_remote {
                let close_msg = TunnelMessage {
                    msg_type: "close".into(),
                    conn_id: conn_id.to_string(),
                    payload: None,
                };
                let _ = self.send_to(&tc.peer_id, &close_msg).await;
            }
        }
    }

    async fn send_to(&self, peer_id: &str, msg: &TunnelMessage) -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        self.rtc_manager.send_tunnel_to(peer_id, data).await?;
        Ok(())
    }

    async fn close_all_for_peer(&self, peer_id: &str) {
        let mut conns = self.conns.write().await;
        let to_remove: Vec<String> = conns
            .iter()
            .filter_map(|(id, tc)| {
                if let Ok(tc) = tc.try_read() {
                    if tc.peer_id == peer_id {
                        Some(id.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for id in to_remove {
            conns.remove(&id);
        }
    }

    fn clone_inner(&self) -> Self {
        Self {
            rtc_manager: self.rtc_manager.clone(),
            conns: self.conns.clone(),
            remote_addr: self.remote_addr.clone(),
        }
    }
}
