use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::rtc::{RTCManager, TunnelMessage};

const UDP_TIMEOUT: Duration = Duration::from_secs(30);
const TUNNEL_READY_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_UDP_SIZE: usize = 65535;
const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

struct UdpConn {
    target_conn: Option<Arc<UdpSocket>>,
    last_seen: Instant,
    peer_id: String,
    client_addr: Option<std::net::SocketAddr>,
}

pub struct UdpManager {
    rtc_manager: RTCManager,
    conns: Arc<RwLock<HashMap<String, UdpConn>>>,
    remote_addr: String,
    local_socket: Arc<RwLock<Option<Arc<UdpSocket>>>>,
}

impl UdpManager {
    pub async fn listen_and_serve(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
    ) -> Result<()> {
        let mgr = Arc::new(Self {
            rtc_manager: rtc_manager.clone(),
            conns: Arc::new(RwLock::new(HashMap::new())),
            remote_addr,
            local_socket: Arc::new(RwLock::new(None)),
        });

        let mgr_msg = mgr.clone();
        rtc_manager
            .on_tunnel_message(move |peer_id, data| {
                let mgr = mgr_msg.clone();
                tokio::spawn(async move { mgr.on_tunnel_message(&peer_id, &data).await });
            })
            .await;

        if listen_port != -1 {
            let addr = format!("0.0.0.0:{}", listen_port);
            let socket = Arc::new(UdpSocket::bind(&addr).await?);
            *mgr.local_socket.write().await = Some(socket.clone());
            debug!("UDP server listening on port {}", listen_port);

            let mgr_read = mgr.clone();
            let sock = socket.clone();
            tokio::spawn(async move { mgr_read.read_local_packets(sock).await });
        }

        let mgr_cleanup = mgr.clone();
        tokio::spawn(async move { mgr_cleanup.cleanup_loop().await });

        Ok(())
    }

    async fn read_local_packets(self: Arc<Self>, socket: Arc<UdpSocket>) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((n, addr)) => {
                    let conn_id = addr.to_string();
                    let peer_id = {
                        let conns = self.conns.read().await;
                        conns.get(&conn_id).map(|c| c.peer_id.clone())
                    };

                    let peer_id = match peer_id {
                        Some(id) => id,
                        None => match self.wait_for_tunnel_ready(TUNNEL_READY_TIMEOUT).await {
                            Ok(id) => {
                                let mut conns = self.conns.write().await;
                                conns.entry(conn_id.clone()).or_insert(UdpConn {
                                    target_conn: None,
                                    last_seen: Instant::now(),
                                    peer_id: id.clone(),
                                    client_addr: Some(addr),
                                });
                                id
                            }
                            Err(e) => {
                                error!("Tunnel not ready for UDP: {}", e);
                                continue;
                            }
                        },
                    };

                    {
                        let mut conns = self.conns.write().await;
                        if let Some(uc) = conns.get_mut(&conn_id) {
                            uc.last_seen = Instant::now();
                        }
                    }

                    let payload = buf[..n].to_vec();
                    let msg = TunnelMessage {
                        msg_type: "data".into(),
                        conn_id,
                        payload: Some(payload),
                    };
                    if let Err(e) = self.send_to(&peer_id, &msg).await {
                        error!("Failed to send UDP data: {}", e);
                    }
                }
                Err(e) => {
                    error!("UDP read error: {}", e);
                    return;
                }
            }
        }
    }

    async fn on_tunnel_message(&self, peer_id: &str, data: &[u8]) {
        let tm: TunnelMessage = match serde_json::from_slice(data) {
            Ok(m) => m,
            Err(_) => return,
        };
        if tm.msg_type == "data" {
            self.handle_data(peer_id, &tm).await;
        }
    }

    async fn handle_data(&self, peer_id: &str, tm: &TunnelMessage) {
        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p.clone(),
            _ => return,
        };

        let conns = self.conns.read().await;
        if let Some(uc) = conns.get(&tm.conn_id) {
            if let Some(ref target) = uc.target_conn {
                let _ = target.send(&payload).await;
            } else if let Some(ref addr) = uc.client_addr {
                if let Some(ref sock) = *self.local_socket.read().await {
                    let _ = sock.send_to(&payload, addr).await;
                }
            }
            return;
        }
        drop(conns);

        if !self.remote_addr.is_empty() {
            match UdpSocket::bind("0.0.0.0:0").await {
                Ok(sock) => {
                    if let Err(e) = sock.connect(&self.remote_addr).await {
                        error!("Failed to connect UDP: {}", e);
                        return;
                    }
                    let sock = Arc::new(sock);
                    let _ = sock.send(&payload).await;

                    let mut conns = self.conns.write().await;
                    conns.insert(
                        tm.conn_id.clone(),
                        UdpConn {
                            target_conn: Some(sock.clone()),
                            last_seen: Instant::now(),
                            peer_id: peer_id.to_string(),
                            client_addr: None,
                        },
                    );

                    let mgr_conns = self.conns.clone();
                    let rtc = self.rtc_manager.clone();
                    let cid = tm.conn_id.clone();
                    let pid = peer_id.to_string();
                    tokio::spawn(async move {
                        Self::forward_target_to_tunnel(sock, mgr_conns, rtc, cid, pid).await;
                    });
                }
                Err(e) => error!("Failed to bind UDP: {}", e),
            }
        } else if let Some(ref sock) = *self.local_socket.read().await {
            if let Ok(addr) = tm.conn_id.parse::<std::net::SocketAddr>() {
                let _ = sock.send_to(&payload, &addr).await;
            }
        }
    }

    async fn forward_target_to_tunnel(
        sock: Arc<UdpSocket>,
        conns: Arc<RwLock<HashMap<String, UdpConn>>>,
        rtc_manager: RTCManager,
        conn_id: String,
        peer_id: String,
    ) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        loop {
            match sock.recv(&mut buf).await {
                Ok(n) => {
                    {
                        let mut conns = conns.write().await;
                        if let Some(uc) = conns.get_mut(&conn_id) {
                            uc.last_seen = Instant::now();
                        }
                    }
                    let msg = TunnelMessage {
                        msg_type: "data".into(),
                        conn_id: conn_id.clone(),
                        payload: Some(buf[..n].to_vec()),
                    };
                    let data = match serde_json::to_vec(&msg) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    let _ = rtc_manager.send_tunnel_to(&peer_id, data).await;
                }
                Err(e) => {
                    error!("UDP target read error: {}", e);
                    return;
                }
            }
        }
    }

    async fn send_to(&self, peer_id: &str, msg: &TunnelMessage) -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        self.rtc_manager.send_tunnel_to(peer_id, data).await?;
        Ok(())
    }

    async fn wait_for_tunnel_ready(&self, timeout: Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let peers = self.rtc_manager.get_server_peers().await;
            if let Some(peer_id) = peers.first() {
                return Ok(peer_id.clone());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timeout");
            }
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }

    async fn cleanup_loop(&self) {
        let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
        loop {
            interval.tick().await;
            let mut conns = self.conns.write().await;
            conns.retain(|id, uc| {
                if uc.last_seen.elapsed() > UDP_TIMEOUT {
                    debug!("Cleaned up UDP session: {}", id);
                    false
                } else {
                    true
                }
            });
        }
    }
}
