use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::forward_runtime::ForwardRuntime;
use crate::rtc::{RTCManager, TunnelMessage};

mod tunnel;

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
    target: String,
    runtime: ForwardRuntime,
}

impl UdpManager {
    #[allow(dead_code)]
    pub async fn listen_and_serve(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
    ) -> Result<()> {
        let target = if remote_addr.is_empty() {
            format!("udp:{}", listen_port)
        } else {
            forward_key("udp", &remote_addr)
        };
        Self::listen_and_serve_with_target(
            rtc_manager,
            listen_port,
            remote_addr,
            target,
            ForwardRuntime::new(),
        )
        .await
    }

    pub async fn listen_and_serve_with_target(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
        target: String,
        runtime: ForwardRuntime,
    ) -> Result<()> {
        let mgr = Arc::new(Self {
            rtc_manager: rtc_manager.clone(),
            conns: Arc::new(RwLock::new(HashMap::new())),
            remote_addr,
            local_socket: Arc::new(RwLock::new(None)),
            target: target.clone(),
            runtime,
        });

        let mgr_msg = mgr.clone();
        rtc_manager
            .on_tunnel_message_for(target.clone(), move |peer_id, data| {
                let mgr = mgr_msg.clone();
                tokio::spawn(async move { mgr.on_tunnel_message(&peer_id, &data).await });
            })
            .await;
        if !mgr.remote_addr.is_empty() {
            rtc_manager.publish_tunnel_target(&target).await;
        }

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
        let mut shutdown = self.runtime.subscribe();
        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, addr)) => {
                            self.handle_local_packet(&buf[..n], addr).await;
                        }
                        Err(e) => {
                            error!("UDP read error: {}", e);
                            return;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    async fn handle_local_packet(&self, payload: &[u8], addr: std::net::SocketAddr) {
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
                    let old = conns.insert(
                        conn_id.clone(),
                        UdpConn {
                            target_conn: None,
                            last_seen: Instant::now(),
                            peer_id: id.clone(),
                            client_addr: Some(addr),
                        },
                    );
                    if old.is_none() {
                        self.runtime.record_conn_open();
                    }
                    id
                }
                Err(e) => {
                    error!("Tunnel not ready for UDP: {}", e);
                    return;
                }
            },
        };

        {
            let mut conns = self.conns.write().await;
            if let Some(uc) = conns.get_mut(&conn_id) {
                uc.last_seen = Instant::now();
            }
        }

        let msg = TunnelMessage {
            msg_type: "data".into(),
            conn_id,
            target: self.target.clone(),
            payload: Some(payload.to_vec()),
        };
        if let Err(e) = self.send_to(&peer_id, &msg).await {
            error!("Failed to send UDP data: {}", e);
        } else {
            self.runtime.record_bytes_in(payload.len());
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

    async fn send_to(&self, peer_id: &str, msg: &TunnelMessage) -> Result<()> {
        let data = serde_json::to_vec(msg)?;
        self.rtc_manager.send_tunnel_to(peer_id, data).await?;
        Ok(())
    }

    async fn wait_for_tunnel_ready(&self, timeout: Duration) -> Result<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let peers = self.rtc_manager.get_server_peers_for(&self.target).await;
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
        let mut shutdown = self.runtime.subscribe();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut removed = 0;
                    let mut conns = self.conns.write().await;
                    conns.retain(|id, uc| {
                        if uc.last_seen.elapsed() > UDP_TIMEOUT {
                            debug!("Cleaned up UDP session: {}", id);
                            removed += 1;
                            false
                        } else {
                            true
                        }
                    });
                    for _ in 0..removed {
                        self.runtime.record_conn_close();
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

#[allow(dead_code)]
fn forward_key(proto: &str, addr: &str) -> String {
    let port = addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(-1);
    format!("{}:{}", proto, port)
}
