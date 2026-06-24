use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::auth::AuthRequest;
use crate::rtc::{RTCManager, TunnelMessage};

use super::{UdpConn, UdpManager, MAX_UDP_SIZE};

impl UdpManager {
    pub(super) async fn handle_data(&self, peer_id: &str, tm: &TunnelMessage) {
        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p,
            _ => return,
        };

        enum SendTarget {
            Connected(Arc<UdpSocket>, String),
            Local(Arc<UdpSocket>, SocketAddr, String),
        }

        let existing = {
            let conns = self.conns.read().await;
            if let Some(uc) = conns.get(&tm.conn_id) {
                if let Some(target) = &uc.target_conn {
                    Some(SendTarget::Connected(target.clone(), uc.peer_id.clone()))
                } else if let Some(addr) = uc.client_addr {
                    let sock = self.local_socket.read().await.clone();
                    sock.map(|sock| SendTarget::Local(sock, addr, uc.peer_id.clone()))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(target) = existing {
            let sent = match &target {
                SendTarget::Connected(sock, _) => sock.send(payload).await.is_ok(),
                SendTarget::Local(sock, addr, _) => sock.send_to(payload, addr).await.is_ok(),
            };
            if sent {
                let peer_id = match target {
                    SendTarget::Connected(_, peer_id) | SendTarget::Local(_, _, peer_id) => peer_id,
                };
                self.runtime.record_bytes_out_for(&peer_id, payload.len());
            }
            return;
        }

        if !self.remote_addr.is_empty() {
            if !self.authorize_remote_session(peer_id).await {
                debug!(
                    "denied udp tunnel session from {} to {}",
                    peer_id, self.target
                );
                return;
            }

            match UdpSocket::bind("0.0.0.0:0").await {
                Ok(sock) => {
                    if let Err(e) = sock.connect(&self.remote_addr).await {
                        error!("Failed to connect UDP: {}", e);
                        return;
                    }
                    let sock = Arc::new(sock);
                    if sock.send(payload).await.is_ok() {
                        self.runtime.record_bytes_out_for(peer_id, payload.len());
                    }

                    let mut conns = self.conns.write().await;
                    let old = conns.insert(
                        tm.conn_id.clone(),
                        UdpConn {
                            target_conn: Some(sock.clone()),
                            last_seen: Instant::now(),
                            peer_id: peer_id.to_string(),
                            client_addr: None,
                        },
                    );
                    if old.is_none() {
                        self.runtime.record_conn_open_for(peer_id);
                    }

                    let mgr_conns = self.conns.clone();
                    let rtc = self.rtc_manager.clone();
                    let cid = tm.conn_id.clone();
                    let pid = peer_id.to_string();
                    let target = self.target.clone();
                    let runtime = self.runtime.clone();
                    tokio::spawn(async move {
                        Self::forward_target_to_tunnel(
                            sock, mgr_conns, rtc, cid, pid, target, runtime,
                        )
                        .await;
                    });
                }
                Err(e) => error!("Failed to bind UDP: {}", e),
            }
        } else if let Some(ref sock) = *self.local_socket.read().await {
            if let Ok(addr) = tm.conn_id.parse::<std::net::SocketAddr>() {
                if sock.send_to(payload, &addr).await.is_ok() {
                    self.runtime.record_bytes_out_for(peer_id, payload.len());
                }
            }
        }
    }

    async fn authorize_remote_session(&self, peer_id: &str) -> bool {
        let req = AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: self.target.clone(),
            target_addr: self.remote_addr.clone(),
            proto: "udp".to_string(),
        };
        self.authorizer.authorize(&req).await.is_allowed()
    }

    async fn forward_target_to_tunnel(
        sock: Arc<UdpSocket>,
        conns: Arc<RwLock<HashMap<String, UdpConn>>>,
        rtc_manager: RTCManager,
        conn_id: String,
        peer_id: String,
        target: String,
        runtime: crate::forward_runtime::ForwardRuntime,
    ) {
        let mut buf = vec![0u8; MAX_UDP_SIZE];
        let mut shutdown = runtime.subscribe();
        loop {
            tokio::select! {
                result = sock.recv(&mut buf) => {
                    match result {
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
                                target: target.clone(),
                                payload: Some(buf[..n].to_vec()),
                            };
                            let data = match serde_json::to_vec(&msg) {
                                Ok(d) => d,
                                Err(_) => continue,
                            };
                            if rtc_manager.send_tunnel_to(&peer_id, data).await.is_ok() {
                                runtime.record_bytes_in_for(&peer_id, n);
                            }
                        }
                        Err(e) => {
                            error!("UDP target read error: {}", e);
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
}
