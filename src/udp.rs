use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::debug;

use crate::auth::{allow_all, SharedAuthorizer};
use crate::forward_runtime::ForwardRuntime;
use crate::rtc::{RTCManager, TunnelMessage};

mod lifecycle;
mod local;
mod tunnel;

use lifecycle::{forward_key, spawn_handler_cleanup};

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
    authorizer: SharedAuthorizer,
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
        Self::listen_and_serve_with_target_and_auth(
            rtc_manager,
            listen_port,
            remote_addr,
            target,
            runtime,
            allow_all(),
        )
        .await
    }

    pub async fn listen_and_serve_with_target_and_auth(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
        target: String,
        runtime: ForwardRuntime,
        authorizer: SharedAuthorizer,
    ) -> Result<()> {
        let mgr = Arc::new(Self {
            rtc_manager: rtc_manager.clone(),
            conns: Arc::new(RwLock::new(HashMap::new())),
            remote_addr,
            local_socket: Arc::new(RwLock::new(None)),
            target: target.clone(),
            runtime,
            authorizer,
        });

        let mgr_msg = mgr.clone();
        let msg_runtime = mgr.runtime.clone();
        let handler_id = rtc_manager
            .on_tunnel_message_for(target.clone(), move |peer_id, data| {
                if msg_runtime.is_cancelled() {
                    return;
                }
                let mgr = mgr_msg.clone();
                tokio::spawn(async move { mgr.on_tunnel_message(&peer_id, &data).await });
            })
            .await;
        spawn_handler_cleanup(rtc_manager.clone(), mgr.runtime.clone(), handler_id);
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
}
