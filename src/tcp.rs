use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::debug;

use crate::forward_runtime::ForwardRuntime;
use crate::rtc::{RTCManager, TunnelMessage};

mod local;
mod tunnel;

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
    target: String,
    runtime: ForwardRuntime,
}

impl TcpManager {
    #[allow(dead_code)]
    pub async fn listen_and_serve(
        rtc_manager: RTCManager,
        listen_port: i32,
        remote_addr: String,
    ) -> Result<()> {
        let target = if remote_addr.is_empty() {
            format!("tcp:{}", listen_port)
        } else {
            forward_key("tcp", &remote_addr)
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

        let mut shutdown = mgr.runtime.subscribe();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, _) = result?;
                    let mgr = mgr.clone();
                    tokio::spawn(async move {
                        mgr.handle_local_connection(stream).await;
                    });
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        Ok(())
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
        let old = self
            .conns
            .write()
            .await
            .insert(conn_id.to_string(), Arc::new(RwLock::new(tc)));
        if old.is_none() {
            self.runtime.record_conn_open();
        }
    }

    async fn close_conn(&self, conn_id: &str, notify_remote: bool) {
        let tc = self.conns.write().await.remove(conn_id);
        if let Some(tc) = tc {
            self.runtime.record_conn_close();
            let tc = tc.read().await;
            if notify_remote && tc.notify_remote {
                let close_msg = TunnelMessage {
                    msg_type: "close".into(),
                    conn_id: conn_id.to_string(),
                    target: self.target.clone(),
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
            if conns.remove(&id).is_some() {
                self.runtime.record_conn_close();
            }
        }
    }

    fn clone_inner(&self) -> Self {
        Self {
            rtc_manager: self.rtc_manager.clone(),
            conns: self.conns.clone(),
            remote_addr: self.remote_addr.clone(),
            target: self.target.clone(),
            runtime: self.runtime.clone(),
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
