use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::debug;

use crate::auth::{allow_all, SharedAuthorizer};
use crate::forward_runtime::{ForwardPeerRuntime, ForwardRuntime};
use crate::rtc::{RTCManager, TunnelMessage};

mod lifecycle;
mod local;
mod tunnel;

use lifecycle::{forward_key, spawn_handler_cleanup};

// Keep tunnel chunks below mistlib/WebRTC's message limit after JSON/hex
// framing overhead. Larger reads can produce "outbound packet larger than
// maximum message size" errors.
const TCP_BUFFER_SIZE: usize = 4096;
const TUNNEL_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

struct TunnelConn {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    peer_id: String,
    metrics: ForwardPeerRuntime,
    notify_remote: bool,
}

pub struct TcpManager {
    rtc_manager: RTCManager,
    conns: Arc<RwLock<HashMap<String, Arc<RwLock<TunnelConn>>>>>,
    remote_addr: String,
    target: String,
    runtime: ForwardRuntime,
    authorizer: SharedAuthorizer,
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
            authorizer,
        });

        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::unbounded_channel::<(String, Vec<u8>)>();
        let mgr_msg = mgr.clone();
        let msg_runtime = mgr.runtime.clone();
        tokio::spawn(async move {
            while let Some((peer_id, data)) = msg_rx.recv().await {
                if msg_runtime.is_cancelled() {
                    break;
                }
                mgr_msg.on_tunnel_message(&peer_id, &data).await;
            }
        });

        let msg_runtime = mgr.runtime.clone();
        let handler_id = rtc_manager
            .on_tunnel_message_for(target.clone(), move |peer_id, data| {
                if msg_runtime.is_cancelled() {
                    return;
                }
                let _ = msg_tx.send((peer_id, data));
            })
            .await;
        spawn_handler_cleanup(rtc_manager.clone(), mgr.runtime.clone(), handler_id);
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
        let metrics = self.runtime.peer(peer_id);
        let tc = TunnelConn {
            writer: Arc::new(tokio::sync::Mutex::new(write_half)),
            peer_id: peer_id.to_string(),
            metrics: metrics.clone(),
            notify_remote,
        };
        let old = self
            .conns
            .write()
            .await
            .insert(conn_id.to_string(), Arc::new(RwLock::new(tc)));
        if old.is_none() {
            metrics.record_conn_open();
        }
    }

    async fn close_conn(&self, conn_id: &str, notify_remote: bool) {
        let tc = self.conns.write().await.remove(conn_id);
        if let Some(tc) = tc {
            let tc = tc.read().await;
            tc.metrics.record_conn_close();
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
        let to_remove: Vec<(String, ForwardPeerRuntime)> = conns
            .iter()
            .filter_map(|(id, tc)| {
                if let Ok(tc) = tc.try_read() {
                    if tc.peer_id == peer_id {
                        Some((id.clone(), tc.metrics.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        for (id, metrics) in to_remove {
            if conns.remove(&id).is_some() {
                metrics.record_conn_close();
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
            authorizer: self.authorizer.clone(),
        }
    }
}

fn is_expected_tcp_close(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
    )
}

fn log_tcp_io_error(context: &str, e: &std::io::Error) {
    if is_expected_tcp_close(e) {
        debug!("{}: {}", context, e);
    } else {
        tracing::error!("{}: {}", context, e);
    }
}
