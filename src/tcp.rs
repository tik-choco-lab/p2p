use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::debug;

use std::sync::atomic::{AtomicBool, Ordering};

use crate::auth::{allow_all, SharedAuthorizer};
use crate::forward_runtime::{ForwardPeerRuntime, ForwardRuntime};
use crate::rtc::{RTCManager, TunnelMessage};

mod lifecycle;
mod local;
mod retry;
mod tunnel;

#[cfg(test)]
mod tests;

use lifecycle::{forward_key, spawn_handler_cleanup};
use retry::retry_with_backoff;

// Keep tunnel chunks below mistlib/WebRTC's message limit after JSON/base64
// framing overhead. Larger reads can produce "outbound packet larger than
// maximum message size" errors.
const TCP_BUFFER_SIZE: usize = 4096;
const TUNNEL_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// How long a peer's tunnel connections are kept alive (unsent, but not torn
/// down) after `EVENT_LEAVE`/tunnel-close fires for that peer, waiting for it
/// to rejoin before giving up. Chosen to comfortably exceed the time a
/// legitimate reconnect (full room rejoin after a failed ICE restart) should
/// take. See `schedule_close_all_for_peer` for the resume mechanism.
const PEER_LEAVE_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// Interval between tunnel keepalive pings sent to each peer that has at
/// least one active `TunnelConn`. Keeps NAT bindings warm and feeds
/// mistlib's liveness/session tracking with reliable-channel traffic even
/// when the tunneled application (e.g. an idle SSH session) sends nothing.
const TUNNEL_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);

/// Tunnel message-type discriminants (the `type` field of [`TunnelMessage`]).
/// Unknown/unrecognized types (e.g. from a newer or older peer) are ignored
/// gracefully by `on_tunnel_message` -- do not add a type here without
/// keeping that fallback in mind.
pub(super) const MSG_TYPE_CONNECT: &str = "connect";
pub(super) const MSG_TYPE_DATA: &str = "data";
pub(super) const MSG_TYPE_CLOSE: &str = "close";
/// Keepalive: purely to keep the channel/NAT mapping warm. Carries no
/// payload and is otherwise ignored by the receiver.
pub(super) const MSG_TYPE_PING: &str = "ping";

struct TunnelConn {
    writer: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    peer_id: String,
    metrics: ForwardPeerRuntime,
    notify_remote: bool,
    /// Tracks the expected next `data` sequence number from the remote peer
    /// for this conn, to detect the gaps/duplicates described on
    /// [`TunnelMessage::seq`]. See [`tunnel::SeqState`].
    recv_seq: tunnel::SeqState,
}

pub struct TcpManager {
    rtc_manager: RTCManager,
    conns: Arc<RwLock<HashMap<String, Arc<RwLock<TunnelConn>>>>>,
    remote_addr: String,
    target: String,
    runtime: ForwardRuntime,
    authorizer: SharedAuthorizer,
    /// Per-peer epoch counter backing the leave-grace window: incremented
    /// each time a peer leaves (scheduling a delayed close) or rejoins
    /// (invalidating any pending close). A scheduled close only proceeds if
    /// the epoch it captured is still current when its timer fires.
    peer_close_epoch: Arc<RwLock<HashMap<String, u64>>>,
    /// Guards against spawning more than one keepalive task at a time. The
    /// task stops itself once no conns remain and is re-armed by
    /// `track_conn` the next time one is added.
    keepalive_running: Arc<AtomicBool>,
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
            peer_close_epoch: Arc::new(RwLock::new(HashMap::new())),
            keepalive_running: Arc::new(AtomicBool::new(false)),
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
                    debug!(
                        "tunnel DC closed for peer {}; starting {:?} grace window before closing TCP conns",
                        peer_id, PEER_LEAVE_GRACE
                    );
                    mgr.schedule_close_all_for_peer(peer_id).await;
                });
            })
            .await;

        // If the peer rejoins (fresh EVENT_JOIN, or simply the first payload
        // we see from it again) within the grace window above, cancel the
        // pending close. Tunnel sends resolve the peer's mistlib session by
        // peer_id at send time (see `send_to`/`send_tunnel_to`), not via a
        // handle cached at connect time, so once mistlib re-establishes a
        // session for this peer_id, in-flight `TunnelConn`s resume sending
        // and receiving with no further action needed here.
        let mgr_open = mgr.clone();
        rtc_manager
            .on_tunnel_open(move |peer_id| {
                let mgr = mgr_open.clone();
                tokio::spawn(async move {
                    mgr.cancel_pending_close(&peer_id).await;
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
            recv_seq: tunnel::SeqState::new(),
        };
        let old = self
            .conns
            .write()
            .await
            .insert(conn_id.to_string(), Arc::new(RwLock::new(tc)));
        if old.is_none() {
            metrics.record_conn_open();
        }
        self.ensure_keepalive_task().await;
    }

    async fn close_conn(&self, conn_id: &str, notify_remote: bool) {
        let tc = self.conns.write().await.remove(conn_id);
        if let Some(tc) = tc {
            let tc = tc.read().await;
            tc.metrics.record_conn_close();
            if notify_remote && tc.notify_remote {
                let close_msg = TunnelMessage {
                    msg_type: MSG_TYPE_CLOSE.into(),
                    conn_id: conn_id.to_string(),
                    target: self.target.clone(),
                    payload: None,
                    seq: None,
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

    /// Sends `msg` to `peer_id`, retrying with backoff on failure (see
    /// [`retry_with_backoff`]) instead of giving up on the first transient
    /// error. Stays responsive to shutdown by racing each backoff sleep
    /// against `shutdown`.
    pub(super) async fn send_to_with_retry(
        &self,
        peer_id: &str,
        msg: &TunnelMessage,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        retry_with_backoff(|| self.send_to(peer_id, msg), shutdown).await
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

    /// Starts (or restarts) the leave-grace window for `peer_id`: bumps its
    /// close epoch and spawns a timer that closes all of the peer's conns
    /// only if that epoch is still current when the timer fires (i.e. the
    /// peer hasn't rejoined in the meantime via `cancel_pending_close`).
    /// Conns are left fully functional during the window -- inbound data is
    /// still accepted and outbound sends are still attempted (and retried,
    /// see `send_to_with_retry`) as normal.
    async fn schedule_close_all_for_peer(self: &Arc<Self>, peer_id: String) {
        let epoch = {
            let mut epochs = self.peer_close_epoch.write().await;
            let e = epochs.entry(peer_id.clone()).or_insert(0);
            *e = e.wrapping_add(1);
            *e
        };

        let mgr = self.clone();
        tokio::spawn(async move {
            let mut shutdown = mgr.runtime.subscribe();
            tokio::select! {
                _ = tokio::time::sleep(PEER_LEAVE_GRACE) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // Shutting down anyway; no need to force a close --
                        // the conns will be dropped along with everything
                        // else.
                        return;
                    }
                }
            }

            let current = mgr.peer_close_epoch.read().await.get(&peer_id).copied();
            if current == Some(epoch) {
                debug!(
                    "peer {} did not rejoin within the {:?} grace window; closing TCP conns",
                    peer_id, PEER_LEAVE_GRACE
                );
                mgr.close_all_for_peer(&peer_id).await;
            } else {
                debug!(
                    "peer {} rejoined within grace window; conns resumed",
                    peer_id
                );
            }
        });
    }

    /// Invalidates any pending close scheduled by
    /// `schedule_close_all_for_peer` for `peer_id`. A no-op if none is
    /// pending.
    async fn cancel_pending_close(&self, peer_id: &str) {
        let mut epochs = self.peer_close_epoch.write().await;
        if let Some(e) = epochs.get_mut(peer_id) {
            *e = e.wrapping_add(1);
        }
    }

    /// Ensures a single keepalive task is running for this manager. Cheap to
    /// call repeatedly (e.g. from `track_conn` on every new connection): the
    /// atomic swap makes it a no-op if a task is already active.
    async fn ensure_keepalive_task(&self) {
        if self.keepalive_running.swap(true, Ordering::SeqCst) {
            return;
        }
        let mgr = Arc::new(self.clone_inner());
        tokio::spawn(async move {
            let mut shutdown = mgr.runtime.subscribe();
            'outer: loop {
                tokio::select! {
                    _ = tokio::time::sleep(TUNNEL_KEEPALIVE_INTERVAL) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break 'outer;
                        }
                    }
                }
                if *shutdown.borrow() {
                    break 'outer;
                }

                let peer_ids = mgr.send_keepalive_pings().await;
                if peer_ids.is_empty() {
                    // Nothing to keep warm; stop rather than spin forever.
                    // `track_conn` re-arms this task on the next conn. (A
                    // conn added in the instant between this check and
                    // clearing `keepalive_running` below just waits for the
                    // next `track_conn` call to re-arm -- benign, since a
                    // brand new conn's own traffic is itself
                    // liveness-preserving.)
                    break 'outer;
                }
            }

            mgr.keepalive_running.store(false, Ordering::SeqCst);
        });
    }

    /// Sends one keepalive ping to every peer with at least one active
    /// `TunnelConn`, returning the set of peers pinged (empty if there was
    /// nothing to do). Send failures are logged and otherwise ignored --
    /// keepalive traffic must never tear down a connection.
    async fn send_keepalive_pings(&self) -> std::collections::HashSet<String> {
        let peer_ids = self.active_peer_ids().await;
        for peer_id in &peer_ids {
            let ping = TunnelMessage {
                msg_type: MSG_TYPE_PING.into(),
                conn_id: String::new(),
                target: self.target.clone(),
                payload: None,
                seq: None,
            };
            if let Err(e) = self.send_to(peer_id, &ping).await {
                debug!("keepalive ping to {} failed: {}", peer_id, e);
            }
        }
        peer_ids
    }

    /// Distinct peer ids with at least one tracked `TunnelConn`.
    async fn active_peer_ids(&self) -> std::collections::HashSet<String> {
        let conns = self.conns.read().await;
        let mut ids = std::collections::HashSet::new();
        for tc in conns.values() {
            ids.insert(tc.read().await.peer_id.clone());
        }
        ids
    }

    fn clone_inner(&self) -> Self {
        Self {
            rtc_manager: self.rtc_manager.clone(),
            conns: self.conns.clone(),
            remote_addr: self.remote_addr.clone(),
            target: self.target.clone(),
            runtime: self.runtime.clone(),
            authorizer: self.authorizer.clone(),
            peer_close_epoch: self.peer_close_epoch.clone(),
            keepalive_running: self.keepalive_running.clone(),
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
