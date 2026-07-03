use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;

use mistlib_core::types::{ConnectionState, NodeId};

#[cfg(test)]
const SWEEPER_INTERVAL_MS: u64 = 10;
#[cfg(not(test))]
const SWEEPER_INTERVAL_MS: u64 = 2000;

use super::{
    WebRtcTransport, CONNECTION_TIMEOUT_MS, DISCONNECTED_GRACE_MS, LAST_DISCONNECT_TTL_MS,
};

impl WebRtcTransport {
    pub(crate) fn spawn_connection_watchdog(&self, node: NodeId, attempt_id: u32) {
        let handles = self.peer_handles();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(CONNECTION_TIMEOUT_MS)).await;

            let is_current_attempt = {
                let lock = handles.connection_attempt_ids.read().unwrap();
                matches!(lock.get(&node), Some(id) if *id == attempt_id)
            };

            if !is_current_attempt {
                return;
            }

            let still_connecting = {
                let lock = handles.connection_states.read().unwrap();
                matches!(lock.get(&node), Some(ConnectionState::Connecting))
            };

            let has_open_channel = {
                let peer_opt = {
                    let lock = handles.peers.read().await;
                    lock.get(&node).cloned()
                };
                if let Some(peer) = peer_opt {
                    let channels = peer.channels.read().await;
                    channels
                        .values()
                        .any(|dc| dc.ready_state() == RTCDataChannelState::Open)
                } else {
                    false
                }
            };

            if still_connecting && !has_open_channel {
                handles.cleanup_session(&node, true).await;
                tracing::warn!(
                    "[Watchdog] Session cleanup on timeout for {} (attempt={})",
                    node,
                    attempt_id
                );
            }
        });
    }

    pub(super) fn ensure_session_sweeper(&self) {
        if self.sweeper_started.swap(true, Ordering::SeqCst) {
            return;
        }

        let cancel = CancellationToken::new();
        {
            let mut lock = self.sweeper_cancel.lock().unwrap();
            *lock = Some(cancel.clone());
        }

        let handles = self.peer_handles();
        let cancel_for_task = cancel.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_for_task.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(SWEEPER_INTERVAL_MS)) => {}
                }

                {
                    let ttl = Duration::from_millis(LAST_DISCONNECT_TTL_MS);
                    let mut lock = handles.last_disconnect_at.write().unwrap();
                    lock.retain(|_, at| at.elapsed() < ttl);
                }

                {
                    let peers_n = handles.peers.read().await.len();
                    let states_n = handles.connection_states.read().unwrap().len();
                    let pending_n = handles.pending_candidates.read().await.len();
                    let last_disc_n = handles.last_disconnect_at.read().unwrap().len();
                    crate::mem::log_mem_tick(peers_n, states_n, pending_n, last_disc_n);
                }

                let nodes = {
                    let lock = handles.connection_states.read().unwrap();
                    lock.keys().cloned().collect::<Vec<_>>()
                };

                for node in nodes {
                    let peer_opt = {
                        let lock = handles.peers.read().await;
                        lock.get(&node).cloned()
                    };

                    let Some(peer) = peer_opt else {
                        {
                            let mut lock = handles.connection_states.write().unwrap();
                            lock.remove(&node);
                        }
                        {
                            let mut lock = handles.disconnected_since.write().unwrap();
                            lock.remove(&node);
                        }
                        {
                            let mut lock = handles.pending_candidates.write().await;
                            lock.remove(&node);
                        }
                        continue;
                    };

                    let pc_state = peer.pc.connection_state();
                    let state_snapshot = {
                        let lock = handles.connection_states.read().unwrap();
                        lock.get(&node)
                            .copied()
                            .unwrap_or(ConnectionState::Disconnected)
                    };
                    let has_open_channel = {
                        let channels = peer.channels.read().await;
                        channels
                            .values()
                            .any(|dc| dc.ready_state() == RTCDataChannelState::Open)
                    };

                    let failed_or_closed = matches!(
                        pc_state,
                        RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
                    );
                    let disconnected_grace_expired =
                        matches!(pc_state, RTCPeerConnectionState::Disconnected)
                            || state_snapshot == ConnectionState::Reconnecting;
                    let disconnected_grace_expired = disconnected_grace_expired
                        && handles
                            .disconnected_since
                            .read()
                            .unwrap()
                            .get(&node)
                            .is_some_and(|at| {
                                at.elapsed() >= Duration::from_millis(DISCONNECTED_GRACE_MS)
                            });
                    let missing_channel =
                        state_snapshot == ConnectionState::Connected && !has_open_channel;

                    if failed_or_closed || disconnected_grace_expired || missing_channel {
                        if disconnected_grace_expired {
                            tracing::warn!("[Sweeper] disconnected grace expired for {}", node);
                        }
                        handles.cleanup_session(&node, true).await;
                        tracing::warn!("[Sweeper] Force cleaned session for {}", node);
                    }
                }
            }
        });
    }

    pub fn stop_session_sweeper(&self) {
        self.sweeper_started.store(false, Ordering::SeqCst);
        let cancel = {
            let mut lock = self.sweeper_cancel.lock().unwrap();
            lock.take()
        };
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
    }
}
