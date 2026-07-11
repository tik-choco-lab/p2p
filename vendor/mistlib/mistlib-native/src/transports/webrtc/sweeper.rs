use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;

use mistlib_core::types::{ConnectionState, DeliveryMethod, NodeId};

#[cfg(test)]
const SWEEPER_INTERVAL_MS: u64 = 10;
#[cfg(not(test))]
const SWEEPER_INTERVAL_MS: u64 = 2000;

use super::{
    Peer, WebRtcTransport, CONNECTION_TIMEOUT_MS, DATA_CHANNEL_OPEN_TIMEOUT_MS,
    DISCONNECTED_GRACE_MS, LAST_DISCONNECT_TTL_MS,
};

impl WebRtcTransport {
    pub(crate) fn data_channel_open_timeout() -> Duration {
        std::env::var("MIST_WEBRTC_DC_OPEN_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(DATA_CHANNEL_OPEN_TIMEOUT_MS))
    }

    pub(crate) async fn has_required_data_channel(peer: &Peer) -> bool {
        let channels = peer.channels.read().await;
        channels
            .get(&DeliveryMethod::ReliableOrdered)
            .is_some_and(|dc| dc.ready_state() == RTCDataChannelState::Open)
    }

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

            let peer_opt = {
                let lock = handles.peers.read().await;
                lock.get(&node).cloned()
            };
            let has_required_data_channel = if let Some(peer) = &peer_opt {
                let channels = peer.channels.read().await;
                channels
                    .get(&DeliveryMethod::ReliableOrdered)
                    .is_some_and(|dc| dc.ready_state() == RTCDataChannelState::Open)
            } else {
                false
            };

            if still_connecting && !has_required_data_channel {
                // Guard on the exact `Peer` snapshot read above (not just
                // `node`): a fresh reconnect can already have replaced it
                // with a new, healthy peer by the time this fires, and an
                // unconditional-by-NodeId cleanup here would silently delete
                // that live registration while `connection_states` (marked
                // `Connected` by the new peer's own DC-open handler) is
                // never touched -- a permanent "Node not found" with no
                // close/state-change log to explain it.
                match peer_opt {
                    Some(peer) => {
                        let expected = Arc::downgrade(&peer);
                        handles
                            .cleanup_session_if_current(
                                &node,
                                &expected,
                                true,
                                "watchdog_connect_timeout",
                            )
                            .await;
                    }
                    // No live peer to protect -- safe to clear whatever
                    // stale bookkeeping remains for `node` unconditionally.
                    None => {
                        handles
                            .cleanup_session_with_reason(&node, true, "watchdog_connect_timeout")
                            .await;
                    }
                }
                tracing::warn!(
                    "[WebRTC DC Zombie] detected: connection timeout before ReliableOrdered data channel opened for {} (attempt={})",
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
                            let mut lock = handles.pc_connected_at.write().unwrap();
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
                    let has_required_data_channel = Self::has_required_data_channel(&peer).await;

                    // Disarm the DC-open zombie timer the moment the required
                    // (ReliableOrdered) data channel is confirmed open. This
                    // normally happens via the channel's own `on_open` handler
                    // (`peer.rs`), but that handler is one-shot in webrtc-rs
                    // and never fires again for a data channel that survived
                    // an ICE restart without needing to actually reopen --
                    // yet `RTCPeerConnectionState::Connected`'s handler
                    // re-arms `pc_connected_at` on *every* such recovery (see
                    // `recover_connected_from_grace`'s doc comment), including
                    // this one. Left uncleared, that stale timestamp would
                    // sit in the map forever: the next time this same
                    // (perfectly healthy) channel's `ready_state()` reports
                    // anything other than `Open`, however transient, the
                    // elapsed-time check below is already far past
                    // `data_channel_open_timeout` and would immediately
                    // force-close an otherwise-healthy session.
                    if has_required_data_channel {
                        handles.pc_connected_at.write().unwrap().remove(&node);
                    }

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
                            .is_some_and(|grace| {
                                grace.started_at.elapsed()
                                    >= Duration::from_millis(DISCONNECTED_GRACE_MS)
                            });
                    let data_channel_open_timeout = Self::data_channel_open_timeout();
                    let missing_required_channel = !has_required_data_channel
                        && handles
                            .pc_connected_at
                            .read()
                            .unwrap()
                            .get(&node)
                            .is_some_and(|at| at.elapsed() >= data_channel_open_timeout);

                    if failed_or_closed || disconnected_grace_expired || missing_required_channel {
                        let close_reason = if missing_required_channel {
                            "sweeper_dc_timeout"
                        } else if disconnected_grace_expired {
                            "sweeper_disconnected_grace_expired"
                        } else {
                            "sweeper_pc_failed_closed"
                        };
                        if disconnected_grace_expired {
                            tracing::warn!("[Sweeper] disconnected grace expired for {}", node);
                        }
                        if missing_required_channel {
                            tracing::warn!(
                                "[WebRTC DC Zombie] detected: ReliableOrdered data channel did not open within {:?} after pc connected for {}",
                                data_channel_open_timeout,
                                node
                            );
                        }
                        // Guard on the exact `peer` snapshot inspected above
                        // (not just `node`): a fresh reconnect racing this
                        // sweep can already have installed a new, healthy
                        // peer under the same `NodeId` between the reads
                        // above and this cleanup. An unconditional-by-NodeId
                        // removal here would silently delete that live
                        // registration from `self.peers` while
                        // `connection_states` (already marked `Connected` by
                        // the new peer's own DC-open handler) is left
                        // untouched -- a permanent "Node not found" with no
                        // close/state-change log to explain it.
                        let expected = Arc::downgrade(&peer);
                        handles
                            .cleanup_session_if_current(&node, &expected, true, close_reason)
                            .await;
                        if missing_required_channel {
                            tracing::warn!(
                                "[WebRTC DC Zombie] recovered: cleaned zombie session for {}",
                                node
                            );
                        }
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
