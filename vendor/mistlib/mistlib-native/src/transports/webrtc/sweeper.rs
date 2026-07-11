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
    DisconnectGrace, GraceOrigin, Peer, WebRtcTransport, CONNECTION_TIMEOUT_MS,
    DATA_CHANNEL_OPEN_TIMEOUT_MS, DISCONNECTED_GRACE_MS, LAST_DISCONNECT_TTL_MS,
};

/// Outcome of evaluating a peer's disconnect grace against its actual health.
/// Pure (no lock/`RTCPeerConnection` access) so the `LivenessSuspect`
/// false-positive suppression below is exhaustively unit-testable without
/// mocking a real `RTCPeerConnection` -- see `GraceOrigin`'s doc for why the
/// distinction exists: a liveness-suspect grace can start against a peer
/// whose SCTP association (and every data channel except the best-effort
/// ping one) never actually had a problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraceExpiryDecision {
    /// No grace is running, or it hasn't reached the grace duration yet.
    Wait,
    /// Grace expired, but the peer looks genuinely healthy (only possible for
    /// a `LivenessSuspect`-origin grace): restore `Connected` instead of
    /// tearing the session down.
    RecoverFalsePositive,
    /// Grace expired and the peer is still unhealthy, or this is an
    /// `Ice`-origin grace (never second-guessed against the live pc state) --
    /// reap as before.
    Reap,
}

/// Decides what the sweeper should do about `node`'s disconnect grace, if
/// any. `grace` is a snapshot of `disconnected_since[node]`; `pc_state` and
/// `has_required_data_channel` are the sweeper's own live reads of the actual
/// `RTCPeerConnection`/data-channel state for the same node.
///
/// Only a `LivenessSuspect`-origin grace is ever second-guessed: an
/// `Ice`-origin grace means the peer connection itself already reported
/// `Disconnected`, so there is nothing to re-validate. A `LivenessSuspect`
/// grace, by contrast, is started purely on a missed-PONG heuristic
/// (`OverlayAction::SuspectDisconnected`, mistlib-core's `stats::ping`) that
/// runs over the best-effort `Unreliable` channel (`max_retransmits: Some(0)`)
/// and never itself inspects the real peer connection -- packet loss on that
/// one channel alone is not evidence that the `ReliableOrdered` channel
/// actually carrying application data (e.g. a tunneled SSH session) is
/// unhealthy. Reaping on that signal alone tears down a perfectly good
/// connection; this lets the sweeper re-check reality before doing so.
pub(crate) fn decide_grace_expiry(
    grace: Option<DisconnectGrace>,
    pc_state: RTCPeerConnectionState,
    has_required_data_channel: bool,
    grace_ms: u64,
) -> GraceExpiryDecision {
    let Some(grace) = grace else {
        return GraceExpiryDecision::Wait;
    };
    if grace.started_at.elapsed() < Duration::from_millis(grace_ms) {
        return GraceExpiryDecision::Wait;
    }
    if grace.origin == GraceOrigin::LivenessSuspect
        && pc_state == RTCPeerConnectionState::Connected
        && has_required_data_channel
    {
        return GraceExpiryDecision::RecoverFalsePositive;
    }
    GraceExpiryDecision::Reap
}

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
                    let grace_snapshot = handles
                        .disconnected_since
                        .read()
                        .unwrap()
                        .get(&node)
                        .copied();
                    let grace_decision = decide_grace_expiry(
                        grace_snapshot,
                        pc_state,
                        has_required_data_channel,
                        DISCONNECTED_GRACE_MS,
                    );
                    if grace_decision == GraceExpiryDecision::RecoverFalsePositive {
                        // The missed-PONG heuristic that started this grace
                        // (mistlib-core's `stats::ping`) only ever watches the
                        // best-effort `Unreliable` channel -- it never checked
                        // the real `RTCPeerConnection`. Confirmed healthy here
                        // (pc Connected + ReliableOrdered DC open), so this was
                        // a false positive: clear the grace and restore
                        // `Connected` instead of destroying a working session
                        // (see `recover_connected_from_grace`'s doc for why
                        // this is safe to reuse for a non-ICE-restart
                        // recovery too -- it only checks that a grace is
                        // pending).
                        tracing::warn!(
                            "[Sweeper] liveness false-positive suppressed for {}: pc is Connected \
                             and the required data channel is open, so the missed-PONG grace is \
                             being cleared instead of reaping the session",
                            node
                        );
                        handles.recover_connected_from_grace(&node);
                        continue;
                    }
                    let disconnected_grace_expired = grace_decision == GraceExpiryDecision::Reap;
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
