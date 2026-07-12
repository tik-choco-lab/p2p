use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, error, warn};

use crate::auth::AuthRequest;
use crate::rtc::TunnelMessage;

use super::{
    log_tcp_io_error, TcpManager, MSG_TYPE_CLOSE, MSG_TYPE_CONNECT, MSG_TYPE_DATA, MSG_TYPE_PING,
};

impl TcpManager {
    pub(super) async fn on_tunnel_message(&self, peer_id: &str, data: &[u8]) {
        if data.is_empty() || data[0] != b'{' {
            return;
        }
        let tm: TunnelMessage = match serde_json::from_slice(data) {
            Ok(m) => m,
            Err(_) => return,
        };
        match tm.msg_type.as_str() {
            MSG_TYPE_CONNECT => self.handle_remote_connect(peer_id, &tm).await,
            MSG_TYPE_DATA => self.handle_remote_data(&tm).await,
            MSG_TYPE_CLOSE => self.close_conn(&tm.conn_id, false).await,
            // Keepalive: purely to keep the channel/NAT mapping warm, no
            // action needed on receipt.
            MSG_TYPE_PING => {}
            // Unknown/unrecognized types (older/newer peer versions) are
            // ignored for cross-version compat.
            _ => {}
        }
    }

    async fn handle_remote_connect(&self, peer_id: &str, tm: &TunnelMessage) {
        let req = AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: self.target.clone(),
            target_addr: self.remote_addr.clone(),
            proto: "tcp".to_string(),
        };
        let decision = self.authorizer.authorize(&req).await;
        if !decision.is_allowed() {
            debug!(
                "denied tcp tunnel connection from {} to {}",
                peer_id, self.target
            );
            self.send_close(peer_id, &tm.conn_id).await;
            return;
        }

        let addr = &self.remote_addr;
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                let (read_half, write_half) = stream.into_split();
                self.track_conn(&tm.conn_id, write_half, peer_id, true)
                    .await;
                let mgr = Arc::new(self.clone_inner());
                let cid = tm.conn_id.clone();
                let pid = peer_id.to_string();
                tokio::spawn(async move { mgr.forward_tcp_to_dc(cid, pid, read_half).await });
            }
            Err(e) => {
                error!("failed to connect to remote ({}): {}", addr, e);
                self.send_close(peer_id, &tm.conn_id).await;
            }
        }
    }

    async fn send_close(&self, peer_id: &str, conn_id: &str) {
        let close_msg = TunnelMessage {
            msg_type: MSG_TYPE_CLOSE.into(),
            conn_id: conn_id.to_string(),
            target: self.target.clone(),
            payload: None,
            seq: None,
        };
        let _ = self.send_to(peer_id, &close_msg).await;
    }

    async fn handle_remote_data(&self, tm: &TunnelMessage) {
        // Look the conn up and run every message (even one with an
        // empty/missing payload) through `recv_seq.observe` *before* any
        // early return on payload emptiness below. A zero-byte `data`
        // message still consumes its seq number; skipping `observe` for it
        // would leave `next_expected` behind, so the *next* (non-empty)
        // message would be misjudged as a gap and the conn would be torn
        // down for no reason.
        let tc = {
            let conns = self.conns.read().await;
            conns.get(&tm.conn_id).cloned()
        };
        let Some(tc) = tc else {
            return;
        };

        let (writer, metrics, decision) = {
            let mut tc = tc.write().await;
            let decision = tc.recv_seq.observe(tm.seq);
            (tc.writer.clone(), tc.metrics.clone(), decision)
        };

        match decision {
            SeqDecision::Duplicate { received } => {
                debug!(
                    "dropping duplicate tunnel data for conn {} (seq {})",
                    tm.conn_id, received
                );
                return;
            }
            SeqDecision::Gap { expected, received } => {
                warn!(
                    "tunnel data gap for conn {}: expected seq {} but got {}; \
                     closing conn to avoid corrupting the downstream stream",
                    tm.conn_id, expected, received
                );
                self.close_conn(&tm.conn_id, true).await;
                return;
            }
            SeqDecision::Inconsistent => {
                warn!(
                    "tunnel data for conn {} switched from sequenced to unsequenced \
                     mid-stream; closing conn",
                    tm.conn_id
                );
                self.close_conn(&tm.conn_id, true).await;
                return;
            }
            SeqDecision::Accept => {}
        }

        let payload = match &tm.payload {
            Some(p) if !p.is_empty() => p,
            _ => return,
        };

        let mut writer = writer.lock().await;
        if let Err(e) = writer.write_all(payload).await {
            log_tcp_io_error("failed to write to tcp", &e);
            drop(writer);
            self.close_conn(&tm.conn_id, true).await;
        } else {
            metrics.record_bytes_out(payload.len());
        }
    }
}

/// Per-conn tracking of the receive-side `data` sequence number, used to
/// detect the gaps and duplicates described on [`TunnelMessage::seq`].
///
/// Kept as a small, tokio-free struct so the decision logic can be unit
/// tested directly without spinning up a [`TcpManager`]/real sockets.
#[derive(Debug)]
pub(super) struct SeqState {
    /// Sequence number expected for the next `data` message, starting at 1.
    next_expected: u64,
    /// Whether we've ever observed a sequenced (`Some`) message on this
    /// conn. Used to flag a peer that switches from sequenced to
    /// unsequenced mid-stream (e.g. a bug, or two mismatched builds talking
    /// to each other in an unexpected way) as [`SeqDecision::Inconsistent`]
    /// rather than silently trusting the unsequenced message.
    seq_seen: bool,
}

/// Outcome of [`SeqState::observe`] for one inbound `data` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SeqDecision {
    /// No gap/duplicate detected (or the sender doesn't use seq numbers at
    /// all yet): write the payload through as usual.
    Accept,
    /// Already-seen seq: a retried send that in fact arrived the first time
    /// (see `send_to_with_retry`). Drop the payload without writing it.
    Duplicate { received: u64 },
    /// A seq was skipped -- mistlib's `ReorderBuffer` most likely dropped a
    /// message delayed more than its reorder window. The conn's byte stream
    /// is now unrecoverably out of sync and must be torn down.
    Gap { expected: u64, received: u64 },
    /// This conn previously received sequenced messages but just received
    /// an unsequenced one (or vice versa isn't representable here -- see
    /// `observe`). Treated conservatively as unsafe to continue.
    Inconsistent,
}

impl SeqState {
    pub(super) fn new() -> Self {
        Self {
            next_expected: 1,
            seq_seen: false,
        }
    }

    /// Validates `seq` (the `data` message's `TunnelMessage::seq`) against
    /// this conn's expected next sequence number, updating internal state
    /// as a side effect for `Accept`/`Duplicate` decisions that involve a
    /// sequenced message.
    fn observe(&mut self, seq: Option<u64>) -> SeqDecision {
        match seq {
            // Legacy sender (predates this field): fall back to unordered,
            // best-effort delivery exactly as before -- unless this conn has
            // already proven the peer *does* send seq numbers, in which case
            // a sudden unsequenced message indicates something is wrong.
            None => {
                if self.seq_seen {
                    SeqDecision::Inconsistent
                } else {
                    SeqDecision::Accept
                }
            }
            Some(received) => {
                self.seq_seen = true;
                if received < self.next_expected {
                    SeqDecision::Duplicate { received }
                } else if received == self.next_expected {
                    self.next_expected += 1;
                    SeqDecision::Accept
                } else {
                    SeqDecision::Gap {
                        expected: self.next_expected,
                        received,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod seq_state_tests {
    use super::{SeqDecision, SeqState};

    #[test]
    fn in_order_sequence_is_accepted() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(3)), SeqDecision::Accept);
    }

    #[test]
    fn duplicate_seq_is_flagged_and_does_not_advance_expectation() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Accept);
        assert_eq!(s.observe(Some(2)), SeqDecision::Duplicate { received: 2 });
        // The tracker should still expect 3 next, unaffected by the dup.
        assert_eq!(s.observe(Some(3)), SeqDecision::Accept);
    }

    #[test]
    fn gap_in_sequence_is_flagged() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(
            s.observe(Some(3)),
            SeqDecision::Gap {
                expected: 2,
                received: 3
            }
        );
    }

    #[test]
    fn unsequenced_legacy_sender_is_always_accepted() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(None), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Accept);
    }

    #[test]
    fn switching_from_sequenced_to_unsequenced_is_inconsistent() {
        let mut s = SeqState::new();
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        assert_eq!(s.observe(None), SeqDecision::Inconsistent);
    }

    #[test]
    fn first_expected_seq_is_one() {
        let mut s = SeqState::new();
        assert_eq!(
            s.observe(Some(2)),
            SeqDecision::Gap {
                expected: 1,
                received: 2
            }
        );
    }

    #[test]
    fn switching_from_unsequenced_to_sequenced_is_accepted_and_enables_gap_detection() {
        let mut s = SeqState::new();
        // A legacy (unsequenced) message arrives first -- accepted as
        // before, and must not itself count as "seq 1".
        assert_eq!(s.observe(None), SeqDecision::Accept);
        // The peer then starts sending seq numbers, starting at 1: also
        // accepted, since this is the first sequenced message seen.
        assert_eq!(s.observe(Some(1)), SeqDecision::Accept);
        // Gap detection is now live: a skipped seq 2 is caught.
        assert_eq!(
            s.observe(Some(3)),
            SeqDecision::Gap {
                expected: 2,
                received: 3
            }
        );
    }
}
