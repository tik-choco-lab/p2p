use super::{
    rollback_to_stable_on_failure, Peer, WebRtcTransport, CONNECT_REQUEST_RETRY_INTERVAL_MS,
    DEFAULT_CONNECT_REQUEST_RETRIES,
};
use async_trait::async_trait;
use mistlib_core::signaling::{MessageContent, SignalingData, SignalingHandler, SignalingType};
use mistlib_core::transport::Transport;
use mistlib_core::types::{ConnectionState, NodeId};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::signaling_state::RTCSignalingState;

impl WebRtcTransport {
    fn connect_request_retry_limit() -> u32 {
        std::env::var("MIST_WEBRTC_CONNECT_REQUEST_RETRIES")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CONNECT_REQUEST_RETRIES)
    }

    fn reserve_connect_request_attempt(&self, node: &NodeId) -> Option<u32> {
        let mut attempts = self.connect_request_attempt_ids.write().unwrap();
        if attempts.contains_key(node) {
            return None;
        }
        let attempt_id = self
            .next_connection_attempt_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        attempts.insert(node.clone(), attempt_id);
        Some(attempt_id)
    }

    pub(crate) fn clear_connect_request_attempt(&self, node: &NodeId) {
        self.connect_request_attempt_ids
            .write()
            .unwrap()
            .remove(node);
    }

    pub(crate) async fn request_lower_id_offer(
        &self,
        node: &NodeId,
    ) -> mistlib_core::error::Result<()> {
        {
            let peers = self.peers.read().await;
            if peers.contains_key(node) {
                return Ok(());
            }
        }
        if self.has_active_session(node) {
            return Ok(());
        }

        let Some(attempt_id) = self.reserve_connect_request_attempt(node) else {
            return Ok(());
        };

        if let Err(err) = self.send_connect_request(node).await {
            tracing::warn!(
                "[WebRTC Request] send_failed node={} attempt={} retry=1 error={:?}",
                node,
                attempt_id,
                err
            );
        }
        self.spawn_connect_request_retry(node.clone(), attempt_id);
        Ok(())
    }

    pub(crate) async fn send_connect_request(
        &self,
        node: &NodeId,
    ) -> mistlib_core::error::Result<()> {
        self.signaler
            .send_signaling(
                node,
                MessageContent::Data(SignalingData {
                    sender_id: self.local_node_id.clone(),
                    receiver_id: node.clone(),
                    room_id: self.get_room_id(),
                    data: String::new(),
                    signaling_type: SignalingType::Request,
                }),
            )
            .await
    }

    fn spawn_connect_request_retry(&self, node: NodeId, attempt_id: u32) {
        let retry_limit = Self::connect_request_retry_limit();
        if retry_limit <= 1 {
            return;
        }

        let signaler = self.signaler.clone();
        let local_node_id = self.local_node_id.clone();
        let room_id = self.get_room_id();
        let pending_attempts = self.connect_request_attempt_ids.clone();
        let peers = self.peers.clone();
        let states = self.connection_states.clone();

        tokio::spawn(async move {
            for retry in 2..=retry_limit {
                tokio::time::sleep(Duration::from_millis(CONNECT_REQUEST_RETRY_INTERVAL_MS)).await;

                let is_current_attempt = {
                    let attempts = pending_attempts.read().unwrap();
                    matches!(attempts.get(&node), Some(id) if *id == attempt_id)
                };
                if !is_current_attempt {
                    return;
                }

                let has_peer = {
                    let peers = peers.read().await;
                    peers.contains_key(&node)
                };
                let has_state = {
                    let states = states.read().unwrap();
                    states.contains_key(&node)
                };
                if has_peer || has_state {
                    pending_attempts.write().unwrap().remove(&node);
                    return;
                }

                let result = signaler
                    .send_signaling(
                        &node,
                        MessageContent::Data(SignalingData {
                            sender_id: local_node_id.clone(),
                            receiver_id: node.clone(),
                            room_id: room_id.clone(),
                            data: String::new(),
                            signaling_type: SignalingType::Request,
                        }),
                    )
                    .await;
                if let Err(err) = result {
                    tracing::warn!(
                        "[WebRTC Request] send_failed node={} attempt={} retry={} error={:?}",
                        node,
                        attempt_id,
                        retry,
                        err
                    );
                }
            }

            let still_pending = {
                let mut attempts = pending_attempts.write().unwrap();
                matches!(attempts.get(&node), Some(id) if *id == attempt_id)
                    .then(|| attempts.remove(&node))
                    .flatten()
                    .is_some()
            };
            if still_pending {
                tracing::warn!(
                    "[WebRTC Request] retry_exhausted node={} attempt={} retries={}",
                    node,
                    attempt_id,
                    retry_limit
                );
            }
        });
    }
}

impl WebRtcTransport {
    pub(crate) async fn handle_offer(
        &self,
        remote_id: NodeId,
        sdp: String,
    ) -> crate::error::Result<()> {
        // An inbound offer means the remote is alive and negotiating with us,
        // so any pending CONNECT_REQUEST retry loop for this node is obsolete
        // regardless of which path below handles the offer.
        self.clear_connect_request_attempt(&remote_id);

        // If we already have a live peer for this remote, this offer is
        // (almost always) a renegotiation on the existing RTCPeerConnection
        // -- e.g. tc-chat's browser side calling `publish_local_track` to add
        // a screen-share track mid-session, which renegotiates with a fresh
        // SDP offer over the same signaling channel rather than opening a
        // new one. Apply it directly to the existing peer instead of falling
        // through to the brand-new-connection path below, which would
        // discard the live peer (`old_peer.close_all()`) and its already-open
        // data channels/tracks out from under an active session.
        let existing_peer = {
            let peers = self.peers.read().await;
            peers.get(&remote_id).cloned()
        };

        if let Some(peer) = existing_peer {
            // Glare resolution: if we're the impolite side (lower ID =
            // initiator) and we have our own offer in flight on this same
            // peer connection, ignore their offer so ours proceeds instead of
            // colliding. This only fires for a genuine collision (our own
            // offer is unanswered, signaling_state == HaveLocalOffer) --
            // legitimate renegotiation offers arriving once negotiation is
            // Stable are not glare and must not be dropped based on ID
            // ordering alone (that would permanently block renegotiation
            // from a peer whenever our local ID happens to sort lower).
            if self.local_node_id.0 < remote_id.0
                && peer.pc.signaling_state() == RTCSignalingState::HaveLocalOffer
            {
                tracing::debug!(
                    "[Glare] ignoring offer from {} (we are impolite side, our offer is in flight)",
                    remote_id
                );
                return Ok(());
            }

            return self.apply_offer(remote_id, sdp, peer).await;
        }

        let mut newly_reserved = false;

        {
            let mut states = self.connection_states.write().unwrap();
            if !states.contains_key(&remote_id) {
                let max = self
                    .max_connections
                    .load(std::sync::atomic::Ordering::Relaxed) as usize;
                let count = states
                    .values()
                    .filter(|s| {
                        matches!(
                            **s,
                            ConnectionState::Connected
                                | ConnectionState::Connecting
                                | ConnectionState::Reconnecting
                        )
                    })
                    .count();
                if count >= max {
                    return Ok(());
                }
                states.insert(remote_id.clone(), ConnectionState::Connecting);
                tracing::warn!(
                    "[CS] INSERT handle_offer: {} total={}",
                    remote_id,
                    states.len()
                );
                newly_reserved = true;
            }
        }

        self.acquire_handshake_permit(&remote_id).await?;

        if !self.has_active_session(&remote_id) {
            self.handshake_permits.write().unwrap().remove(&remote_id);
            return Ok(());
        }

        let peer = match self.create_pc(remote_id.clone()).await {
            Ok(p) => p,
            Err(e) => {
                self.handshake_permits.write().unwrap().remove(&remote_id);
                if newly_reserved {
                    let mut states = self.connection_states.write().unwrap();
                    states.remove(&remote_id);
                    tracing::warn!(
                        "[CS] REMOVE handle_offer_create_err: {} total={}",
                        remote_id,
                        states.len()
                    );
                }
                return Err(e);
            }
        };

        let old_peer = {
            let mut peers = self.peers.write().await;
            peers.insert(remote_id.clone(), peer.clone())
        };
        if let Some(old_peer) = old_peer {
            tracing::warn!(
                "[WebRTC Close] reason=handle_offer_replace_peer node={}",
                remote_id
            );
            old_peer.close_all().await;
            crate::mem::record_peer_cleaned();
        }
        crate::mem::record_peer_inserted();

        let attempt_id = self.reserve_connection_attempt(&remote_id);
        self.spawn_connection_watchdog(remote_id.clone(), attempt_id);

        let result = self.apply_offer(remote_id.clone(), sdp, peer.clone()).await;

        if result.is_err() {
            // Guard on the peer this call just inserted: a concurrent
            // `connect_inner` for the same `NodeId` racing this failed
            // answer attempt may have already installed its own, healthy
            // peer by the time we get here. An unconditional-by-NodeId
            // cleanup would delete that live registration instead of this
            // attempt's own (failed) one -- see
            // `PeerSharedHandles::cleanup_session_if_current`'s doc comment.
            let expected = Arc::downgrade(&peer);
            self.cleanup_session_if_current(&remote_id, &expected, true, "handle_offer_error")
                .await;
        } else if self.has_published_tracks() {
            // New-peer hook, answer-side completion: `create_pc` (called
            // above) already attached every published track to `peer`'s
            // RTCPeerConnection before `apply_offer` ran, but per JSEP the
            // answer we just sent cannot introduce m= sections beyond what
            // the remote's offer contained -- `webrtc-rs`'s `create_answer`
            // silently omits any local transceiver that doesn't match one in
            // the remote offer (`generate_matched_sdp(..., includeUnmatched:
            // false, ...)`). So the published tracks' transceivers exist on
            // the peer connection but are not yet negotiated. Once signaling
            // has settled back to Stable (which `apply_offer` just did), send
            // a follow-up offer of our own -- webrtc-rs's `create_offer` does
            // include unmatched local transceivers once a remote description
            // is already set, so this picks them up. A live network test of
            // this exact path isn't possible in this sandbox (no real UDP
            // ICE, see `tests/loopback_media.rs`); this is deliberately a
            // separate, best-effort step so a renegotiation failure here
            // doesn't undo the connection that `apply_offer` already
            // established.
            if let Err(err) = self.send_offer(&remote_id, &peer).await {
                tracing::warn!(
                    "failed to renegotiate published tracks with new peer {}: {:?}",
                    remote_id,
                    err
                );
            }
        }

        result
    }

    /// Applies an inbound offer to `peer`'s RTCPeerConnection and answers it:
    /// `set_remote_description` -> `create_answer` -> `set_local_description`
    /// -> send the answer back over signaling. Shared by both `handle_offer`
    /// call sites -- a brand-new peer (right after `create_pc`) and an
    /// existing live peer being renegotiated -- since the offer/answer
    /// mechanics are identical either way; only what happens on error differs
    /// (the caller decides whether to tear the peer down).
    async fn apply_offer(
        &self,
        remote_id: NodeId,
        sdp: String,
        peer: Arc<Peer>,
    ) -> crate::error::Result<()> {
        // Held for the whole set_remote_description -> create_answer ->
        // set_local_description -> send sequence below -- see
        // `Peer::negotiating`'s doc comment. Both of `handle_offer`'s call
        // sites read `signaling_state` before reaching here without holding
        // this lock, so re-check it immediately below now that we actually
        // hold it: a concurrent negotiation step on this same peer (e.g.
        // another `apply_offer` for a second offer that arrived moments
        // later via the overlay signaling path, which dispatches each
        // inbound message on its own unserialized `tokio::spawn`) may have
        // already changed the state by the time this call gets its turn.
        let _negotiating = peer.negotiating.lock().await;

        if peer.pc.signaling_state() != RTCSignalingState::Stable {
            return Err(crate::error::MistError::Internal(format!(
                "Offer precondition failed: signaling_state={:?}",
                peer.pc.signaling_state()
            )));
        }

        let offer = parse_offer_payload(&sdp)?;
        if let Err(e) = peer.pc.set_remote_description(offer).await {
            rollback_to_stable_on_failure(&peer.pc, &remote_id).await;
            return Err(e.into());
        }

        let answer = match peer.pc.create_answer(None).await {
            Ok(answer) => answer,
            Err(e) => {
                rollback_to_stable_on_failure(&peer.pc, &remote_id).await;
                return Err(e.into());
            }
        };
        if let Err(e) = peer.pc.set_local_description(answer).await {
            rollback_to_stable_on_failure(&peer.pc, &remote_id).await;
            return Err(e.into());
        }

        let cands = {
            let mut pc_lock = self.pending_candidates.write().await;
            pc_lock.remove(&remote_id)
        };

        if let Some(cands) = cands {
            for cand_json in cands {
                match serde_json::from_str::<RTCIceCandidateInit>(&cand_json) {
                    Ok(candidate) => {
                        if let Err(err) = peer.pc.add_ice_candidate(candidate).await {
                            tracing::warn!(
                                "failed to apply buffered ICE candidate for {}: {}",
                                remote_id.0,
                                err
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            "failed to parse buffered ICE candidate for {}: {}",
                            remote_id.0,
                            err
                        );
                    }
                }
            }
        }

        if let Some(answer_desc) = peer.pc.local_description().await {
            let data = answer_desc.sdp;
            self.signaler
                .send_signaling(
                    &remote_id,
                    MessageContent::Data(SignalingData {
                        sender_id: self.local_node_id.clone(),
                        receiver_id: remote_id.clone(),
                        room_id: self.get_room_id(),
                        data,
                        signaling_type: SignalingType::Answer,
                    }),
                )
                .await
                .map_err(crate::error::MistError::Core)?;
        }
        Ok(())
    }

    pub(crate) async fn handle_answer(
        &self,
        remote_id: NodeId,
        sdp: String,
    ) -> crate::error::Result<()> {
        let peer = {
            let peers = self.peers.read().await;
            peers.get(&remote_id).cloned()
        };

        if let Some(peer) = peer {
            let signaling_state = peer.pc.signaling_state();
            if signaling_state != RTCSignalingState::HaveLocalOffer {
                return Err(crate::error::MistError::Internal(format!(
                    "Answer precondition failed: signaling_state={:?}",
                    signaling_state
                )));
            }

            let answer = parse_answer_payload(&sdp)?;
            if let Err(e) = peer.pc.set_remote_description(answer).await {
                // A malformed/rejected answer would otherwise leave this peer
                // stuck at HaveLocalOffer forever (our own offer already
                // applied earlier by `send_offer`) -- see
                // `rollback_to_stable_on_failure`'s doc comment.
                rollback_to_stable_on_failure(&peer.pc, &remote_id).await;
                return Err(e.into());
            }

            let cands = {
                let mut pc_lock = self.pending_candidates.write().await;
                pc_lock.remove(&remote_id)
            };

            if let Some(cands) = cands {
                for cand_json in cands {
                    match serde_json::from_str::<RTCIceCandidateInit>(&cand_json) {
                        Ok(candidate) => {
                            if let Err(err) = peer.pc.add_ice_candidate(candidate).await {
                                tracing::warn!(
                                    "failed to apply buffered ICE candidate for {}: {}",
                                    remote_id.0,
                                    err
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                "failed to parse buffered ICE candidate for {}: {}",
                                remote_id.0,
                                err
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_candidate(
        &self,
        remote_id: NodeId,
        cand_json: String,
    ) -> crate::error::Result<()> {
        let peer = {
            let peers = self.peers.read().await;
            peers.get(&remote_id).cloned()
        };

        if let Some(peer) = peer {
            if peer.pc.remote_description().await.is_some() {
                let candidate = serde_json::from_str::<RTCIceCandidateInit>(&cand_json)?;
                peer.pc.add_ice_candidate(candidate).await?;
                return Ok(());
            }
        }

        // Only buffer candidates for nodes with an active connection state.
        // Late candidates for already-disconnected nodes would accumulate unboundedly otherwise.
        {
            let states = self.connection_states.read().unwrap();
            if !states.contains_key(&remote_id) {
                return Ok(());
            }
        }

        let node_str = remote_id.0.clone();
        let dropped_oldest = {
            let mut pc_lock = self.pending_candidates.write().await;
            let list = pc_lock.entry(remote_id).or_default();
            super::push_pending_candidate(list, cand_json)
        };

        if dropped_oldest {
            tracing::warn!(
                "pending ICE candidates for {} exceeded {}; dropped oldest",
                node_str,
                super::MAX_PENDING_CANDIDATES_PER_NODE
            );
        }
        Ok(())
    }
}

#[async_trait]
impl SignalingHandler for WebRtcTransport {
    async fn handle_message(&self, msg: MessageContent) -> mistlib_core::error::Result<()> {
        let data = match msg {
            MessageContent::Data(d) => d,
            _ => return Ok(()),
        };

        let current_room_id = self.get_room_id();
        if !data.room_id.is_empty() && data.room_id != current_room_id {
            tracing::warn!(
                "WebRtcTransport: ignore signaling from different room_id {} (current={})",
                data.room_id,
                current_room_id
            );
            return Ok(());
        }

        match data.signaling_type {
            SignalingType::Offer => self
                .handle_offer(data.sender_id.clone(), data.data)
                .await
                .map_err(|e| mistlib_core::error::MistError::Internal(e.to_string())),
            SignalingType::Answer => self
                .handle_answer(data.sender_id.clone(), data.data)
                .await
                .map_err(|e| mistlib_core::error::MistError::Internal(e.to_string())),
            SignalingType::Candidate => self
                .handle_candidate(data.sender_id.clone(), data.data)
                .await
                .map_err(|e| mistlib_core::error::MistError::Internal(e.to_string())),
            SignalingType::Candidates => {
                let candidates: Vec<String> =
                    serde_json::from_str(&data.data).map_err(|e: serde_json::Error| {
                        mistlib_core::error::MistError::Internal(e.to_string())
                    })?;
                for cand in candidates {
                    let _ = self.handle_candidate(data.sender_id.clone(), cand).await;
                }
                Ok(())
            }
            SignalingType::Request => {
                let direct_to_local = data.receiver_id == self.local_node_id;
                let legacy_broadcast_tiebreak =
                    data.receiver_id.is_broadcast() && self.local_node_id.0 < data.sender_id.0;
                let deterministic_offerer = self.local_node_id.0 < data.sender_id.0;
                if self.local_node_id != data.sender_id
                    && deterministic_offerer
                    && (direct_to_local || legacy_broadcast_tiebreak)
                {
                    let _ = self.connect(&data.sender_id).await;
                }
                Ok(())
            }
        }
    }
}

pub(crate) fn parse_offer_payload(payload: &str) -> crate::error::Result<RTCSessionDescription> {
    if let Ok(description) = serde_json::from_str::<RTCSessionDescription>(payload) {
        return Ok(description);
    }
    RTCSessionDescription::offer(payload.to_string()).map_err(Into::into)
}

pub(crate) fn parse_answer_payload(payload: &str) -> crate::error::Result<RTCSessionDescription> {
    if let Ok(description) = serde_json::from_str::<RTCSessionDescription>(payload) {
        return Ok(description);
    }
    RTCSessionDescription::answer(payload.to_string()).map_err(Into::into)
}

#[cfg(test)]
mod payload_tests {
    use super::*;

    const MINIMAL_SDP: &str = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n";

    #[test]
    fn parse_offer_payload_accepts_raw_sdp() {
        let parsed = parse_offer_payload(MINIMAL_SDP).unwrap();
        assert_eq!(parsed.sdp, MINIMAL_SDP);
    }

    #[test]
    fn parse_offer_payload_accepts_legacy_json_description() {
        let json = serde_json::json!({ "type": "offer", "sdp": MINIMAL_SDP }).to_string();
        let parsed = parse_offer_payload(&json).unwrap();
        assert_eq!(parsed.sdp, MINIMAL_SDP);
    }

    #[test]
    fn parse_answer_payload_accepts_raw_sdp() {
        let parsed = parse_answer_payload(MINIMAL_SDP).unwrap();
        assert_eq!(parsed.sdp, MINIMAL_SDP);
    }
}
