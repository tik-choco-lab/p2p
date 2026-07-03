use super::*;
use mistlib_core::transport::Transport;
use mistlib_core::types::{ConnectionState, NodeId};
use std::time::Instant;

#[tokio::test]
async fn close_all_peer_connections_clears_all_transport_state() {
    let t = make_transport();
    let node = NodeId("peer-to-close".to_string());
    let peer = t
        .create_pc(node.clone())
        .await
        .expect("peer connection should be created for cleanup test");

    t.peers.write().await.insert(node.clone(), peer);
    t.pending_candidates
        .write()
        .await
        .insert(node.clone(), vec!["late-candidate".to_string()]);
    t.connection_attempt_ids
        .write()
        .unwrap()
        .insert(node.clone(), 42);
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connecting);
    t.last_disconnect_at
        .write()
        .unwrap()
        .insert(node.clone(), Instant::now());
    t.disconnected_since
        .write()
        .unwrap()
        .insert(node.clone(), Instant::now());

    t.close_all_peer_connections().await;

    assert!(
        t.peers.read().await.is_empty(),
        "peers must be empty after room-level WebRTC cleanup"
    );
    assert!(
        t.pending_candidates.read().await.is_empty(),
        "pending ICE candidates must be empty after room-level WebRTC cleanup"
    );
    assert!(
        t.connection_attempt_ids.read().unwrap().is_empty(),
        "connection attempt ids must be empty after room-level WebRTC cleanup"
    );
    assert!(
        t.connection_states.read().unwrap().is_empty(),
        "connection states must be empty after room-level WebRTC cleanup"
    );
    assert!(
        t.last_disconnect_at.read().unwrap().is_empty(),
        "disconnect cooldown entries must be empty after room-level WebRTC cleanup"
    );
    assert!(
        t.disconnected_since.read().unwrap().is_empty(),
        "disconnected grace entries must be empty after room-level WebRTC cleanup"
    );
}

#[tokio::test]
async fn force_failed_cleanup_removes_peer_state_and_pending_candidates() {
    let t = make_transport();
    let node = NodeId("force-failed-peer".to_string());
    let peer = t
        .create_pc(node.clone())
        .await
        .expect("peer connection should be created for cleanup test");

    t.peers.write().await.insert(node.clone(), peer);
    t.pending_candidates
        .write()
        .await
        .insert(node.clone(), vec!["late-candidate".to_string()]);
    t.connection_attempt_ids
        .write()
        .unwrap()
        .insert(node.clone(), 7);
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connecting);

    t.cleanup_session(&node, true).await;

    assert!(
        !t.peers.read().await.contains_key(&node),
        "force-failed cleanup must remove the peer entry"
    );
    assert!(
        !t.pending_candidates.read().await.contains_key(&node),
        "force-failed cleanup must remove pending ICE candidates"
    );
    assert!(
        !t.connection_attempt_ids.read().unwrap().contains_key(&node),
        "force-failed cleanup must remove the attempt id"
    );
    assert_eq!(
        t.get_connection_state(&node),
        ConnectionState::Disconnected,
        "force-failed cleanup must not leave a Failed state behind"
    );
    assert!(
        t.last_disconnect_at.read().unwrap().contains_key(&node),
        "force-failed cleanup should retain a disconnect cooldown entry"
    );
    assert!(
        !t.disconnected_since.read().unwrap().contains_key(&node),
        "cleanup must clear any disconnected grace entry"
    );
}

#[tokio::test]
async fn cleanup_unknown_node_does_not_create_disconnect_cooldown_entry() {
    let t = make_transport();
    let node = NodeId("never-seen-peer".to_string());

    t.cleanup_session(&node, false).await;

    assert!(
        !t.last_disconnect_at.read().unwrap().contains_key(&node),
        "cleanup for an unknown node must not grow the reconnect cooldown map"
    );
}

#[tokio::test]
async fn late_candidate_for_inactive_node_is_not_buffered() {
    let t = make_transport();
    let node = NodeId("inactive-peer".to_string());

    t.handle_candidate(node.clone(), "not-json-but-should-be-ignored".to_string())
        .await
        .expect("inactive candidate should be ignored before parsing");

    assert!(
        !t.pending_candidates.read().await.contains_key(&node),
        "late ICE candidates for inactive nodes must not accumulate"
    );
}

#[tokio::test]
async fn candidate_for_active_node_is_buffered_until_peer_can_accept_it() {
    let t = make_transport();
    let node = NodeId("active-peer".to_string());
    let candidate = "candidate-json-is-not-parsed-until-a-peer-is-ready".to_string();

    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connecting);

    t.handle_candidate(node.clone(), candidate.clone())
        .await
        .expect("active candidate should be buffered");

    let pending = t.pending_candidates.read().await;
    assert_eq!(
        pending.get(&node),
        Some(&vec![candidate]),
        "active nodes should still buffer candidates until the peer can accept them"
    );
}

#[tokio::test]
async fn cleanup_resets_signaling_session_only_when_transport_stays_isolated() {
    use async_trait::async_trait;
    use mistlib_core::error::Result as MistResult;
    use mistlib_core::signaling::{MessageContent, Signaler};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct ResetCountingSignaler(AtomicUsize);

    #[async_trait]
    impl Signaler for ResetCountingSignaler {
        async fn send_signaling(&self, _to: &NodeId, _msg: MessageContent) -> MistResult<()> {
            Ok(())
        }

        async fn reset_session(&self) -> MistResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&self) -> MistResult<()> {
            Ok(())
        }
    }

    let signaler = Arc::new(ResetCountingSignaler(AtomicUsize::new(0)));
    let t = WebRtcTransport::new(signaler.clone(), NodeId("local".to_string()));
    let node = NodeId("peer".to_string());
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connected);

    t.cleanup_session(&node, false).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(signaler.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cleanup_does_not_reset_signaling_session_when_another_peer_is_active() {
    use async_trait::async_trait;
    use mistlib_core::error::Result as MistResult;
    use mistlib_core::signaling::{MessageContent, Signaler};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct ResetCountingSignaler(AtomicUsize);

    #[async_trait]
    impl Signaler for ResetCountingSignaler {
        async fn send_signaling(&self, _to: &NodeId, _msg: MessageContent) -> MistResult<()> {
            Ok(())
        }

        async fn reset_session(&self) -> MistResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&self) -> MistResult<()> {
            Ok(())
        }
    }

    let signaler = Arc::new(ResetCountingSignaler(AtomicUsize::new(0)));
    let t = WebRtcTransport::new(signaler.clone(), NodeId("local".to_string()));
    let disconnected = NodeId("disconnected".to_string());
    t.connection_states
        .write()
        .unwrap()
        .insert(disconnected.clone(), ConnectionState::Connected);
    t.connection_states.write().unwrap().insert(
        NodeId("still-active".to_string()),
        ConnectionState::Connected,
    );

    t.cleanup_session(&disconnected, false).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(signaler.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn disconnected_grace_marks_reconnecting_and_recovers_without_cleanup() {
    let t = make_transport();
    let node = NodeId("grace-recover-peer".to_string());
    let peer = t
        .create_pc(node.clone())
        .await
        .expect("peer connection should be created for grace test");

    t.peers.write().await.insert(node.clone(), peer);
    t.connection_attempt_ids
        .write()
        .unwrap()
        .insert(node.clone(), 99);
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connected);

    let handles = t.peer_handles();
    assert!(handles.mark_disconnected_grace(&node));

    assert_eq!(t.get_connection_state(&node), ConnectionState::Reconnecting);
    assert!(t.peers.read().await.contains_key(&node));
    assert!(t.connection_attempt_ids.read().unwrap().contains_key(&node));
    assert!(t.disconnected_since.read().unwrap().contains_key(&node));

    assert!(handles.mark_connection_state(&node, ConnectionState::Connected));

    assert_eq!(t.get_connection_state(&node), ConnectionState::Connected);
    assert!(t.peers.read().await.contains_key(&node));
    assert!(t.disconnected_since.read().unwrap().is_empty());
    assert!(
        t.last_disconnect_at.read().unwrap().is_empty(),
        "recovering during grace must not start reconnect cooldown"
    );
}

#[tokio::test]
async fn disconnected_grace_expiry_is_cleaned_by_sweeper() {
    use super::DISCONNECTED_GRACE_MS;
    use tokio::time::{sleep, timeout, Duration};

    let t = make_transport();
    let node = NodeId("grace-expired-peer".to_string());
    let peer = t
        .create_pc(node.clone())
        .await
        .expect("peer connection should be created for grace expiry test");

    t.peers.write().await.insert(node.clone(), peer);
    t.pending_candidates
        .write()
        .await
        .insert(node.clone(), vec!["candidate".to_string()]);
    t.connection_attempt_ids
        .write()
        .unwrap()
        .insert(node.clone(), 101);
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Reconnecting);
    t.disconnected_since.write().unwrap().insert(
        node.clone(),
        Instant::now() - Duration::from_millis(DISCONNECTED_GRACE_MS + 1),
    );

    t.ensure_session_sweeper();

    timeout(Duration::from_secs(2), async {
        loop {
            if !t.peers.read().await.contains_key(&node) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sweeper should clean expired disconnected grace session");
    t.stop_session_sweeper();

    assert_eq!(t.get_connection_state(&node), ConnectionState::Disconnected);
    assert!(!t.pending_candidates.read().await.contains_key(&node));
    assert!(!t.connection_attempt_ids.read().unwrap().contains_key(&node));
    assert!(!t.disconnected_since.read().unwrap().contains_key(&node));
    assert!(t.last_disconnect_at.read().unwrap().contains_key(&node));
}
