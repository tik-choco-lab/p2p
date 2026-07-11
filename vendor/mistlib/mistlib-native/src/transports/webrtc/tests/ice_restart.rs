use super::disconnect::{make_connected_pair, wait_for_state};
use crate::transports::webrtc::{is_ice_restart_initiator, Peer};
use mistlib_core::transport::Transport;
use mistlib_core::types::{ConnectionState, DeliveryMethod, NodeId};
use std::sync::Arc as StdArc;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;

#[test]
fn is_ice_restart_initiator_uses_lower_id_wins_direction() {
    let a = NodeId("aaa".to_string());
    let b = NodeId("bbb".to_string());
    assert!(
        is_ice_restart_initiator(&a, &b),
        "the lower node ID must be the ICE-restart initiator"
    );
    assert!(
        !is_ice_restart_initiator(&b, &a),
        "the higher node ID must not be the ICE-restart initiator"
    );
    assert!(
        !is_ice_restart_initiator(&a, &a),
        "a node is never its own initiator"
    );
}

/// Polls (rather than sleeping a fixed duration) `peer`'s `method` DataChannel
/// until it reports `Open`. Needed because the aggregate
/// `RTCPeerConnectionState` (what `wait_for_state`/`get_connection_state`
/// observe) can flip to `Connected` slightly *before* a DataChannel finishes
/// reopening its SCTP stream after an ICE restart -- checking the DC itself
/// is the only way to know `Transport::send` won't race a not-actually-open
/// channel (see the "not open (state: Connecting)" failure this replaced).
async fn wait_for_dc_open(peer: &Peer, method: DeliveryMethod, timeout_ms: u64) -> bool {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let is_open = {
            let channels = peer.channels.read().await;
            channels
                .get(&method)
                .is_some_and(|dc| dc.ready_state() == RTCDataChannelState::Open)
        };
        if is_open {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// `try_ice_restart` renegotiates ICE on the SAME `RTCPeerConnection` (an
/// offer with `ice_restart: true`, applied by the peer in place via the
/// existing Stable-signaling-state renegotiation path in `signaling.rs` --
/// the same mechanism `renegotiation_offer_on_existing_peer_is_applied_in_place`
/// in `signaling.rs` already exercises for a track-add renegotiation). It
/// must not tear down and recreate either side's `Peer`, and the data
/// channel must remain usable afterward.
///
/// This calls `try_ice_restart` directly (bypassing the
/// `is_ice_restart_initiator` gate that guards the real ICE-`Disconnected`
/// trigger) so the test exercises the restart mechanism itself without
/// depending on actually forcing an ICE disconnection, which would be far
/// more timing-sensitive to set up reliably. It also polls for the reliable
/// DataChannel's own `Open` state (`wait_for_dc_open`) instead of racing on
/// the aggregate connection state, and gives real ICE renegotiation a
/// generous budget -- this sandbox's network stack fails to bind several of
/// its own advertised interfaces during candidate gathering (observed via
/// `RUST_LOG=debug`), which can slow an ICE restart's reconvergence well
/// beyond the ~1s common case.
///
/// Known flaky, same as `disconnect.rs`'s tests: a real ICE renegotiation
/// occasionally exceeds even this generous budget in this sandbox. Treat a
/// failure here the same way -- rerun in isolation before treating it as a
/// regression.
///
/// multi_thread required: see the reasoning in disconnect.rs / signaling.rs --
/// A/B are independent peers and must not share an OS thread for realistic
/// scheduling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_ice_restart_keeps_peer_and_data_channel_alive() {
    let (ta, tb, id_a, id_b) = make_connected_pair();

    ta.connect(&id_b).await.expect("connect should not fail");
    assert!(
        wait_for_state(&ta, &id_b, ConnectionState::Connected, 10_000).await,
        "A did not reach Connected state"
    );
    assert!(
        wait_for_state(&tb, &id_a, ConnectionState::Connected, 10_000).await,
        "B did not reach Connected state"
    );

    let peer_a_before = ta
        .peers
        .read()
        .await
        .get(&id_b)
        .cloned()
        .expect("A should have a live peer for B");
    let peer_b_before = tb
        .peers
        .read()
        .await
        .get(&id_a)
        .cloned()
        .expect("B should have a live peer for A");

    // A is the initiator toward B for this fixed pair ("peer-a" < "peer-b"),
    // matching the direction `try_ice_restart`'s real trigger would use.
    assert!(is_ice_restart_initiator(&id_a, &id_b));

    ta.peer_handles().try_ice_restart(&id_b).await;

    assert!(
        wait_for_dc_open(&peer_a_before, DeliveryMethod::ReliableOrdered, 25_000).await,
        "A's reliable data channel did not reopen after the ICE restart"
    );
    assert!(
        wait_for_dc_open(&peer_b_before, DeliveryMethod::ReliableOrdered, 25_000).await,
        "B's reliable data channel did not reopen after the ICE restart"
    );

    // Peer identity must be unchanged throughout: `apply_offer`'s
    // Stable-signaling-state branch (taken because both sides already have a
    // live `existing_peer`) only ever mutates `peer.pc` in place, never
    // touching the transport's `peers` map -- so this is really re-checking
    // the same objects captured in `peer_{a,b}_before`, not a new lookup that
    // could coincidentally match.
    let peer_a_after = ta
        .peers
        .read()
        .await
        .get(&id_b)
        .cloned()
        .expect("A should still have a live peer for B after the restart");
    let peer_b_after = tb
        .peers
        .read()
        .await
        .get(&id_a)
        .cloned()
        .expect("B should still have a live peer for A after the restart");
    assert!(
        StdArc::ptr_eq(&peer_a_before, &peer_a_after),
        "A's peer for B must be renegotiated in place, not replaced"
    );
    assert!(
        StdArc::ptr_eq(&peer_b_before, &peer_b_after),
        "B's peer for A must be renegotiated in place, not replaced"
    );

    ta.send(
        &id_b,
        bytes::Bytes::from_static(b"ping-after-ice-restart"),
        DeliveryMethod::ReliableOrdered,
    )
    .await
    .expect("data channel must still be usable after an ICE restart");
}

/// Regression test for the enhance/simulation x develop merge: after a
/// successful ICE restart the RTCPeerConnection re-enters `Connected`, but
/// the ReliableOrdered DC's `on_open` (the normal place `Connected` is set
/// since the zombie-cleanup work) never re-fires -- webrtc-rs consumes that
/// handler on first invocation. `recover_connected_from_grace` is the state
/// handler's replacement recovery path; without it the peer sits in
/// `Reconnecting` until the sweeper tears the healthy connection down at
/// grace expiry.
#[tokio::test]
async fn pc_reconnect_during_grace_recovers_connected_state() {
    let t = super::make_transport();
    let node = NodeId("grace-recovery-peer".to_string());
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connected);

    // The ICE `Disconnected` arm starts the grace and moves us to Reconnecting.
    let handles = t.peer_handles();
    let (reserved, freshly_started) = handles.mark_disconnected_grace(&node);
    assert!(reserved && freshly_started);
    assert_eq!(t.get_connection_state(&node), ConnectionState::Reconnecting);

    // A successful restart flips the pc back to `Connected`; the state
    // handler's recovery path must clear the grace and restore the state.
    assert!(handles.recover_connected_from_grace(&node));
    assert_eq!(t.get_connection_state(&node), ConnectionState::Connected);
    assert!(
        !t.disconnected_since.read().unwrap().contains_key(&node),
        "grace entry must be cleared so the sweeper cannot reap the recovered peer"
    );
}

/// The recovery path must NOT fire for a fresh connect: `Connected` there is
/// still owed to the ReliableOrdered DC actually opening (zombie rule), not
/// to the aggregate pc state flipping first.
#[tokio::test]
async fn recover_connected_from_grace_is_noop_without_pending_grace() {
    let t = super::make_transport();
    let node = NodeId("fresh-connect-peer".to_string());
    t.connection_states
        .write()
        .unwrap()
        .insert(node.clone(), ConnectionState::Connecting);

    assert!(!t.peer_handles().recover_connected_from_grace(&node));
    assert_eq!(t.get_connection_state(&node), ConnectionState::Connecting);
}
