use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

use crate::auth::allow_all;
use crate::forward_runtime::ForwardRuntime;
use crate::rtc::RTCManager;

use super::*;

/// Builds a `TcpManager` backed by a test-only `RTCManagerHandle` (no real
/// mistlib session), so `send_to`/`send_tunnel_to` calls deterministically
/// fail with "no active session" instead of touching the mistlib native
/// singleton or requiring a real WebRTC peer.
fn test_manager() -> Arc<TcpManager> {
    Arc::new(TcpManager {
        rtc_manager: RTCManager::for_test("self-node"),
        conns: Arc::new(RwLock::new(HashMap::new())),
        remote_addr: String::new(),
        target: "tcp:22".to_string(),
        runtime: ForwardRuntime::new(),
        authorizer: allow_all(),
        peer_close_epoch: Arc::new(RwLock::new(HashMap::new())),
        keepalive_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    })
}

/// A real (loopback) `OwnedWriteHalf` to satisfy `TunnelConn`'s field type.
/// The paired listener-side socket is intentionally dropped; these tests
/// never write through it, they only exercise conn bookkeeping.
async fn dummy_write_half() -> tokio::net::tcp::OwnedWriteHalf {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let client = client.unwrap();
    let (_server, _) = accepted.unwrap();
    let (_read, write) = client.into_split();
    write
}

async fn yield_many() {
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
}

// --- Finding 2: peer-leave grace window / resume ---------------------------

#[tokio::test(start_paused = true)]
async fn peer_rejoin_within_grace_window_cancels_pending_close() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;

    mgr.schedule_close_all_for_peer("peer-1".to_string()).await;

    // Peer rejoins partway through the grace window.
    tokio::time::advance(Duration::from_secs(2)).await;
    yield_many().await;
    mgr.cancel_pending_close("peer-1").await;

    // Let the original grace window fully elapse.
    tokio::time::advance(PEER_LEAVE_GRACE + Duration::from_secs(1)).await;
    yield_many().await;

    assert!(
        mgr.conns.read().await.contains_key("conn-1"),
        "conn should have survived: peer rejoined before the grace window expired"
    );
}

#[tokio::test(start_paused = true)]
async fn peer_that_does_not_rejoin_is_closed_after_grace_window() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;

    mgr.schedule_close_all_for_peer("peer-1".to_string()).await;
    // Let the freshly spawned grace-window task run far enough to register
    // its `tokio::time::sleep(PEER_LEAVE_GRACE)` timer *before* jumping the
    // clock -- otherwise the jump happens before the timer exists and the
    // task ends up sleeping the full duration starting from the new time.
    yield_many().await;

    tokio::time::advance(PEER_LEAVE_GRACE + Duration::from_secs(1)).await;
    yield_many().await;

    assert!(
        !mgr.conns.read().await.contains_key("conn-1"),
        "conn should be closed once the grace window expires with no rejoin"
    );
}

#[tokio::test(start_paused = true)]
async fn inbound_data_is_still_accepted_while_a_peer_close_is_pending() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;

    mgr.schedule_close_all_for_peer("peer-1".to_string()).await;

    // Still within the grace window: the conn must remain tracked and
    // reachable by `on_tunnel_message`'s data path (it looks the conn up by
    // id, same as before any leave/close scheduling).
    tokio::time::advance(Duration::from_secs(1)).await;
    yield_many().await;
    assert!(mgr.conns.read().await.contains_key("conn-1"));
}

#[tokio::test]
async fn cancel_pending_close_is_a_no_op_without_a_scheduled_close() {
    let mgr = test_manager();
    // Must not panic and must not create a spurious epoch entry for a peer
    // that never had a close scheduled.
    mgr.cancel_pending_close("peer-never-left").await;
    assert!(mgr
        .peer_close_epoch
        .read()
        .await
        .get("peer-never-left")
        .is_none());
}

// --- Finding 3 (keepalive) plumbing -----------------------------------------

#[tokio::test]
async fn keepalive_ping_failures_do_not_close_the_connection() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;

    // The test double has no real mistlib session, so this send is
    // guaranteed to fail ("no active session") -- exactly the kind of
    // keepalive failure that must never tear down a conn.
    let pinged = mgr.send_keepalive_pings().await;

    assert!(pinged.contains("peer-1"));
    assert!(mgr.conns.read().await.contains_key("conn-1"));
}

#[tokio::test]
async fn send_keepalive_pings_is_a_no_op_with_no_active_conns() {
    let mgr = test_manager();
    let pinged = mgr.send_keepalive_pings().await;
    assert!(pinged.is_empty());
}

#[tokio::test]
async fn active_peer_ids_dedupes_multiple_conns_for_the_same_peer() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;
    mgr.track_conn("conn-2", dummy_write_half().await, "peer-1", true)
        .await;
    mgr.track_conn("conn-3", dummy_write_half().await, "peer-2", true)
        .await;

    let ids = mgr.active_peer_ids().await;
    let mut ids: Vec<&String> = ids.iter().collect();
    ids.sort();
    assert_eq!(ids, vec!["peer-1", "peer-2"]);
}

#[tokio::test(start_paused = true)]
async fn keepalive_task_starts_on_first_conn_and_stops_once_conns_are_gone() {
    let mgr = test_manager();
    assert!(!mgr.keepalive_running.load(Ordering::SeqCst));

    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;
    assert!(
        mgr.keepalive_running.load(Ordering::SeqCst),
        "track_conn should arm the keepalive task"
    );

    // Let the freshly spawned keepalive task run far enough to register its
    // `tokio::time::sleep(TUNNEL_KEEPALIVE_INTERVAL)` timer before jumping
    // the clock (see the comment in the grace-window test above for why).
    yield_many().await;

    mgr.close_conn("conn-1", false).await;

    // Let the background task wake on its next tick, observe there are no
    // active peers, and stop itself.
    tokio::time::advance(TUNNEL_KEEPALIVE_INTERVAL + Duration::from_millis(100)).await;
    yield_many().await;

    assert!(
        !mgr.keepalive_running.load(Ordering::SeqCst),
        "keepalive task should stop once no conns remain"
    );
}

// --- ping / unknown message-type handling -----------------------------------

#[tokio::test]
async fn ping_and_unknown_message_types_are_ignored_gracefully() {
    let mgr = test_manager();
    mgr.track_conn("conn-1", dummy_write_half().await, "peer-1", true)
        .await;

    let ping = TunnelMessage {
        msg_type: MSG_TYPE_PING.into(),
        conn_id: String::new(),
        target: mgr.target.clone(),
        payload: None,
    };
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&ping).unwrap())
        .await;

    let unknown = TunnelMessage {
        msg_type: "future-version-type".into(),
        conn_id: "conn-1".into(),
        target: mgr.target.clone(),
        payload: None,
    };
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&unknown).unwrap())
        .await;

    // Neither message type is recognized as a close/data/connect, so the
    // existing conn must be untouched.
    assert!(mgr.conns.read().await.contains_key("conn-1"));
}
