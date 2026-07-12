use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::AsyncReadExt;
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

/// A real (loopback) TCP pair whose write half can be handed to `track_conn`
/// (as the "downstream" socket `handle_remote_data` writes into) while the
/// paired read half lets a test observe exactly what was written -- unlike
/// `dummy_write_half`, which drops the peer side.
async fn connected_write_and_readback() -> (
    tokio::net::tcp::OwnedWriteHalf,
    tokio::net::tcp::OwnedReadHalf,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let client = client.unwrap();
    let (server, _) = accepted.unwrap();
    let (_client_read, client_write) = client.into_split();
    let (server_read, _server_write) = server.into_split();
    (client_write, server_read)
}

fn data_msg(conn_id: &str, target: &str, payload: &[u8], seq: Option<u64>) -> TunnelMessage {
    TunnelMessage {
        msg_type: MSG_TYPE_DATA.into(),
        conn_id: conn_id.to_string(),
        target: target.to_string(),
        payload: Some(payload.to_vec()),
        seq,
    }
}

// --- data seq gap/duplicate handling (end-to-end via on_tunnel_message) ----

#[tokio::test]
async fn in_order_seq_data_is_all_delivered() {
    let mgr = test_manager();
    let (write_half, mut read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    for (i, byte) in [1u8, 2, 3].into_iter().enumerate() {
        let msg = data_msg("conn-1", &mgr.target, &[byte], Some((i + 1) as u64));
        mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg).unwrap())
            .await;
    }

    let mut buf = [0u8; 3];
    tokio::time::timeout(Duration::from_secs(1), read_half.read_exact(&mut buf))
        .await
        .expect("timed out waiting for data")
        .unwrap();
    assert_eq!(buf, [1, 2, 3]);
}

#[tokio::test]
async fn duplicate_seq_is_written_only_once() {
    let mgr = test_manager();
    let (write_half, mut read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    // seq 2 is redelivered (as `send_to_with_retry` can do when a send that
    // actually succeeded looked like a failure to the sender).
    for (seq, byte) in [(1u64, 1u8), (2, 2), (2, 2), (3, 3)] {
        let msg = data_msg("conn-1", &mgr.target, &[byte], Some(seq));
        mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg).unwrap())
            .await;
    }

    let mut buf = [0u8; 3];
    tokio::time::timeout(Duration::from_secs(1), read_half.read_exact(&mut buf))
        .await
        .expect("timed out waiting for data")
        .unwrap();
    assert_eq!(buf, [1, 2, 3], "duplicate seq 2 must not be written twice");

    // Nothing else should ever arrive -- if the duplicate had been written,
    // the extra byte `2` would be waiting here.
    let mut extra = [0u8; 1];
    let res = tokio::time::timeout(Duration::from_millis(100), read_half.read(&mut extra)).await;
    assert!(
        res.is_err(),
        "no further bytes expected after the 3 deduped ones"
    );
}

#[tokio::test]
async fn seq_gap_closes_the_conn() {
    let mgr = test_manager();
    let (write_half, _read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    let msg1 = data_msg("conn-1", &mgr.target, &[1], Some(1));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg1).unwrap())
        .await;
    assert!(mgr.conns.read().await.contains_key("conn-1"));

    // seq 2 never arrives (dropped by mistlib's ReorderBuffer) -- seq 3
    // shows up next.
    let msg3 = data_msg("conn-1", &mgr.target, &[3], Some(3));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg3).unwrap())
        .await;

    assert!(
        !mgr.conns.read().await.contains_key("conn-1"),
        "conn should be closed once a seq gap is detected"
    );
}

#[tokio::test]
async fn late_data_after_gap_close_is_dropped_safely() {
    let mgr = test_manager();
    let (write_half, mut read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    let msg1 = data_msg("conn-1", &mgr.target, &[1], Some(1));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg1).unwrap())
        .await;

    // seq 1 is accepted and legitimately written -- drain that expected
    // byte before checking for any further (unexpected) writes below.
    let mut first = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(1), read_half.read_exact(&mut first))
        .await
        .expect("timed out waiting for seq-1 data")
        .unwrap();
    assert_eq!(first, [1]);

    // seq 2 never arrives -- seq 3 triggers a gap close.
    let msg3 = data_msg("conn-1", &mgr.target, &[3], Some(3));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg3).unwrap())
        .await;
    assert!(!mgr.conns.read().await.contains_key("conn-1"));

    // The "missing" seq 2 finally shows up late (e.g. mistlib's
    // ReorderBuffer flushing a stale entry after the fact). The conn is
    // already gone, so this must be a safe, silent no-op -- no panic, no
    // resurrecting the conn, and nothing further written anywhere.
    let msg2_late = data_msg("conn-1", &mgr.target, &[2], Some(2));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg2_late).unwrap())
        .await;

    assert!(!mgr.conns.read().await.contains_key("conn-1"));

    // Nothing further must ever be written: either the read times out (the
    // socket is still open but idle) or it sees a clean EOF (the closed
    // conn's `TunnelConn`, and with it its `OwnedWriteHalf`, was dropped) --
    // but never an actual byte from the dropped late message.
    let mut extra = [0u8; 1];
    match tokio::time::timeout(Duration::from_millis(100), read_half.read(&mut extra)).await {
        Err(_) => {}    // timed out waiting -- nothing arrived
        Ok(Ok(0)) => {} // EOF from the write half being dropped on close
        Ok(Ok(n)) => panic!("unexpected {n} byte(s) written for the dropped late seq-2 message"),
        Ok(Err(e)) => panic!("unexpected read error: {e}"),
    }
}

#[tokio::test]
async fn empty_payload_with_seq_still_consumes_the_seq_number() {
    let mgr = test_manager();
    let (write_half, mut read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    // seq 1 carries no payload (e.g. forwarded from a zero-byte TCP read) --
    // it must still advance `next_expected`. Before the fix, the early
    // return on an empty/missing payload in `handle_remote_data` bypassed
    // `recv_seq.observe` entirely, so this seq would never be consumed and
    // the next (non-empty) message below would be misjudged as a gap.
    let empty_msg = TunnelMessage {
        msg_type: MSG_TYPE_DATA.into(),
        conn_id: "conn-1".into(),
        target: mgr.target.clone(),
        payload: None,
        seq: Some(1),
    };
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&empty_msg).unwrap())
        .await;

    let msg2 = data_msg("conn-1", &mgr.target, &[9], Some(2));
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg2).unwrap())
        .await;

    assert!(
        mgr.conns.read().await.contains_key("conn-1"),
        "conn should still be open: the empty seq-1 message must have been \
         consumed rather than treated as a gap"
    );

    let mut buf = [0u8; 1];
    tokio::time::timeout(Duration::from_millis(500), read_half.read_exact(&mut buf))
        .await
        .expect("timed out waiting for data")
        .unwrap();
    assert_eq!(buf, [9]);
}

#[tokio::test]
async fn legacy_sender_without_seq_still_delivers_data() {
    let mgr = test_manager();
    let (write_half, mut read_half) = connected_write_and_readback().await;
    mgr.track_conn("conn-1", write_half, "peer-1", true).await;

    for byte in [1u8, 2, 3] {
        let msg = data_msg("conn-1", &mgr.target, &[byte], None);
        mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&msg).unwrap())
            .await;
    }

    let mut buf = [0u8; 3];
    tokio::time::timeout(Duration::from_secs(1), read_half.read_exact(&mut buf))
        .await
        .expect("timed out waiting for data")
        .unwrap();
    assert_eq!(buf, [1, 2, 3]);
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
        seq: None,
    };
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&ping).unwrap())
        .await;

    let unknown = TunnelMessage {
        msg_type: "future-version-type".into(),
        conn_id: "conn-1".into(),
        target: mgr.target.clone(),
        payload: None,
        seq: None,
    };
    mgr.on_tunnel_message("peer-1", &serde_json::to_vec(&unknown).unwrap())
        .await;

    // Neither message type is recognized as a close/data/connect, so the
    // existing conn must be untouched.
    assert!(mgr.conns.read().await.contains_key("conn-1"));
}
