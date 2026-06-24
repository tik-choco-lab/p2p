use std::sync::{Arc, Mutex};

use super::super::event::handle_payload;
use super::super::payload::P2pPayload;
use super::super::state::PeerRole;
use super::{encode, test_manager};
use crate::rtc::TunnelMessage;

fn encode_tunnel(target: &str, conn_id: &str) -> Vec<u8> {
    serde_json::to_vec(&TunnelMessage {
        msg_type: "data".to_string(),
        conn_id: conn_id.to_string(),
        target: target.to_string(),
        payload: Some(vec![42]),
    })
    .unwrap()
}

#[test]
fn tunnel_message_payload_is_encoded_as_hex_string() {
    let encoded = encode_tunnel("tcp:80", "conn-1");

    assert_eq!(
        String::from_utf8(encoded).unwrap(),
        r#"{"type":"data","conn_id":"conn-1","target":"tcp:80","payload":"b64:Kg=="}"#
    );
}

#[test]
fn legacy_tunnel_message_payload_array_is_still_accepted() {
    let decoded: TunnelMessage =
        serde_json::from_slice(br#"{"type":"data","conn_id":"conn-1","payload":[42]}"#).unwrap();

    assert_eq!(decoded.payload, Some(vec![42]));
}

#[test]
fn legacy_tunnel_message_payload_hex_is_still_accepted() {
    let decoded: TunnelMessage =
        serde_json::from_slice(br#"{"type":"data","conn_id":"conn-1","payload":"2a"}"#).unwrap();

    assert_eq!(decoded.payload, Some(vec![42]));
}

#[tokio::test]
async fn capability_payload_tracks_targeted_server_peers() {
    let manager = test_manager("self", PeerRole::Client);

    handle_payload(
        manager.inner.clone(),
        "server-1".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:80".to_string()],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "server-2".to_string(),
        encode(P2pPayload::Capabilities {
            forwards: vec!["tcp:5432".to_string()],
        }),
    )
    .await;

    assert_eq!(
        manager.get_server_peers_for("tcp:5432").await,
        vec!["server-2".to_string()]
    );
}

#[tokio::test]
async fn select_server_peer_round_robins_across_advertised_peers() {
    let manager = test_manager("self", PeerRole::Client);

    for peer in ["server-b", "server-a", "server-c"] {
        handle_payload(
            manager.inner.clone(),
            peer.to_string(),
            encode(P2pPayload::Capabilities {
                forwards: vec!["tcp:80".to_string()],
            }),
        )
        .await;
    }

    // Deterministic ordering (sorted) with a round-robin cursor.
    let mut picks = Vec::new();
    for _ in 0..6 {
        picks.push(manager.select_server_peer_for("tcp:80").await.unwrap());
    }
    assert_eq!(
        picks,
        vec![
            "server-a".to_string(),
            "server-b".to_string(),
            "server-c".to_string(),
            "server-a".to_string(),
            "server-b".to_string(),
            "server-c".to_string(),
        ]
    );
}

#[tokio::test]
async fn select_server_peer_returns_none_without_advertised_peers() {
    let manager = test_manager("self", PeerRole::Client);
    assert_eq!(manager.select_server_peer_for("tcp:80").await, None);
}

#[tokio::test]
async fn targeted_tunnel_handlers_receive_only_matching_targets() {
    let manager = test_manager("self", PeerRole::Client);
    let tcp_80 = Arc::new(Mutex::new(Vec::new()));
    let tcp_5432 = Arc::new(Mutex::new(Vec::new()));

    {
        let tcp_80 = tcp_80.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                tcp_80.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }
    {
        let tcp_5432 = tcp_5432.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                tcp_5432.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:5432", "conn-5432"),
        }),
    )
    .await;

    assert!(tcp_80.lock().unwrap().is_empty());
    assert_eq!(
        *tcp_5432.lock().unwrap(),
        vec![("peer-1".to_string(), "conn-5432".to_string())]
    );
}

#[tokio::test]
async fn empty_tunnel_target_routes_to_first_registered_target() {
    let manager = test_manager("self", PeerRole::Client);
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));

    {
        let first = first.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                first.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }
    {
        let second = second.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                second.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("", "legacy-conn"),
        }),
    )
    .await;

    assert_eq!(
        *first.lock().unwrap(),
        vec![("peer-1".to_string(), "legacy-conn".to_string())]
    );
    assert!(second.lock().unwrap().is_empty());
}

#[tokio::test]
async fn removed_tunnel_handler_no_longer_receives_targeted_messages() {
    let manager = test_manager("self", PeerRole::Client);
    let received = Arc::new(Mutex::new(Vec::new()));

    let handler_id = {
        let received = received.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                received.lock().unwrap().push((peer, tm.conn_id));
            })
            .await
    };

    assert!(manager.remove_tunnel_message_handler(handler_id).await);
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("tcp:80", "conn-80"),
        }),
    )
    .await;

    assert!(received.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_tunnel_target_uses_next_handler_after_default_removed() {
    let manager = test_manager("self", PeerRole::Client);
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));

    let first_handler = {
        let first = first.clone();
        manager
            .on_tunnel_message_for("tcp:80".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                first.lock().unwrap().push((peer, tm.conn_id));
            })
            .await
    };
    {
        let second = second.clone();
        manager
            .on_tunnel_message_for("tcp:5432".to_string(), move |peer, data| {
                let tm: TunnelMessage = serde_json::from_slice(&data).unwrap();
                second.lock().unwrap().push((peer, tm.conn_id));
            })
            .await;
    }

    assert!(manager.remove_tunnel_message_handler(first_handler).await);
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: encode_tunnel("", "legacy-conn"),
        }),
    )
    .await;

    assert!(first.lock().unwrap().is_empty());
    assert_eq!(
        *second.lock().unwrap(),
        vec![("peer-1".to_string(), "legacy-conn".to_string())]
    );
}
