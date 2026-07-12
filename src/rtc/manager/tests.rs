use std::sync::{Arc, Mutex};

use super::event::{handle_leave, handle_payload};
use super::payload::P2pPayload;
use super::state::{PeerRole, RTCManagerInner};
use super::{mistlib_config, RTCManagerHandle};

mod ordering;
mod routing;

fn test_manager(self_id: &str, self_role: PeerRole) -> RTCManagerHandle {
    RTCManagerHandle {
        inner: Arc::new(RTCManagerInner::new(self_id.to_string(), self_role)),
    }
}

fn encode(payload: P2pPayload) -> Vec<u8> {
    serde_json::to_vec(&payload).unwrap()
}

#[test]
fn mistlib_config_is_unset_by_default() {
    temp_env::with_var("P2P_MISTLIB_CONFIG_JSON", None::<&str>, || {
        assert!(mistlib_config().is_none());
    });
}

#[test]
fn mistlib_config_uses_explicit_env_only() {
    temp_env::with_var(
        "P2P_MISTLIB_CONFIG_JSON",
        Some("{\"signaling\":{}}"),
        || {
            assert_eq!(mistlib_config(), Some(br#"{"signaling":{}}"#.to_vec()));
        },
    );
}

#[tokio::test]
async fn role_payload_tracks_server_peers() {
    let manager = test_manager("self", PeerRole::Client);

    handle_payload(
        manager.inner.clone(),
        "server-1".to_string(),
        encode(P2pPayload::Role {
            role: "server".to_string(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "client-1".to_string(),
        encode(P2pPayload::Role {
            role: "client".to_string(),
        }),
    )
    .await;

    assert_eq!(
        manager.get_server_peers().await,
        vec!["server-1".to_string()]
    );
}

#[tokio::test]
async fn message_payloads_are_dispatched_to_registered_handlers() {
    let manager = test_manager("self", PeerRole::Client);
    let chats = Arc::new(Mutex::new(Vec::new()));
    let tunnels = Arc::new(Mutex::new(Vec::new()));
    let stdio = Arc::new(Mutex::new(Vec::new()));

    {
        let chats = chats.clone();
        manager
            .on_chat_message(move |peer, text| {
                chats.lock().unwrap().push((peer, text));
            })
            .await;
    }
    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }
    {
        let stdio = stdio.clone();
        manager
            .on_stdio_message(move |peer, data| {
                stdio.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Chat {
            text: "hello".to_string(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Tunnel {
            data: vec![1, 2, 3],
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        encode(P2pPayload::Stdio { data: vec![4, 5] }),
    )
    .await;

    assert_eq!(
        *chats.lock().unwrap(),
        vec![("peer-1".to_string(), "hello".to_string())]
    );
    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
    assert_eq!(
        *stdio.lock().unwrap(),
        vec![("peer-1".to_string(), vec![4, 5])]
    );
}

#[test]
fn byte_payloads_are_encoded_as_hex_strings() {
    let encoded_tunnel = encode(P2pPayload::Tunnel {
        data: vec![1, 2, 3, 255],
    });
    let encoded_stdio = encode(P2pPayload::Stdio { data: vec![4, 5] });

    assert_eq!(
        String::from_utf8(encoded_tunnel).unwrap(),
        r#"{"kind":"tunnel","data":"b64:AQID/w=="}"#
    );
    assert_eq!(
        String::from_utf8(encoded_stdio).unwrap(),
        r#"{"kind":"stdio","data":"b64:BAU="}"#
    );
}

#[tokio::test]
async fn legacy_byte_array_payloads_are_still_accepted() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"kind":"tunnel","data":[1,2,3]}"#.to_vec(),
    )
    .await;

    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
}

#[tokio::test]
async fn legacy_hex_byte_payloads_are_still_accepted() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnels = Arc::new(Mutex::new(Vec::new()));

    {
        let tunnels = tunnels.clone();
        manager
            .on_tunnel_message(move |peer, data| {
                tunnels.lock().unwrap().push((peer, data));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        br#"{"kind":"tunnel","data":"010203"}"#.to_vec(),
    )
    .await;

    assert_eq!(
        *tunnels.lock().unwrap(),
        vec![("peer-1".to_string(), vec![1, 2, 3])]
    );
}

#[tokio::test]
async fn invalid_and_self_payloads_are_ignored() {
    let manager = test_manager("self", PeerRole::Client);
    let chats = Arc::new(Mutex::new(Vec::new()));

    {
        let chats = chats.clone();
        manager
            .on_chat_message(move |peer, text| {
                chats.lock().unwrap().push((peer, text));
            })
            .await;
    }

    handle_payload(
        manager.inner.clone(),
        "self".to_string(),
        encode(P2pPayload::Chat {
            text: "self-message".to_string(),
        }),
    )
    .await;
    handle_payload(
        manager.inner.clone(),
        "peer-1".to_string(),
        b"not json".to_vec(),
    )
    .await;

    assert!(chats.lock().unwrap().is_empty());
}

#[tokio::test]
async fn leave_removes_peer_state_and_notifies_close_handlers() {
    let manager = test_manager("self", PeerRole::Client);
    let tunnel_closed = Arc::new(Mutex::new(Vec::new()));
    let stdio_closed = Arc::new(Mutex::new(Vec::new()));

    manager
        .inner
        .peers
        .write()
        .await
        .insert("peer-1".to_string());
    manager
        .inner
        .peer_roles
        .write()
        .await
        .insert("peer-1".to_string(), PeerRole::Server);

    {
        let tunnel_closed = tunnel_closed.clone();
        manager
            .on_tunnel_close(move |peer| {
                tunnel_closed.lock().unwrap().push(peer);
            })
            .await;
    }
    {
        let stdio_closed = stdio_closed.clone();
        manager
            .on_stdio_close(move |peer| {
                stdio_closed.lock().unwrap().push(peer);
            })
            .await;
    }

    handle_leave(manager.inner.clone(), "peer-1".to_string()).await;

    assert!(!manager.inner.peers.read().await.contains("peer-1"));
    assert!(manager
        .inner
        .peer_roles
        .read()
        .await
        .get("peer-1")
        .is_none());
    assert_eq!(*tunnel_closed.lock().unwrap(), vec!["peer-1".to_string()]);
    assert_eq!(*stdio_closed.lock().unwrap(), vec!["peer-1".to_string()]);
}
