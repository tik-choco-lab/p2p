use super::*;

#[test]
fn relay_frames_do_not_expose_invite_or_room_material() {
    let (raw, codec, crypto) = config();
    let alice = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let bob = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([2u8; 32]),
    );
    let data = SignalingData {
        sender_id: NodeId("alice".to_string()),
        receiver_id: NodeId("bob".to_string()),
        room_id: "secret-room".to_string(),
        data: "kind 25050 encrypted payload test".to_string(),
        signaling_type: SignalingType::Offer,
    };

    let discovery = build_discovery_event(&codec, &crypto, &alice, "secret-room").unwrap();
    let message = build_message_event(&codec, &crypto, &alice, &bob.public_key, &data).unwrap();
    let discovery_req = req_frame_json(
        &random_subscription_id(),
        &[discovery_filter(&codec, "secret-room")],
    )
    .unwrap();
    let message_req = req_frame_json(
        &random_subscription_id(),
        &[message_filter(&codec, "secret-room")],
    )
    .unwrap();
    let capture = [
        event_frame_json(&discovery).unwrap(),
        event_frame_json(&message).unwrap(),
        discovery_req,
        message_req,
    ]
    .join("\n");

    for hidden in [
        raw.invite_salt.as_str(),
        raw.invite_code.as_str(),
        "secret-room",
        "alice",
        "bob",
        "kind 25050 encrypted payload test",
        "mistlib",
        "webrtc",
        "mistpsk1",
        "room",
        "joined_at",
        "discovery",
        "messages",
    ] {
        assert!(!capture.contains(hidden), "{hidden} leaked in relay frame");
    }
}

#[test]
fn subscription_ids_are_random_opaque_hex() {
    let first = random_subscription_id();
    let second = random_subscription_id();

    assert_eq!(first.len(), 32);
    assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(first, second);
}

#[test]
fn relay_room_scope_differs_per_room() {
    let (_raw, codec, _crypto) = config();
    let first = discovery_filter(&codec, "first-room");
    let second = discovery_filter(&codec, "second-room");

    assert_ne!(
        first.tag_filters.get("#d").unwrap(),
        second.tag_filters.get("#d").unwrap()
    );
}

#[test]
fn discovery_scope_rotates_by_time_bucket() {
    let (_raw, codec, _crypto) = config();
    let current = current_rotation_bucket(codec.room_scope_rotation_seconds());

    assert_ne!(
        codec.room_scope("secret-room", current),
        codec.room_scope("secret-room", current + 1)
    );
}

#[test]
fn topology_rank_is_secret_room_scoped_material() {
    let (_raw, codec, _crypto) = config();
    let alice = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );

    let first = codec.topology_rank("first-room", &alice.public_key);
    let second = codec.topology_rank("second-room", &alice.public_key);

    assert_eq!(first.len(), 64);
    assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    assert_ne!(first, second);
}

#[test]
fn message_event_uses_room_mailbox_without_receiver_tag() {
    let (_raw, codec, crypto) = config();
    let alice = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let bob = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([2u8; 32]),
    );
    let data = SignalingData {
        sender_id: NodeId("alice".to_string()),
        receiver_id: NodeId("bob".to_string()),
        room_id: "secret-room".to_string(),
        data: "v=0
sdp"
        .to_string(),
        signaling_type: SignalingType::Offer,
    };

    let event = build_message_event(&codec, &crypto, &alice, &bob.public_key, &data).unwrap();
    let room_scope = codec.current_room_scope(&data.room_id);
    let other_room_scope = codec.current_room_scope("other-room");
    let filter = message_filter(&codec, &data.room_id);
    let event_json = serde_json::to_string(&event).unwrap();

    assert_eq!(event.tag_value(TAG_INVITE_SCOPE), Some(room_scope.as_str()));
    assert_ne!(room_scope, other_room_scope);
    assert!(event.tag_value(TAG_P).is_none());
    assert!(event.tag_value(TAG_NONCE).is_some());
    assert!(!event_json.contains(&bob.public_key));
    assert!(filter.tag_filters.get("#d").unwrap().contains(&room_scope));
    assert!(!filter.tag_filters.contains_key("#p"));
}

#[test]
fn message_payloads_use_random_cover_size_buckets() {
    let (_raw, codec, crypto) = config();
    let alice = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let bob = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([2u8; 32]),
    );
    let data = SignalingData {
        sender_id: NodeId("alice".to_string()),
        receiver_id: NodeId("bob".to_string()),
        room_id: "secret-room".to_string(),
        data: "x".to_string(),
        signaling_type: SignalingType::Offer,
    };
    let allowed_content_lengths = [1404usize, 2770, 5500, 10962];
    let mut observed_lengths = std::collections::BTreeSet::new();
    let mut first_event = None;
    let mut fixed_hex_prefix_count = 0usize;

    for _ in 0..64 {
        let event = build_message_event(&codec, &crypto, &alice, &bob.public_key, &data).unwrap();
        assert!(
            allowed_content_lengths.contains(&event.content.len()),
            "unexpected encrypted content length {}",
            event.content.len()
        );
        assert!(event
            .content
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert!(!event.content.chars().all(|c| c.is_ascii_hexdigit()));
        if event.content.starts_with("03") {
            fixed_hex_prefix_count += 1;
        }
        observed_lengths.insert(event.content.len());
        first_event.get_or_insert(event);
    }

    assert!(
        observed_lengths.len() > 1,
        "Nostr padding should vary cover size across messages"
    );
    assert!(
        fixed_hex_prefix_count < 64,
        "Nostr content must not use a fixed 03 hex version prefix"
    );
    let decoded = decode_message_event(
        &codec,
        &crypto,
        &bob,
        &NodeId("bob".to_string()),
        first_event.as_ref().unwrap(),
        "secret-room",
    )
    .unwrap();
    assert_eq!(decoded.data.data, data.data);
}
