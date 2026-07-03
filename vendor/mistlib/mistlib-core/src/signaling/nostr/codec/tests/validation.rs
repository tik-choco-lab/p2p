use super::*;

#[test]
fn wrong_room_is_rejected() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let event = build_discovery_event(&codec, &crypto, &identity, "secret-room").unwrap();

    assert!(decode_discovery_event(&codec, &crypto, &event, "other").is_err());
}

#[test]
fn tampered_signature_is_rejected() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let mut event = build_discovery_event(&codec, &crypto, &identity, "room").unwrap();
    event.sig.replace_range(0..2, "00");

    assert!(decode_discovery_event(&codec, &crypto, &event, "room").is_err());
}

#[test]
fn tampered_pubkey_is_rejected() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let mut event = build_discovery_event(&codec, &crypto, &identity, "room").unwrap();
    event.pubkey = "00".repeat(32);
    event.refresh_id();

    assert!(decode_discovery_event(&codec, &crypto, &event, "room").is_err());
}

#[test]
fn tampered_event_id_is_rejected() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let mut event = build_discovery_event(&codec, &crypto, &identity, "room").unwrap();
    event.id.replace_range(0..2, "00");

    assert!(decode_discovery_event(&codec, &crypto, &event, "room").is_err());
}

#[test]
fn far_future_expiration_is_rejected() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let mut event = build_discovery_event(&codec, &crypto, &identity, "room").unwrap();
    let far_future = codec.expires_at() + 10_000;
    for tag in &mut event.tags {
        if tag.first().map(String::as_str) == Some(TAG_EXPIRATION) {
            tag[1] = far_future.to_string();
        }
    }
    event.refresh_id();
    event.sig = crypto.sign_event(&identity, &event).unwrap();

    assert!(decode_discovery_event(&codec, &crypto, &event, "room").is_err());
}

#[test]
fn stale_created_at_is_rejected_even_with_future_expiration() {
    let (_raw, codec, crypto) = config();
    let identity = TemporarySignalingIdentity::from_secret_key(
        SignalingSecretKey::from_bytes_for_tests([1u8; 32]),
    );
    let mut event = build_discovery_event(&codec, &crypto, &identity, "room").unwrap();
    event.created_at = now_unix_seconds()
        .saturating_sub(codec.ttl_seconds)
        .saturating_sub(MAX_EVENT_CLOCK_SKEW_SECONDS)
        .saturating_sub(1);
    event.refresh_id();
    event.sig = crypto.sign_event(&identity, &event).unwrap();

    assert!(decode_discovery_event(&codec, &crypto, &event, "room").is_err());
}
