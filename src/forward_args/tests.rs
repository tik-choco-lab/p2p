use super::*;

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn connect_forward_defaults_remote_port_to_listen_port() {
    assert_eq!(
        parse_connect_forward(":8080"),
        ("tcp", 8080, "tcp:8080".to_string())
    );
    assert_eq!(
        parse_connect_forward("udp://9000"),
        ("udp", 9000, "udp:9000".to_string())
    );
}

#[test]
fn connect_forward_maps_listen_port_to_remote_target() {
    assert_eq!(
        parse_connect_forward("15432:5432"),
        ("tcp", 15432, "tcp:5432".to_string())
    );
    assert_eq!(
        parse_connect_forward("udp://19000:9000"),
        ("udp", 19000, "udp:9000".to_string())
    );
}

#[test]
fn serve_args_keep_all_forwards_when_room_is_present() {
    let (room, forwards) =
        split_serve_args(&strings(&["my-room", ":80", "tcp://127.0.0.1:5432"]));

    assert_eq!(room, Some("my-room".to_string()));
    assert_eq!(forwards, strings(&[":80", "tcp://127.0.0.1:5432"]));
}

#[test]
fn serve_args_treat_leading_forward_as_generated_room_mode() {
    let (room, forwards) = split_serve_args(&strings(&[":80", "udp://127.0.0.1:9000"]));

    assert_eq!(room, None);
    assert_eq!(forwards, strings(&[":80", "udp://127.0.0.1:9000"]));
}

#[test]
fn forward_key_uses_protocol_and_port() {
    assert_eq!(forward_key("tcp", "127.0.0.1:80"), "tcp:80");
    assert_eq!(forward_key("udp", ":9000"), "udp:9000");
}
