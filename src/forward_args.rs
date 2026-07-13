pub(crate) fn parse_forward(f: &str) -> (&str, &str, i32) {
    let (proto, addr) = if let Some(rest) = f.strip_prefix("tcp://") {
        ("tcp", rest)
    } else if let Some(rest) = f.strip_prefix("udp://") {
        ("udp", rest)
    } else {
        ("tcp", f)
    };

    let port = if let Ok(p) = addr.parse::<i32>() {
        p
    } else if let Some(port_str) = addr.rsplit(':').next() {
        port_str.parse::<i32>().unwrap_or(-1)
    } else {
        -1
    };

    (proto, addr, port)
}

/// Parses a connect-side forward argument, e.g. `"10022:22"`, `"udp://19000:9000"`,
/// or `"8080"`. The argument may carry an optional `@node` suffix (see
/// [`split_node_scope`]) that pins the forward to a specific peer node, e.g.
/// `"10022:22@node-a"`; the scope is reapplied to the returned target so the
/// listen/remote-port parsing below is unaffected by its presence.
pub(crate) fn parse_connect_forward(f: &str) -> (&str, i32, String) {
    let (f, scope) = split_node_scope(f);
    let (proto, addr, fallback_port) = parse_forward(f);
    let addr = addr.trim_start_matches(':');

    let (listen_port, remote_port) = if let Some((listen, remote)) = addr.split_once(':') {
        let listen_port = listen.parse::<i32>().unwrap_or(fallback_port);
        let remote_port = remote.parse::<i32>().unwrap_or(listen_port);
        (listen_port, remote_port)
    } else {
        (fallback_port, fallback_port)
    };

    let target = format!("{}:{}", proto, remote_port);
    let target = match scope {
        Some(peer_id) => node_scoped_target(&target, peer_id),
        None => target,
    };

    (proto, listen_port, target)
}

pub(crate) fn split_serve_args(args: &[String]) -> (Option<String>, Vec<String>) {
    let Some(first) = args.first() else {
        return (None, Vec::new());
    };

    let is_forward =
        first.contains(':') || first.starts_with("tcp://") || first.starts_with("udp://");
    if is_forward {
        (None, args.to_vec())
    } else {
        (
            Some(first.clone()),
            args.iter().skip(1).cloned().collect::<Vec<_>>(),
        )
    }
}

/// Splits a node-scoped target into its base target and the pinned node id:
/// `"tcp:127.0.0.1:22@node-a"` -> `("tcp:127.0.0.1:22", Some("node-a"))`.
/// A target without a scope comes back unchanged with `None`. The split is
/// on the *last* `'@'` so a base containing one (e.g. a user@host-ish addr)
/// still round-trips as long as the scope itself has none.
pub(crate) fn split_node_scope(target: &str) -> (&str, Option<&str>) {
    match target.rsplit_once('@') {
        Some((base, scope)) if !scope.is_empty() && !base.is_empty() => (base, Some(scope)),
        _ => (target, None),
    }
}

/// Builds a node-scoped target: `("tcp:127.0.0.1:22", "node-a")` ->
/// `"tcp:127.0.0.1:22@node-a"`. Scoping an already-scoped target replaces
/// the existing scope rather than stacking a second one.
pub(crate) fn node_scoped_target(base: &str, peer_id: &str) -> String {
    let (base, _) = split_node_scope(base);
    format!("{}@{}", base, peer_id)
}

pub(crate) fn forward_key(proto: &str, addr: &str) -> String {
    let port = addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(-1);
    format!("{}:{}", proto, port)
}

#[cfg(test)]
mod tests;
