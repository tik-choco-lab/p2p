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

pub(crate) fn parse_connect_forward(f: &str) -> (&str, i32, String) {
    let (proto, addr, fallback_port) = parse_forward(f);
    let addr = addr.trim_start_matches(':');

    let (listen_port, remote_port) = if let Some((listen, remote)) = addr.split_once(':') {
        let listen_port = listen.parse::<i32>().unwrap_or(fallback_port);
        let remote_port = remote.parse::<i32>().unwrap_or(listen_port);
        (listen_port, remote_port)
    } else {
        (fallback_port, fallback_port)
    };

    (proto, listen_port, format!("{}:{}", proto, remote_port))
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
