use anyhow::Result;

use crate::auth::{AuthDecision, AuthEvent, AuthEventSource, TrustDecision};
use crate::controller::{Direction, ForwardSpec, ForwardState, Proto};
use crate::forward_args::{forward_key, parse_connect_forward, parse_forward};

pub(super) fn parse_add_line(line: &str) -> Result<ForwardSpec> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 2 {
        anyhow::bail!("`<serve|connect> <forward>` の形式で入力");
    }
    match parts[0] {
        "serve" | "s" => {
            let (proto, addr, _) = parse_forward(parts[1]);
            Ok(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::from_name(proto)?,
                addr: addr.to_string(),
                listen_port: -1,
                target: forward_key(proto, addr),
            })
        }
        "connect" | "c" => {
            let (proto, listen_port, target) = parse_connect_forward(parts[1]);
            Ok(ForwardSpec {
                direction: Direction::Connect,
                proto: Proto::from_name(proto)?,
                addr: String::new(),
                listen_port,
                target,
            })
        }
        _ => anyhow::bail!("方向は serve / connect"),
    }
}

pub(super) fn dir_arrow(d: Direction) -> &'static str {
    match d {
        Direction::Serve => "→",
        Direction::Connect => "←",
    }
}

pub(super) fn proto_name(p: Proto) -> &'static str {
    match p {
        Proto::Tcp => "TCP",
        Proto::Udp => "UDP",
    }
}

pub(super) fn state_name(s: &ForwardState) -> &str {
    match s {
        ForwardState::Listening => "listening",
        ForwardState::Error(_) => "error",
        ForwardState::Stopped => "stopped",
    }
}

pub(super) fn endpoint(spec: &ForwardSpec) -> String {
    match spec.direction {
        Direction::Serve => spec.addr.clone(),
        Direction::Connect => format!(":{}→{}", spec.listen_port, spec.target),
    }
}

pub(super) fn trust_name(d: TrustDecision) -> &'static str {
    match d {
        TrustDecision::Allow => "allow",
        TrustDecision::Deny => "deny ",
    }
}

pub(super) fn format_event(e: &AuthEvent) -> String {
    let decision = match e.decision {
        AuthDecision::Allow => "allow",
        AuthDecision::AllowAlways => "allow*",
        AuthDecision::Deny => "deny",
        AuthDecision::DenyAlways => "deny*",
    };
    let source = match e.source {
        AuthEventSource::Policy => "policy",
        AuthEventSource::TrustStore => "trust",
        AuthEventSource::Pending => "prompt",
    };
    format!(
        "#{} {:<6} {:<6} {} → {} ({})",
        e.sequence,
        decision,
        source,
        short_id(&e.peer_id),
        e.forward_key,
        e.target_addr
    )
}

pub(super) fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

pub(super) fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "K", "M", "G"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{}", n)
    } else {
        format!("{:.1}{}", value, UNITS[unit])
    }
}
