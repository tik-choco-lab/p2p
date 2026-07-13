//! JSON DTOs for the Web UI's state contract (see the module doc on
//! `crate::web`) and the `AppState` shared across axum handlers.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::app::session::{NoticeKind, SessionContext, SessionNotice};
use crate::auth::{AuthDecision, AuthEventSource, PendingAuthorization, TrustDecision, TrustEntry};
use crate::controller::{Direction, ForwardSpec, ForwardState, ForwardStatus};
use crate::negotiation::{IncomingForward, OutgoingForward};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PeerDto {
    pub(crate) id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ForwardDto {
    pub(crate) id: String,
    pub(crate) proto: String,
    pub(crate) direction: String,
    pub(crate) local: String,
    pub(crate) target: String,
    pub(crate) status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PendingAuthDto {
    pub(crate) id: u64,
    pub(crate) peer_id: String,
    pub(crate) forward_key: String,
    pub(crate) target_addr: String,
    pub(crate) proto: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PendingForwardDto {
    pub(crate) id: u64,
    pub(crate) peer_id: String,
    pub(crate) proto: String,
    pub(crate) remote_addr: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PendingOutgoingDto {
    pub(crate) peer_id: String,
    pub(crate) proto: String,
    pub(crate) local: String,
    pub(crate) remote: String,
    pub(crate) target: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct TrustDto {
    pub(crate) peer_id: String,
    pub(crate) forward_key: String,
    pub(crate) decision: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct EventDto {
    pub(crate) time: String,
    pub(crate) peer_id: String,
    pub(crate) forward_key: String,
    pub(crate) decision: String,
    pub(crate) source: String,
}

/// A user-facing notice (see `SessionNotice`), e.g. a forward-negotiation
/// outcome. `kind` is `"info"` or `"error"`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct NoticeDto {
    pub(crate) time: String,
    pub(crate) kind: String,
    pub(crate) text: String,
}

/// The full `GET /api/state` payload; also what's flattened into the
/// `{"type":"state", ...}` WebSocket push (see `web::ws`).
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub(crate) struct StateDto {
    pub(crate) node_id: String,
    pub(crate) room_id: String,
    pub(crate) peers: Vec<PeerDto>,
    pub(crate) forwards: Vec<ForwardDto>,
    pub(crate) pending_auth: Vec<PendingAuthDto>,
    pub(crate) pending_forwards: Vec<PendingForwardDto>,
    pub(crate) pending_outgoing: Vec<PendingOutgoingDto>,
    pub(crate) trust: Vec<TrustDto>,
    pub(crate) events: Vec<EventDto>,
    /// Recent user-facing notices, oldest first, capped at 50 (see
    /// `SessionContext::notices`).
    pub(crate) notices: Vec<NoticeDto>,
}

/// Bodies accepted by the mutating endpoints.
#[derive(Debug, Deserialize)]
pub(crate) struct AuthDecisionBody {
    pub(crate) decision: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ForwardAcceptBody {
    pub(crate) accept: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AddForwardBody {
    pub(crate) proto: String,
    pub(crate) local: String,
    pub(crate) remote: String,
    pub(crate) peer_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RemoveTrustBody {
    pub(crate) peer_id: String,
    pub(crate) forward_key: String,
}

/// The uniform envelope for mutating endpoints:
/// `{"ok":true}` or `{"ok":false,"error":"..."}`.
#[derive(Debug, Serialize)]
pub(crate) struct ActionResult {
    pub(crate) ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

/// Parses the JSON `decision` string used by `POST /api/pending/auth/{id}`.
pub(crate) fn parse_auth_decision(s: &str) -> Option<AuthDecision> {
    match s {
        "allow" => Some(AuthDecision::Allow),
        "allow_always" => Some(AuthDecision::AllowAlways),
        "deny" => Some(AuthDecision::Deny),
        "deny_always" => Some(AuthDecision::DenyAlways),
        _ => None,
    }
}

/// Shared state handed to every axum handler: the session context plus a
/// `watch` channel broadcasting the latest [`StateDto`] to WebSocket
/// subscribers. `watch` always retains the last-sent value, so a new
/// subscriber immediately sees current state on connect.
pub(crate) struct AppState {
    pub(crate) ctx: SessionContext,
    pub(crate) state_tx: watch::Sender<Arc<StateDto>>,
}

impl AppState {
    /// Recomputes the snapshot and pushes it to WS subscribers, but only if
    /// it differs from the last broadcast value (coalesces bursts of
    /// activity into a single push). Call after every mutating API call and
    /// from the periodic background loop.
    pub(crate) async fn broadcast_now(&self) {
        let dto = build_state_dto(&self.ctx).await;
        let changed = *self.state_tx.borrow().as_ref() != dto;
        if changed {
            let _ = self.state_tx.send(Arc::new(dto));
        }
    }
}

/// Gathers a fresh snapshot from the session core and adapts it to the
/// Web API's JSON shape.
pub(crate) async fn build_state_dto(ctx: &SessionContext) -> StateDto {
    let snap = ctx.snapshot().await;
    StateDto {
        node_id: snap.self_id,
        room_id: snap.room,
        peers: snap.peers.into_iter().map(|id| PeerDto { id }).collect(),
        forwards: snap.forwards.iter().map(forward_dto).collect(),
        pending_auth: snap.pending_auth.iter().map(pending_auth_dto).collect(),
        pending_forwards: snap
            .pending_forwards
            .iter()
            .map(pending_forward_dto)
            .collect(),
        pending_outgoing: snap
            .pending_outgoing
            .iter()
            .map(pending_outgoing_dto)
            .collect(),
        trust: snap.trust.iter().map(trust_dto).collect(),
        events: snap.events.iter().map(event_dto).collect(),
        notices: snap.notices.iter().map(notice_dto).collect(),
    }
}

fn forward_dto(status: &ForwardStatus) -> ForwardDto {
    let spec = &status.spec;
    ForwardDto {
        id: status.key.clone(),
        proto: spec.proto.as_str().to_string(),
        direction: match spec.direction {
            Direction::Serve => "serve".to_string(),
            Direction::Connect => "connect".to_string(),
        },
        local: local_endpoint(spec),
        target: spec.target.clone(),
        status: forward_state_str(&status.state),
    }
}

/// The forward's local endpoint: `addr` when populated (serve forwards, and
/// negotiated connect forwards which carry the full `ip:port`), otherwise
/// just the listen port (CLI-added connect forwards persist only that).
fn local_endpoint(spec: &ForwardSpec) -> String {
    if !spec.addr.is_empty() {
        spec.addr.clone()
    } else {
        format!(":{}", spec.listen_port)
    }
}

fn forward_state_str(state: &ForwardState) -> String {
    match state {
        ForwardState::Listening => "listening".to_string(),
        ForwardState::Error(msg) => format!("error: {}", msg),
        ForwardState::Stopped => "stopped".to_string(),
    }
}

fn pending_auth_dto(item: &PendingAuthorization) -> PendingAuthDto {
    PendingAuthDto {
        id: item.id,
        peer_id: item.request.peer_id.clone(),
        forward_key: item.request.forward_key.clone(),
        target_addr: item.request.target_addr.clone(),
        proto: item.request.proto.clone(),
    }
}

fn pending_forward_dto(item: &IncomingForward) -> PendingForwardDto {
    PendingForwardDto {
        id: item.id,
        peer_id: item.peer_id.clone(),
        proto: item.proto.clone(),
        remote_addr: item.remote_addr.clone(),
        target: item.target.clone(),
    }
}

fn pending_outgoing_dto(item: &OutgoingForward) -> PendingOutgoingDto {
    PendingOutgoingDto {
        peer_id: item.peer_id.clone(),
        proto: item.proto.clone(),
        local: item.local_addr.clone(),
        remote: item.remote_addr.clone(),
        target: item.target.clone(),
    }
}

fn trust_dto(entry: &TrustEntry) -> TrustDto {
    TrustDto {
        peer_id: entry.key.peer_id.clone(),
        forward_key: entry.key.forward_key.clone(),
        decision: match entry.decision {
            TrustDecision::Allow => "allow".to_string(),
            TrustDecision::Deny => "deny".to_string(),
        },
    }
}

fn event_dto(ev: &crate::auth::AuthEvent) -> EventDto {
    EventDto {
        time: format_timestamp_ms(ev.timestamp_ms),
        peer_id: ev.peer_id.clone(),
        forward_key: ev.forward_key.clone(),
        decision: match ev.decision {
            AuthDecision::Allow => "allow",
            AuthDecision::Deny => "deny",
            AuthDecision::AllowAlways => "allow_always",
            AuthDecision::DenyAlways => "deny_always",
        }
        .to_string(),
        source: match ev.source {
            AuthEventSource::Policy => "policy",
            AuthEventSource::TrustStore => "trust_store",
            AuthEventSource::Pending => "pending",
        }
        .to_string(),
    }
}

fn notice_dto(notice: &SessionNotice) -> NoticeDto {
    NoticeDto {
        time: format_timestamp_ms(notice.timestamp_ms),
        kind: match notice.kind {
            NoticeKind::Info => "info",
            NoticeKind::Error => "error",
        }
        .to_string(),
        text: notice.text.clone(),
    }
}

/// Formats a Unix timestamp in milliseconds as an RFC3339 UTC string (e.g.
/// `2026-07-12T09:30:00.123Z`) so the frontend's `new Date(...)` parses it
/// directly. Implemented by hand with Howard Hinnant's `civil_from_days`
/// algorithm to avoid pulling in a datetime crate for one display field.
fn format_timestamp_ms(ms: u128) -> String {
    let ms = ms as i64;
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let secs_of_day = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y, m, d, hh, mm, ss, millis
    )
}

/// Days-since-epoch (1970-01-01) to a proleptic-Gregorian (year, month,
/// day). See http://howardhinnant.github.io/date_algorithms.html.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Proto;

    #[test]
    fn state_dto_round_trips_through_json() {
        let dto = StateDto {
            node_id: "node-1".into(),
            room_id: "room-1".into(),
            peers: vec![PeerDto {
                id: "peer-1".into(),
            }],
            forwards: vec![ForwardDto {
                id: "tcp:127.0.0.1:80".into(),
                proto: "tcp".into(),
                direction: "serve".into(),
                local: "127.0.0.1:80".into(),
                // A node-scoped target (see `split_node_scope`) must survive
                // JSON serialization untouched: callers key forward removal
                // on the full scoped string.
                target: "tcp:127.0.0.1:80@node-a".into(),
                status: "listening".into(),
            }],
            pending_auth: vec![PendingAuthDto {
                id: 1,
                peer_id: "peer-1".into(),
                forward_key: "tcp:127.0.0.1:80".into(),
                target_addr: "127.0.0.1:80".into(),
                proto: "tcp".into(),
            }],
            pending_forwards: vec![PendingForwardDto {
                id: 2,
                peer_id: "peer-1".into(),
                proto: "tcp".into(),
                remote_addr: "127.0.0.1:80".into(),
                target: "tcp:127.0.0.1:80".into(),
            }],
            pending_outgoing: vec![PendingOutgoingDto {
                peer_id: "peer-1".into(),
                proto: "tcp".into(),
                local: "127.0.0.1:8080".into(),
                remote: "10.0.0.5:80".into(),
                target: "tcp:10.0.0.5:80".into(),
            }],
            trust: vec![TrustDto {
                peer_id: "peer-1".into(),
                forward_key: "tcp:127.0.0.1:80".into(),
                decision: "allow".into(),
            }],
            events: vec![EventDto {
                time: "2026-01-01T00:00:00.000Z".into(),
                peer_id: "peer-1".into(),
                forward_key: "tcp:127.0.0.1:80".into(),
                decision: "allow".into(),
                source: "policy".into(),
            }],
            notices: vec![NoticeDto {
                time: "2026-01-01T00:00:00.000Z".into(),
                kind: "info".into(),
                text: "forward established: tcp:127.0.0.1:80".into(),
            }],
        };

        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["node_id"], "node-1");
        assert_eq!(json["room_id"], "room-1");
        assert_eq!(json["peers"][0]["id"], "peer-1");
        assert_eq!(json["forwards"][0]["proto"], "tcp");
        assert_eq!(json["forwards"][0]["direction"], "serve");
        assert_eq!(json["forwards"][0]["target"], "tcp:127.0.0.1:80@node-a");
        assert_eq!(json["pending_auth"][0]["id"], 1);
        assert_eq!(json["pending_forwards"][0]["id"], 2);
        assert_eq!(json["pending_outgoing"][0]["local"], "127.0.0.1:8080");
        assert_eq!(json["pending_outgoing"][0]["remote"], "10.0.0.5:80");
        assert_eq!(json["trust"][0]["decision"], "allow");
        assert_eq!(json["events"][0]["source"], "policy");
        assert_eq!(json["notices"][0]["kind"], "info");
        assert_eq!(
            json["notices"][0]["text"],
            "forward established: tcp:127.0.0.1:80"
        );

        // Round trip back through serde_json::Value to ensure the shape is
        // stable (keys are exactly what's expected, nothing extra/missing).
        let mut keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "events",
                "forwards",
                "node_id",
                "notices",
                "peers",
                "pending_auth",
                "pending_forwards",
                "pending_outgoing",
                "room_id",
                "trust",
            ]
        );
    }

    #[test]
    fn notice_dto_maps_kind_and_formats_timestamp() {
        let info = notice_dto(&SessionNotice {
            timestamp_ms: 1_609_459_200_000,
            kind: NoticeKind::Info,
            text: "forward established: tcp:127.0.0.1:80".into(),
        });
        assert_eq!(info.kind, "info");
        assert_eq!(info.time, "2021-01-01T00:00:00.000Z");

        let error = notice_dto(&SessionNotice {
            timestamp_ms: 1_609_459_200_000,
            kind: NoticeKind::Error,
            text: "peer denied tcp:127.0.0.1:80".into(),
        });
        assert_eq!(error.kind, "error");
    }

    #[test]
    fn formats_known_timestamp() {
        // 2021-01-01T00:00:00.000Z
        assert_eq!(
            format_timestamp_ms(1_609_459_200_000),
            "2021-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn parses_known_decision_strings() {
        assert_eq!(parse_auth_decision("allow"), Some(AuthDecision::Allow));
        assert_eq!(
            parse_auth_decision("allow_always"),
            Some(AuthDecision::AllowAlways)
        );
        assert_eq!(parse_auth_decision("deny"), Some(AuthDecision::Deny));
        assert_eq!(
            parse_auth_decision("deny_always"),
            Some(AuthDecision::DenyAlways)
        );
        assert_eq!(parse_auth_decision("nope"), None);
    }

    // Node-scoped targets (`"tcp:127.0.0.1:80@node-a"`, see
    // `crate::forward_args::split_node_scope`) must flow through the DTO
    // mappers untouched: the frontend keys forward removal on the full
    // string, so trimming the scope here would break `DELETE
    // /api/forwards/{id}`.

    fn scoped_forward_status(target: &str) -> ForwardStatus {
        ForwardStatus {
            key: "tcp:127.0.0.1:80".into(),
            spec: ForwardSpec {
                direction: Direction::Connect,
                proto: Proto::Tcp,
                addr: "127.0.0.1:9000".into(),
                listen_port: 9000,
                target: target.into(),
            },
            active_conns: 0,
            bytes_in: 0,
            bytes_out: 0,
            state: ForwardState::Listening,
            peers: Vec::new(),
        }
    }

    #[test]
    fn forward_dto_keeps_full_scoped_target() {
        let dto = forward_dto(&scoped_forward_status("tcp:127.0.0.1:80@node-a"));
        assert_eq!(dto.target, "tcp:127.0.0.1:80@node-a");
        // local_endpoint reads `spec.addr`/`spec.listen_port`, not `target`,
        // so it's unaffected by scoping either way.
        assert_eq!(dto.local, "127.0.0.1:9000");
    }

    #[test]
    fn forward_dto_leaves_unscoped_target_unchanged() {
        let dto = forward_dto(&scoped_forward_status("tcp:127.0.0.1:80"));
        assert_eq!(dto.target, "tcp:127.0.0.1:80");
    }

    #[test]
    fn pending_forward_dto_keeps_full_scoped_target() {
        let item = IncomingForward {
            id: 2,
            req_id: "req-1".into(),
            peer_id: "peer-1".into(),
            proto: "tcp".into(),
            remote_addr: "127.0.0.1:80".into(),
            target: "tcp:127.0.0.1:80@node-a".into(),
        };
        let dto = pending_forward_dto(&item);
        assert_eq!(dto.target, "tcp:127.0.0.1:80@node-a");
    }

    #[test]
    fn pending_outgoing_dto_keeps_full_scoped_target() {
        let item = OutgoingForward {
            peer_id: "peer-1".into(),
            proto: "tcp".into(),
            listen_port: 8080,
            local_addr: "127.0.0.1:8080".into(),
            remote_addr: "10.0.0.5:80".into(),
            target: "tcp:10.0.0.5:80@node-a".into(),
        };
        let dto = pending_outgoing_dto(&item);
        assert_eq!(dto.target, "tcp:10.0.0.5:80@node-a");
    }
}
