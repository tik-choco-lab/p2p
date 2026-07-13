//! Shared session core used by both the TUI (`src/tui/*`) and the Web UI
//! (`src/web/*`). Holds the P2P session context (RTC manager, forward
//! controller, auth/trust stores, forward-negotiation queue) and the async
//! operations that mutate it, so the two front ends don't duplicate
//! side-effect logic (approving forwards, sending requests, persisting
//! trust, etc).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::auth::{
    default_trust_store_path, AuthAuditLog, AuthDecision, AuthEvent, PendingAuthorization,
    PendingAuthorizations, PendingAuthorizer, TrustDecision, TrustEntry, TrustKey, TrustStore,
};
use crate::controller::{Direction, ForwardController, ForwardSpec, ForwardStatus, Proto};
use crate::forward_store::{default_forward_store_path, ForwardStore};
use crate::negotiation::{ForwardNegotiator, ForwardOutcome, IncomingForward, OutgoingForward};
use crate::rtc::RTCManager;

/// How many notices `SessionContext::push_notice` retains before dropping the
/// oldest -- mirrors `AuthAuditLog`'s capacity-trim pattern (see
/// `auth::audit::AuthAuditLog::record`), just with a smaller cap since
/// notices are meant to be a short "what just happened" strip, not a full
/// audit trail (that's what the audit log/events pane is for).
const MAX_NOTICES: usize = 50;

/// A user-facing "something happened" message surfaced by both the TUI and
/// Web UI -- e.g. a forward negotiation outcome that would otherwise only be
/// visible via `tracing` logs (which default to the `error` level and so are
/// invisible in normal use; see `SessionContext::apply_forward_outcome`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionNotice {
    pub(crate) timestamp_ms: u128,
    pub(crate) kind: NoticeKind,
    pub(crate) text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoticeKind {
    Info,
    Error,
}

/// How long to wait after a peer's `EVENT_LEAVE` before treating pending
/// auth requests / forward negotiations addressed to it as abandoned.
/// Mirrors the grace window `crate::tcp::PEER_LEAVE_GRACE` uses for
/// tunnel-close cleanup (duplicated here since that constant isn't `pub`).
const PEER_LEAVE_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// The shared state backing a running P2P session: the RTC manager, the
/// forward controller, auth/trust bookkeeping, and the forward-negotiation
/// queue. Built once per `p2p` (TUI) / `p2p web` invocation and driven by
/// whichever front end is active. All fields are `Clone`-cheap (`Arc`-backed
/// handles), so the context itself derives `Clone` for sharing across tasks
/// (e.g. axum handlers).
#[derive(Clone)]
pub(crate) struct SessionContext {
    pub(crate) room: String,
    pub(crate) manager: RTCManager,
    pub(crate) controller: ForwardController,
    pub(crate) trust_store: TrustStore,
    pub(crate) audit_log: AuthAuditLog,
    pub(crate) pending_auth: PendingAuthorizations,
    pub(crate) negotiator: ForwardNegotiator,
    pub(crate) forward_store: ForwardStore,
    /// Recent user-facing notices (forward-negotiation outcomes, etc), newest
    /// last, capped at `MAX_NOTICES`. Shared (not per-clone) so every handle
    /// to this `SessionContext` sees the same feed.
    pub(crate) notices: Arc<Mutex<Vec<SessionNotice>>>,
}

/// A recoverable failure from a session operation. Carries a human-readable
/// message (the same text the TUI has always shown for the equivalent
/// failure) plus enough structure for the Web API to pick an HTTP status.
#[derive(Debug, Clone)]
pub(crate) enum SessionError {
    /// The referenced pending item / forward no longer exists.
    NotFound(String),
    /// The request was rejected for some other reason (bad input, send
    /// failure, etc).
    Invalid(String),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::NotFound(m) | SessionError::Invalid(m) => write!(f, "{}", m),
        }
    }
}

/// A point-in-time snapshot of everything the UIs display. Plain domain
/// types; each front end adapts this to its own presentation (TUI widgets,
/// Web JSON DTOs).
pub(crate) struct Snapshot {
    pub(crate) self_id: String,
    pub(crate) room: String,
    pub(crate) peers: Vec<String>,
    pub(crate) forwards: Vec<ForwardStatus>,
    pub(crate) pending_auth: Vec<PendingAuthorization>,
    pub(crate) pending_forwards: Vec<IncomingForward>,
    /// Forward requests this node has sent and is still waiting on a peer
    /// response for.
    pub(crate) pending_outgoing: Vec<OutgoingForward>,
    pub(crate) trust: Vec<TrustEntry>,
    pub(crate) events: Vec<AuthEvent>,
    /// Recent user-facing notices, oldest first (see `SessionContext::notices`).
    pub(crate) notices: Vec<SessionNotice>,
}

impl SessionContext {
    /// Builds the shared session context: loads the trust/forward stores,
    /// wires the RTC manager's forward-request/response handlers into the
    /// negotiator queue, and re-establishes previously approved forwards.
    /// Used by both `p2p` (TUI) and `p2p web`.
    pub(crate) async fn build(self_id: String, room: String, is_server: bool) -> Result<Self> {
        let manager = RTCManager::new(self_id, room.clone(), is_server).await;
        let trust_store = TrustStore::load(default_trust_store_path()).await?;
        let audit_log = AuthAuditLog::default();
        let pending_auth = PendingAuthorizations::new();
        let negotiator = ForwardNegotiator::new();
        let forward_store = ForwardStore::load(default_forward_store_path()).await?;
        let controller = ForwardController::with_authorizer(
            manager.clone(),
            PendingAuthorizer::shared_with_audit_log(
                trust_store.clone(),
                pending_auth.clone(),
                audit_log.clone(),
            ),
        );

        // Re-establish previously approved forwards.
        for entry in forward_store.list().await {
            match entry.to_spec() {
                Ok(spec) => {
                    if let Err(e) = controller.add_forward(spec).await {
                        warn!("failed to restore forward {}: {}", entry.target, e);
                    }
                }
                Err(e) => warn!("skipping invalid persisted forward: {}", e),
            }
        }

        // Surface incoming forward proposals into the negotiator queue.
        {
            let neg = negotiator.clone();
            manager
                .on_forward_request(move |peer_id, ev| {
                    let neg = neg.clone();
                    tokio::spawn(async move {
                        neg.record_incoming(
                            ev.req_id,
                            peer_id,
                            ev.proto,
                            ev.remote_addr,
                            ev.target,
                        )
                        .await;
                    });
                })
                .await;
        }
        {
            let neg = negotiator.clone();
            manager
                .on_forward_response(move |peer_id, ev| {
                    let neg = neg.clone();
                    tokio::spawn(async move {
                        neg.record_response(&ev.req_id, &peer_id, ev.accepted).await;
                    });
                })
                .await;
        }

        register_peer_leave_purge(
            manager.clone(),
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        Ok(Self {
            room,
            manager,
            controller,
            trust_store,
            audit_log,
            pending_auth,
            negotiator,
            forward_store,
            notices: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Appends a notice, trimming the oldest entries past `MAX_NOTICES`. The
    /// single choke point for user-facing notices so both the TUI and Web UI
    /// pick them up via `snapshot()` regardless of which one happened to
    /// trigger the underlying event.
    async fn push_notice(&self, kind: NoticeKind, text: String) {
        let mut notices = self.notices.lock().await;
        notices.push(SessionNotice {
            timestamp_ms: now_ms(),
            kind,
            text,
        });
        if notices.len() > MAX_NOTICES {
            let drop = notices.len() - MAX_NOTICES;
            notices.drain(0..drop);
        }
    }

    /// Gathers current state for display. Cheap-ish (a handful of lock
    /// acquisitions); safe to call frequently (e.g. from a polling loop).
    pub(crate) async fn snapshot(&self) -> Snapshot {
        let mut events = self.audit_log.list().await;
        // audit_log.list() is chronological (oldest first); keep only the
        // most recent 100.
        if events.len() > 100 {
            let drop = events.len() - 100;
            events.drain(0..drop);
        }
        Snapshot {
            self_id: self.manager.self_id().to_string(),
            room: self.room.clone(),
            peers: self.manager.connected_peers().await,
            forwards: self.controller.list_forwards().await,
            pending_auth: self.pending_auth.list().await,
            pending_forwards: self.negotiator.list_incoming().await,
            pending_outgoing: self.negotiator.list_outgoing().await,
            trust: self.trust_store.list().await,
            events,
            notices: self.notices.lock().await.clone(),
        }
    }

    /// Resolves a pending connection authorization (the auth-request pane).
    /// Returns `false` if `id` is stale.
    pub(crate) async fn resolve_pending_auth(&self, id: u64, decision: AuthDecision) -> bool {
        self.pending_auth.resolve(id, decision).await
    }

    /// Approves or denies an incoming forward proposal: on approval, creates
    /// a matching serve forward, remembers trust, persists it, and answers
    /// the peer. Mirrors the TUI's original `resolve_forward`.
    pub(crate) async fn resolve_forward(
        &self,
        id: u64,
        allow: bool,
    ) -> Result<String, SessionError> {
        let Some(req) = self.negotiator.take_incoming(id).await else {
            return Err(SessionError::NotFound(
                "forward request no longer exists".into(),
            ));
        };

        if !allow {
            self.manager
                .send_forward_response(&req.peer_id, forward_response(&req, false))
                .await
                .map_err(|e| {
                    SessionError::Invalid(format!(
                        "denied locally, but failed to notify peer: {}",
                        e
                    ))
                })?;
            return Ok(format!("denied forward {}", req.target));
        }

        let proto = Proto::from_name(&req.proto)
            .map_err(|e| SessionError::Invalid(format!("invalid proto: {}", e)))?;
        let spec = ForwardSpec {
            direction: Direction::Serve,
            proto,
            addr: req.remote_addr.clone(),
            listen_port: -1,
            target: req.target.clone(),
        };
        self.controller
            .add_forward(spec.clone())
            .await
            .map_err(|e| SessionError::Invalid(format!("add failed: {}", e)))?;
        let _ = self.forward_store.add(&spec).await;
        // Approving the forward also trusts subsequent connections for it.
        let _ = self
            .trust_store
            .remember(
                TrustKey {
                    peer_id: req.peer_id.clone(),
                    forward_key: req.target.clone(),
                },
                TrustDecision::Allow,
            )
            .await;
        // The local forward/trust bookkeeping above already succeeded and is
        // intentionally not rolled back here: if notifying the peer fails,
        // the forward still works locally, and the caller learns via the
        // returned error that the requester wasn't told.
        self.manager
            .send_forward_response(&req.peer_id, forward_response(&req, true))
            .await
            .map_err(|e| {
                SessionError::Invalid(format!(
                    "accepted locally, but failed to notify peer: {}",
                    e
                ))
            })?;
        Ok(format!("accepted forward {}", req.target))
    }

    /// Sends an outgoing forward request to a peer and records it so the
    /// matching response can be matched up later. Mirrors the TUI's
    /// original `send_request`.
    ///
    /// Records the outgoing request *before* sending it, not after: if the
    /// peer's response raced in ahead of a post-send `record_outgoing`, it
    /// would arrive as an unknown req id and be silently dropped by
    /// `ForwardNegotiator::record_response`, leaving the requester waiting
    /// forever for an answer that already came back. Recording first closes
    /// that window; if the send itself then fails, the just-recorded entry
    /// is rolled back with `remove_outgoing`.
    pub(crate) async fn send_outgoing_forward(
        &self,
        draft: OutgoingForward,
    ) -> Result<String, SessionError> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let ev = crate::rtc::ForwardRequestEvent {
            req_id: req_id.clone(),
            proto: draft.proto.clone(),
            remote_addr: draft.remote_addr.clone(),
            target: draft.target.clone(),
        };
        self.negotiator
            .record_outgoing(req_id.clone(), draft.clone())
            .await;
        match self.manager.send_forward_request(&draft.peer_id, ev).await {
            Ok(()) => Ok(format!("request sent: {}", draft.target)),
            Err(e) => {
                self.negotiator.remove_outgoing(&req_id).await;
                Err(SessionError::Invalid(format!("send failed: {}", e)))
            }
        }
    }

    /// Requester-side handling of a peer's answer to a forward we sent:
    /// establishes the local connect-forward on acceptance. Mirrors the
    /// TUI's original `apply_outcome`. Used by both the TUI (via
    /// `tui/app.rs::apply_outcome`, which now just delegates here) and the
    /// Web UI's background outcome-drain loop (`drain_and_apply_outcomes`
    /// below) -- the single choke point through which every forward
    /// negotiation outcome passes, so it's also where `SessionNotice`s are
    /// pushed (see `push_notice`): otherwise a denied/failed/duplicate
    /// outcome would only ever reach `tracing::warn!`, which is invisible at
    /// the default "error" log level (see `main.rs`'s log filter) and left
    /// users with no explanation for a pending-outgoing entry that just
    /// vanished.
    pub(crate) async fn apply_forward_outcome(
        &self,
        outcome: &ForwardOutcome,
    ) -> Result<String, SessionError> {
        let out = &outcome.outgoing;
        if !outcome.accepted {
            let text = match &outcome.reason {
                Some(reason) => format!("forward {} failed: {}", out.target, reason),
                None => format!("peer denied {}", out.target),
            };
            self.push_notice(NoticeKind::Error, text.clone()).await;
            return Ok(text);
        }
        let proto = Proto::from_name(&out.proto)
            .map_err(|e| SessionError::Invalid(format!("invalid proto: {}", e)))?;
        let spec = ForwardSpec {
            direction: Direction::Connect,
            proto,
            addr: out.local_addr.clone(),
            listen_port: out.listen_port,
            target: out.target.clone(),
        };
        if let Err(first_err) = self.controller.add_forward(spec.clone()).await {
            // A forward restored from `forward_store` at startup (see
            // `SessionContext::build`) can occupy the same key (`target`) a
            // freshly negotiated one now wants. A peer's live approval
            // should win over that stale/persisted entry, so replace it and
            // retry once rather than surfacing "forward already exists" for
            // a forward the user just approved.
            let key_taken = self
                .controller
                .list_forwards()
                .await
                .iter()
                .any(|f| f.key == spec.target);
            let retry = if key_taken {
                let _ = self.controller.remove_forward(&spec.target).await;
                self.controller.add_forward(spec.clone()).await
            } else {
                Err(first_err)
            };
            if let Err(e) = retry {
                let text = format!("add failed: {}", e);
                self.push_notice(NoticeKind::Error, text.clone()).await;
                return Err(SessionError::Invalid(text));
            }
        }
        let _ = self.forward_store.add(&spec).await;
        let text = format!("forward established: {}", out.target);
        self.push_notice(NoticeKind::Info, text.clone()).await;
        Ok(text)
    }

    /// Drains any outcomes of forwards this node requested and applies them.
    /// Used by the Web UI's background loop in place of the TUI's per-tick
    /// `refresh()`. User-facing surfacing happens inside
    /// `apply_forward_outcome` itself (via `SessionNotice`s, visible to both
    /// front ends); this loop only additionally logs at the `tracing` level
    /// for anyone tailing logs.
    pub(crate) async fn drain_and_apply_outcomes(&self) {
        for outcome in self.negotiator.drain_outcomes().await {
            match self.apply_forward_outcome(&outcome).await {
                Ok(msg) => tracing::info!("{}", msg),
                Err(e) => warn!("{}", e),
            }
        }
    }

    /// Removes a forward by its key (the target string controller uses as
    /// the forward's id) and drops it from the persisted forward store.
    pub(crate) async fn remove_forward(&self, key: &str) -> Result<(), SessionError> {
        self.controller
            .remove_forward(key)
            .await
            .map_err(|e| SessionError::Invalid(format!("remove failed: {}", e)))?;
        let _ = self.forward_store.remove(key).await;
        Ok(())
    }

    /// Removes a trust entry. Returns `Ok(true)` if an entry was removed,
    /// `Ok(false)` if no matching entry existed.
    pub(crate) async fn remove_trust(
        &self,
        peer_id: &str,
        forward_key: &str,
    ) -> Result<bool, SessionError> {
        self.trust_store
            .remove(&TrustKey {
                peer_id: peer_id.to_string(),
                forward_key: forward_key.to_string(),
            })
            .await
            .map_err(|e| SessionError::Invalid(format!("remove failed: {}", e)))
    }
}

/// Registers a hook that ties a peer's departure to cleanup of state keyed
/// to it: pending connection authorizations always, and (when a negotiator
/// is supplied -- the control-shell path doesn't have one) in-flight
/// forward proposals/requests. Used by both `SessionContext::build` and
/// `app::shell::run_control_shell`, the two places that own a
/// `PendingAuthorizations` alongside an `RTCManager`.
pub(crate) async fn register_peer_leave_purge(
    manager: RTCManager,
    pending_auth: PendingAuthorizations,
    negotiator: Option<ForwardNegotiator>,
) {
    let hook_manager = manager.clone();
    manager
        .on_peer_leave(move |peer_id, epoch| {
            let manager = hook_manager.clone();
            let pending_auth = pending_auth.clone();
            let negotiator = negotiator.clone();
            tokio::spawn(purge_after_grace(
                peer_id,
                epoch,
                manager,
                pending_auth,
                negotiator,
            ));
        })
        .await;
}

/// Waits out `PEER_LEAVE_GRACE`, then -- unless `peer_id` rejoined in the
/// meantime (its session epoch has moved on from `epoch_at_leave`) --
/// denies/removes anything still keyed to it. Split out from
/// `register_peer_leave_purge` so it can be driven directly in tests
/// without depending on a real `EVENT_LEAVE`/`EVENT_JOIN` round trip.
async fn purge_after_grace(
    peer_id: String,
    epoch_at_leave: u64,
    manager: RTCManager,
    pending_auth: PendingAuthorizations,
    negotiator: Option<ForwardNegotiator>,
) {
    tokio::time::sleep(PEER_LEAVE_GRACE).await;
    if manager.peer_epoch(&peer_id).await != epoch_at_leave {
        // The peer rejoined within the grace window; nothing to purge.
        return;
    }

    let auth_count = pending_auth.purge_peer(&peer_id).await;
    let (incoming, outgoing) = match &negotiator {
        Some(neg) => neg.purge_peer(&peer_id).await,
        None => (0, 0),
    };
    if auth_count > 0 || incoming > 0 || outgoing > 0 {
        info!(
            "purged state for departed peer {}: {} pending auth, {} incoming forwards, {} outgoing forwards",
            peer_id, auth_count, incoming, outgoing
        );
    }
}

fn forward_response(req: &IncomingForward, accepted: bool) -> crate::rtc::ForwardResponseEvent {
    crate::rtc::ForwardResponseEvent {
        req_id: req.req_id.clone(),
        target: req.target.clone(),
        accepted,
    }
}

/// Parses the port from an `ip:port` string. Shared by the TUI's add-forward
/// form and the Web API's `POST /api/forwards` handler.
pub(crate) fn parse_addr_port(addr: &str) -> Option<i32> {
    addr.rsplit(':').next()?.parse::<i32>().ok()
}

/// Current Unix time in milliseconds. Mirrors `auth::audit::now_ms` (kept
/// as a separate copy since that one is private to its module).
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthRequest;

    #[test]
    fn parses_port_from_addr() {
        assert_eq!(parse_addr_port("127.0.0.1:8080"), Some(8080));
        assert_eq!(parse_addr_port("8080"), Some(8080));
        assert_eq!(parse_addr_port(""), None);
        assert_eq!(parse_addr_port("127.0.0.1:abc"), None);
    }

    #[test]
    fn session_error_display_matches_inner_message() {
        let e = SessionError::NotFound("forward request no longer exists".into());
        assert_eq!(e.to_string(), "forward request no longer exists");
        let e = SessionError::Invalid("invalid proto: nope".into());
        assert_eq!(e.to_string(), "invalid proto: nope");
    }

    fn sample_request(peer_id: &str) -> AuthRequest {
        AuthRequest {
            peer_id: peer_id.to_string(),
            forward_key: "tcp:80".to_string(),
            target_addr: "127.0.0.1:80".to_string(),
            proto: "tcp".to_string(),
        }
    }

    /// Builds a `SessionContext` backed by inert/for-test components (no
    /// real network, disk state scoped to a throwaway temp dir) so
    /// `apply_forward_outcome` can be exercised directly.
    async fn test_session_context() -> SessionContext {
        let dir = std::env::temp_dir().join(format!("p2p-session-test-{}", uuid::Uuid::new_v4()));
        SessionContext {
            room: "room".to_string(),
            manager: RTCManager::for_test("self"),
            controller: ForwardController::new_inert(),
            trust_store: TrustStore::load(dir.join("trust.json")).await.unwrap(),
            audit_log: AuthAuditLog::default(),
            pending_auth: PendingAuthorizations::new(),
            negotiator: ForwardNegotiator::new(),
            forward_store: ForwardStore::load(dir.join("forwards.json")).await.unwrap(),
            notices: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sample_outgoing_forward(target: &str) -> OutgoingForward {
        OutgoingForward {
            peer_id: "peer-1".to_string(),
            proto: "tcp".to_string(),
            listen_port: 8080,
            local_addr: "127.0.0.1:8080".to_string(),
            remote_addr: "127.0.0.1:80".to_string(),
            target: target.to_string(),
        }
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_info_notice_on_success() {
        let ctx = test_session_context().await;
        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:80"),
            accepted: true,
            reason: None,
        };

        let msg = ctx.apply_forward_outcome(&outcome).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:80");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Info);
        assert_eq!(notices[0].text, "forward established: tcp:127.0.0.1:80");
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_error_notice_on_add_failure() {
        let ctx = test_session_context().await;
        // An empty target is rejected by `ForwardController::add_forward`
        // ("forward target must not be empty") and isn't a duplicate-key
        // situation, so this exercises the plain add-failure path.
        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward(""),
            accepted: true,
            reason: None,
        };

        let err = ctx.apply_forward_outcome(&outcome).await.unwrap_err();
        assert!(err.to_string().contains("add failed"));

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Error);
        assert!(notices[0].text.contains("add failed"));
    }

    #[tokio::test]
    async fn apply_forward_outcome_pushes_error_notice_on_denied_and_failed() {
        let ctx = test_session_context().await;

        let denied = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:80"),
            accepted: false,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&denied).await.unwrap();
        assert_eq!(msg, "peer denied tcp:127.0.0.1:80");

        let failed = ForwardOutcome {
            outgoing: sample_outgoing_forward("tcp:127.0.0.1:81"),
            accepted: false,
            reason: Some("peer disconnected".to_string()),
        };
        let msg = ctx.apply_forward_outcome(&failed).await.unwrap();
        assert_eq!(msg, "forward tcp:127.0.0.1:81 failed: peer disconnected");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 2);
        assert!(notices.iter().all(|n| n.kind == NoticeKind::Error));
    }

    #[tokio::test]
    async fn apply_forward_outcome_replaces_stale_duplicate_key() {
        let ctx = test_session_context().await;
        let target = "tcp:127.0.0.1:80";

        // Stands in for a forward restored from `forward_store` at startup
        // (see `SessionContext::build`) that happens to share a key with a
        // forward now being negotiated fresh.
        ctx.controller
            .add_forward(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::Tcp,
                addr: "stale:1".to_string(),
                listen_port: -1,
                target: target.to_string(),
            })
            .await
            .unwrap();

        let outcome = ForwardOutcome {
            outgoing: sample_outgoing_forward(target),
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome).await.unwrap();
        assert_eq!(msg, format!("forward established: {}", target));

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(statuses.len(), 1, "the stale entry must be replaced, not duplicated");
        assert_eq!(statuses[0].spec.direction, Direction::Connect);
        assert_eq!(statuses[0].spec.addr, "127.0.0.1:8080");

        let notices = ctx.notices.lock().await.clone();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].kind, NoticeKind::Info);
    }

    #[tokio::test]
    async fn apply_forward_outcome_scoped_targets_on_different_peers_coexist() {
        let ctx = test_session_context().await;

        // Regression test for the original node-scoping bug: two forwards to
        // the *same* remote addr but on different peer nodes previously
        // collided on the unscoped target key (second replaced the first).
        // With `@node` scoping baked into `target`, they must coexist.
        let mut on_a = sample_outgoing_forward("tcp:127.0.0.1:22@node-a");
        on_a.listen_port = 10022;
        let outcome_a = ForwardOutcome {
            outgoing: on_a,
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome_a).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:22@node-a");

        let mut on_b = sample_outgoing_forward("tcp:127.0.0.1:22@node-b");
        on_b.listen_port = 10023;
        let outcome_b = ForwardOutcome {
            outgoing: on_b,
            accepted: true,
            reason: None,
        };
        let msg = ctx.apply_forward_outcome(&outcome_b).await.unwrap();
        assert_eq!(msg, "forward established: tcp:127.0.0.1:22@node-b");

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(
            statuses.len(),
            2,
            "forwards to the same remote addr but scoped to different peers must coexist"
        );
    }

    #[tokio::test]
    async fn apply_forward_outcome_replaces_same_scoped_target() {
        let ctx = test_session_context().await;
        let target = "tcp:127.0.0.1:22@node-a";

        // The existing replace-on-duplicate-key behavior, now exercised
        // per-node: a second accepted outcome for the *same* scoped target
        // still replaces the first rather than duplicating it.
        let mut first = sample_outgoing_forward(target);
        first.listen_port = 10022;
        let outcome_first = ForwardOutcome {
            outgoing: first,
            accepted: true,
            reason: None,
        };
        ctx.apply_forward_outcome(&outcome_first).await.unwrap();

        let mut second = sample_outgoing_forward(target);
        second.listen_port = 10099;
        let outcome_second = ForwardOutcome {
            outgoing: second,
            accepted: true,
            reason: None,
        };
        ctx.apply_forward_outcome(&outcome_second).await.unwrap();

        let statuses = ctx.controller.list_forwards().await;
        assert_eq!(
            statuses.len(),
            1,
            "second outcome for the same scoped target must replace, not duplicate"
        );
        assert_eq!(statuses[0].spec.listen_port, 10099);
    }

    // --- purge_after_grace: the epoch-changed ⇒ no-purge wiring rule -------
    //
    // These drive `purge_after_grace` directly rather than through a real
    // `EVENT_LEAVE`/`EVENT_JOIN` round trip: simulating an actual mistlib
    // rejoin (which would bump `RTCManager::peer_epoch`) requires the
    // `rtc` module's internal `handle_join`, which isn't exposed outside
    // `crate::rtc::manager`. Passing `epoch_at_leave` directly exercises
    // the same comparison `register_peer_leave_purge`'s real hook relies
    // on: `manager.peer_epoch(peer_id) != epoch_at_leave` ⇒ treat as
    // rejoined.

    #[tokio::test(start_paused = true)]
    async fn purge_after_grace_skips_when_epoch_moved_on() {
        let manager = RTCManager::for_test("self");
        let pending_auth = PendingAuthorizations::new();
        let receiver = pending_auth
            .enqueue_for_test(sample_request("peer-1"))
            .await;
        let negotiator = ForwardNegotiator::new();
        negotiator
            .record_incoming(
                "req-1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await;

        // `manager` never actually joined peer-1 (its epoch stays 0); a
        // mismatched `epoch_at_leave` stands in for "the peer rejoined
        // (bumping its epoch) before the grace window elapsed".
        purge_after_grace(
            "peer-1".to_string(),
            1,
            manager,
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        assert_eq!(
            pending_auth.list().await.len(),
            1,
            "a peer whose epoch moved on must not have its pending auth purged"
        );
        assert_eq!(
            negotiator.list_incoming().await.len(),
            1,
            "a peer whose epoch moved on must not have its forward proposals purged"
        );
        drop(receiver);
    }

    #[tokio::test(start_paused = true)]
    async fn purge_after_grace_purges_when_epoch_unchanged() {
        let manager = RTCManager::for_test("self");
        let pending_auth = PendingAuthorizations::new();
        let receiver = pending_auth
            .enqueue_for_test(sample_request("peer-1"))
            .await;
        let negotiator = ForwardNegotiator::new();
        negotiator
            .record_incoming(
                "req-1".into(),
                "peer-1".into(),
                "tcp".into(),
                "127.0.0.1:80".into(),
                "tcp:127.0.0.1:80".into(),
            )
            .await;

        // `epoch_at_leave` (0) matches the never-joined manager's current
        // epoch for peer-1 (also 0): no rejoin happened.
        purge_after_grace(
            "peer-1".to_string(),
            0,
            manager,
            pending_auth.clone(),
            Some(negotiator.clone()),
        )
        .await;

        assert!(pending_auth.list().await.is_empty());
        assert_eq!(receiver.await.unwrap(), AuthDecision::Deny);
        assert!(negotiator.list_incoming().await.is_empty());
    }
}
