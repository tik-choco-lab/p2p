use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::auth::{
    AuthDecision, AuthEvent, PendingAuthorization, TrustDecision, TrustEntry, TrustKey,
};
use crate::controller::{Direction, ForwardSpec, ForwardStatus, Proto};
use crate::negotiation::{IncomingForward, OutgoingForward};
use crate::rtc::ForwardRequestEvent;

use super::TuiContext;

#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum Focus {
    Forwards,
    Pending,
}

/// Fields of the add-forward form. Direction is implicit: the local node
/// listens locally and reaches the peer's `remote` address.
#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum AddField {
    Proto,
    Local,
    Remote,
}

#[derive(Clone)]
pub(super) struct AddForm {
    pub(super) proto_tcp: bool,
    pub(super) local: String,
    pub(super) remote: String,
    pub(super) field: AddField,
}

impl Default for AddForm {
    fn default() -> Self {
        Self {
            proto_tcp: true,
            local: String::new(),
            remote: String::new(),
            field: AddField::Proto,
        }
    }
}

/// Peer picker shown when more than one peer is connected at submit time.
#[derive(Clone)]
pub(super) struct PeerSelect {
    pub(super) peers: Vec<String>,
    pub(super) sel: usize,
    pub(super) draft: OutgoingForward,
}

/// A row in the unified pending pane.
pub(super) enum PendingRow {
    Conn(PendingAuthorization),
    Forward(IncomingForward),
}

pub(super) enum Popup {
    None,
    Add(AddForm),
    SelectPeer(PeerSelect),
    Trust(usize),
}

pub(super) struct App {
    pub(super) ctx: TuiContext,
    pub(super) focus: Focus,
    pub(super) popup: Popup,
    pub(super) forwards: Vec<ForwardStatus>,
    pub(super) pending: Vec<PendingAuthorization>,
    pub(super) forward_pending: Vec<IncomingForward>,
    pub(super) events: Vec<AuthEvent>,
    pub(super) trust: Vec<TrustEntry>,
    pub(super) peers: Vec<String>,
    pub(super) forwards_sel: usize,
    pub(super) pending_sel: usize,
    pub(super) expanded: bool,
    pub(super) message: Option<String>,
    pub(super) should_quit: bool,
}

impl App {
    pub(super) fn new(ctx: TuiContext) -> Self {
        Self {
            ctx,
            focus: Focus::Forwards,
            popup: Popup::None,
            forwards: Vec::new(),
            pending: Vec::new(),
            forward_pending: Vec::new(),
            events: Vec::new(),
            trust: Vec::new(),
            peers: Vec::new(),
            forwards_sel: 0,
            pending_sel: 0,
            expanded: false,
            message: None,
            should_quit: false,
        }
    }

    pub(super) fn pending_rows(&self) -> Vec<PendingRow> {
        let mut rows: Vec<PendingRow> = self
            .forward_pending
            .iter()
            .cloned()
            .map(PendingRow::Forward)
            .collect();
        rows.extend(self.pending.iter().cloned().map(PendingRow::Conn));
        rows
    }

    pub(super) async fn refresh(&mut self) {
        // Apply outcomes of forward requests this node initiated.
        for outcome in self.ctx.negotiator.drain_outcomes().await {
            self.apply_outcome(outcome).await;
        }

        self.forwards = self.ctx.controller.list_forwards().await;
        self.pending = self.ctx.pending_auth.list().await;
        self.forward_pending = self.ctx.negotiator.list_incoming().await;
        self.events = self.ctx.audit_log.list().await;
        self.trust = self.ctx.trust_store.list().await;
        self.peers = self.ctx.manager.connected_peers().await;
        if self.forwards_sel >= self.forwards.len() {
            self.forwards_sel = self.forwards.len().saturating_sub(1);
        }
        let pending_len = self.pending_rows().len();
        if self.pending_sel >= pending_len {
            self.pending_sel = pending_len.saturating_sub(1);
        }
    }

    pub(super) async fn handle_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        self.message = None;
        match std::mem::replace(&mut self.popup, Popup::None) {
            Popup::Add(form) => self.handle_add_key(key, form).await,
            Popup::SelectPeer(sel) => self.handle_peer_select_key(key, sel).await,
            Popup::Trust(sel) => self.handle_trust_key(key, sel).await,
            Popup::None => self.handle_main_key(key).await,
        }
    }

    async fn handle_main_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Forwards => Focus::Pending,
                    Focus::Pending => Focus::Forwards,
                };
            }
            KeyCode::Char('a') => self.popup = Popup::Add(AddForm::default()),
            KeyCode::Char('t') => self.popup = Popup::Trust(0),
            KeyCode::Enter | KeyCode::Char(' ') if self.focus == Focus::Forwards => {
                self.expanded = !self.expanded;
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_sel(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_sel(1),
            KeyCode::Char('d') if self.focus == Focus::Forwards => self.remove_selected().await,
            KeyCode::Char('y') if self.focus == Focus::Pending => {
                self.resolve_pending(AuthDecision::Allow).await
            }
            KeyCode::Char('Y') if self.focus == Focus::Pending => {
                self.resolve_pending(AuthDecision::AllowAlways).await
            }
            KeyCode::Char('n') if self.focus == Focus::Pending => {
                self.resolve_pending(AuthDecision::Deny).await
            }
            KeyCode::Char('N') if self.focus == Focus::Pending => {
                self.resolve_pending(AuthDecision::DenyAlways).await
            }
            _ => {}
        }
    }

    fn move_sel(&mut self, delta: i32) {
        let pending_len = self.pending_rows().len();
        let (sel, len) = match self.focus {
            Focus::Forwards => (&mut self.forwards_sel, self.forwards.len()),
            Focus::Pending => (&mut self.pending_sel, pending_len),
        };
        if len == 0 {
            return;
        }
        let next = (*sel as i32 + delta).rem_euclid(len as i32);
        *sel = next as usize;
    }

    async fn remove_selected(&mut self) {
        let Some(status) = self.forwards.get(self.forwards_sel) else {
            return;
        };
        let key = status.key.clone();
        match self.ctx.controller.remove_forward(&key).await {
            Ok(()) => {
                let _ = self.ctx.forward_store.remove(&key).await;
                self.message = Some(format!("removed {}", key));
            }
            Err(e) => self.message = Some(format!("remove failed: {}", e)),
        }
    }

    async fn resolve_pending(&mut self, decision: AuthDecision) {
        let rows = self.pending_rows();
        let Some(row) = rows.get(self.pending_sel) else {
            return;
        };
        match row {
            PendingRow::Conn(item) => {
                let id = item.id;
                if self.ctx.pending_auth.resolve(id, decision).await {
                    self.message = Some(format!("resolved #{}", id));
                } else {
                    self.message = Some(format!("pending #{} no longer exists", id));
                }
            }
            PendingRow::Forward(item) => {
                let id = item.id;
                let allow = matches!(decision, AuthDecision::Allow | AuthDecision::AllowAlways);
                self.resolve_forward(id, allow).await;
            }
        }
    }

    /// Approves or denies an incoming forward proposal: on approval, creates a
    /// matching serve forward, remembers trust, persists, and answers the peer.
    async fn resolve_forward(&mut self, id: u64, allow: bool) {
        let Some(req) = self.ctx.negotiator.take_incoming(id).await else {
            self.message = Some("forward request no longer exists".into());
            return;
        };

        if !allow {
            let _ = self
                .ctx
                .manager
                .send_forward_response(&req.peer_id, response(&req, false))
                .await;
            self.message = Some(format!("denied forward {}", req.target));
            return;
        }

        let proto = match Proto::from_name(&req.proto) {
            Ok(p) => p,
            Err(e) => {
                self.message = Some(format!("invalid proto: {}", e));
                return;
            }
        };
        let spec = ForwardSpec {
            direction: Direction::Serve,
            proto,
            addr: req.remote_addr.clone(),
            listen_port: -1,
            target: req.target.clone(),
        };
        if let Err(e) = self.ctx.controller.add_forward(spec.clone()).await {
            self.message = Some(format!("add failed: {}", e));
            return;
        }
        let _ = self.ctx.forward_store.add(&spec).await;
        // Approving the forward also trusts subsequent connections for it.
        let _ = self
            .ctx
            .trust_store
            .remember(
                TrustKey {
                    peer_id: req.peer_id.clone(),
                    forward_key: req.target.clone(),
                },
                TrustDecision::Allow,
            )
            .await;
        let _ = self
            .ctx
            .manager
            .send_forward_response(&req.peer_id, response(&req, true))
            .await;
        self.message = Some(format!("accepted forward {}", req.target));
    }

    /// Requester-side handling of a peer's answer to our forward request.
    async fn apply_outcome(&mut self, outcome: crate::negotiation::ForwardOutcome) {
        let out = outcome.outgoing;
        if !outcome.accepted {
            self.message = Some(format!("peer denied {}", out.target));
            return;
        }
        let proto = match Proto::from_name(&out.proto) {
            Ok(p) => p,
            Err(_) => return,
        };
        let spec = ForwardSpec {
            direction: Direction::Connect,
            proto,
            addr: out.local_addr.clone(),
            listen_port: out.listen_port,
            target: out.target.clone(),
        };
        if let Err(e) = self.ctx.controller.add_forward(spec.clone()).await {
            self.message = Some(format!("add failed: {}", e));
            return;
        }
        let _ = self.ctx.forward_store.add(&spec).await;
        self.message = Some(format!("forward established: {}", out.target));
    }

    async fn handle_add_key(&mut self, key: KeyEvent, mut form: AddForm) {
        match key.code {
            KeyCode::Esc => return,
            KeyCode::Enter => {
                self.submit_add(form).await;
                return;
            }
            KeyCode::Tab | KeyCode::Down => form.field = next_field(form.field),
            KeyCode::Up => form.field = prev_field(form.field),
            KeyCode::Left | KeyCode::Right if form.field == AddField::Proto => {
                form.proto_tcp = !form.proto_tcp;
            }
            KeyCode::Char(' ') if form.field == AddField::Proto => {
                form.proto_tcp = !form.proto_tcp;
            }
            KeyCode::Backspace => match form.field {
                AddField::Local => {
                    form.local.pop();
                }
                AddField::Remote => {
                    form.remote.pop();
                }
                AddField::Proto => {}
            },
            KeyCode::Char(c) => match form.field {
                AddField::Local => form.local.push(c),
                AddField::Remote => form.remote.push(c),
                AddField::Proto => {}
            },
            _ => {}
        }
        self.popup = Popup::Add(form);
    }

    async fn submit_add(&mut self, form: AddForm) {
        let local = form.local.trim();
        let remote = form.remote.trim();
        if local.is_empty() || remote.is_empty() {
            self.message = Some("local と remote の ip:port を入力".into());
            self.popup = Popup::Add(form);
            return;
        }
        let Some(listen_port) = parse_port(local) else {
            self.message = Some("local は ip:port 形式".into());
            self.popup = Popup::Add(form);
            return;
        };
        if parse_port(remote).is_none() {
            self.message = Some("remote は ip:port 形式".into());
            self.popup = Popup::Add(form);
            return;
        }
        let proto = if form.proto_tcp { "tcp" } else { "udp" };
        let target = format!("{}:{}", proto, remote);
        let draft = OutgoingForward {
            peer_id: String::new(),
            proto: proto.to_string(),
            listen_port,
            local_addr: local.to_string(),
            remote_addr: remote.to_string(),
            target,
        };

        let peers = self.ctx.manager.connected_peers().await;
        match peers.len() {
            0 => {
                self.message = Some("接続中のピアがいません".into());
                self.popup = Popup::Add(form);
            }
            1 => {
                let mut draft = draft;
                draft.peer_id = peers[0].clone();
                self.send_request(draft).await;
            }
            _ => {
                self.popup = Popup::SelectPeer(PeerSelect {
                    peers,
                    sel: 0,
                    draft,
                });
            }
        }
    }

    async fn handle_peer_select_key(&mut self, key: KeyEvent, mut sel: PeerSelect) {
        let len = sel.peers.len();
        match key.code {
            KeyCode::Esc => return,
            KeyCode::Up | KeyCode::Char('k') => {
                sel.sel = (sel.sel as i32 - 1).rem_euclid(len as i32) as usize;
                self.popup = Popup::SelectPeer(sel);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                sel.sel = (sel.sel + 1) % len;
                self.popup = Popup::SelectPeer(sel);
            }
            KeyCode::Enter => {
                let mut draft = sel.draft;
                draft.peer_id = sel.peers[sel.sel].clone();
                self.send_request(draft).await;
            }
            _ => self.popup = Popup::SelectPeer(sel),
        }
    }

    async fn send_request(&mut self, draft: OutgoingForward) {
        let req_id = uuid::Uuid::new_v4().to_string();
        let ev = ForwardRequestEvent {
            req_id: req_id.clone(),
            proto: draft.proto.clone(),
            remote_addr: draft.remote_addr.clone(),
            target: draft.target.clone(),
        };
        match self
            .ctx
            .manager
            .send_forward_request(&draft.peer_id, ev)
            .await
        {
            Ok(()) => {
                self.ctx
                    .negotiator
                    .record_outgoing(req_id, draft.clone())
                    .await;
                self.message = Some(format!("request sent: {}", draft.target));
            }
            Err(e) => self.message = Some(format!("send failed: {}", e)),
        }
    }

    async fn handle_trust_key(&mut self, key: KeyEvent, mut sel: usize) {
        let len = self.trust.len();
        match key.code {
            KeyCode::Esc | KeyCode::Char('t') | KeyCode::Char('q') => return,
            KeyCode::Up | KeyCode::Char('k') if len > 0 => {
                sel = (sel as i32 - 1).rem_euclid(len as i32) as usize;
            }
            KeyCode::Down | KeyCode::Char('j') if len > 0 => {
                sel = (sel + 1) % len;
            }
            KeyCode::Char('x') | KeyCode::Delete if len > 0 => {
                if let Some(entry) = self.trust.get(sel) {
                    let tk = TrustKey {
                        peer_id: entry.key.peer_id.clone(),
                        forward_key: entry.key.forward_key.clone(),
                    };
                    match self.ctx.trust_store.remove(&tk).await {
                        Ok(true) => self.message = Some("trust entry removed".into()),
                        Ok(false) => self.message = Some("trust entry not found".into()),
                        Err(e) => self.message = Some(format!("remove failed: {}", e)),
                    }
                }
                if sel > 0 {
                    sel -= 1;
                }
            }
            _ => {}
        }
        self.popup = Popup::Trust(sel);
    }
}

fn response(req: &IncomingForward, accepted: bool) -> crate::rtc::ForwardResponseEvent {
    crate::rtc::ForwardResponseEvent {
        req_id: req.req_id.clone(),
        target: req.target.clone(),
        accepted,
    }
}

fn next_field(f: AddField) -> AddField {
    match f {
        AddField::Proto => AddField::Local,
        AddField::Local => AddField::Remote,
        AddField::Remote => AddField::Proto,
    }
}

fn prev_field(f: AddField) -> AddField {
    match f {
        AddField::Proto => AddField::Remote,
        AddField::Local => AddField::Proto,
        AddField::Remote => AddField::Local,
    }
}

/// Parses the port from an `ip:port` string.
fn parse_port(addr: &str) -> Option<i32> {
    addr.rsplit(':').next()?.parse::<i32>().ok()
}
