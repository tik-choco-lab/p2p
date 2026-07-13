use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::auth::{AuthDecision, AuthEvent, PendingAuthorization, TrustEntry};
use crate::controller::ForwardStatus;
use crate::negotiation::{IncomingForward, OutgoingForward};

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
    /// URL of the in-process Web UI once started via the `w` key; also the
    /// "already running" latch so repeated presses reuse the same server.
    web_url: Option<String>,
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
            web_url: None,
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
            KeyCode::Char('w') => self.open_web_ui().await,
            _ => {}
        }
    }

    /// Starts the in-process Web UI on first use (sharing this session's
    /// context), then opens a browser and copies the URL — both
    /// best-effort; the URL is always shown in the footer as a fallback.
    async fn open_web_ui(&mut self) {
        let url = match &self.web_url {
            Some(url) => url.clone(),
            None => match crate::web::start_in_process(self.ctx.clone(), crate::web::DEFAULT_PORT)
                .await
            {
                Ok(url) => {
                    self.web_url = Some(url.clone());
                    url
                }
                Err(e) => {
                    self.message = Some(format!("web ui failed to start: {}", e));
                    return;
                }
            },
        };
        let opened = crate::web::best_effort_open(&url);
        let copied = crate::web::best_effort_copy_clipboard(&url);
        let note = match (opened, copied) {
            (true, true) => "browser opened, url copied",
            (true, false) => "browser opened",
            (false, true) => "url copied - open it manually",
            (false, false) => "open it manually",
        };
        self.message = Some(format!("web ui: {} ({})", url, note));
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
        self.message = Some(match self.ctx.remove_forward(&key).await {
            Ok(()) => format!("removed {}", key),
            Err(e) => e.to_string(),
        });
    }

    async fn resolve_pending(&mut self, decision: AuthDecision) {
        let rows = self.pending_rows();
        let Some(row) = rows.get(self.pending_sel) else {
            return;
        };
        match row {
            PendingRow::Conn(item) => {
                let id = item.id;
                if self.ctx.resolve_pending_auth(id, decision).await {
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

    /// Approves or denies an incoming forward proposal. The side-effect
    /// chain (creating the serve forward, remembering trust, persisting,
    /// answering the peer) lives in `SessionContext::resolve_forward`,
    /// shared with the Web UI.
    async fn resolve_forward(&mut self, id: u64, allow: bool) {
        self.message = Some(match self.ctx.resolve_forward(id, allow).await {
            Ok(msg) => msg,
            Err(e) => e.to_string(),
        });
    }

    /// Requester-side handling of a peer's answer to our forward request.
    /// The side-effect chain (establishing the connect-forward, duplicate-key
    /// replacement, pushing a `SessionNotice`) lives in
    /// `SessionContext::apply_forward_outcome`, shared with the Web UI's
    /// background outcome-drain loop.
    async fn apply_outcome(&mut self, outcome: crate::negotiation::ForwardOutcome) {
        self.message = Some(match self.ctx.apply_forward_outcome(&outcome).await {
            Ok(msg) => msg,
            Err(e) => e.to_string(),
        });
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
        self.message = Some(match self.ctx.send_outgoing_forward(draft).await {
            Ok(msg) => msg,
            Err(e) => e.to_string(),
        });
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
                    match self
                        .ctx
                        .remove_trust(&entry.key.peer_id, &entry.key.forward_key)
                        .await
                    {
                        Ok(true) => self.message = Some("trust entry removed".into()),
                        Ok(false) => self.message = Some("trust entry not found".into()),
                        Err(e) => self.message = Some(e.to_string()),
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

/// Parses the port from an `ip:port` string. Delegates to the shared
/// session helper (also used by the Web API's `POST /api/forwards`).
fn parse_port(addr: &str) -> Option<i32> {
    crate::app::session::parse_addr_port(addr)
}
