use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::auth::{AuthDecision, AuthEvent, PendingAuthorization, TrustEntry, TrustKey};
use crate::controller::ForwardStatus;

use super::format::parse_add_line;
use super::TuiContext;

#[derive(PartialEq, Eq, Clone, Copy)]
pub(super) enum Focus {
    Forwards,
    Pending,
}

pub(super) enum Popup {
    None,
    Add(String),
    Trust(usize),
}

pub(super) struct App {
    pub(super) ctx: TuiContext,
    pub(super) focus: Focus,
    pub(super) popup: Popup,
    pub(super) forwards: Vec<ForwardStatus>,
    pub(super) pending: Vec<PendingAuthorization>,
    pub(super) events: Vec<AuthEvent>,
    pub(super) trust: Vec<TrustEntry>,
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
            events: Vec::new(),
            trust: Vec::new(),
            forwards_sel: 0,
            pending_sel: 0,
            expanded: false,
            message: None,
            should_quit: false,
        }
    }

    pub(super) async fn refresh(&mut self) {
        self.forwards = self.ctx.controller.list_forwards().await;
        self.pending = self.ctx.pending_auth.list().await;
        self.events = self.ctx.audit_log.list().await;
        self.trust = self.ctx.trust_store.list().await;
        if self.forwards_sel >= self.forwards.len() {
            self.forwards_sel = self.forwards.len().saturating_sub(1);
        }
        if self.pending_sel >= self.pending.len() {
            self.pending_sel = self.pending.len().saturating_sub(1);
        }
    }

    pub(super) async fn handle_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        self.message = None;
        match std::mem::replace(&mut self.popup, Popup::None) {
            Popup::Add(buf) => self.handle_add_key(key, buf).await,
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
            KeyCode::Char('a') => self.popup = Popup::Add(String::new()),
            KeyCode::Char('t') => self.popup = Popup::Trust(0),
            KeyCode::Enter | KeyCode::Char(' ') if self.focus == Focus::Forwards => {
                self.expanded = !self.expanded;
            }
            KeyCode::Char('r') => {}
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
        let (sel, len) = match self.focus {
            Focus::Forwards => (&mut self.forwards_sel, self.forwards.len()),
            Focus::Pending => (&mut self.pending_sel, self.pending.len()),
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
            Ok(()) => self.message = Some(format!("removed {}", key)),
            Err(e) => self.message = Some(format!("remove failed: {}", e)),
        }
    }

    async fn resolve_pending(&mut self, decision: AuthDecision) {
        let Some(item) = self.pending.get(self.pending_sel) else {
            return;
        };
        let id = item.id;
        if self.ctx.pending_auth.resolve(id, decision).await {
            self.message = Some(format!("resolved #{}", id));
        } else {
            self.message = Some(format!("pending #{} no longer exists", id));
        }
    }

    async fn handle_add_key(&mut self, key: KeyEvent, mut buf: String) {
        match key.code {
            KeyCode::Esc => {}
            KeyCode::Enter => match parse_add_line(&buf) {
                Ok(spec) => match self.ctx.controller.add_forward(spec).await {
                    Ok(k) => self.message = Some(format!("added {}", k)),
                    Err(e) => {
                        self.message = Some(format!("add failed: {}", e));
                        self.popup = Popup::Add(buf);
                    }
                },
                Err(e) => {
                    self.message = Some(format!("invalid: {}", e));
                    self.popup = Popup::Add(buf);
                }
            },
            KeyCode::Backspace => {
                buf.pop();
                self.popup = Popup::Add(buf);
            }
            KeyCode::Char(c) => {
                buf.push(c);
                self.popup = Popup::Add(buf);
            }
            _ => self.popup = Popup::Add(buf),
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
