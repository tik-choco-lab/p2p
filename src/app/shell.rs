use anyhow::Result;

use crate::auth::{
    default_trust_store_path, AuthAuditLog, PendingAuthorizations, PendingAuthorizer, TrustStore,
};
use crate::controller::ForwardController;
use crate::rtc::RTCManager;
use crate::{control_shell, tui};

use super::session::{register_peer_leave_purge, SessionContext};
use super::{generate_room_id, load_or_create_node_id};

pub(crate) async fn run_tui(room_id: Option<&str>) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = load_or_create_node_id().await?;
    let ctx = SessionContext::build(self_id, room, true).await?;
    let manager = ctx.manager.clone();

    let result = tui::run(ctx).await;

    manager.close().await;
    result
}

#[allow(dead_code)]
pub(crate) async fn run_control_shell(room_id: Option<&str>) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = load_or_create_node_id().await?;
    let manager = RTCManager::new(self_id, room, false).await;
    let trust_store = TrustStore::load(default_trust_store_path()).await?;
    let audit_log = AuthAuditLog::default();
    let pending_auth = PendingAuthorizations::new();
    let controller = ForwardController::with_authorizer(
        manager.clone(),
        PendingAuthorizer::shared_with_audit_log(
            trust_store.clone(),
            pending_auth.clone(),
            audit_log.clone(),
        ),
    );
    // No forward negotiator here: `run_control_shell` doesn't wire forward
    // request/response handlers at all (unlike `SessionContext::build`), so
    // there's nothing to purge on that front -- just the pending auth queue.
    register_peer_leave_purge(manager.clone(), pending_auth.clone(), None).await;

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    tokio::select! {
        result = control_shell::run(controller, trust_store, audit_log, pending_auth, stdin, stdout) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {}
    }

    manager.close().await;
    Ok(())
}
