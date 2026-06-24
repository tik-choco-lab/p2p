use anyhow::Result;

use crate::auth::{
    default_trust_store_path, AuthAuditLog, PendingAuthorizations, PendingAuthorizer, TrustStore,
};
use crate::controller::ForwardController;
use crate::rtc::RTCManager;
use crate::{control_shell, tui};

use super::generate_room_id;

pub(crate) async fn run_tui(room_id: Option<&str>) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = uuid::Uuid::new_v4().to_string();
    let manager = RTCManager::new(self_id, room.clone(), true).await;
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

    let result = tui::run(tui::TuiContext {
        room,
        controller,
        trust_store,
        audit_log,
        pending_auth,
    })
    .await;

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

    let self_id = uuid::Uuid::new_v4().to_string();
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
