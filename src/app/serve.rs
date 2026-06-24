use anyhow::Result;
use tracing::error;

use crate::auth::{
    default_trust_store_path, AuthPolicy, PolicyAuthorizer, SharedAuthorizer, TrustStore,
};
use crate::controller::{Direction, ForwardController, ForwardSpec, Proto};
use crate::forward_args::{forward_key, parse_forward, split_serve_args};
use crate::rtc::RTCManager;
use crate::proxy;

use super::generate_room_id;

pub(crate) async fn run_serve(
    args: &[String],
    command: &[String],
    auto_accept: bool,
    allow_peer: &[String],
) -> Result<()> {
    let (room_id, forwards) = split_serve_args(args);
    let mut room_id = room_id.unwrap_or_default();

    if room_id.is_empty() {
        room_id = generate_room_id();
        eprintln!("Room ID: {}", room_id);
    }

    let self_id = uuid::Uuid::new_v4().to_string();
    let manager = RTCManager::new(self_id, room_id, true).await;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mgr = manager.clone();
    let stx = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        mgr.close().await;
        let _ = stx.send(()).await;
    });

    let controller = ForwardController::with_authorizer(
        manager.clone(),
        build_authorizer(auto_accept, allow_peer).await?,
    );
    for f in &forwards {
        let (proto, addr, _) = parse_forward(f);
        let target = forward_key(proto, addr);
        controller
            .add_forward(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::from_name(proto)?,
                addr: addr.to_string(),
                listen_port: -1,
                target,
            })
            .await?;
    }

    if !command.is_empty() {
        let executor = proxy::Executor::new(manager.clone(), command.to_vec());
        let stx = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = executor.run().await {
                error!("proxy executor error: {}", e);
            }
            let _ = stx.send(()).await;
        });
    } else if forwards.is_empty() {
        error!("No command or forwards specified. Usage: p2p serve [room-id] [forward-target] ...");
        return Ok(());
    }

    shutdown_rx.recv().await;
    manager.close().await;
    Ok(())
}

async fn build_authorizer(auto_accept: bool, allow_peer: &[String]) -> Result<SharedAuthorizer> {
    let store = TrustStore::load(default_trust_store_path()).await?;
    let policy = if !allow_peer.is_empty() {
        AuthPolicy::allow_peers(allow_peer.iter().cloned())
    } else if auto_accept {
        AuthPolicy::AutoAccept
    } else {
        AuthPolicy::DenyUnknown
    };
    Ok(PolicyAuthorizer::shared(policy, store))
}
