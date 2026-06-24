use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncBufReadExt;
use tracing::error;

use crate::auth::{
    default_trust_store_path, AuthAuditLog, AuthPolicy, PolicyAuthorizer, SharedAuthorizer,
    TrustStore,
};
use crate::controller::{Direction, ForwardController, ForwardSpec, Proto};
use crate::forward_args::{forward_key, parse_connect_forward, parse_forward, split_serve_args};
use crate::rtc::RTCManager;
use crate::{control_shell, proxy, stdio};

fn generate_room_id() -> String {
    let mut buf = [0u8; 4];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    hex::encode(buf)
}

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
    let controller = ForwardController::with_authorizer(
        manager.clone(),
        PolicyAuthorizer::shared_with_audit_log(
            AuthPolicy::DenyUnknown,
            trust_store.clone(),
            audit_log.clone(),
        ),
    );

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    tokio::select! {
        result = control_shell::run(controller, trust_store, audit_log, stdin, stdout) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {}
    }

    manager.close().await;
    Ok(())
}

pub(crate) async fn run_chat(room_id: Option<&str>) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = uuid::Uuid::new_v4().to_string();
    let manager = RTCManager::new(self_id.clone(), room, false).await;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mgr = manager.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        mgr.close().await;
        let _ = shutdown_tx.send(()).await;
    });

    println!("=== Chat Mode ===");
    println!("Type a message and press Enter to send.");

    manager
        .on_chat_message(|peer_id, msg| {
            let short_id = &peer_id[..peer_id.len().min(8)];
            println!("[{}] {}", short_id, msg);
        })
        .await;

    let mgr = manager.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = tokio::io::BufReader::new(stdin);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    let msg = line.trim_end();
                    if !msg.is_empty() {
                        mgr.send_chat_to_all(msg).await;
                    }
                }
                Err(_) => break,
            }
        }
    });

    shutdown_rx.recv().await;
    Ok(())
}

pub(crate) async fn run_connect(room_id: &str, forwards: &[String]) -> Result<()> {
    let self_id = uuid::Uuid::new_v4().to_string();
    let manager = RTCManager::new(self_id, room_id.to_string(), false).await;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let bridge = Arc::new(stdio::Bridge::new(manager.clone()));

    let br = bridge.clone();
    let mgr = manager.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        br.close();
        mgr.close().await;
        let _ = shutdown_tx.send(()).await;
    });

    let controller = ForwardController::new(manager.clone());
    for f in forwards {
        let (proto, listen_port, target) = parse_connect_forward(f);
        controller
            .add_forward(ForwardSpec {
                direction: Direction::Connect,
                proto: Proto::from_name(proto)?,
                addr: String::new(),
                listen_port,
                target,
            })
            .await?;
    }

    let br = bridge.clone();
    tokio::spawn(async move {
        br.run().await;
    });

    shutdown_rx.recv().await;
    manager.close().await;
    Ok(())
}

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
