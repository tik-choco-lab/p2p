use std::sync::Arc;

use anyhow::Result;

use crate::controller::{Direction, ForwardController, ForwardSpec, Proto};
use crate::forward_args::parse_connect_forward;
use crate::rtc::RTCManager;
use crate::stdio;

use super::load_or_create_node_id;

pub(crate) async fn run_connect(room_id: &str, forwards: &[String]) -> Result<()> {
    let self_id = load_or_create_node_id().await?;
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
