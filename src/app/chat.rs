use anyhow::Result;
use tokio::io::AsyncBufReadExt;

use crate::rtc::RTCManager;

use super::generate_room_id;

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
