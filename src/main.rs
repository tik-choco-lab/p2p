mod proxy;
mod rtc;
mod stdio;
mod tcp;
mod udp;

use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::io::AsyncBufReadExt;
use tracing::error;
use tracing_subscriber::EnvFilter;

use rtc::RTCManager;

#[derive(Parser)]
#[command(name = "p2p", about = "WebRTC P2P Tunnel CLI")]
#[command(
    long_about = "A P2P tunnel application using WebRTC.\nSupports TCP/UDP forwarding and standard I/O bridging.\n\nIf no command is specified, p2p starts in Chat Mode."
)]
struct Cli {
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Option<Commands>,

    room_id: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Connect {
        room_id: String,

        forwards: Vec<String>,
    },

    Serve {
        args: Vec<String>,

        #[arg(last = true)]
        command: Vec<String>,
    },

    Chat {
        room_id: Option<String>,
    },
}

fn generate_room_id() -> String {
    let mut buf = [0u8; 4];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut buf);
    hex::encode(buf)
}

fn parse_forward(f: &str) -> (&str, &str, i32) {
    let (proto, addr) = if let Some(rest) = f.strip_prefix("tcp://") {
        ("tcp", rest)
    } else if let Some(rest) = f.strip_prefix("udp://") {
        ("udp", rest)
    } else {
        ("tcp", f)
    };

    let port = if let Ok(p) = addr.parse::<i32>() {
        p
    } else if let Some(port_str) = addr.rsplit(':').next() {
        port_str.parse::<i32>().unwrap_or(-1)
    } else {
        -1
    };

    (proto, addr, port)
}

fn parse_connect_forward(f: &str) -> (&str, i32, String) {
    let (proto, addr, fallback_port) = parse_forward(f);
    let addr = addr.trim_start_matches(':');

    let (listen_port, remote_port) =
        if let Some((listen, remote)) = addr.split_once(':') {
            let listen_port = listen.parse::<i32>().unwrap_or(fallback_port);
            let remote_port = remote.parse::<i32>().unwrap_or(listen_port);
            (listen_port, remote_port)
        } else {
            (fallback_port, fallback_port)
        };

    (proto, listen_port, format!("{}:{}", proto, remote_port))
}

fn split_serve_args(args: &[String]) -> (Option<String>, Vec<String>) {
    let Some(first) = args.first() else {
        return (None, Vec::new());
    };

    let is_forward =
        first.contains(':') || first.starts_with("tcp://") || first.starts_with("udp://");
    if is_forward {
        (None, args.to_vec())
    } else {
        (
            Some(first.clone()),
            args.iter().skip(1).cloned().collect::<Vec<_>>(),
        )
    }
}

fn init_tracing(verbose: u8) {
    let filter = match verbose {
        0 => "error",
        1 => "info",
        _ => "debug",
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match cli.command {
        Some(Commands::Connect { room_id, forwards }) => run_connect(&room_id, &forwards).await,
        Some(Commands::Serve { args, command }) => run_serve(&args, &command).await,
        Some(Commands::Chat { room_id }) => run_chat(room_id.as_deref()).await,
        None => run_chat(cli.room_id.as_deref()).await,
    }
}

async fn run_chat(room_id: Option<&str>) -> Result<()> {
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

async fn run_connect(room_id: &str, forwards: &[String]) -> Result<()> {
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

    for f in forwards {
        let (proto, listen_port, target) = parse_connect_forward(f);
        let mgr = manager.clone();
        if proto == "tcp" {
            tokio::spawn(async move {
                if let Err(e) =
                    tcp::TcpManager::listen_and_serve_with_target(
                        mgr,
                        listen_port,
                        String::new(),
                        target,
                    )
                    .await
                {
                    error!("TCP error: {}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) =
                    udp::UdpManager::listen_and_serve_with_target(
                        mgr,
                        listen_port,
                        String::new(),
                        target,
                    )
                    .await
                {
                    error!("UDP error: {}", e);
                }
            });
        }
    }

    let br = bridge.clone();
    tokio::spawn(async move {
        br.run().await;
    });

    shutdown_rx.recv().await;
    manager.close().await;
    Ok(())
}

async fn run_serve(args: &[String], command: &[String]) -> Result<()> {
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

    for f in &forwards {
        let (proto, addr, _) = parse_forward(f);
        let target = forward_key(proto, addr);
        let mgr = manager.clone();
        let addr = addr.to_string();
        if proto == "tcp" {
            tokio::spawn(async move {
                if let Err(e) =
                    tcp::TcpManager::listen_and_serve_with_target(mgr, -1, addr, target).await
                {
                    error!("TCP error: {}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) =
                    udp::UdpManager::listen_and_serve_with_target(mgr, -1, addr, target).await
                {
                    error!("UDP error: {}", e);
                }
            });
        }
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

fn forward_key(proto: &str, addr: &str) -> String {
    let port = addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(-1);
    format!("{}:{}", proto, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn connect_forward_defaults_remote_port_to_listen_port() {
        assert_eq!(
            parse_connect_forward(":8080"),
            ("tcp", 8080, "tcp:8080".to_string())
        );
        assert_eq!(
            parse_connect_forward("udp://9000"),
            ("udp", 9000, "udp:9000".to_string())
        );
    }

    #[test]
    fn connect_forward_maps_listen_port_to_remote_target() {
        assert_eq!(
            parse_connect_forward("15432:5432"),
            ("tcp", 15432, "tcp:5432".to_string())
        );
        assert_eq!(
            parse_connect_forward("udp://19000:9000"),
            ("udp", 19000, "udp:9000".to_string())
        );
    }

    #[test]
    fn serve_args_keep_all_forwards_when_room_is_present() {
        let (room, forwards) =
            split_serve_args(&strings(&["my-room", ":80", "tcp://127.0.0.1:5432"]));

        assert_eq!(room, Some("my-room".to_string()));
        assert_eq!(forwards, strings(&[":80", "tcp://127.0.0.1:5432"]));
    }

    #[test]
    fn serve_args_treat_leading_forward_as_generated_room_mode() {
        let (room, forwards) = split_serve_args(&strings(&[":80", "udp://127.0.0.1:9000"]));

        assert_eq!(room, None);
        assert_eq!(forwards, strings(&[":80", "udp://127.0.0.1:9000"]));
    }

    #[test]
    fn forward_key_uses_protocol_and_port() {
        assert_eq!(forward_key("tcp", "127.0.0.1:80"), "tcp:80");
        assert_eq!(forward_key("udp", ":9000"), "udp:9000");
    }
}
