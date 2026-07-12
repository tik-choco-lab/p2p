mod app;
mod auth;
mod control_shell;
mod controller;
mod forward_args;
mod forward_runtime;
mod forward_store;
mod negotiation;
mod proxy;
mod rtc;
mod stdio;
mod tcp;
mod tui;
mod udp;
mod web;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "p2p", about = "WebRTC P2P Tunnel CLI")]
#[command(
    long_about = "A P2P tunnel application using WebRTC.\nSupports TCP/UDP forwarding and standard I/O bridging.\n\nIf no command is specified, p2p starts the interactive TUI."
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
        #[arg(long)]
        auto_accept: bool,

        #[arg(long = "allow-peer", value_delimiter = ',')]
        allow_peer: Vec<String>,

        args: Vec<String>,

        #[arg(last = true)]
        command: Vec<String>,
    },

    Chat {
        room_id: Option<String>,
    },

    /// Runs the same P2P session as the TUI but exposes an HTTP + WebSocket
    /// control API (and the embedded frontend) on 127.0.0.1 instead of a
    /// terminal UI.
    Web {
        room_id: Option<String>,

        #[arg(long, default_value_t = 8787)]
        port: u16,

        /// Best-effort: opens the UI in the default browser once the server
        /// is listening. Failures (e.g. no browser available) are logged
        /// and otherwise ignored.
        #[arg(long)]
        open: bool,
    },
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
        Some(Commands::Connect { room_id, forwards }) => {
            app::run_connect(&room_id, &forwards).await
        }
        Some(Commands::Serve {
            auto_accept,
            allow_peer,
            args,
            command,
        }) => app::run_serve(&args, &command, auto_accept, &allow_peer).await,
        Some(Commands::Chat { room_id }) => app::run_chat(room_id.as_deref()).await,
        Some(Commands::Web {
            room_id,
            port,
            open,
        }) => web::run(room_id.as_deref(), port, open).await,
        None => app::run_tui(cli.room_id.as_deref()).await,
    }
}
