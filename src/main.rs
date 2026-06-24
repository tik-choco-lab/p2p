mod app;
mod auth;
mod control_shell;
mod controller;
mod forward_args;
mod forward_runtime;
mod proxy;
mod rtc;
mod stdio;
mod tcp;
mod udp;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

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
        None => app::run_control_shell(cli.room_id.as_deref()).await,
    }
}
