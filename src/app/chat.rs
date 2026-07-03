use std::io::{self, Write};

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::terminal;

use crate::rtc::RTCManager;

use super::{generate_room_id, load_or_create_node_id};

pub(crate) async fn run_chat(room_id: Option<&str>) -> Result<()> {
    let room = match room_id {
        Some(r) => r.to_string(),
        None => {
            let id = generate_room_id();
            eprintln!("Room ID: {}", id);
            id
        }
    };

    let self_id = load_or_create_node_id().await?;
    let manager = RTCManager::new(self_id.clone(), room, false).await;

    println!("=== Chat Mode ===");
    println!("Type a message and press Enter to send.");
    println!("Ctrl+T or /id: toggle peer ID display. Ctrl+C: quit.");

    let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    manager
        .on_chat_message(move |peer_id, msg| {
            let _ = msg_tx.send((peer_id, msg));
        })
        .await;

    let (key_tx, key_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    std::thread::spawn(move || {
        while let Ok(ev) = event::read() {
            if key_tx.send(ev).is_err() {
                break;
            }
        }
    });

    terminal::enable_raw_mode()?;
    let result = chat_loop(&manager, msg_rx, key_rx).await;
    terminal::disable_raw_mode()?;
    println!();

    manager.close().await;
    result
}

async fn chat_loop(
    manager: &RTCManager,
    mut msg_rx: tokio::sync::mpsc::UnboundedReceiver<(String, String)>,
    mut key_rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
) -> Result<()> {
    let mut show_ids = true;
    let mut input = String::new();
    redraw_input(&input)?;

    loop {
        tokio::select! {
            Some((peer_id, msg)) = msg_rx.recv() => {
                let line = if show_ids {
                    let short_id = &peer_id[..peer_id.len().min(8)];
                    format!("[{}] {}", short_id, msg)
                } else {
                    msg
                };
                print_line(&line, &input)?;
            }
            Some(ev) = key_rx.recv() => {
                let Event::Key(key) = ev else { continue };
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    match key.code {
                        KeyCode::Char('c') => return Ok(()),
                        KeyCode::Char('t') => {
                            show_ids = toggle_ids(show_ids, &input)?;
                        }
                        _ => {}
                    }
                    continue;
                }
                match key.code {
                    KeyCode::Enter => {
                        let msg = input.trim_end().to_string();
                        input.clear();
                        if msg == "/id" {
                            show_ids = toggle_ids(show_ids, &input)?;
                        } else if !msg.is_empty() {
                            print_line(&format!("> {}", msg), &input)?;
                            manager.send_chat_to_all(&msg).await;
                        } else {
                            redraw_input(&input)?;
                        }
                    }
                    KeyCode::Backspace => {
                        input.pop();
                        redraw_input(&input)?;
                    }
                    KeyCode::Char(c) => {
                        input.push(c);
                        redraw_input(&input)?;
                    }
                    _ => {}
                }
            }
            else => return Ok(()),
        }
    }
}

fn toggle_ids(show_ids: bool, input: &str) -> Result<bool> {
    let now = !show_ids;
    print_line(
        &format!("(peer ID display: {})", if now { "on" } else { "off" }),
        input,
    )?;
    Ok(now)
}

/// Print a finished line above the input line, then restore the input prompt.
fn print_line(line: &str, input: &str) -> Result<()> {
    let mut out = io::stdout();
    // \r + clear-line erases the current input prompt before printing.
    write!(out, "\r\x1b[K{}\r\n", line)?;
    write!(out, "> {}", input)?;
    out.flush()?;
    Ok(())
}

fn redraw_input(input: &str) -> Result<()> {
    let mut out = io::stdout();
    write!(out, "\r\x1b[K> {}", input)?;
    out.flush()?;
    Ok(())
}
