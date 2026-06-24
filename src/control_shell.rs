use anyhow::{anyhow, Result};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

use crate::auth::{AuthAuditLog, TrustStore};
use crate::controller::{Direction, ForwardController, ForwardSpec, ForwardState, Proto};
use crate::forward_args::{forward_key, parse_connect_forward, parse_forward};

mod events;
mod trust;

enum ShellCommand {
    Add(ForwardSpec),
    Remove(String),
    List,
    Events,
    Trust(trust::Command),
    Help,
    Quit,
}

#[derive(Debug)]
pub(crate) struct ShellOutcome {
    pub(crate) output: String,
    pub(crate) should_quit: bool,
}

pub(crate) async fn run<R, W>(
    controller: ForwardController,
    trust_store: TrustStore,
    audit_log: AuthAuditLog,
    mut reader: R,
    mut writer: W,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    writer.write_all(help_text().as_bytes()).await?;
    writer.write_all(b"> ").await?;
    writer.flush().await?;

    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }

        let outcome =
            execute_line_with_context(&controller, Some(&trust_store), Some(&audit_log), &line)
                .await?;
        if !outcome.output.is_empty() {
            writer.write_all(outcome.output.as_bytes()).await?;
        }
        if outcome.should_quit {
            break;
        }
        writer.write_all(b"> ").await?;
        writer.flush().await?;
    }

    Ok(())
}

#[cfg(test)]
pub(crate) async fn execute_line(
    controller: &ForwardController,
    line: &str,
) -> Result<ShellOutcome> {
    execute_line_with_trust(controller, None, line).await
}

#[cfg(test)]
pub(crate) async fn execute_line_with_trust(
    controller: &ForwardController,
    trust_store: Option<&TrustStore>,
    line: &str,
) -> Result<ShellOutcome> {
    execute_line_with_context(controller, trust_store, None, line).await
}

pub(crate) async fn execute_line_with_context(
    controller: &ForwardController,
    trust_store: Option<&TrustStore>,
    audit_log: Option<&AuthAuditLog>,
    line: &str,
) -> Result<ShellOutcome> {
    let Some(command) = parse_command(line)? else {
        return Ok(ShellOutcome {
            output: String::new(),
            should_quit: false,
        });
    };

    match command {
        ShellCommand::Add(spec) => {
            let key = controller.add_forward(spec).await?;
            Ok(ShellOutcome {
                output: format!("added {}\n", key),
                should_quit: false,
            })
        }
        ShellCommand::Remove(key) => {
            controller.remove_forward(&key).await?;
            Ok(ShellOutcome {
                output: format!("removed {}\n", key),
                should_quit: false,
            })
        }
        ShellCommand::List => Ok(ShellOutcome {
            output: format_statuses(controller.list_forwards().await),
            should_quit: false,
        }),
        ShellCommand::Events => Ok(ShellOutcome {
            output: events::format(audit_log).await?,
            should_quit: false,
        }),
        ShellCommand::Trust(command) => Ok(ShellOutcome {
            output: trust::execute(trust_store, command).await?,
            should_quit: false,
        }),
        ShellCommand::Help => Ok(ShellOutcome {
            output: help_text(),
            should_quit: false,
        }),
        ShellCommand::Quit => Ok(ShellOutcome {
            output: "bye\n".to_string(),
            should_quit: true,
        }),
    }
}

fn parse_command(line: &str) -> Result<Option<ShellCommand>> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    let Some(command) = parts.first().copied() else {
        return Ok(None);
    };

    match command {
        "add" if parts.len() == 3 => parse_add(parts[1], parts[2]).map(Some),
        "remove" | "rm" if parts.len() == 2 => Ok(Some(ShellCommand::Remove(parts[1].into()))),
        "list" | "ls" if parts.len() == 1 => Ok(Some(ShellCommand::List)),
        "events" | "ev" if parts.len() == 1 => Ok(Some(ShellCommand::Events)),
        "trust" | "t" => trust::parse(&parts).map(ShellCommand::Trust).map(Some),
        "help" | "h" if parts.len() == 1 => Ok(Some(ShellCommand::Help)),
        "quit" | "q" | "exit" if parts.len() == 1 => Ok(Some(ShellCommand::Quit)),
        _ => Err(anyhow!("unknown command; type `help` for usage")),
    }
}

fn parse_add(direction: &str, forward: &str) -> Result<ShellCommand> {
    match direction {
        "serve" => {
            let (proto, addr, _) = parse_forward(forward);
            Ok(ShellCommand::Add(ForwardSpec {
                direction: Direction::Serve,
                proto: Proto::from_name(proto)?,
                addr: addr.to_string(),
                listen_port: -1,
                target: forward_key(proto, addr),
            }))
        }
        "connect" => {
            let (proto, listen_port, target) = parse_connect_forward(forward);
            Ok(ShellCommand::Add(ForwardSpec {
                direction: Direction::Connect,
                proto: Proto::from_name(proto)?,
                addr: String::new(),
                listen_port,
                target,
            }))
        }
        _ => Err(anyhow!("direction must be `serve` or `connect`")),
    }
}

fn format_statuses(statuses: Vec<crate::controller::ForwardStatus>) -> String {
    if statuses.is_empty() {
        return "no forwards\n".to_string();
    }

    let mut out = String::from("key direction proto endpoint state conns in out\n");
    for status in statuses {
        out.push_str(&format!(
            "{} {} {} {} {} {} {} {}\n",
            status.key,
            direction_name(status.spec.direction),
            status.spec.proto.as_str(),
            endpoint(&status.spec),
            state_name(&status.state),
            status.active_conns,
            status.bytes_in,
            status.bytes_out
        ));
    }
    out
}

fn endpoint(spec: &ForwardSpec) -> String {
    match spec.direction {
        Direction::Serve => spec.addr.clone(),
        Direction::Connect => format!(":{}", spec.listen_port),
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Serve => "serve",
        Direction::Connect => "connect",
    }
}

fn state_name(state: &ForwardState) -> &str {
    match state {
        ForwardState::Listening => "listening",
        ForwardState::Error(_) => "error",
        ForwardState::Stopped => "stopped",
    }
}

fn help_text() -> String {
    [
        "commands:",
        "  add serve <[proto://]<addr>>",
        "  add connect <[proto://]<listen-port>[:remote-port]>",
        "  remove <target>",
        "  list",
        "  events",
        "  trust list",
        "  trust allow <peer-id> <target>",
        "  trust deny <peer-id> <target>",
        "  trust remove <peer-id> <target>",
        "  quit",
        "",
    ]
    .join("\n")
}

#[cfg(test)]
mod tests;
