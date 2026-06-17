use std::io;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::time::{sleep, timeout, Duration};

const E2E_TIMEOUT: Duration = Duration::from_secs(60);

struct P2pProcess {
    child: Child,
    stdin: Option<ChildStdin>,
}

impl P2pProcess {
    async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_p2p")
}

fn unique_id(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{prefix}-{now}")
}

fn spawn_p2p(args: &[&str]) -> io::Result<(P2pProcess, BufReader<impl tokio::io::AsyncRead>)> {
    let mut child = Command::new(binary())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "stdin pipe was not created"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "stdout pipe was not created"))?;

    Ok((
        P2pProcess {
            child,
            stdin: Some(stdin),
        },
        BufReader::new(stdout),
    ))
}

async fn read_until<R, F>(reader: &mut R, mut matches: F, label: &str) -> io::Result<String>
where
    R: AsyncBufRead + Unpin,
    F: FnMut(&str) -> bool,
{
    timeout(E2E_TIMEOUT, async {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("process ended before {label}"),
                ));
            }
            if matches(line.trim_end()) {
                return Ok(line.trim_end().to_string());
            }
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("timed out waiting for {label}"),
        )
    })?
}

async fn read_room_id(
    stderr: &mut BufReader<impl tokio::io::AsyncRead + Unpin>,
) -> io::Result<String> {
    let line = read_until(
        stderr,
        |line| line.starts_with("Room ID: "),
        "generated room id",
    )
    .await?;
    Ok(line.trim_start_matches("Room ID: ").to_string())
}

#[tokio::test]
#[ignore = "requires working mistlib default Nostr relay access"]
async fn chat_message_crosses_mistlib_nostr_signaling() -> io::Result<()> {
    if std::env::var("P2P_NOSTR_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set P2P_NOSTR_E2E=1 to run the live Nostr signaling test");
        return Ok(());
    }

    let (mut alice, mut alice_stdout) = spawn_p2p(&[])?;
    let mut alice_stderr =
        BufReader::new(
            alice.child.stderr.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "stderr pipe was not created")
            })?,
        );
    let room_id = read_room_id(&mut alice_stderr).await?;

    let (mut bob, mut bob_stdout) = spawn_p2p(&[&room_id])?;
    let message = unique_id("nostr-e2e-message");

    let mut alice_stdin = alice
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "alice stdin was already taken"))?;
    let send_message = message.clone();
    let writer = tokio::spawn(async move {
        for _ in 0..30 {
            alice_stdin.write_all(send_message.as_bytes()).await?;
            alice_stdin.write_all(b"\n").await?;
            alice_stdin.flush().await?;
            sleep(Duration::from_secs(1)).await;
        }
        Ok::<_, io::Error>(())
    });

    let received = read_until(
        &mut bob_stdout,
        |line| line.contains(&message),
        "chat message on peer stdout",
    )
    .await;

    alice.kill().await;
    bob.kill().await;
    writer.abort();

    let received = received?;
    assert!(
        received.contains(&message),
        "expected forwarded chat message, got: {received}"
    );

    // Keep alice_stdout alive until after process cleanup so stdout is drained by the OS pipe owner.
    let _ = &mut alice_stdout;
    Ok(())
}
