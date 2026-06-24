use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use ratatui::crossterm::event::{self, Event, KeyEventKind};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::prelude::*;

use crate::auth::{AuthAuditLog, PendingAuthorizations, TrustStore};
use crate::controller::ForwardController;

mod app;
mod format;
mod ui;

use app::App;

const TICK: Duration = Duration::from_millis(120);
const EVENT_POLL: Duration = Duration::from_millis(100);

pub(crate) struct TuiContext {
    pub(crate) room: String,
    pub(crate) controller: ForwardController,
    pub(crate) trust_store: TrustStore,
    pub(crate) audit_log: AuthAuditLog,
    pub(crate) pending_auth: PendingAuthorizations,
}

pub(crate) async fn run(ctx: TuiContext) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, ctx).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_loop<B: Backend>(terminal: &mut Terminal<B>, ctx: TuiContext) -> Result<()> {
    let running = Arc::new(AtomicBool::new(true));
    let (tx, rx) = mpsc::channel::<Event>();
    let reader_flag = running.clone();
    std::thread::spawn(move || {
        while reader_flag.load(Ordering::Relaxed) {
            if event::poll(EVENT_POLL).unwrap_or(false) {
                match event::read() {
                    Ok(ev) => {
                        if tx.send(ev).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });

    let mut app = App::new(ctx);
    app.refresh().await;

    loop {
        terminal.draw(|f| ui::draw(f, &app))?;

        while let Ok(ev) = rx.try_recv() {
            if let Event::Key(key) = ev {
                if key.kind == KeyEventKind::Press {
                    app.handle_key(key).await;
                }
            }
        }

        if app.should_quit {
            break;
        }

        tokio::time::sleep(TICK).await;
        app.refresh().await;
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}
