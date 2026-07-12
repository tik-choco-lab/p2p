use ratatui::prelude::*;
use ratatui::widgets::{Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table};

use super::app::{AddField, AddForm, App, Focus, PeerSelect, PendingRow, Popup};
use super::format::{
    endpoint, format_event, human_bytes, proto_name, role_label, short_id, state_name, trust_name,
};

pub(super) fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(6),
        Constraint::Length(8),
        Constraint::Length(1),
    ])
    .split(f.area());

    draw_header(f, chunks[0], app);

    let body = Layout::horizontal([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(chunks[1]);
    draw_forwards(f, body[0], app);
    draw_pending(f, body[1], app);

    draw_events(f, chunks[2], app);
    draw_footer(f, chunks[3], app);

    match &app.popup {
        Popup::Add(form) => draw_add_popup(f, form, app.message.as_deref()),
        Popup::SelectPeer(sel) => draw_peer_select_popup(f, sel),
        Popup::Trust(sel) => draw_trust_popup(f, app, *sel),
        Popup::None => {}
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let peers = if app.peers.is_empty() {
        "peers: 0 (waiting…)".to_string()
    } else {
        format!(
            "peers: {} [{}]",
            app.peers.len(),
            app.peers
                .iter()
                .map(|p| short_id(p))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let serve = app
        .forwards
        .iter()
        .filter(|s| matches!(s.spec.direction, crate::controller::Direction::Serve))
        .count();
    let conn = app.forwards.len() - serve;
    let text = format!(
        " p2p — room: {}   {}   serve: {} / conn: {}   pending: {} ",
        app.ctx.room,
        peers,
        serve,
        conn,
        app.pending_rows().len()
    );
    let p = Paragraph::new(text).style(Style::default().add_modifier(Modifier::BOLD));
    f.render_widget(p, area);
}

fn draw_forwards(f: &mut Frame, area: Rect, app: &App) {
    let header = Row::new(vec![
        "ROLE", "KEY", "PROTO", "ENDPOINT", "STATE", "CONNS", "IN", "OUT",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let mut rows: Vec<Row> = Vec::new();
    for (i, s) in app.forwards.iter().enumerate() {
        let selected = app.focus == Focus::Forwards && i == app.forwards_sel;
        let style = if selected {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default()
        };
        rows.push(
            Row::new(vec![
                Cell::from(role_label(s.spec.direction)),
                Cell::from(s.key.clone()),
                Cell::from(proto_name(s.spec.proto)),
                Cell::from(endpoint(&s.spec)),
                Cell::from(state_name(&s.state)),
                Cell::from(s.active_conns.to_string()),
                Cell::from(human_bytes(s.bytes_in)),
                Cell::from(human_bytes(s.bytes_out)),
            ])
            .style(style),
        );

        // Per-peer breakdown for the selected, expanded forward.
        if selected && app.expanded {
            if s.peers.is_empty() {
                rows.push(
                    Row::new(vec![Cell::from(""), Cell::from("  └ (no peers)")])
                        .style(Style::default().add_modifier(Modifier::DIM)),
                );
            }
            for peer in &s.peers {
                rows.push(
                    Row::new(vec![
                        Cell::from(""),
                        Cell::from(format!("  └ {}", short_id(&peer.peer_id))),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(""),
                        Cell::from(peer.active_conns.to_string()),
                        Cell::from(human_bytes(peer.bytes_in)),
                        Cell::from(human_bytes(peer.bytes_out)),
                    ])
                    .style(Style::default().add_modifier(Modifier::DIM)),
                );
            }
        }
    }

    let widths = [
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(5),
        Constraint::Min(14),
        Constraint::Length(9),
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Length(8),
    ];

    let border_style = focus_border(app.focus == Focus::Forwards);
    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(" Forwards "),
    );
    f.render_widget(table, area);
}

fn draw_pending(f: &mut Frame, area: Rect, app: &App) {
    let rows = app.pending_rows();
    let items: Vec<ListItem> = if rows.is_empty() {
        vec![ListItem::new("(none)")]
    } else {
        rows.iter()
            .enumerate()
            .map(|(i, row)| {
                let selected = app.focus == Focus::Pending && i == app.pending_sel;
                let text = match row {
                    PendingRow::Forward(r) => format!(
                        "[FWD] {} wants {} {} (you become the server)",
                        short_id(&r.peer_id),
                        r.proto.to_uppercase(),
                        r.remote_addr
                    ),
                    PendingRow::Conn(p) => format!(
                        "[CONN] {} → {} ({})",
                        short_id(&p.request.peer_id),
                        p.request.forward_key,
                        p.request.target_addr
                    ),
                };
                let style = if selected {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                ListItem::new(text).style(style)
            })
            .collect()
    };

    let border_style = focus_border(app.focus == Focus::Pending);
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(" Pending (y/Y allow, n/N deny) "),
    );
    f.render_widget(list, area);
}

fn draw_events(f: &mut Frame, area: Rect, app: &App) {
    let max = area.height.saturating_sub(2) as usize;
    let items: Vec<ListItem> = app
        .events
        .iter()
        .rev()
        .take(max)
        .map(|e| ListItem::new(format_event(e)))
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(" Audit log "));
    f.render_widget(list, area);
}

fn draw_footer(f: &mut Frame, area: Rect, app: &App) {
    let hint = if let Some(msg) = &app.message {
        format!(" {} ", msg)
    } else {
        " [a]dd forward  [d]elete  [Enter]expand  [Tab]focus  [t]rust  [y/n]approve  [w]eb ui  [q]uit "
            .to_string()
    };
    let p = Paragraph::new(hint).style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(p, area);
}

fn draw_add_popup(f: &mut Frame, form: &AddForm, message: Option<&str>) {
    let area = centered_rect(70, 45, f.area());
    f.render_widget(Clear, area);
    f.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(" Add forward "),
        area,
    );
    let inner = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Length(2),
        Constraint::Min(1),
    ])
    .margin(1)
    .split(area);

    let proto_label = if form.proto_tcp {
        "( TCP )  UDP "
    } else {
        " TCP  ( UDP )"
    };
    f.render_widget(
        field_line("Proto", proto_label, form.field == AddField::Proto),
        inner[0],
    );
    f.render_widget(
        field_line("Local ", &form.local, form.field == AddField::Local),
        inner[1],
    );
    f.render_widget(
        field_line("Remote", &form.remote, form.field == AddField::Remote),
        inner[2],
    );

    let help = Paragraph::new(
        "Local=自分のlisten ip:port  Remote=相手のip:port\nTab/↑↓=項目  ←→/Space=proto  Enter=要求送信  Esc=中止",
    )
    .style(Style::default().add_modifier(Modifier::DIM));
    f.render_widget(help, inner[3]);

    if let Some(msg) = message {
        let err = Paragraph::new(msg).style(Style::default().add_modifier(Modifier::BOLD));
        f.render_widget(err, inner[4]);
    }
}

fn field_line<'a>(label: &'a str, value: &'a str, focused: bool) -> Paragraph<'a> {
    let marker = if focused { "▶ " } else { "  " };
    let cursor = if focused { "_" } else { "" };
    let style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Paragraph::new(format!("{}{}: {}{}", marker, label, value, cursor)).style(style)
}

fn draw_peer_select_popup(f: &mut Frame, sel: &PeerSelect) {
    let area = centered_rect(60, 50, f.area());
    f.render_widget(Clear, area);
    let items: Vec<ListItem> = sel
        .peers
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let style = if i == sel.sel {
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default()
            };
            ListItem::new(short_id(p)).style(style)
        })
        .collect();
    let list = List::new(items).block(Block::default().borders(Borders::ALL).title(format!(
        " Send '{}' to ([Enter]選択 [Esc]中止) ",
        sel.draft.target
    )));
    f.render_widget(list, area);
}

fn draw_trust_popup(f: &mut Frame, app: &App, sel: usize) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);
    let items: Vec<ListItem> = if app.trust.is_empty() {
        vec![ListItem::new("(empty)")]
    } else {
        app.trust
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let text = format!(
                    "{} {} → {}",
                    trust_name(e.decision),
                    short_id(&e.key.peer_id),
                    e.key.forward_key
                );
                let style = if i == sel {
                    Style::default().add_modifier(Modifier::REVERSED)
                } else {
                    Style::default()
                };
                ListItem::new(text).style(style)
            })
            .collect()
    };
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Trust store ([x]remove  [Esc]close) "),
    );
    f.render_widget(list, area);
}

fn focus_border(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical[1])[1]
}
