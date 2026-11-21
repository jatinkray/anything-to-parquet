//! ratatui front-end. All decisions live in `atp_core::tui::App`; this module
//! only renders it and moves events between the terminal and the engine.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use atp_core::engine::{self, Event, Preview, RunOptions, Summary};
use atp_core::sink::SinkOptions;
use atp_core::tui::{Action, App, Key, Screen};
use atp_core::{Error, Result};
use crossterm::event::{self, Event as CEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Gauge, Paragraph, Row, Table, Wrap};
use ratatui::Frame;

use crate::cli::{human_bytes, EsArgs, SourceArgs};

enum Msg {
    Preview(Result<Preview>),
    Engine(Event),
    Done(Result<Summary>),
}

pub async fn run(initial: Option<String>) -> Result<()> {
    if !std::io::stdout().is_terminal() {
        return Err(Error::msg(
            "atp tui needs an interactive terminal; use `atp convert` for scripts and CI",
        ));
    }
    let mut app = App::default();
    if let Some(source) = initial {
        app.input = source;
    }
    let mut terminal = ratatui::init();
    let outcome = event_loop(&mut terminal, &mut app).await;
    ratatui::restore();
    outcome
}

async fn event_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut App) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Msg>();
    let mut cancel: Option<Arc<AtomicBool>> = None;
    loop {
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Msg::Preview(Ok(preview)) => app.set_preview(preview),
                Msg::Preview(Err(err)) => app.fail(err.to_string()),
                Msg::Engine(Event::Opened { info }) => {
                    app.note(format!("reading {} from {}", info.kind, info.location));
                    if let Some(rows) = info.rows_estimate {
                        app.note(format!("source reports ~{rows} rows"));
                    }
                }
                Msg::Engine(Event::SchemaReady { schema }) => {
                    app.note(format!(
                        "schema {} ({} columns)",
                        schema.hash(),
                        schema.fields.len()
                    ));
                }
                Msg::Engine(Event::SchemaWidened { schema, reason }) => {
                    app.note(format!("schema widened to {} ({reason})", schema.hash()));
                }
                Msg::Engine(Event::Progress {
                    rows, bytes, files, ..
                }) => app.progress(rows, bytes, files),
                Msg::Engine(Event::Finished { .. }) => {}
                Msg::Done(Ok(summary)) => {
                    cancel = None;
                    app.finish(summary);
                }
                Msg::Done(Err(err)) => {
                    cancel = None;
                    app.fail(err.to_string());
                }
            }
        }
        terminal.draw(|frame| draw(frame, app))?;
        if !event::poll(Duration::from_millis(80))? {
            continue;
        }
        let CEvent::Key(key) = event::read()? else {
            continue;
        };
        let Some(key) = map_key(key) else { continue };
        match app.on_key(key) {
            Action::Quit => return Ok(()),
            Action::Cancel => match &cancel {
                Some(flag) => {
                    flag.store(true, Ordering::Relaxed);
                    app.note("cancelling after the current batch");
                }
                None => app.note("nothing is running"),
            },
            Action::Inspect => {
                let args = source_args(app);
                let tx = tx.clone();
                app.status = "reading schema and sample rows…".to_string();
                tokio::spawn(async move {
                    let result = match build_source_blocking(args).await {
                        Ok(source) => engine::preview(source, 50).await,
                        Err(err) => Err(err),
                    };
                    let _ = tx.send(Msg::Preview(result));
                });
            }
            Action::Run => {
                let Some(job) = app.job() else {
                    app.fail("no source selected");
                    continue;
                };
                let out = job.out.clone();
                let args = source_args(app);
                {
                    {
                        let opts = RunOptions {
                            out: out.clone(),
                            partition: job.partition,
                            strict: job.strict,
                            batch_rows: 8192,
                            sink: SinkOptions {
                                compression: job.compression,
                                row_group_rows: job.row_group_rows,
                                ..SinkOptions::default()
                            },
                            max_rows: None,
                        };
                        let flag = Arc::new(AtomicBool::new(false));
                        cancel = Some(flag.clone());
                        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(64);
                        let ui_tx = tx.clone();
                        tokio::spawn(async move {
                            let result = match build_source_blocking(args).await {
                                Ok(source)
                                    if source.timestamp_column().is_none()
                                        && !matches!(
                                            opts.partition,
                                            atp_core::sink::Partition::None
                                        ) =>
                                {
                                    Err(atp_core::Error::msg(
                                        "time partitioning needs a timestamp column",
                                    ))
                                }
                                Ok(source) => engine::run(source, opts, Some(event_tx), flag).await,
                                Err(err) => Err(err),
                            };
                            let _ = ui_tx.send(Msg::Done(result));
                        });
                        let forward = tx.clone();
                        tokio::spawn(async move {
                            while let Some(ev) = event_rx.recv().await {
                                let _ = forward.send(Msg::Engine(ev));
                            }
                        });
                    }
                }
            }
            Action::None => {}
        }
    }
}

/// Building a source can parse an index or stage a remote prefix; keep it off
/// the render thread so the TUI never freezes on I/O.
async fn build_source_blocking(args: SourceArgs) -> Result<Box<dyn atp_core::source::Source>> {
    tokio::task::spawn_blocking(move || crate::cli::build_source(&args))
        .await
        .map_err(|e| Error::msg(format!("source task failed: {e}")))?
}

fn source_args(app: &App) -> SourceArgs {
    SourceArgs {
        source: app.input.clone(),
        format: None,
        timestamp: app.preview.as_ref().and_then(|p| {
            p.schema
                .fields
                .iter()
                .find(|f| f.ty == atp_core::Ty::TimestampMillis)
                .map(|f| f.name.clone())
        }),
        delimiter: None,
        sample_rows: 10_000,
        es: EsArgs::default(),
        tsdb: crate::cli::TsdbArgs::default(),
    }
}

fn map_key(key: KeyEvent) -> Option<Key> {
    if key.kind != KeyEventKind::Press {
        return None;
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Some(Key::CtrlC);
    }
    Some(match key.code {
        KeyCode::Up => Key::Up,
        KeyCode::Down => Key::Down,
        KeyCode::Left => Key::Left,
        KeyCode::Right => Key::Right,
        KeyCode::Enter => Key::Enter,
        KeyCode::Esc => Key::Esc,
        KeyCode::Tab => Key::Tab,
        KeyCode::Backspace => Key::Backspace,
        KeyCode::Char(c) => Key::Char(c),
        _ => return None,
    })
}

fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(2),
    ])
    .split(area);
    draw_header(frame, chunks[0], app);
    match app.screen {
        Screen::Source => draw_source(frame, chunks[1], app),
        Screen::Preview => draw_preview(frame, chunks[1], app),
        Screen::Options => draw_options(frame, chunks[1], app),
        Screen::Running => draw_running(frame, chunks[1], app),
        Screen::Done => draw_done(frame, chunks[1], app),
        Screen::Failed => draw_failed(frame, chunks[1], app),
    }
    draw_footer(frame, chunks[2], app);
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let step = match app.screen {
        Screen::Source => "1/4 source",
        Screen::Preview => "2/4 schema",
        Screen::Options => "3/4 options",
        Screen::Running => "4/4 convert",
        Screen::Done => "done",
        Screen::Failed => "failed",
    };
    let title = Line::from(vec![
        Span::styled(" atp ", Style::default().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" anything-to-parquet "),
        Span::styled(step, Style::default().fg(Color::DarkGray)),
    ]);
    let block = Block::bordered().title(title);
    let text = Paragraph::new(app.status.clone())
        .block(block)
        .wrap(Wrap { trim: true });
    frame.render_widget(text, area);
}

fn draw_source(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::bordered().title(" source ");
    let mut lines = vec![
        Line::from(vec![
            Span::styled("> ", Style::default().fg(Color::Cyan)),
            Span::raw(app.input.clone()),
            Span::styled("█", Style::default().fg(Color::Cyan)),
        ]),
        Line::raw(""),
        Line::styled("examples", Style::default().add_modifier(Modifier::BOLD)),
        Line::raw("  /data/events.jsonl"),
        Line::raw("  /data/exports/*.csv  (one file)"),
        Line::raw("  https://es.example:9200/logs-2026.11.*"),
        Line::raw(""),
        Line::styled(
            "formats: csv/tsv, jsonl/ndjson/.json (optionally .gz), Elasticsearch scroll",
            Style::default().fg(Color::DarkGray),
        ),
    ];
    if let Some(kind) = source_kind_hint(&app.input) {
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            format!("detected: {}", kind),
            Style::default().fg(Color::Green),
        ));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn source_kind_hint(input: &str) -> Option<&'static str> {
    if input.starts_with("http://") || input.starts_with("https://") {
        return Some("elasticsearch");
    }
    if ["s3://", "gs://", "az://"]
        .iter()
        .any(|scheme| input.starts_with(scheme))
    {
        return Some("tsdb");
    }
    let path = std::path::Path::new(input);
    if path.is_dir()
        && (path.join("meta.json").is_file()
            || std::fs::read_dir(path).is_ok_and(|entries| {
                entries.flatten().any(|entry| {
                    let child = entry.path();
                    child.is_dir() && child.join("meta.json").is_file()
                })
            }))
    {
        return Some("tsdb");
    }
    let lower = input.to_ascii_lowercase();
    if lower.ends_with(".csv") || lower.ends_with(".tsv") || lower.ends_with(".csv.gz") {
        Some("csv")
    } else if lower.ends_with(".jsonl")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".json")
        || lower.ends_with(".jsonl.gz")
        || lower.ends_with(".ndjson.gz")
    {
        Some("jsonl")
    } else {
        None
    }
}

fn draw_preview(frame: &mut Frame, area: Rect, app: &App) {
    let Some(preview) = &app.preview else {
        frame.render_widget(Paragraph::new("no preview yet"), area);
        return;
    };
    let rows: Vec<Row> = preview
        .schema
        .fields
        .iter()
        .map(|f| Row::new(vec![Cell::from(f.name.clone()), Cell::from(f.ty.as_str())]))
        .collect();
    let table = Table::new(
        rows,
        [Constraint::Percentage(60), Constraint::Percentage(40)],
    )
    .header(Row::new(vec!["column", "type"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(Block::bordered().title(format!(
        " schema · {} columns · {} rows sampled ",
        preview.schema.fields.len(),
        preview.rows.len()
    )));
    frame.render_widget(table, area);
}

fn draw_options(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::bordered().title(" options ");
    let mut lines = Vec::new();
    for i in 0..atp_core::tui::FormOptions::FIELDS {
        let selected = i == app.options.focus;
        let marker = if selected { "› " } else { "  " };
        let style = if selected {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        lines.push(Line::styled(
            format!("{marker}{}", app.options.label(i)),
            style,
        ));
    }
    lines.push(Line::raw(""));
    lines.push(Line::styled(
        "↑/↓ move · ←/→ or space change · Enter convert · Esc back",
        Style::default().fg(Color::DarkGray),
    ));
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn draw_running(frame: &mut Frame, area: Rect, app: &App) {
    let parts = Layout::vertical([Constraint::Length(3), Constraint::Min(3)]).split(area);
    let total = app
        .preview
        .as_ref()
        .and_then(|p| p.info.rows_estimate)
        .filter(|t| *t > 0);
    let ratio = match total {
        Some(t) => (app.progress_rows as f64 / t as f64).clamp(0.0, 1.0),
        None => spinner_ratio(),
    };
    let label = if let Some(t) = total {
        format!(
            "{} / {} rows · {} · {} file(s)",
            app.progress_rows,
            t,
            human_bytes(app.progress_bytes),
            app.progress_files
        )
    } else {
        format!(
            "{} rows · {} · {} file(s)",
            app.progress_rows,
            human_bytes(app.progress_bytes),
            app.progress_files
        )
    };
    let gauge = Gauge::default()
        .block(Block::bordered().title(" converting "))
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(ratio)
        .label(label);
    frame.render_widget(gauge, parts[0]);

    let start = app.log.len().saturating_sub(8);
    let lines: Vec<Line> = app.log[start..]
        .iter()
        .map(|l| Line::raw(l.clone()))
        .collect();
    let block = Block::bordered().title(" log · q cancels ");
    frame.render_widget(Paragraph::new(lines).block(block), parts[1]);
}

fn spinner_ratio() -> f64 {
    let millis = Instant::now().elapsed().as_millis();
    // A quiet pulse so the bar moves when the total is unknown.
    ((millis % 2000) as f64 / 2000.0).clamp(0.0, 1.0)
}

fn draw_done(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::bordered().title(" done ");
    let Some(summary) = &app.summary else {
        frame.render_widget(Paragraph::new("finished").block(block), area);
        return;
    };
    let lines = vec![
        Line::from(vec![
            Span::styled("rows   ", Style::default().fg(Color::DarkGray)),
            Span::raw(summary.rows.to_string()),
        ]),
        Line::from(vec![
            Span::styled("files  ", Style::default().fg(Color::DarkGray)),
            Span::raw(summary.files.to_string()),
        ]),
        Line::from(vec![
            Span::styled("bytes  ", Style::default().fg(Color::DarkGray)),
            Span::raw(human_bytes(summary.bytes)),
        ]),
        Line::from(vec![
            Span::styled("time   ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:.2}s", summary.elapsed.as_secs_f64())),
        ]),
        Line::from(vec![
            Span::styled("schema ", Style::default().fg(Color::DarkGray)),
            Span::raw(
                summary
                    .schemas
                    .iter()
                    .map(|s| s.hash())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        ]),
        Line::from(vec![
            Span::styled("verify ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "atp verify {}",
                summary
                    .manifest
                    .parent()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            )),
        ]),
        Line::raw(""),
        Line::styled("Enter to quit", Style::default().fg(Color::DarkGray)),
    ];
    frame.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_failed(frame: &mut Frame, area: Rect, app: &App) {
    let block = Block::bordered().title(" failed ");
    let lines = vec![
        Line::styled(
            app.error
                .clone()
                .unwrap_or_else(|| "unknown error".to_string()),
            Style::default().fg(Color::Red),
        ),
        Line::raw(""),
        Line::styled(
            "completed files are kept; rerun to a fresh --out or fix and retry",
            Style::default().fg(Color::DarkGray),
        ),
        Line::styled("Enter to quit", Style::default().fg(Color::DarkGray)),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(block).wrap(Wrap { trim: true }),
        area,
    );
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &App) {
    let hints = match app.screen {
        Screen::Source => "Enter inspect · Esc quit",
        Screen::Preview => "Enter options · Esc back",
        Screen::Options => "Enter convert · Esc back",
        Screen::Running => "q cancel · Ctrl-C quit",
        _ => "Enter quit",
    };
    let line = Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray)));
    frame.render_widget(Paragraph::new(line), area);
}
