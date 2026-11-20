//! Terminal UI state machine, independent of the renderer.
//!
//! Keeping the state machine here (and rendering in the binary) means the
//! navigation rules are unit-testable without a terminal, and the CLI can drive
//! the same `Job` the TUI builds. The TUI never converts anything itself.

use std::path::PathBuf;

use crate::engine::{Preview, Summary};
use crate::sink::{Compression, Partition};
use crate::source::SourceKind;
use crate::value::{Schema, Ty};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Screen {
    Source,
    Preview,
    Options,
    Running,
    Done,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    Up,
    Down,
    Left,
    Right,
    Enter,
    Esc,
    Tab,
    Backspace,
    Char(char),
    CtrlC,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    None,
    Inspect,
    Run,
    Cancel,
    Quit,
}

#[derive(Clone, Debug)]
pub struct FormOptions {
    pub compression: Compression,
    pub partition: Partition,
    pub strict: bool,
    pub row_group_rows: usize,
    pub out: PathBuf,
    pub focus: usize,
}

impl Default for FormOptions {
    fn default() -> Self {
        FormOptions {
            compression: Compression::Zstd,
            partition: Partition::None,
            strict: false,
            row_group_rows: 100_000,
            out: PathBuf::from("./out"),
            focus: 0,
        }
    }
}

impl FormOptions {
    pub fn cycle_compression(&mut self) {
        self.compression = match self.compression {
            Compression::Zstd => Compression::Snappy,
            Compression::Snappy => Compression::Uncompressed,
            Compression::Uncompressed => Compression::Zstd,
        };
    }

    pub fn cycle_partition(&mut self) {
        self.partition = match self.partition {
            Partition::None => Partition::Day,
            Partition::Day => Partition::Hour,
            Partition::Hour => Partition::None,
        };
    }

    pub fn adjust_row_group(&mut self, forward: bool) {
        let steps = [10_000usize, 50_000, 100_000, 250_000, 1_000_000];
        let pos = steps
            .iter()
            .position(|s| *s == self.row_group_rows)
            .unwrap_or(2);
        let next = if forward {
            (pos + 1).min(steps.len() - 1)
        } else {
            pos.saturating_sub(1)
        };
        self.row_group_rows = steps[next];
    }

    pub const FIELDS: usize = 6;

    pub fn label(&self, index: usize) -> String {
        match index {
            0 => format!("compression   {}", self.compression.as_str()),
            1 => format!("partition by  {}", self.partition.as_str()),
            2 => format!("strict mode   {}", if self.strict { "on" } else { "off" }),
            3 => format!("row group     {} rows", self.row_group_rows),
            4 => format!("output dir    {}", self.out.display()),
            _ => "run".to_string(),
        }
    }
}

/// What the user asked for, frozen at the moment they pressed `run`.
#[derive(Clone, Debug)]
pub struct Job {
    pub location: String,
    pub kind: SourceKind,
    pub timestamp_column: Option<String>,
    pub out: PathBuf,
    pub compression: Compression,
    pub partition: Partition,
    pub strict: bool,
    pub row_group_rows: usize,
}

pub struct App {
    pub screen: Screen,
    pub input: String,
    pub kind: Option<SourceKind>,
    pub preview: Option<Preview>,
    pub options: FormOptions,
    pub progress_rows: u64,
    pub progress_bytes: u64,
    pub progress_files: usize,
    pub log: Vec<String>,
    pub summary: Option<Summary>,
    pub error: Option<String>,
    pub help: bool,
    pub status: String,
}

impl Default for App {
    fn default() -> Self {
        App {
            screen: Screen::Source,
            input: String::new(),
            kind: None,
            preview: None,
            options: FormOptions::default(),
            progress_rows: 0,
            progress_bytes: 0,
            progress_files: 0,
            log: Vec::new(),
            summary: None,
            error: None,
            help: false,
            status: "type a path or Elasticsearch URL, then Enter".to_string(),
        }
    }
}

impl App {
    pub fn on_key(&mut self, key: Key) -> Action {
        if key == Key::CtrlC {
            return Action::Quit;
        }
        match self.screen {
            Screen::Source => match key {
                Key::Enter => Action::Inspect,
                Key::Backspace => {
                    self.input.pop();
                    Action::None
                }
                Key::Char(c) => {
                    self.input.push(c);
                    Action::None
                }
                Key::Esc => Action::Quit,
                _ => Action::None,
            },
            Screen::Preview => match key {
                Key::Enter => {
                    self.screen = Screen::Options;
                    Action::None
                }
                Key::Esc => {
                    self.screen = Screen::Source;
                    Action::None
                }
                _ => Action::None,
            },
            Screen::Options => match key {
                Key::Up => {
                    self.options.focus = self.options.focus.saturating_sub(1);
                    Action::None
                }
                Key::Down | Key::Tab => {
                    self.options.focus = (self.options.focus + 1).min(FormOptions::FIELDS - 1);
                    Action::None
                }
                Key::Left | Key::Right | Key::Char(' ') => {
                    self.adjust_option(key);
                    Action::None
                }
                Key::Enter => {
                    self.screen = Screen::Running;
                    self.progress_rows = 0;
                    self.progress_bytes = 0;
                    self.progress_files = 0;
                    Action::Run
                }
                Key::Esc => {
                    self.screen = Screen::Preview;
                    Action::None
                }
                _ => Action::None,
            },
            Screen::Running => match key {
                Key::Char('q') | Key::Esc => Action::Cancel,
                _ => Action::None,
            },
            Screen::Done => match key {
                Key::Enter | Key::Esc | Key::Char('q') => Action::Quit,
                _ => Action::None,
            },
            Screen::Failed => match key {
                Key::Enter | Key::Esc | Key::Char('q') => Action::Quit,
                _ => Action::None,
            },
        }
    }

    fn adjust_option(&mut self, key: Key) {
        match self.options.focus {
            0 => self.options.cycle_compression(),
            1 => self.options.cycle_partition(),
            2 => self.options.strict = !self.options.strict,
            3 => self.options.adjust_row_group(!matches!(key, Key::Left)),
            4 => {
                let bump = self.options.out.clone();
                self.options.out = if bump.ends_with(".parquet") {
                    PathBuf::from("./out")
                } else {
                    PathBuf::from(format!("{}.parquet", bump.display()))
                };
            }
            _ => {}
        }
    }

    pub fn set_preview(&mut self, preview: Preview) {
        self.kind = SourceKind::parse(&preview.info.kind);
        self.options.out = default_out(&self.input);
        self.status = format!(
            "{} columns, {} rows sampled — Enter to choose options",
            preview.schema.fields.len(),
            preview.rows.len()
        );
        self.preview = Some(preview);
        self.screen = Screen::Preview;
    }

    pub fn fail(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
        self.screen = Screen::Failed;
    }

    pub fn progress(&mut self, rows: u64, bytes: u64, files: usize) {
        self.progress_rows = rows;
        self.progress_bytes = bytes;
        self.progress_files = files;
    }

    pub fn note(&mut self, line: impl Into<String>) {
        self.log.push(line.into());
        if self.log.len() > 200 {
            self.log.remove(0);
        }
    }

    pub fn finish(&mut self, summary: Summary) {
        self.log.push(format!(
            "wrote {} files, {} rows, {} bytes in {:.2}s",
            summary.files,
            summary.rows,
            summary.bytes,
            summary.elapsed.as_secs_f64()
        ));
        self.summary = Some(summary);
        self.screen = Screen::Done;
    }

    pub fn job(&self) -> Option<Job> {
        let preview = self.preview.as_ref()?;
        Some(Job {
            location: self.input.clone(),
            kind: self.kind?,
            timestamp_column: preview
                .schema
                .fields
                .iter()
                .find(|f| f.ty == Ty::TimestampMillis)
                .map(|f| f.name.clone()),
            out: self.options.out.clone(),
            compression: self.options.compression,
            partition: self.options.partition,
            strict: self.options.strict,
            row_group_rows: self.options.row_group_rows,
        })
    }
}

fn default_out(input: &str) -> PathBuf {
    if input.starts_with("http://") || input.starts_with("https://") {
        return PathBuf::from("./out");
    }
    let stem = std::path::Path::new(input)
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "out".to_string());
    PathBuf::from(format!("./{stem}.parquet"))
}

/// Human-readable schema listing used by the preview screen and `inspect`.
pub fn schema_lines(schema: &Schema) -> Vec<String> {
    schema
        .fields
        .iter()
        .map(|f| format!("{:<40} {}", f.name, f.ty.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Preview;
    use crate::source::SourceInfo;

    fn preview() -> Preview {
        Preview {
            info: SourceInfo {
                kind: "csv".into(),
                location: "events.csv".into(),
                rows_estimate: Some(2),
                bytes_estimate: None,
                notes: vec![],
            },
            schema: Schema::new(vec![
                crate::value::Field::new("ts", Ty::TimestampMillis),
                crate::value::Field::new("n", Ty::I64),
            ]),
            columns: vec!["ts".into(), "n".into()],
            rows: vec![vec!["2026-11-21T00:00:00Z".into(), "1".into()]],
        }
    }

    #[test]
    fn full_walkthrough_reaches_run_and_done() {
        let mut app = App::default();
        for c in "events.csv".chars() {
            assert_eq!(app.on_key(Key::Char(c)), Action::None);
        }
        assert_eq!(app.on_key(Key::Enter), Action::Inspect);
        app.set_preview(preview());
        assert_eq!(app.screen, Screen::Preview);
        assert_eq!(app.on_key(Key::Enter), Action::None);
        assert_eq!(app.screen, Screen::Options);

        // compression, partition, strict, row group
        for _ in 0..4 {
            app.on_key(Key::Right);
            app.on_key(Key::Down);
        }
        assert_eq!(app.options.compression, Compression::Snappy);
        assert_eq!(app.options.partition, Partition::Day);
        assert!(app.options.strict);

        assert_eq!(app.on_key(Key::Enter), Action::Run);
        assert_eq!(app.screen, Screen::Running);
        app.progress(10, 100, 1);
        assert_eq!(app.progress_rows, 10);
        assert_eq!(app.on_key(Key::Char('q')), Action::Cancel);
    }

    #[test]
    fn escape_walks_back_and_ctrl_c_always_quits() {
        let mut app = App::default();
        app.set_preview(preview());
        app.on_key(Key::Enter);
        assert_eq!(app.screen, Screen::Options);
        app.on_key(Key::Esc);
        assert_eq!(app.screen, Screen::Preview);
        app.on_key(Key::Esc);
        assert_eq!(app.screen, Screen::Source);
        assert_eq!(app.on_key(Key::CtrlC), Action::Quit);
    }

    #[test]
    fn job_uses_detected_timestamp_and_options() {
        let mut app = App::default();
        for c in "/data/events.csv".chars() {
            app.on_key(Key::Char(c));
        }
        app.set_preview(preview());
        let job = app.job().expect("job");
        assert_eq!(job.timestamp_column.as_deref(), Some("ts"));
        assert_eq!(job.out, PathBuf::from("./events.parquet"));
        assert_eq!(job.kind, SourceKind::Csv);
    }

    #[test]
    fn failure_screen_quits_on_enter() {
        let mut app = App::default();
        app.fail("boom");
        assert_eq!(app.screen, Screen::Failed);
        assert_eq!(app.error.as_deref(), Some("boom"));
        assert_eq!(app.on_key(Key::Enter), Action::Quit);
    }
}
