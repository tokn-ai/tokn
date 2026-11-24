use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum SessionsCmd {
  /// Replay a requests day database into a tree-shaped sessions database.
  Playback(PlaybackArgs),
}

#[derive(Args, Debug)]
pub struct PlaybackArgs {
  /// Source requests/<YYYY-MM-DD>.db file to read. Overrides --requests-dir.
  #[arg(long)]
  pub requests_db: Option<PathBuf>,

  /// Source requests directory to replay in filename order.
  #[arg(long)]
  pub requests_dir: Option<PathBuf>,

  /// Destination sessions.db file to create or update.
  #[arg(long)]
  pub sessions_db: Option<PathBuf>,

  /// Reprocess rows even when their session/request node already exists.
  #[arg(long)]
  pub force: bool,
}

pub async fn run(cmd: SessionsCmd) -> Result<()> {
  match cmd {
    SessionsCmd::Playback(args) => playback(args).await,
  }
}

async fn playback(args: PlaybackArgs) -> Result<()> {
  let sessions_db = args.sessions_db.map(Ok).unwrap_or_else(default_playback_sessions_db)?;
  let source = match args.requests_db {
    Some(path) => crate::db::sessions::PlaybackSource::File(path),
    None => crate::db::sessions::PlaybackSource::Dir(
      args
        .requests_dir
        .map(Ok)
        .unwrap_or_else(crate::config::paths::default_requests_dir)?,
    ),
  };
  let mut progress = PlaybackProgressDisplay::new(std::io::stdout().is_terminal());
  let report = crate::db::sessions::playback_requests_source_into_sessions_with_progress(
    source.clone(),
    &sessions_db,
    crate::db::sessions::PlaybackOptions { force: args.force },
    |event| progress.on_event(event),
  )?;
  progress.finish();
  match source {
    crate::db::sessions::PlaybackSource::File(path) => println!("requests_db={}", path.display()),
    crate::db::sessions::PlaybackSource::Dir(path) => println!("requests_dir={}", path.display()),
  }
  println!("sessions_db={}", sessions_db.display());
  println!("force={}", args.force);
  println!("rows_seen={}", report.rows_seen);
  println!("rows_with_session={}", report.rows_with_session);
  println!("rows_recorded={}", report.rows_recorded);
  println!("rows_existing={}", report.rows_existing);
  println!("rows_skipped={}", report.rows_skipped);
  println!("decode_errors={}", report.decode_errors);
  println!("reduction_mismatches={}", report.reduction_mismatches);
  println!("latest_mismatches={}", report.latest_mismatches.len());
  for mismatch in &report.latest_mismatches {
    println!(
      "latest_mismatch session_id={} expected_request_id={} actual_request_id={}",
      mismatch.session_id,
      mismatch.expected_request_id,
      mismatch.actual_request_id.as_deref().unwrap_or("<missing>")
    );
  }
  if !report.latest_mismatches.is_empty() {
    bail!("session latest view did not match requests DB latest rows");
  }
  Ok(())
}

fn default_playback_sessions_db() -> crate::config::Result<PathBuf> {
  Ok(crate::config::paths::data_dir()?.join("sessions.playback.db"))
}

struct PlaybackProgressDisplay {
  enabled: bool,
  file_bar: Option<ProgressBar>,
  global_bar: Option<ProgressBar>,
  file_style: ProgressStyle,
  global_style: ProgressStyle,
}

impl PlaybackProgressDisplay {
  fn new(enabled: bool) -> Self {
    Self {
      enabled,
      file_bar: None,
      global_bar: None,
      file_style: ProgressStyle::with_template("{spinner:.cyan} {msg} [{wide_bar:.cyan/blue}] {pos}/{len}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> "),
      global_style: ProgressStyle::with_template("{spinner:.green} {msg} [{wide_bar:.green/blue}] {pos}/{len}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> "),
    }
  }

  fn on_event(&mut self, event: crate::db::sessions::PlaybackProgressEvent) {
    if !self.enabled {
      return;
    }
    match event {
      crate::db::sessions::PlaybackProgressEvent::Started {
        files_total,
        rows_total,
      } => {
        let bar = crate::progress::multi().add(ProgressBar::new(rows_total));
        bar.set_style(self.global_style.clone());
        bar.set_message(format!(
          "global files=0/{files_total} {}",
          format_stats(Default::default())
        ));
        self.global_bar = Some(bar);
      }
      crate::db::sessions::PlaybackProgressEvent::FileStarted {
        path,
        file_index,
        files_total,
        rows_total,
      } => {
        if let Some(bar) = self.file_bar.take() {
          bar.finish_and_clear();
        }
        let bar = if let Some(global_bar) = &self.global_bar {
          crate::progress::multi().insert_before(global_bar, ProgressBar::new(rows_total))
        } else {
          crate::progress::multi().add(ProgressBar::new(rows_total))
        };
        bar.set_style(self.file_style.clone());
        bar.set_message(format!(
          "file {} {}/{}",
          playback_filename(&path),
          file_index + 1,
          files_total
        ));
        self.file_bar = Some(bar);
      }
      crate::db::sessions::PlaybackProgressEvent::RowProcessed {
        path,
        file_index,
        files_total,
        rows_seen,
        file_stats,
        global_stats,
        ..
      } => {
        if let Some(bar) = &self.file_bar {
          bar.set_position(rows_seen);
          bar.set_message(format!(
            "file {} {}/{} {}",
            playback_filename(&path),
            file_index + 1,
            files_total,
            format_stats(file_stats)
          ));
          bar.tick();
        }
        if let Some(bar) = &self.global_bar {
          bar.set_position(global_stats.rows_seen);
          bar.set_message(format!(
            "global files={}/{} {}",
            file_index + 1,
            files_total,
            format_stats(global_stats)
          ));
          bar.tick();
        }
      }
      crate::db::sessions::PlaybackProgressEvent::FileFinished {
        path,
        file_index,
        files_total,
        file_stats,
        global_stats,
      } => {
        if let Some(bar) = self.file_bar.take() {
          bar.finish_and_clear();
        }
        let _ = crate::progress::multi().println(format!(
          "file {} {}/{} done {}",
          playback_filename(&path),
          file_index + 1,
          files_total,
          format_stats(file_stats)
        ));
        if let Some(bar) = &self.global_bar {
          bar.set_position(global_stats.rows_seen);
          bar.set_message(format!(
            "global files={}/{} {}",
            file_index + 1,
            files_total,
            format_stats(global_stats)
          ));
        }
      }
      crate::db::sessions::PlaybackProgressEvent::Finished { global_stats } => {
        if let Some(bar) = &self.global_bar {
          bar.set_position(global_stats.rows_seen);
          bar.set_message(format!("global {}", format_stats(global_stats)));
        }
      }
    }
  }

  fn finish(&mut self) {
    if let Some(bar) = self.file_bar.take() {
      bar.finish_and_clear();
    }
    if let Some(bar) = self.global_bar.take() {
      bar.finish_and_clear();
    }
  }
}

fn playback_filename(path: &std::path::Path) -> String {
  path
    .file_name()
    .and_then(|value| value.to_str())
    .unwrap_or("<unknown>")
    .to_string()
}

fn format_stats(stats: crate::db::sessions::PlaybackStats) -> String {
  format!(
    "seen={} recorded={} existing={} skipped={} decode_errors={} reductions={}",
    stats.rows_seen,
    stats.rows_recorded,
    stats.rows_existing,
    stats.rows_skipped,
    stats.decode_errors,
    stats.reduction_mismatches
  )
}
