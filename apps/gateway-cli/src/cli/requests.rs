use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use console::{style, Color, StyledObject};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use std::collections::BTreeMap;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use time::Date;
use tokn_maintenance::{
  ArchivePruneEntry, ArchivePruneOutcome, MigrationEntry, MigrationFileStats, MigrationInputUnit, MigrationOutcome,
  MigrationPayloadSize, MigrationProgressEvent, PruneEntry, PruneOutcome, PruneProgressEvent, PruneReport,
};

#[derive(Subcommand, Debug)]
pub enum RequestsCmd {
  /// Convert existing request archives to the configured format.
  Migrate(MigrateArgs),
  /// Remove archived request databases and redundant archive copies after verification.
  Prune(PruneArgs),
}

#[derive(Args, Debug)]
pub struct MigrateArgs {
  /// Compress, write, and verify archives. The default dry run measures uncompressed target payloads.
  #[arg(long)]
  pub commit: bool,
}

#[derive(Args, Debug)]
pub struct PruneArgs {
  /// Delete verified request files. The default is a dry run.
  #[arg(long)]
  pub commit: bool,
}

pub async fn run(cfg_path: Option<PathBuf>, cmd: RequestsCmd) -> Result<()> {
  match cmd {
    RequestsCmd::Migrate(args) => migrate(cfg_path.as_deref(), args),
    RequestsCmd::Prune(args) => prune(cfg_path.as_deref(), args),
  }
}

fn migrate(explicit_config: Option<&Path>, args: MigrateArgs) -> Result<()> {
  let config = tokn_config::load_config(explicit_config)?;
  let persistence = config.persistence();
  let paths = persistence.resolve_paths()?;
  let stdout = io::stdout();
  let multi = stdout.is_terminal().then(crate::progress::multi);
  let mut display = MigrationDisplay::new(
    stdout,
    &paths.requests_dir,
    args.commit,
    multi,
    console::colors_enabled(),
  );
  let mut output_result = Ok(());
  let result = tokn_maintenance::migrate_request_archives_with_progress(
    &paths.requests_dir,
    persistence.archive_extension(),
    args.commit,
    |event| {
      if output_result.is_ok() {
        output_result = display.on_event(event);
      }
    },
  );
  display.clear_progress();
  result?;
  output_result?;
  display.write_summary()?;
  let counts = &display.counts;
  let blocked = counts.conflicts + counts.failed;
  if blocked > 0 {
    let days = if blocked == 1 { "day" } else { "days" };
    bail!(
      "Migration blocked on {blocked} {days} (conflicts {}, errors {})",
      counts.conflicts,
      counts.failed
    );
  }
  Ok(())
}

fn prune(explicit_config: Option<&Path>, args: PruneArgs) -> Result<()> {
  let config = tokn_config::load_config(explicit_config)?;
  let persistence = config.persistence();
  let paths = persistence.resolve_paths()?;
  let mut progress = PruneProgressDisplay::new(io::stdout().is_terminal());
  let result = tokn_maintenance::prune_request_dbs_with_progress(
    &paths.requests_dir,
    persistence.archive_extension(),
    persistence.prune_after_days(),
    args.commit,
    |event| progress.on_event(event),
  );
  progress.finish();
  let report = result?;

  let counts = write_prune_report(
    &mut io::stdout().lock(),
    &paths.requests_dir,
    &report,
    args.commit,
    console::colors_enabled(),
  )?;
  let blocked = counts.blocked();
  if blocked > 0 {
    let files = if blocked == 1 { "file" } else { "files" };
    bail!(
      "Prune blocked {blocked} {files} (missing archives {}, conflicts {}, errors {})",
      counts.missing,
      counts.conflicts,
      counts.failed
    );
  }
  Ok(())
}

struct PruneProgressDisplay {
  enabled: bool,
  multi: MultiProgress,
  file_bar: Option<ProgressBar>,
  global_bar: Option<ProgressBar>,
  file_style: ProgressStyle,
  global_style: ProgressStyle,
}

impl PruneProgressDisplay {
  fn new(enabled: bool) -> Self {
    Self::with_multi(enabled, crate::progress::multi().clone())
  }

  fn with_multi(enabled: bool, multi: MultiProgress) -> Self {
    Self {
      enabled,
      multi,
      file_bar: None,
      global_bar: None,
      file_style: ProgressStyle::with_template("{spinner:.cyan} {msg} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> "),
      global_style: ProgressStyle::with_template("{spinner:.green} {msg} [{wide_bar:.green/blue}] {pos}/{len}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> "),
    }
  }

  fn on_event(&mut self, event: PruneProgressEvent) {
    if !self.enabled {
      return;
    }
    match event {
      PruneProgressEvent::Started { files_total } => {
        let bar = self.multi.add(ProgressBar::new(files_total as u64));
        bar.set_style(self.global_style.clone());
        bar.set_message("request days");
        self.global_bar = Some(bar);
      }
      PruneProgressEvent::FileStarted {
        path,
        file_index,
        files_total,
        bytes_total,
      } => {
        if let Some(bar) = self.file_bar.take() {
          bar.finish_and_clear();
        }
        let bar = if bytes_total == 0 {
          let bar = ProgressBar::new_spinner();
          bar.enable_steady_tick(std::time::Duration::from_millis(100));
          bar
        } else {
          let bar = ProgressBar::new(bytes_total);
          bar.set_style(self.file_style.clone());
          bar
        };
        let bar = if let Some(global_bar) = &self.global_bar {
          self.multi.insert_before(global_bar, bar)
        } else {
          self.multi.add(bar)
        };
        bar.set_message(format!("verify {} {}/{}", filename(&path), file_index + 1, files_total));
        self.file_bar = Some(bar);
      }
      PruneProgressEvent::FileProgress {
        bytes_processed,
        bytes_total,
      } => {
        if let Some(bar) = &self.file_bar {
          if bar.length().is_some() {
            bar.set_length(bytes_total.max(bytes_processed));
            bar.set_position(bytes_processed);
          }
          bar.tick();
        }
      }
      PruneProgressEvent::FileFinished {
        path,
        file_index,
        files_total,
      } => {
        if let Some(bar) = self.file_bar.take() {
          bar.finish_and_clear();
        }
        if let Some(bar) = &self.global_bar {
          bar.set_position((file_index + 1) as u64);
          bar.set_message(format!(
            "processed {} {}/{}",
            filename(&path),
            file_index + 1,
            files_total
          ));
          bar.tick();
        }
      }
      PruneProgressEvent::Finished { files_total } => {
        if let Some(bar) = &self.global_bar {
          bar.set_position(files_total as u64);
          bar.set_message("request days processed");
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

fn filename(path: &Path) -> String {
  path
    .file_name()
    .map(|name| name.to_string_lossy().into_owned())
    .unwrap_or_else(|| path.display().to_string())
}

fn day_filename(path: &Path, day: &str) -> String {
  let name = filename(path);
  name
    .strip_prefix(day)
    .filter(|suffix| suffix.starts_with('.'))
    .unwrap_or(&name)
    .to_owned()
}

fn styled_status(label: &str, color: Color, enabled: bool) -> StyledObject<&str> {
  style(label).fg(color).bold().force_styling(enabled)
}

fn inline_message(message: &str) -> String {
  message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn write_heading(
  out: &mut impl Write,
  command: &str,
  requests_dir: &Path,
  target_extension: &str,
  commit: bool,
) -> io::Result<()> {
  writeln!(out, "{command} ({})", if commit { "commit" } else { "dry run" })?;
  writeln!(out, "  Directory  {}", requests_dir.display())?;
  writeln!(out, "  Target     .{}", target_extension.trim_start_matches('.'))
}

#[derive(Default)]
struct MigrationCounts {
  changed: usize,
  verified: usize,
  retained: usize,
  conflicts: usize,
  failed: usize,
}

#[derive(Clone, Copy)]
struct RowStyle {
  color: bool,
  compact: bool,
}

struct MigrationStage {
  bar: ProgressBar,
  output_bytes: Arc<AtomicU64>,
}

struct MigrationDisplay<'a, W> {
  out: W,
  requests_dir: &'a Path,
  commit: bool,
  multi: Option<&'a MultiProgress>,
  overall_bar: Option<ProgressBar>,
  active_day: Option<Date>,
  stage: Option<MigrationStage>,
  counts: MigrationCounts,
  row_style: RowStyle,
}

impl<'a, W: Write> MigrationDisplay<'a, W> {
  fn new(out: W, requests_dir: &'a Path, commit: bool, multi: Option<&'a MultiProgress>, color: bool) -> Self {
    let overall_bar = multi.map(|multi| {
      let bar = multi.add(ProgressBar::new_spinner());
      let style =
        ProgressStyle::with_template("{spinner:.cyan} {msg}").unwrap_or_else(|_| ProgressStyle::default_spinner());
      bar.set_style(style);
      bar.set_message("Scan archives");
      bar.enable_steady_tick(Duration::from_millis(100));
      bar
    });
    Self {
      out,
      requests_dir,
      commit,
      multi,
      overall_bar,
      active_day: None,
      stage: None,
      counts: MigrationCounts::default(),
      row_style: RowStyle {
        color,
        compact: multi.is_some(),
      },
    }
  }

  fn on_event(&mut self, event: MigrationProgressEvent<'_>) -> io::Result<()> {
    match event {
      MigrationProgressEvent::Started { target_extension } => {
        let requests_dir = self.requests_dir;
        let commit = self.commit;
        self.write_output(|out, _| {
          write_heading(out, "Request archive migration", requests_dir, target_extension, commit)?;
          writeln!(out)
        })?;
      }
      MigrationProgressEvent::DayStarted { day, total, .. } => {
        self.clear_stage();
        self.active_day = Some(day);
        if let Some(overall) = self.overall_bar.as_ref().filter(|bar| bar.length().is_none()) {
          let style = ProgressStyle::with_template("{msg} [{wide_bar:.green/blue}] {pos}/{len}")
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=> ");
          overall.disable_steady_tick();
          overall.set_length(total as u64);
          overall.set_style(style);
          overall.set_message("Days");
        }
      }
      MigrationProgressEvent::StageStarted {
        label,
        input_unit,
        input_total,
        has_output,
      } => {
        self.start_stage(label, input_unit, input_total, has_output);
      }
      MigrationProgressEvent::StageProgress { input, output_bytes } => {
        if let Some(stage) = &self.stage {
          stage.output_bytes.fetch_add(output_bytes, Ordering::Relaxed);
          stage.bar.inc(input);
        }
      }
      MigrationProgressEvent::DayFinished(entry) => {
        self.clear_stage();
        self.active_day = None;
        if let Some(bar) = &self.overall_bar {
          bar.inc(1);
        }
        let row_style = self.row_style;
        self.write_output(|out, counts| write_migration_day(out, entry, counts, row_style))?
      }
    }
    Ok(())
  }

  fn start_stage(&mut self, label: &'static str, input_unit: MigrationInputUnit, input_total: u64, has_output: bool) {
    let (Some(multi), Some(overall), Some(day)) = (self.multi, &self.overall_bar, self.active_day) else {
      return;
    };
    let stage = self.stage.get_or_insert_with(|| {
      let bar = ProgressBar::new(input_total);
      bar.set_prefix(format!("{day} "));
      let bar = multi.insert_before(overall, bar);
      bar.enable_steady_tick(Duration::from_millis(100));
      MigrationStage {
        bar,
        output_bytes: Arc::new(AtomicU64::new(0)),
      }
    });
    let output_bytes = Arc::clone(&stage.output_bytes);
    let style = ProgressStyle::with_template("{prefix}{msg} [{wide_bar:.cyan/blue}] {stage_stats}")
      .unwrap_or_else(|_| ProgressStyle::default_bar())
      .progress_chars("=> ")
      .with_key(
        "stage_stats",
        move |state: &ProgressState, out: &mut dyn std::fmt::Write| {
          let output = has_output.then(|| output_bytes.load(Ordering::Relaxed));
          let stats = migration_stage_stats(input_unit, state.pos(), input_total, output, state.elapsed());
          let _ = out.write_str(&stats);
        },
      );
    stage.output_bytes.store(0, Ordering::Relaxed);
    stage.bar.reset();
    stage.bar.set_length(input_total);
    stage.bar.set_style(style);
    stage.bar.set_message(label);
  }

  fn write_output(&mut self, render: impl FnOnce(&mut W, &mut MigrationCounts) -> io::Result<()>) -> io::Result<()> {
    let out = &mut self.out;
    let counts = &mut self.counts;
    let write = || {
      render(out, counts)?;
      out.flush()
    };
    if let Some(multi) = self.multi {
      multi.suspend(write)
    } else {
      write()
    }
  }

  fn clear_progress(&mut self) {
    self.clear_stage();
    if let Some(bar) = self.overall_bar.take() {
      bar.disable_steady_tick();
      bar.finish_and_clear();
    }
  }

  fn clear_stage(&mut self) {
    if let Some(stage) = self.stage.take() {
      stage.bar.disable_steady_tick();
      stage.bar.finish_and_clear();
    }
  }

  fn write_summary(&mut self) -> io::Result<()> {
    let commit = self.commit;
    self.write_output(|out, counts| write_migration_summary(out, counts, commit))
  }
}

fn migration_stage_stats(
  unit: MigrationInputUnit,
  position: u64,
  total: u64,
  output_bytes: Option<u64>,
  elapsed: Duration,
) -> String {
  let rate = |count: u64| {
    if elapsed.is_zero() {
      0.0
    } else {
      count as f64 / elapsed.as_secs_f64()
    }
  };
  let input = match unit {
    MigrationInputUnit::Bytes => format!(
      "{}/{} ↓ {}/s",
      HumanBytes(position),
      HumanBytes(total),
      HumanBytes(rate(position) as u64)
    ),
    MigrationInputUnit::Tasks => format!("{position}/{total} tasks ↓ {:.1} tasks/s", rate(position)),
    MigrationInputUnit::Entries => format!("{position}/{total} entries ↓ {:.1} entries/s", rate(position)),
  };
  match output_bytes {
    Some(bytes) => format!("{input} ↑ {}/s", HumanBytes(rate(bytes) as u64)),
    None => input,
  }
}

fn write_migration_day(
  out: &mut impl Write,
  entry: &MigrationEntry,
  counts: &mut MigrationCounts,
  row_style: RowStyle,
) -> io::Result<()> {
  let day = entry.day.to_string();
  let (status, color, reason) = match &entry.outcome {
    MigrationOutcome::WouldMigrate { .. } => {
      counts.changed += 1;
      ("WOULD WRITE", Color::Cyan, None)
    }
    MigrationOutcome::Migrated => {
      counts.changed += 1;
      ("WROTE", Color::Green, None)
    }
    MigrationOutcome::Verified => {
      counts.verified += 1;
      ("VERIFIED", Color::Green, None)
    }
    MigrationOutcome::InUse => {
      counts.retained += 1;
      ("IN USE", Color::Yellow, None)
    }
    MigrationOutcome::Conflict { first, other } => {
      counts.conflicts += 1;
      let reason = format!(
        "{} differs from {}",
        day_filename(first, &day),
        day_filename(other, &day)
      );
      ("BLOCKED", Color::Red, Some(reason))
    }
    MigrationOutcome::Failed { error } => {
      counts.failed += 1;
      ("BLOCKED", Color::Red, Some(inline_message(error)))
    }
  };
  let style_day = style(&day).bold().force_styling(row_style.color);
  let status = styled_status(status, color, row_style.color);
  write!(out, "{style_day}  {status:<11}  ")?;
  let sources = entry
    .sources
    .iter()
    .map(|source| migration_file_label(source, &day, &entry.files, "original", row_style.compact))
    .collect::<Vec<_>>()
    .join(", ");
  if !sources.is_empty() {
    write!(out, "{sources}")?;
  }
  if !matches!(entry.outcome, MigrationOutcome::InUse) {
    if !sources.is_empty() {
      write!(out, " -> ")?;
    }
    let destination = match &entry.outcome {
      MigrationOutcome::WouldMigrate { size } => format!(
        "{} ({})",
        day_filename(&entry.destination, &day),
        migration_payload_label(*size, row_style.compact)
      ),
      outcome => {
        let size_label = if matches!(outcome, MigrationOutcome::Migrated | MigrationOutcome::Verified) {
          "final"
        } else {
          "destination"
        };
        migration_file_label(&entry.destination, &day, &entry.files, size_label, row_style.compact)
      }
    };
    write!(out, "{destination}")?;
  }
  if let Some(reason) = reason {
    write!(out, " | {reason}")?;
  }
  writeln!(out)
}

fn migration_file_label(
  path: &Path,
  day: &str,
  files: &[MigrationFileStats],
  size_label: &str,
  compact: bool,
) -> String {
  let name = day_filename(path, day);
  let Some(file) = files.iter().find(|file| file.path == path) else {
    return name;
  };
  let mut fields = vec![format!(
    "{size_label} {}",
    migration_bytes(file.compressed_bytes, compact)
  )];
  if let Some(payload) = file.uncompressed {
    fields.push(migration_payload_label(payload, compact));
  }
  if let Some(digest) = &file.sqlite_sha256 {
    let label = if let Some(prefix) = digest.get(..12).filter(|_| compact && digest.len() > 12) {
      format!("DB sha256 {prefix}…")
    } else {
      format!("DB sha256 {digest}")
    };
    fields.push(label);
  }
  format!("{name} ({})", fields.join(", "))
}

fn migration_payload_label(size: MigrationPayloadSize, compact: bool) -> String {
  let (label, bytes) = match size {
    MigrationPayloadSize::Sqlite(bytes) => ("DB", bytes),
    MigrationPayloadSize::Pack(bytes) => ("pack", bytes),
  };
  format!("{label} {}", migration_bytes(bytes, compact))
}

fn migration_bytes(bytes: u64, compact: bool) -> String {
  if compact {
    HumanBytes(bytes).to_string()
  } else {
    format!("{} ({bytes} bytes)", HumanBytes(bytes))
  }
}

fn write_migration_summary(out: &mut impl Write, counts: &MigrationCounts, commit: bool) -> io::Result<()> {
  let change = if commit { "written" } else { "would write" };
  writeln!(
    out,
    "\nSummary  {} {change}, {} already verified, {} retained, {} blocked",
    counts.changed,
    counts.verified,
    counts.retained,
    counts.conflicts + counts.failed
  )?;
  if !commit && counts.changed > 0 {
    writeln!(out, "Rerun with --commit to write the planned archives.")?;
  }
  Ok(())
}

#[derive(Default)]
struct PruneCounts {
  changed: usize,
  retained: usize,
  missing: usize,
  conflicts: usize,
  failed: usize,
}

impl PruneCounts {
  fn blocked(&self) -> usize {
    self.missing + self.conflicts + self.failed
  }
}

#[derive(Default)]
struct PruneDay<'a> {
  databases: Vec<&'a PruneEntry>,
  archives: Vec<&'a ArchivePruneEntry>,
}

fn write_prune_report(
  out: &mut impl Write,
  requests_dir: &Path,
  report: &PruneReport,
  commit: bool,
  color: bool,
) -> io::Result<PruneCounts> {
  write_heading(
    out,
    "Request archive prune",
    requests_dir,
    report.target_extension,
    commit,
  )?;
  writeln!(out, "  Cutoff     {} (inclusive)", report.cutoff)?;

  let mut days = BTreeMap::<String, PruneDay<'_>>::new();
  for entry in &report.entries {
    let day = entry
      .path
      .file_stem()
      .map(|stem| stem.to_string_lossy().into_owned())
      .unwrap_or_else(|| filename(&entry.path));
    days.entry(day).or_default().databases.push(entry);
  }
  for entry in &report.archives {
    days.entry(entry.day.to_string()).or_default().archives.push(entry);
  }

  let mut counts = PruneCounts::default();
  if !days.is_empty() {
    writeln!(out)?;
  }
  for (day, plan) in days {
    write!(out, "{}", style(&day).bold().force_styling(color))?;
    let mut separator = "  ";
    for entry in plan.databases {
      write!(out, "{separator}")?;
      write_database_prune(out, entry, &day, &mut counts, color)?;
      separator = " | ";
    }
    for entry in plan.archives {
      write!(out, "{separator}")?;
      write_archive_prune(out, entry, &day, &mut counts, color)?;
      separator = " | ";
    }
    writeln!(out)?;
  }
  let change = if commit { "deleted" } else { "would delete" };
  writeln!(
    out,
    "\nSummary  {} {change}, {} retained, {} blocked",
    counts.changed,
    counts.retained,
    counts.blocked()
  )?;
  if !commit && counts.changed > 0 {
    writeln!(out, "Rerun with --commit to delete the verified files.")?;
  }
  Ok(counts)
}

fn write_database_prune(
  out: &mut impl Write,
  entry: &PruneEntry,
  day: &str,
  counts: &mut PruneCounts,
  color: bool,
) -> io::Result<()> {
  let file = day_filename(&entry.path, day);
  let archive = day_filename(&entry.archive, day);
  let (status, outcome_color, detail) = match &entry.outcome {
    PruneOutcome::Verified { .. } => {
      counts.changed += 1;
      ("WOULD DELETE", Color::Cyan, format!("keep {archive}"))
    }
    PruneOutcome::Deleted { .. } => {
      counts.changed += 1;
      ("DELETED", Color::Green, format!("kept {archive}"))
    }
    PruneOutcome::InUse => {
      counts.retained += 1;
      ("KEEP", Color::Yellow, "database in use".into())
    }
    PruneOutcome::MissingArchive => {
      counts.missing += 1;
      ("BLOCKED", Color::Red, format!("missing archive {archive}"))
    }
    PruneOutcome::ArchiveConflict => {
      counts.conflicts += 1;
      ("BLOCKED", Color::Red, "archives for this day differ".into())
    }
    PruneOutcome::HashMismatch { .. } => {
      counts.conflicts += 1;
      ("BLOCKED", Color::Red, format!("content differs from {archive}"))
    }
    PruneOutcome::Failed { error } => {
      counts.failed += 1;
      ("BLOCKED", Color::Red, inline_message(error))
    }
  };
  let status = styled_status(status, outcome_color, color);
  write!(out, "{status} {file} ({detail})")
}

fn write_archive_prune(
  out: &mut impl Write,
  entry: &ArchivePruneEntry,
  day: &str,
  counts: &mut PruneCounts,
  color: bool,
) -> io::Result<()> {
  let file = day_filename(&entry.path, day);
  let keeper = day_filename(&entry.keeper, day);
  let (status, outcome_color, detail) = match &entry.outcome {
    ArchivePruneOutcome::WouldRemove => {
      counts.changed += 1;
      ("WOULD DELETE", Color::Cyan, format!("keep {keeper}"))
    }
    ArchivePruneOutcome::Removed => {
      counts.changed += 1;
      ("DELETED", Color::Green, format!("kept {keeper}"))
    }
    ArchivePruneOutcome::InUse => {
      counts.retained += 1;
      ("KEEP", Color::Yellow, "request day in use".into())
    }
    ArchivePruneOutcome::MissingTarget => {
      counts.retained += 1;
      ("KEEP", Color::Yellow, format!("waiting for {keeper}"))
    }
    ArchivePruneOutcome::Conflict { first, other } => {
      counts.conflicts += 1;
      let detail = format!("{} differs from {}", day_filename(first, day), day_filename(other, day));
      ("BLOCKED", Color::Red, detail)
    }
    ArchivePruneOutcome::Failed { error } => {
      counts.failed += 1;
      ("BLOCKED", Color::Red, inline_message(error))
    }
  };
  let status = styled_status(status, outcome_color, color);
  write!(out, "{status} {file} ({detail})")
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cli::{Cli, Cmd};
  use clap::Parser;
  use indicatif::ProgressDrawTarget;
  use time::macros::date;

  fn write_v2_config(directory: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    write_v2_config_with_archive_extension(directory, Some("db.zstd"))
  }

  fn write_v2_config_with_archive_extension(
    directory: &tempfile::TempDir,
    archive_extension: Option<&str>,
  ) -> (PathBuf, PathBuf) {
    let requests_dir = directory.path().join("requests");
    std::fs::create_dir(&requests_dir).unwrap();
    let config_path = directory.path().join("config.toml");
    let serialized_requests_dir = serde_json::to_string(&requests_dir).unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../smoke.toml");
    let mut config = std::fs::read_to_string(fixture).unwrap();
    config.push_str(&format!(
      "\n[service.persistence]\nrequests_dir = {serialized_requests_dir}\n"
    ));
    if let Some(extension) = archive_extension {
      let serialized_extension = serde_json::to_string(extension).unwrap();
      config.push_str(&format!("archive_extension = {serialized_extension}\n"));
    }
    std::fs::write(&config_path, config).unwrap();
    (config_path, requests_dir)
  }

  fn write_v2_config_with_ages(
    directory: &tempfile::TempDir,
    archive_after_days: u64,
    prune_after_days: u64,
  ) -> (PathBuf, PathBuf) {
    let (config_path, requests_dir) = write_v2_config(directory);
    let mut config = std::fs::read_to_string(&config_path).unwrap();
    config.push_str(&format!(
      "archive_after_days = {archive_after_days}\nprune_after_days = {prune_after_days}\n"
    ));
    std::fs::write(&config_path, config).unwrap();
    (config_path, requests_dir)
  }

  #[test]
  fn parses_migrate_as_dry_run_or_commit() {
    let cli = Cli::try_parse_from(["tokn-router", "requests", "migrate"]).unwrap();
    let Cmd::Requests(RequestsCmd::Migrate(args)) = cli.cmd else {
      panic!("expected requests migrate command");
    };
    assert!(!args.commit);

    let cli = Cli::try_parse_from(["tokn-router", "requests", "migrate", "--commit"]).unwrap();
    let Cmd::Requests(RequestsCmd::Migrate(args)) = cli.cmd else {
      panic!("expected requests migrate command");
    };
    assert!(args.commit);
  }

  #[test]
  fn parses_prune_as_dry_run_by_default() {
    let cli = Cli::try_parse_from(["tokn-router", "requests", "prune"]).unwrap();
    let Cmd::Requests(RequestsCmd::Prune(args)) = cli.cmd else {
      panic!("expected requests prune command");
    };
    assert!(!args.commit);
  }

  #[test]
  fn parses_prune_commit() {
    let cli = Cli::try_parse_from(["tokn-router", "requests", "prune", "--commit"]).unwrap();
    let Cmd::Requests(RequestsCmd::Prune(args)) = cli.cmd else {
      panic!("expected requests prune command");
    };
    assert!(args.commit);
  }

  #[test]
  fn streams_one_plain_migration_row_per_day() {
    let directory = tempfile::tempdir().unwrap();
    let requests_dir = directory.path().join("fixture-requests");
    std::fs::create_dir(&requests_dir).unwrap();
    let original = requests_dir.join("2026-05-01.db.zstd");
    let destination = requests_dir.join("2026-05-01.pack.zst");
    let first = requests_dir.join("2026-05-02.db.xz");
    let other = requests_dir.join("2026-05-02.db.zstd");
    let entries = [
      MigrationEntry {
        day: date!(2026 - 05 - 01),
        sources: vec![original.clone()],
        destination: destination.clone(),
        outcome: MigrationOutcome::WouldMigrate {
          size: MigrationPayloadSize::Pack(4096),
        },
        files: vec![MigrationFileStats {
          path: original,
          compressed_bytes: 1024,
          sha256: "aa".repeat(32),
          uncompressed: Some(MigrationPayloadSize::Sqlite(8192)),
          sqlite_sha256: Some("bb".repeat(32)),
        }],
      },
      MigrationEntry {
        day: date!(2026 - 05 - 02),
        sources: vec![first.clone(), other.clone()],
        destination: requests_dir.join("2026-05-02.pack.zst"),
        outcome: MigrationOutcome::Conflict { first, other },
        files: Vec::new(),
      },
      MigrationEntry {
        day: date!(2026 - 05 - 03),
        sources: vec![requests_dir.join("2026-05-03.db.zstd")],
        destination: requests_dir.join("2026-05-03.pack.zst"),
        outcome: MigrationOutcome::InUse,
        files: Vec::new(),
      },
      MigrationEntry {
        day: date!(2026 - 05 - 04),
        sources: vec![requests_dir.join("2026-05-04.db.zstd")],
        destination: requests_dir.join("2026-05-04.pack.zst"),
        outcome: MigrationOutcome::Failed {
          error: "could not decode\narchive\r\nmetadata".into(),
        },
        files: Vec::new(),
      },
    ];
    let mut display = MigrationDisplay::new(io::BufWriter::new(Vec::new()), &requests_dir, false, None, false);
    display
      .on_event(MigrationProgressEvent::Started {
        target_extension: "pack.zst",
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::DayStarted {
        day: entries[0].day,
        index: 1,
        total: entries.len(),
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::StageStarted {
        label: "Decode",
        input_unit: MigrationInputUnit::Bytes,
        input_total: 8192,
        has_output: true,
      })
      .unwrap();
    assert!(display.overall_bar.is_none() && display.stage.is_none());
    let output = std::str::from_utf8(display.out.get_ref()).unwrap();
    assert!(output.contains("Target     .pack.zst"));
    assert!(!output.contains("2026-05-01"));

    display
      .on_event(MigrationProgressEvent::DayFinished(&entries[0]))
      .unwrap();
    let output = std::str::from_utf8(display.out.get_ref()).unwrap();
    let row = output.lines().find(|row| row.starts_with("2026-05-01")).unwrap();
    assert!(row.contains("WOULD WRITE"));
    assert!(row.contains(".db.zstd (original 1.00 KiB (1024 bytes)"));
    assert!(row.contains("DB 8.00 KiB (8192 bytes)"));
    assert!(row.contains(&format!("DB sha256 {}", "bb".repeat(32))));
    assert!(!row.contains(&"aa".repeat(32)));
    let (_, planned) = row.split_once(" -> ").unwrap();
    assert_eq!(planned, ".pack.zst (pack 4.00 KiB (4096 bytes))");
    assert!(!destination.exists());
    assert_eq!(output.matches("2026-05-01").count(), 1);
    assert!(!output.contains("2026-05-02"));

    for entry in &entries[1..] {
      display.on_event(MigrationProgressEvent::DayFinished(entry)).unwrap();
    }
    display.write_summary().unwrap();
    let output = std::str::from_utf8(display.out.get_ref()).unwrap();

    assert_eq!(output.matches("fixture-requests").count(), 1);
    assert_eq!(output.lines().filter(|row| row.starts_with("2026-05-")).count(), 4);
    assert_eq!(output.matches("WOULD WRITE").count(), 1);
    let conflict = output.lines().find(|row| row.starts_with("2026-05-02")).unwrap();
    assert!(conflict.contains("BLOCKED"));
    assert!(conflict.contains(".db.xz differs from .db.zstd"));
    assert_eq!(output.matches("2026-05-02").count(), 1);
    let busy = output.lines().find(|row| row.starts_with("2026-05-03")).unwrap();
    assert!(busy.contains("IN USE"));
    assert!(busy.contains(".db.zstd"));
    assert_eq!(output.matches("2026-05-03").count(), 1);
    let failed = output.lines().find(|row| row.starts_with("2026-05-04")).unwrap();
    assert!(failed.contains("BLOCKED"));
    assert!(failed.contains("could not decode archive metadata"));
    assert_eq!(output.matches("2026-05-04").count(), 1);
    assert!(output.contains("Summary  1 would write, 0 already verified, 1 retained, 2 blocked"));
    assert!(!output.contains("\x1b["));
    assert_eq!(display.counts.conflicts, 1);
    assert_eq!(display.counts.failed, 1);
  }

  #[test]
  fn migration_rows_format_measurements_in_plain_and_colored_output() {
    let requests_dir = Path::new("fixture-requests");
    let source = requests_dir.join("2026-05-01.db.xz");
    let destination = requests_dir.join("2026-05-01.pack.zst");
    let source_hash = "ab".repeat(32);
    let source_db_hash = "bc".repeat(32);
    let destination_hash = "cd".repeat(32);
    let mut entry = MigrationEntry {
      day: date!(2026 - 05 - 01),
      sources: vec![source.clone()],
      destination: destination.clone(),
      outcome: MigrationOutcome::Migrated,
      files: vec![
        MigrationFileStats {
          path: source,
          compressed_bytes: 1024,
          sha256: source_hash.clone(),
          uncompressed: Some(MigrationPayloadSize::Sqlite(8192)),
          sqlite_sha256: Some(source_db_hash.clone()),
        },
        MigrationFileStats {
          path: destination,
          compressed_bytes: 512,
          sha256: destination_hash.clone(),
          uncompressed: Some(MigrationPayloadSize::Pack(16384)),
          sqlite_sha256: None,
        },
      ],
    };
    let mut output = Vec::new();
    let mut counts = MigrationCounts::default();
    let plain = RowStyle {
      color: false,
      compact: false,
    };
    write_migration_day(&mut output, &entry, &mut counts, plain).unwrap();
    let row = std::str::from_utf8(&output).unwrap();
    assert_eq!(row.lines().count(), 1);
    assert_eq!(row.matches("2026-05-01").count(), 1);
    assert!(row.starts_with("2026-05-01"));
    assert!(row.contains("WROTE"));
    assert!(!row.contains("\x1b["));
    let (source, destination) = row.split_once(" -> ").unwrap();
    assert!(source.contains(".db.xz"));
    assert!(!source.contains(&source_hash));
    assert!(source.contains("original 1.00 KiB (1024 bytes)"));
    assert!(source.contains("DB 8.00 KiB (8192 bytes)"));
    assert!(source.contains(&format!("DB sha256 {source_db_hash}")));
    assert!(destination.contains(".pack.zst"));
    assert!(!destination.contains("sha256"));
    assert!(!destination.contains(&destination_hash));
    assert!(destination.contains("final 512 B (512 bytes)"));
    assert!(destination.contains("16.00 KiB (16384 bytes)"));
    assert_eq!(counts.changed, 1);

    output.clear();
    entry.outcome = MigrationOutcome::Verified;
    let terminal = RowStyle {
      color: true,
      compact: true,
    };
    write_migration_day(&mut output, &entry, &mut counts, terminal).unwrap();
    let colored = std::str::from_utf8(&output).unwrap();
    assert_eq!(colored.lines().count(), 1);
    assert!(colored.contains("\x1b["));
    let row = console::strip_ansi_codes(colored);
    assert!(row.starts_with("2026-05-01"));
    assert_eq!(row.matches("2026-05-01").count(), 1);
    assert!(row.contains("VERIFIED"));
    assert!(row.contains(".pack.zst"));
    assert!(row.contains("final 512 B"));
    assert!(row.contains("16.00 KiB"));
    assert!(row.contains(&format!("DB sha256 {}…", &source_db_hash[..12])));
    assert!(!row.contains(&source_hash[..12]));
    assert!(!row.contains(&destination_hash[..12]));
    assert!(!row.contains(&source_db_hash[..13]));
    assert!(!row.contains("16384 bytes"));
    let (_, destination) = row.trim_end().split_once(" -> ").unwrap();
    assert_eq!(destination, ".pack.zst (final 512 B, pack 16.00 KiB)");
    assert_eq!(counts.verified, 1);

    output.clear();
    entry.destination = requests_dir.join("2026-05-01.db.zstd");
    entry.files.truncate(1);
    entry.outcome = MigrationOutcome::WouldMigrate {
      size: MigrationPayloadSize::Sqlite(8192),
    };
    write_migration_day(&mut output, &entry, &mut counts, terminal).unwrap();
    let row = console::strip_ansi_codes(std::str::from_utf8(&output).unwrap());
    let (_, planned) = row.trim_end().split_once(" -> ").unwrap();
    assert_eq!(planned, ".db.zstd (DB 8.00 KiB)");
  }

  #[test]
  fn migration_progress_resets_input_and_output_without_losing_the_active_day() {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut display = MigrationDisplay::new(Vec::new(), Path::new("fixture-requests"), true, Some(&multi), true);
    let day = date!(2026 - 05 - 01);
    display
      .on_event(MigrationProgressEvent::DayStarted {
        day,
        index: 1,
        total: 3,
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::StageStarted {
        label: "Decode",
        input_unit: MigrationInputUnit::Bytes,
        input_total: 4096,
        has_output: true,
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::StageProgress {
        input: 2048,
        output_bytes: 4096,
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::StageProgress {
        input: 1024,
        output_bytes: 2048,
      })
      .unwrap();

    let overall = display.overall_bar.clone().unwrap();
    assert_eq!(overall.length(), Some(3));
    assert_eq!(overall.position(), 0);
    let stage = display.stage.as_ref().unwrap();
    let bar = stage.bar.clone();
    let output_bytes = Arc::clone(&stage.output_bytes);
    assert_eq!(bar.prefix(), "2026-05-01 ");
    assert_eq!(bar.message(), "Decode");
    assert_eq!(bar.position(), 3072);
    assert_eq!(bar.length(), Some(4096));
    assert_eq!(output_bytes.load(Ordering::Relaxed), 6144);
    display
      .on_event(MigrationProgressEvent::StageProgress {
        input: 0,
        output_bytes: 128,
      })
      .unwrap();
    assert_eq!(bar.position(), 3072);
    assert_eq!(output_bytes.load(Ordering::Relaxed), 6272);
    display
      .on_event(MigrationProgressEvent::StageStarted {
        label: "Check tasks",
        input_unit: MigrationInputUnit::Tasks,
        input_total: 3,
        has_output: false,
      })
      .unwrap();
    assert_eq!(bar.prefix(), "2026-05-01 ");
    assert_eq!(bar.message(), "Check tasks");
    assert_eq!(bar.position(), 0);
    assert_eq!(bar.length(), Some(3));
    assert_eq!(output_bytes.load(Ordering::Relaxed), 0);
    display
      .on_event(MigrationProgressEvent::StageProgress {
        input: 1,
        output_bytes: 0,
      })
      .unwrap();
    assert_eq!(bar.position(), 1);

    let mut entry = MigrationEntry {
      day,
      sources: vec![PathBuf::from("2026-05-01.db.zstd")],
      destination: PathBuf::from("2026-05-01.pack.zst"),
      outcome: MigrationOutcome::InUse,
      files: Vec::new(),
    };
    display.on_event(MigrationProgressEvent::DayFinished(&entry)).unwrap();
    assert_eq!(overall.position(), 1);
    assert!(display.stage.is_none());
    assert_eq!(display.counts.retained, 1);
    assert!(std::str::from_utf8(&display.out).unwrap().contains("IN USE"));

    entry.day = date!(2026 - 05 - 02);
    entry.outcome = MigrationOutcome::Failed {
      error: "fixture error".into(),
    };
    display
      .on_event(MigrationProgressEvent::DayStarted {
        day: entry.day,
        index: 2,
        total: 3,
      })
      .unwrap();
    assert_eq!(overall.position(), 1);
    display.on_event(MigrationProgressEvent::DayFinished(&entry)).unwrap();
    assert_eq!(overall.position(), 2);
    assert_eq!(display.counts.failed, 1);
    assert!(std::str::from_utf8(&display.out).unwrap().contains("BLOCKED"));

    display
      .on_event(MigrationProgressEvent::DayStarted {
        day: date!(2026 - 05 - 03),
        index: 3,
        total: 3,
      })
      .unwrap();
    assert!(display.stage.is_none());
    display
      .on_event(MigrationProgressEvent::StageStarted {
        label: "Hash input",
        input_unit: MigrationInputUnit::Bytes,
        input_total: 4096,
        has_output: false,
      })
      .unwrap();
    assert!(display.stage.is_some());
    display.clear_progress();
    assert!(display.stage.is_none() && display.overall_bar.is_none());
  }

  #[test]
  fn migration_stage_stats_show_input_units_and_separate_output_rates() {
    let elapsed = Duration::from_secs(2);
    assert_eq!(
      migration_stage_stats(MigrationInputUnit::Bytes, 3072, 4096, Some(1024), elapsed),
      "3.00 KiB/4.00 KiB ↓ 1.50 KiB/s ↑ 512 B/s"
    );
    assert_eq!(
      migration_stage_stats(MigrationInputUnit::Bytes, 0, 4096, Some(0), elapsed),
      "0 B/4.00 KiB ↓ 0 B/s ↑ 0 B/s"
    );
    assert_eq!(
      migration_stage_stats(MigrationInputUnit::Tasks, 3, 8, None, elapsed),
      "3/8 tasks ↓ 1.5 tasks/s"
    );
    assert_eq!(
      migration_stage_stats(MigrationInputUnit::Entries, 5, 8, Some(2048), elapsed),
      "5/8 entries ↓ 2.5 entries/s ↑ 1.00 KiB/s"
    );
    assert_eq!(
      migration_stage_stats(MigrationInputUnit::Bytes, 3072, 4096, None, elapsed),
      "3.00 KiB/4.00 KiB ↓ 1.50 KiB/s"
    );
  }

  #[test]
  fn shows_raw_and_redundant_prune_candidates_for_the_same_day() {
    let requests_dir = Path::new("fixture-requests");
    let keeper = requests_dir.join("2026-05-01.pack.zst");
    let report = PruneReport {
      cutoff: date!(2026 - 05 - 02),
      target_extension: "pack.zst",
      entries: vec![
        PruneEntry {
          path: requests_dir.join("2026-05-01.db"),
          archive: keeper.clone(),
          outcome: PruneOutcome::Verified {
            sha256: "digest omitted in human report".to_owned(),
          },
        },
        PruneEntry {
          path: requests_dir.join("2026-05-02.db"),
          archive: requests_dir.join("2026-05-02.pack.zst"),
          outcome: PruneOutcome::ArchiveConflict,
        },
      ],
      archives: vec![
        ArchivePruneEntry {
          day: date!(2026 - 05 - 01),
          path: requests_dir.join("2026-05-01.db.xz"),
          keeper,
          outcome: ArchivePruneOutcome::WouldRemove,
        },
        ArchivePruneEntry {
          day: date!(2026 - 05 - 02),
          path: requests_dir.join("2026-05-02.db.xz"),
          keeper: requests_dir.join("2026-05-02.pack.zst"),
          outcome: ArchivePruneOutcome::MissingTarget,
        },
        ArchivePruneEntry {
          day: date!(2026 - 05 - 02),
          path: requests_dir.join("2026-05-02.db.zstd"),
          keeper: requests_dir.join("2026-05-02.pack.zst"),
          outcome: ArchivePruneOutcome::InUse,
        },
      ],
    };
    let mut output = Vec::new();
    let counts = write_prune_report(&mut output, requests_dir, &report, false, false).unwrap();
    let output = String::from_utf8(output).unwrap();

    assert_eq!(output.matches("fixture-requests").count(), 1);
    let result_rows = output
      .lines()
      .filter(|row| row.starts_with("2026-05-"))
      .collect::<Vec<_>>();
    assert_eq!(result_rows.len(), 2);
    let first = result_rows[0];
    assert!(first.contains("WOULD DELETE .db (keep .pack.zst)"));
    assert!(first.contains(" | WOULD DELETE .db.xz (keep .pack.zst)"));
    assert_eq!(first.matches("2026-05-01").count(), 1);
    let second = result_rows[1];
    assert!(second.contains("BLOCKED .db (archives for this day differ)"));
    assert!(second.contains(".db.xz (waiting for .pack.zst)"));
    assert!(second.contains(".db.zstd"));
    assert!(second.to_ascii_lowercase().contains("in use"));
    assert_eq!(second.matches("2026-05-02").count(), 1);
    assert!(output.contains("Summary  2 would delete, 2 retained, 1 blocked"));
    assert!(!output.contains("digest omitted"));
    assert!(!output.contains("\x1b["));
    assert_eq!(counts.blocked(), 1);

    let mut colored = Vec::new();
    write_prune_report(&mut colored, requests_dir, &report, false, true).unwrap();
    let colored = String::from_utf8(colored).unwrap();
    assert!(colored
      .lines()
      .filter(|row| row.contains("WOULD DELETE"))
      .any(|row| row.contains("\x1b[")));
    assert_eq!(console::strip_ansi_codes(&colored), output);
  }

  #[test]
  fn progress_display_tracks_file_and_global_progress() {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut display = PruneProgressDisplay::with_multi(true, multi);
    let path = PathBuf::from("2026-05-01.db");

    display.on_event(PruneProgressEvent::Started { files_total: 1 });
    assert_eq!(display.global_bar.as_ref().and_then(ProgressBar::length), Some(1));
    display.on_event(PruneProgressEvent::FileStarted {
      path: path.clone(),
      file_index: 0,
      files_total: 1,
      bytes_total: 20,
    });
    display.on_event(PruneProgressEvent::FileProgress {
      bytes_processed: 10,
      bytes_total: 20,
    });
    assert_eq!(display.file_bar.as_ref().map(ProgressBar::position), Some(10));
    display.on_event(PruneProgressEvent::FileFinished {
      path,
      file_index: 0,
      files_total: 1,
    });
    assert!(display.file_bar.is_none());
    assert_eq!(display.global_bar.as_ref().map(ProgressBar::position), Some(1));
    display.on_event(PruneProgressEvent::Finished { files_total: 1 });
    display.finish();
    assert!(display.global_bar.is_none());
  }

  #[test]
  fn archive_only_progress_uses_a_spinner() {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut display = PruneProgressDisplay::with_multi(true, multi);
    display.on_event(PruneProgressEvent::Started { files_total: 1 });
    display.on_event(PruneProgressEvent::FileStarted {
      path: PathBuf::from("2026-05-01.db.zstd"),
      file_index: 0,
      files_total: 1,
      bytes_total: 0,
    });
    display.on_event(PruneProgressEvent::FileProgress {
      bytes_processed: 0,
      bytes_total: 0,
    });
    assert!(display.file_bar.is_some());
    assert_eq!(display.file_bar.as_ref().and_then(ProgressBar::length), None);
    display.finish();
  }

  #[tokio::test]
  async fn prune_loads_v2_persistence_settings() {
    let directory = tempfile::tempdir().unwrap();
    let (config_path, _) = write_v2_config(&directory);

    run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: false }))
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn migrate_and_prune_use_the_same_configured_archive_target() {
    assert_migrate_and_prune_use_pack(Some("pack.zst")).await;
  }

  #[tokio::test]
  async fn migrate_and_prune_default_to_pack_when_archive_extension_is_omitted() {
    assert_migrate_and_prune_use_pack(None).await;
  }

  async fn assert_migrate_and_prune_use_pack(archive_extension: Option<&str>) {
    let directory = tempfile::tempdir().unwrap();
    let (config_path, requests_dir) = write_v2_config_with_archive_extension(&directory, archive_extension);
    let database = requests_dir.join("2000-01-01.db");
    let snapshot = requests_dir.join("2000-01-01.db.zstd");
    let destination = requests_dir.join("2000-01-01.pack.zst");
    tokn_persistence::requests::open_day_db(&database)
      .unwrap()
      .execute("INSERT INTO request_connection (request_id, ts) VALUES ('test', 1)", [])
      .unwrap();
    tokn_maintenance::archive_requests_once(
      &requests_dir,
      date!(2000 - 01 - 10),
      7,
      tokn_maintenance::ArchiveFormat::Zstd,
    )
    .unwrap();
    std::fs::remove_file(database).unwrap();

    run(
      Some(config_path.clone()),
      RequestsCmd::Migrate(MigrateArgs { commit: false }),
    )
    .await
    .unwrap();
    assert!(snapshot.is_file() && !destination.exists());
    run(
      Some(config_path.clone()),
      RequestsCmd::Migrate(MigrateArgs { commit: true }),
    )
    .await
    .unwrap();
    assert!(snapshot.is_file() && destination.is_file());
    run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: true }))
      .await
      .unwrap();
    assert!(!snapshot.exists() && destination.is_file());
  }

  #[tokio::test]
  async fn prune_default_cutoff_excludes_a_seven_day_database() {
    let directory = tempfile::tempdir().unwrap();
    let (config_path, requests_dir) = write_v2_config(&directory);
    let day = time::OffsetDateTime::now_utc().date() - time::Duration::days(7);
    let database = requests_dir.join(format!("{day}.db"));
    std::fs::write(&database, b"request database awaiting archive").unwrap();

    run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: false }))
      .await
      .unwrap();

    assert!(database.exists());
  }

  #[tokio::test]
  async fn prune_reports_out_of_range_cutoff_without_panicking() {
    let directory = tempfile::tempdir().unwrap();
    let (config_path, _) = write_v2_config_with_ages(&directory, 1_000_000_000, 1_000_000_001);

    let error = run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: false }))
      .await
      .unwrap_err();

    assert_eq!(
      error.to_string(),
      "prune retention cutoff is outside the supported date range"
    );
  }

  #[tokio::test]
  async fn prune_errors_and_distinguishes_missing_archives() {
    let directory = tempfile::tempdir().unwrap();
    let (config_path, requests_dir) = write_v2_config(&directory);
    std::fs::write(requests_dir.join("2000-01-01.db"), b"request database").unwrap();
    std::fs::write(requests_dir.join("2000-01-02.db"), b"another request database").unwrap();
    std::fs::write(requests_dir.join("2000-01-02.db.zstd"), b"corrupt archive").unwrap();

    let error = run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: false }))
      .await
      .unwrap_err();

    assert_eq!(
      error.to_string(),
      "Prune blocked 2 files (missing archives 1, conflicts 0, errors 1)"
    );
  }

  #[tokio::test]
  async fn prune_loads_legacy_persistence_settings() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let requests_dir = directory.path().join("legacy-requests");
    std::fs::create_dir(&requests_dir).unwrap();
    let serialized_requests_dir = serde_json::to_string(&requests_dir).unwrap();
    std::fs::write(
      &config_path,
      format!("[db]\nenabled = true\nrequests_dir = {serialized_requests_dir}\narchive_extension = \"db.zstd\"\n"),
    )
    .unwrap();

    run(Some(config_path), RequestsCmd::Prune(PruneArgs { commit: false }))
      .await
      .unwrap();
  }
}
