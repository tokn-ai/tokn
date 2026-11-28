use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use console::{style, Color, StyledObject};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokn_maintenance::{
  ArchivePruneEntry, ArchivePruneOutcome, MigrationEntry, MigrationFileStats, MigrationOutcome, MigrationPayloadSize,
  MigrationProgressEvent, PruneEntry, PruneOutcome, PruneProgressEvent, PruneReport,
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
  /// Write and verify archives in the configured format. The default is a dry run.
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

struct MigrationDisplay<'a, W> {
  out: W,
  requests_dir: &'a Path,
  commit: bool,
  bar: Option<ProgressBar>,
  counts: MigrationCounts,
  row_style: RowStyle,
}

impl<'a, W: Write> MigrationDisplay<'a, W> {
  fn new(out: W, requests_dir: &'a Path, commit: bool, multi: Option<&MultiProgress>, color: bool) -> Self {
    let bar = multi.map(|multi| {
      let bar = multi.add(ProgressBar::new_spinner());
      let style = ProgressStyle::with_template("{spinner:.cyan} {prefix}{msg} [{bytes} at {bytes_per_sec}]")
        .unwrap_or_else(|_| ProgressStyle::default_spinner());
      bar.set_style(style);
      bar.set_message("Finding request archives");
      bar.enable_steady_tick(Duration::from_millis(100));
      bar
    });
    Self {
      out,
      requests_dir,
      commit,
      bar,
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
      MigrationProgressEvent::DayStarted { day, index, total } => {
        if let Some(bar) = &self.bar {
          bar.reset();
          bar.set_prefix(format!("{day} ({index}/{total}) "));
          bar.set_message("Checking request archives");
        }
      }
      MigrationProgressEvent::StageStarted { label } => {
        if let Some(bar) = &self.bar {
          bar.reset();
          bar.set_message(label);
        }
      }
      MigrationProgressEvent::BytesProcessed { bytes } => {
        if let Some(bar) = &self.bar {
          bar.inc(bytes);
        }
      }
      MigrationProgressEvent::DayFinished(entry) => {
        let row_style = self.row_style;
        self.write_output(|out, counts| write_migration_day(out, entry, counts, row_style))?
      }
    }
    Ok(())
  }

  fn write_output(&mut self, render: impl FnOnce(&mut W, &mut MigrationCounts) -> io::Result<()>) -> io::Result<()> {
    let out = &mut self.out;
    let counts = &mut self.counts;
    let write = || {
      render(out, counts)?;
      out.flush()
    };
    if let Some(bar) = &self.bar {
      bar.suspend(write)
    } else {
      write()
    }
  }

  fn clear_progress(&mut self) {
    if let Some(bar) = self.bar.take() {
      bar.finish_and_clear();
    }
  }

  fn write_summary(&mut self) -> io::Result<()> {
    let commit = self.commit;
    self.write_output(|out, counts| write_migration_summary(out, counts, commit))
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
    MigrationOutcome::WouldMigrate => {
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
    let size_label = match entry.outcome {
      MigrationOutcome::WouldMigrate => "preview",
      MigrationOutcome::Migrated | MigrationOutcome::Verified => "final",
      _ => "destination",
    };
    write!(
      out,
      "{}",
      migration_file_label(&entry.destination, &day, &entry.files, size_label, row_style.compact)
    )?;
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
  let size = |bytes| {
    if compact {
      HumanBytes(bytes).to_string()
    } else {
      format!("{} ({bytes} bytes)", HumanBytes(bytes))
    }
  };
  let mut fields = vec![format!("{size_label} {}", size(file.compressed_bytes))];
  match file.uncompressed {
    Some(MigrationPayloadSize::Sqlite(bytes)) => fields.push(format!("DB {}", size(bytes))),
    Some(MigrationPayloadSize::Pack(bytes)) => fields.push(format!("pack {}", size(bytes))),
    None => (),
  }
  if let Some(prefix) = file.sha256.get(..12).filter(|_| compact && file.sha256.len() > 12) {
    fields.push(format!("sha256 {prefix}…"));
  } else {
    fields.push(format!("sha256 {}", file.sha256));
  }
  format!("{name} ({})", fields.join(", "))
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
    let requests_dir = directory.path().join("requests");
    std::fs::create_dir(&requests_dir).unwrap();
    let config_path = directory.path().join("config.toml");
    let serialized_requests_dir = serde_json::to_string(&requests_dir).unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../smoke.toml");
    let mut config = std::fs::read_to_string(fixture).unwrap();
    config.push_str(&format!(
      "\n[service.persistence]\nrequests_dir = {serialized_requests_dir}\narchive_extension = \"db.zstd\"\n"
    ));
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
        outcome: MigrationOutcome::WouldMigrate,
        files: vec![
          MigrationFileStats {
            path: original,
            compressed_bytes: 1024,
            sha256: "aa".repeat(32),
            uncompressed: Some(MigrationPayloadSize::Sqlite(8192)),
          },
          MigrationFileStats {
            path: destination.clone(),
            compressed_bytes: 512,
            sha256: "bb".repeat(32),
            uncompressed: Some(MigrationPayloadSize::Pack(4096)),
          },
        ],
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
    assert!(row.contains(&format!("sha256 {}", "aa".repeat(32))));
    assert!(row.contains(" -> .pack.zst (preview 512 B (512 bytes), pack 4.00 KiB (4096 bytes)"));
    assert!(row.contains(&format!("sha256 {}", "bb".repeat(32))));
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
  fn migration_rows_show_final_measurements_in_plain_and_colored_output() {
    let requests_dir = Path::new("fixture-requests");
    let source = requests_dir.join("2026-05-01.db.xz");
    let destination = requests_dir.join("2026-05-01.pack.zst");
    let source_hash = "ab".repeat(32);
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
          uncompressed: None,
        },
        MigrationFileStats {
          path: destination,
          compressed_bytes: 512,
          sha256: destination_hash.clone(),
          uncompressed: Some(MigrationPayloadSize::Pack(16384)),
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
    assert!(source.contains(&source_hash));
    assert!(source.contains("original 1.00 KiB (1024 bytes)"));
    assert!(destination.contains(".pack.zst"));
    assert!(destination.contains(&destination_hash));
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
    assert!(row.contains(&format!("sha256 {}…", &destination_hash[..12])));
    assert!(!row.contains(&destination_hash[..13]));
    assert!(!row.contains("16384 bytes"));
    assert_eq!(counts.verified, 1);
  }

  #[test]
  fn migration_progress_resets_stage_bytes_without_losing_the_active_day() {
    let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
    let mut display = MigrationDisplay::new(Vec::new(), Path::new("fixture-requests"), true, Some(&multi), true);
    display
      .on_event(MigrationProgressEvent::DayStarted {
        day: date!(2026 - 05 - 01),
        index: 2,
        total: 3,
      })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::StageStarted { label: "Decode" })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::BytesProcessed { bytes: 2048 })
      .unwrap();
    display
      .on_event(MigrationProgressEvent::BytesProcessed { bytes: 1024 })
      .unwrap();

    let bar = display.bar.clone().unwrap();
    assert_eq!(bar.prefix(), "2026-05-01 (2/3) ");
    assert_eq!(bar.message(), "Decode");
    assert_eq!(bar.position(), 3072);
    assert_eq!(bar.length(), None);
    display
      .on_event(MigrationProgressEvent::StageStarted { label: "Compress" })
      .unwrap();
    assert_eq!(bar.prefix(), "2026-05-01 (2/3) ");
    assert_eq!(bar.message(), "Compress");
    assert_eq!(bar.position(), 0);
    display.clear_progress();
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
    let directory = tempfile::tempdir().unwrap();
    let (config_path, requests_dir) = write_v2_config(&directory);
    let config = std::fs::read_to_string(&config_path)
      .unwrap()
      .replace("archive_extension = \"db.zstd\"", "archive_extension = \"pack.zst\"");
    std::fs::write(&config_path, config).unwrap();
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
