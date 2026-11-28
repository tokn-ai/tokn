use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use tokn_maintenance::{
  ArchivePruneEntry, ArchivePruneOutcome, MigrationOutcome, MigrationReport, PruneEntry, PruneOutcome,
  PruneProgressEvent, PruneReport,
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
  let progress = io::stdout().is_terminal().then(|| {
    let bar = crate::progress::multi().add(ProgressBar::new_spinner());
    bar.set_message("Finding request archives");
    bar.enable_steady_tick(std::time::Duration::from_millis(100));
    bar
  });
  let result = tokn_maintenance::migrate_request_archives_with_progress(
    &paths.requests_dir,
    persistence.archive_extension(),
    args.commit,
    |day, index, total| {
      if let Some(bar) = &progress {
        let action = if args.commit { "Migrate" } else { "Verify" };
        bar.set_message(format!("{action} {day} ({index}/{total})"));
      }
    },
  );
  if let Some(bar) = progress {
    bar.finish_and_clear();
  }
  let report = result?;
  let counts = write_migration_report(&mut io::stdout().lock(), &paths.requests_dir, &report, args.commit)?;
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

  let counts = write_prune_report(&mut io::stdout().lock(), &paths.requests_dir, &report, args.commit)?;
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
  conflicts: usize,
  failed: usize,
}

fn write_migration_report(
  out: &mut impl Write,
  requests_dir: &Path,
  report: &MigrationReport,
  commit: bool,
) -> io::Result<MigrationCounts> {
  write_heading(
    out,
    "Request archive migration",
    requests_dir,
    report.target_extension,
    commit,
  )?;
  let mut counts = MigrationCounts::default();
  for entry in &report.entries {
    writeln!(out, "\n{}", entry.day)?;
    let destination = filename(&entry.destination);
    match &entry.outcome {
      MigrationOutcome::WouldMigrate => {
        counts.changed += 1;
        writeln!(out, "  WOULD WRITE   {destination}")?;
      }
      MigrationOutcome::Migrated => {
        counts.changed += 1;
        writeln!(out, "  WROTE         {destination} (verified)")?;
      }
      MigrationOutcome::Verified => {
        counts.verified += 1;
        writeln!(out, "  KEEP          {destination} (verified)")?;
      }
      MigrationOutcome::Conflict { first, other } => {
        counts.conflicts += 1;
        writeln!(
          out,
          "  BLOCKED       {destination} ({} differs from {})",
          filename(first),
          filename(other)
        )?;
      }
      MigrationOutcome::Failed { error } => {
        counts.failed += 1;
        writeln!(out, "  BLOCKED       {destination} ({error})")?;
      }
    }
    if !entry.sources.is_empty() {
      let sources = entry.sources.iter().map(|source| filename(source)).collect::<Vec<_>>();
      writeln!(out, "  KEEP          {}", sources.join(", "))?;
    }
  }
  let change = if commit { "written" } else { "would write" };
  writeln!(
    out,
    "\nSummary  {} {change}, {} already verified, {} blocked",
    counts.changed,
    counts.verified,
    counts.conflicts + counts.failed
  )?;
  if !commit && counts.changed > 0 {
    writeln!(out, "Rerun with --commit to write the planned archives.")?;
  }
  Ok(counts)
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
  for (day, plan) in days {
    writeln!(out, "\n{day}")?;
    for entry in plan.databases {
      write_database_prune(out, entry, &mut counts)?;
    }
    for entry in plan.archives {
      write_archive_prune(out, entry, &mut counts)?;
    }
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

fn write_database_prune(out: &mut impl Write, entry: &PruneEntry, counts: &mut PruneCounts) -> io::Result<()> {
  let file = filename(&entry.path);
  let archive = filename(&entry.archive);
  match &entry.outcome {
    PruneOutcome::Verified { .. } => {
      counts.changed += 1;
      writeln!(out, "  WOULD DELETE  {file} (keep {archive})")
    }
    PruneOutcome::Deleted { .. } => {
      counts.changed += 1;
      writeln!(out, "  DELETED       {file} (kept {archive})")
    }
    PruneOutcome::InUse => {
      counts.retained += 1;
      writeln!(out, "  KEEP          {file} (database in use)")
    }
    PruneOutcome::MissingArchive => {
      counts.missing += 1;
      writeln!(out, "  BLOCKED       {file} (missing archive {archive})")
    }
    PruneOutcome::ArchiveConflict => {
      counts.conflicts += 1;
      writeln!(out, "  BLOCKED       {file} (archives for this day differ)")
    }
    PruneOutcome::HashMismatch { .. } => {
      counts.conflicts += 1;
      writeln!(out, "  BLOCKED       {file} (content differs from {archive})")
    }
    PruneOutcome::Failed { error } => {
      counts.failed += 1;
      writeln!(out, "  BLOCKED       {file} ({error})")
    }
  }
}

fn write_archive_prune(out: &mut impl Write, entry: &ArchivePruneEntry, counts: &mut PruneCounts) -> io::Result<()> {
  let file = filename(&entry.path);
  let keeper = filename(&entry.keeper);
  match &entry.outcome {
    ArchivePruneOutcome::WouldRemove => {
      counts.changed += 1;
      writeln!(out, "  WOULD DELETE  {file} (keep {keeper})")
    }
    ArchivePruneOutcome::Removed => {
      counts.changed += 1;
      writeln!(out, "  DELETED       {file} (kept {keeper})")
    }
    ArchivePruneOutcome::MissingTarget => {
      counts.retained += 1;
      writeln!(out, "  KEEP          {file} (waiting for {keeper})")
    }
    ArchivePruneOutcome::Conflict { first, other } => {
      counts.conflicts += 1;
      writeln!(
        out,
        "  BLOCKED       {file} ({} differs from {})",
        filename(first),
        filename(other)
      )
    }
    ArchivePruneOutcome::Failed { error } => {
      counts.failed += 1;
      writeln!(out, "  BLOCKED       {file} ({error})")
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cli::{Cli, Cmd};
  use clap::Parser;
  use indicatif::ProgressDrawTarget;
  use time::macros::date;
  use tokn_maintenance::MigrationEntry;

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
  fn shows_migration_plan_and_conflicts_with_file_names() {
    let requests_dir = Path::new("fixture-requests");
    let first = requests_dir.join("2026-05-02.db.xz");
    let other = requests_dir.join("2026-05-02.db.zstd");
    let report = MigrationReport {
      target_extension: "pack.zst",
      entries: vec![
        MigrationEntry {
          day: date!(2026 - 05 - 01),
          sources: vec![requests_dir.join("2026-05-01.db.zstd")],
          destination: requests_dir.join("2026-05-01.pack.zst"),
          outcome: MigrationOutcome::WouldMigrate,
        },
        MigrationEntry {
          day: date!(2026 - 05 - 02),
          sources: vec![first.clone(), other.clone()],
          destination: requests_dir.join("2026-05-02.pack.zst"),
          outcome: MigrationOutcome::Conflict { first, other },
        },
      ],
    };
    let mut output = Vec::new();
    let counts = write_migration_report(&mut output, requests_dir, &report, false).unwrap();
    let output = String::from_utf8(output).unwrap();

    assert_eq!(output.matches("fixture-requests").count(), 1);
    assert!(output.contains("Target     .pack.zst"));
    assert!(output.contains("2026-05-01\n  WOULD WRITE   2026-05-01.pack.zst\n  KEEP          2026-05-01.db.zstd"));
    assert!(output.contains("BLOCKED       2026-05-02.pack.zst (2026-05-02.db.xz differs from 2026-05-02.db.zstd)"));
    assert!(output.contains("Summary  1 would write, 0 already verified, 1 blocked"));
    assert_eq!(counts.conflicts, 1);
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
      ],
    };
    let mut output = Vec::new();
    let counts = write_prune_report(&mut output, requests_dir, &report, false).unwrap();
    let output = String::from_utf8(output).unwrap();

    assert_eq!(output.matches("fixture-requests").count(), 1);
    assert!(output.contains(concat!(
      "2026-05-01\n",
      "  WOULD DELETE  2026-05-01.db (keep 2026-05-01.pack.zst)\n",
      "  WOULD DELETE  2026-05-01.db.xz (keep 2026-05-01.pack.zst)"
    )));
    assert!(output.contains("KEEP          2026-05-02.db.xz (waiting for 2026-05-02.pack.zst)"));
    assert!(output.contains("BLOCKED       2026-05-02.db (archives for this day differ)"));
    assert!(output.contains("Summary  2 would delete, 1 retained, 1 blocked"));
    assert!(!output.contains("digest omitted"));
    assert_eq!(counts.blocked(), 1);
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
