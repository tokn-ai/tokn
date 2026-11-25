//! `tokn-router migration` subcommand.
//!
//! Default mode is a dry-run: inspect configured DB files and their `.bak`
//! snapshots, then print current/latest versions and aggregate row counts.
//! `--commit` applies pending migrations; `--rollback` restores backups.

use crate::db::{migrate, requests, requests::RequestsDb, sessions::SessionsDb, usage::UsageDb};
use anyhow::Result;
use clap::Args;
use std::path::{Path, PathBuf};

#[derive(Args, Debug)]
pub struct MigrationArgs {
  /// Restore each database from its `<path>.bak` snapshot.
  #[arg(long)]
  pub rollback: bool,

  /// Apply pending migrations. Without this flag, only prints a dry-run
  /// status report unless `--rollback` is supplied.
  #[arg(long)]
  pub commit: bool,
}

pub async fn run(cfg_path: Option<PathBuf>, args: MigrationArgs) -> Result<()> {
  let config = tokn_config::load_config(cfg_path.as_deref())?;
  let paths = config.persistence().resolve_paths()?;
  let usage_db = paths.usage_db;
  let sessions_db = paths.sessions_db;
  let requests_dir = paths.requests_dir;

  if args.rollback {
    rollback_one("usage", &usage_db)?;
    rollback_one("sessions", &sessions_db)?;
    for path in RequestsDb::day_files(&requests_dir)? {
      rollback_one("requests", &path)?;
    }
    return Ok(());
  }

  if !args.commit {
    println!("migration dry-run (use --commit to apply migrations, or --rollback to restore .bak files)");
    print_status("usage", &usage_db, crate::db::usage::latest_version())?;
    print_status("sessions", &sessions_db, crate::db::sessions::latest_version())?;
    for path in RequestsDb::day_files(&requests_dir)? {
      print_status("requests", &path, crate::db::requests::latest_version())?;
    }
    return Ok(());
  }

  apply_one("usage", &usage_db, |p| UsageDb::open(p).map(|_| ()))?;
  apply_one("sessions", &sessions_db, |p| SessionsDb::open(p).map(|_| ()))?;
  for path in RequestsDb::day_files(&requests_dir)? {
    apply_one("requests", &path, |p| requests::open_day_db(p).map(|_| ()))?;
  }
  Ok(())
}

fn print_status(label: &str, path: &Path, latest: u32) -> Result<()> {
  let status = migrate::status(path)?;
  println!(
    "{:<8} {:<48} current={} rows={} bak_current={} bak_rows={} latest={}",
    label,
    path.display(),
    value_text(status.current.map(|db| db.version)),
    value_text(status.current.map(|db| db.rows)),
    value_text(status.backup.map(|db| db.version)),
    value_text(status.backup.map(|db| db.rows)),
    latest,
  );
  Ok(())
}

fn value_text(value: Option<impl ToString>) -> String {
  value.map(|value| value.to_string()).unwrap_or_else(|| "missing".into())
}

fn apply_one(label: &str, path: &Path, open: impl FnOnce(&Path) -> crate::db::Result<()>) -> Result<()> {
  if !path.exists() && label != "requests" {
    println!("{label}: {} (not present, will be created)", path.display());
  }
  open(path)?;
  println!("{label}: {} ok", path.display());
  Ok(())
}

fn rollback_one(label: &str, path: &Path) -> Result<()> {
  if migrate::rollback(path)? {
    println!(
      "{label}: {} rolled back from {}",
      path.display(),
      migrate::backup_path(path).display()
    );
  } else {
    println!("{label}: {} (no backup, skipped)", path.display());
  }
  Ok(())
}
