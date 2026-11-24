//! Native import of history exported from isolated gateway storage.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokn_persistence::history_import::{import_history, ImportReport};
use tokn_persistence::DbPaths;

#[derive(Subcommand, Debug)]
pub enum HistoryCmd {
  /// Compare captured history with local storage; use --commit to insert new rows
  Import(ImportArgs),
}

#[derive(Args, Debug)]
pub struct ImportArgs {
  /// Export root containing usage.db, sessions.db, and requests/YYYY-MM-DD.db
  #[arg(long)]
  pub source: PathBuf,

  /// Destination root; otherwise use the effective configuration's database paths
  #[arg(long)]
  pub destination: Option<PathBuf>,

  /// Insert new rows after validating the entire import; the default is a dry run
  #[arg(long)]
  pub commit: bool,

  /// Print the import report as JSON
  #[arg(long)]
  pub json: bool,
}

pub fn run(config_path: Option<PathBuf>, command: HistoryCmd) -> Result<()> {
  let HistoryCmd::Import(args) = command;
  execute(config_path.as_deref(), &args, &mut std::io::stdout().lock())
}

fn execute(config_path: Option<&Path>, args: &ImportArgs, output: &mut dyn Write) -> Result<()> {
  let destination = resolve_destination(config_path, args.destination.as_deref())?;
  let report = import_history(&args.source, &destination, args.commit).context("import captured history")?;
  write_report(&report, args.json, output)
}

fn resolve_destination(config_path: Option<&Path>, destination: Option<&Path>) -> Result<DbPaths> {
  if let Some(root) = destination {
    return Ok(DbPaths {
      usage_db: root.join("usage.db"),
      sessions_db: root.join("sessions.db"),
      requests_dir: root.join("requests"),
    });
  }
  tokn_config::load_config(config_path)?
    .persistence()
    .resolve_paths()
    .context("resolve destination history paths")
}

fn write_report(report: &ImportReport, json: bool, output: &mut dyn Write) -> Result<()> {
  if json {
    serde_json::to_writer_pretty(&mut *output, report).context("serialize history import report")?;
    writeln!(output)?;
  } else {
    writeln!(
      output,
      "history import {}: {} new rows, {} identical rows skipped",
      if report.committed { "committed" } else { "dry-run" },
      report.inserted_total,
      report.identical_skipped_total,
    )?;
    if !report.committed {
      writeln!(output, "dry-run: rerun with --commit to insert the new rows")?;
    }
  }
  output.flush().context("write history import report")
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::cli::{Cli, Cmd};
  use clap::Parser;

  #[test]
  fn import_requires_source_and_defaults_to_dry_run() {
    assert!(Cli::try_parse_from(["tokn-router", "history", "import"]).is_err());
    let cli = Cli::try_parse_from(["tokn-router", "history", "import", "--source", "/capture"]).unwrap();
    let Cmd::History(HistoryCmd::Import(args)) = cli.cmd else {
      panic!("expected history import command");
    };
    assert_eq!(args.source, Path::new("/capture"));
    assert!(!args.commit);
    assert!(!args.json);
    assert!(args.destination.is_none());
  }

  #[test]
  fn import_accepts_commit_json_and_destination() {
    let cli = Cli::try_parse_from([
      "tokn-router",
      "history",
      "import",
      "--source",
      "/capture",
      "--destination",
      "/destination",
      "--commit",
      "--json",
    ])
    .unwrap();
    let Cmd::History(HistoryCmd::Import(args)) = cli.cmd else {
      panic!("expected history import command");
    };
    assert!(args.commit);
    assert!(args.json);
    assert_eq!(args.destination.as_deref(), Some(Path::new("/destination")));
  }

  #[test]
  fn explicit_destination_does_not_load_or_create_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let absent_config = directory.path().join("config.toml");
    let destination = directory.path().join("destination");
    let paths = resolve_destination(Some(&absent_config), Some(&destination)).unwrap();
    assert_eq!(paths.usage_db, destination.join("usage.db"));
    assert_eq!(paths.sessions_db, destination.join("sessions.db"));
    assert_eq!(paths.requests_dir, destination.join("requests"));
    assert!(!absent_config.exists());
    assert!(!destination.exists());
  }

  #[test]
  fn destination_uses_effective_legacy_persistence_paths() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let usage = directory.path().join("custom-usage.sqlite");
    let sessions = directory.path().join("custom-sessions.sqlite");
    let requests = directory.path().join("custom-requests");
    std::fs::write(
      &config_path,
      format!(
        "[db]\nusage_db_path = {}\nsessions_db_path = {}\nrequests_dir = {}\n",
        serde_json::to_string(&usage).unwrap(),
        serde_json::to_string(&sessions).unwrap(),
        serde_json::to_string(&requests).unwrap(),
      ),
    )
    .unwrap();
    let paths = resolve_destination(Some(&config_path), None).unwrap();
    assert_eq!(paths.usage_db, usage);
    assert_eq!(paths.sessions_db, sessions);
    assert_eq!(paths.requests_dir, requests);
    assert!(!usage.exists());
    assert!(!sessions.exists());
    assert!(!requests.exists());
  }

  #[test]
  fn destination_uses_effective_v2_persistence_paths() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let usage = directory.path().join("custom-usage.sqlite");
    let sessions = directory.path().join("custom-sessions.sqlite");
    let requests = directory.path().join("custom-requests");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../smoke.toml");
    let mut config = std::fs::read_to_string(fixture).unwrap();
    config.push_str(&format!(
      "\n[service.persistence]\nusage_db_path = {}\nsessions_db_path = {}\nrequests_dir = {}\n",
      serde_json::to_string(&usage).unwrap(),
      serde_json::to_string(&sessions).unwrap(),
      serde_json::to_string(&requests).unwrap(),
    ));
    std::fs::write(&config_path, &config).unwrap();
    let paths = resolve_destination(Some(&config_path), None).unwrap();
    assert_eq!(paths.usage_db, usage);
    assert_eq!(paths.sessions_db, sessions);
    assert_eq!(paths.requests_dir, requests);
    assert_eq!(std::fs::read_to_string(config_path).unwrap(), config);
  }

  #[test]
  fn json_preview_and_commit_use_the_same_import_and_leave_config_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let destination = directory.path().join("destination");
    for root in [&source, &destination] {
      std::fs::create_dir_all(root.join("requests")).unwrap();
      drop(tokn_persistence::UsageDb::open(&root.join("usage.db")).unwrap());
      drop(tokn_persistence::sessions::SessionsDb::open(&root.join("sessions.db")).unwrap());
    }
    let source_db = rusqlite::Connection::open(source.join("usage.db")).unwrap();
    source_db
      .execute(
        "INSERT INTO requests (ts, request_id, model) VALUES (1, 'cli-history-test', 'test-model')",
        [],
      )
      .unwrap();
    drop(source_db);
    let config_path = directory.path().join("invalid-config.toml");
    let invalid_config = "this intentionally is not a valid configuration";
    std::fs::write(&config_path, invalid_config).unwrap();
    let mut args = ImportArgs {
      source,
      destination: Some(destination.clone()),
      commit: false,
      json: true,
    };
    let mut output = Vec::new();
    execute(Some(&config_path), &args, &mut output).unwrap();
    let preview: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(preview["committed"], false);
    assert_eq!(preview["inserted_total"], 1);
    assert_eq!(destination_row_count(&destination), 0);

    args.commit = true;
    output.clear();
    execute(Some(&config_path), &args, &mut output).unwrap();
    let committed: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(committed["committed"], true);
    assert_eq!(committed["inserted_total"], 1);
    assert_eq!(destination_row_count(&destination), 1);
    assert_eq!(std::fs::read_to_string(config_path).unwrap(), invalid_config);
  }

  #[tokio::test]
  async fn history_dispatch_does_not_create_configured_log_files() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.toml");
    let log_dir = directory.path().join("logs");
    std::fs::write(
      &config_path,
      format!(
        "[logging]\ntarget = 'file'\ndir = {}\n",
        serde_json::to_string(&log_dir).unwrap(),
      ),
    )
    .unwrap();
    let cli = Cli {
      config: Some(config_path),
      cmd: Cmd::History(HistoryCmd::Import(ImportArgs {
        source: directory.path().join("missing-source"),
        destination: Some(directory.path().join("destination")),
        commit: false,
        json: true,
      })),
    };
    assert!(cli.run().await.is_err());
    assert!(!log_dir.exists());
  }

  fn destination_row_count(root: &Path) -> i64 {
    rusqlite::Connection::open_with_flags(root.join("usage.db"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
      .unwrap()
      .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
      .unwrap()
  }
}
