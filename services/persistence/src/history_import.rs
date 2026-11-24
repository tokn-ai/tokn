//! Insert closed, private capture databases into native history without replacing rows.
//!
//! Call this on the host after stopping and exporting the private router. Source
//! databases must be checkpointed; destination usage/session databases must already
//! exist. Missing request days are initialized under the request maintenance lock.
//! Request captures accept only raw day databases and the maintenance lock; archives,
//! sidecars, and other entries are rejected so an incomplete capture cannot succeed.
//! A single DELETE-journal transaction covers every destination database (at most
//! eleven files). Dry runs execute the same checks and writes, then roll back.

use crate::{archive::RequestMaintenanceLock, DbPaths};
use rusqlite::{params_from_iter, types::Value, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use snafu::Snafu;
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SESSION_TABLES: &[&str] = &[
  "sessions",
  "part_blobs",
  "message_tree",
  "message_parts",
  "session_nodes",
  "node_messages",
  "node_parts",
  "session_threads",
  "session_relations",
  "session_heads",
];
const REQUEST_TABLES: &[&str] = &[
  "request_connection",
  "request_metadata",
  "request_downstream",
  "request_upstream",
];

#[derive(Debug, Snafu)]
pub enum ImportError {
  #[snafu(display("history import I/O: {source}"))]
  Io { source: std::io::Error },
  #[snafu(display("history import SQLite: {source}"))]
  Sqlite { source: rusqlite::Error },
  #[snafu(display("history import initialization: {source}"))]
  Persistence { source: crate::Error },
  #[snafu(display("{message}"))]
  Invalid { message: String },
}
impl From<std::io::Error> for ImportError {
  fn from(source: std::io::Error) -> Self {
    Self::Io { source }
  }
}
impl From<rusqlite::Error> for ImportError {
  fn from(source: rusqlite::Error) -> Self {
    Self::Sqlite { source }
  }
}
impl From<crate::Error> for ImportError {
  fn from(source: crate::Error) -> Self {
    Self::Persistence { source }
  }
}
type Result<T> = std::result::Result<T, ImportError>;

#[derive(Debug, Serialize)]
pub struct ImportReport {
  pub committed: bool,
  pub inserted_total: usize,
  pub identical_skipped_total: usize,
  pub databases: Vec<DatabaseImportReport>,
}
#[derive(Debug, Serialize)]
pub struct DatabaseImportReport {
  pub source: PathBuf,
  pub destination: PathBuf,
  pub source_sha256: String,
  pub created: bool,
  pub tables: Vec<TableImportReport>,
}
#[derive(Debug, Serialize)]
pub struct TableImportReport {
  pub table: String,
  pub inserted: usize,
  pub identical_skipped: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Column {
  name: String,
  data_type: String,
  not_null: bool,
  default: Option<String>,
  primary_key: i64,
}
struct TableCapture {
  name: String,
  schema: Vec<Column>,
  columns: Vec<String>,
  key_indexes: Vec<usize>,
  foreign_keys: Vec<Vec<Value>>,
  rows: Vec<Vec<Value>>,
}
struct Capture {
  source: PathBuf,
  destination: PathBuf,
  sha256: String,
  version: i64,
  tables: Vec<TableCapture>,
  is_day: bool,
}

fn invalid(message: impl Into<String>) -> ImportError {
  ImportError::Invalid {
    message: message.into(),
  }
}
fn quote(value: &str) -> String {
  format!("\"{}\"", value.replace('"', "\"\""))
}
fn qualified(alias: &str, table: &str) -> String {
  format!("{}.{}", quote(alias), quote(table))
}

fn regular_file(path: &Path) -> Result<()> {
  if !fs::symlink_metadata(path)?.file_type().is_file() {
    return Err(invalid(format!(
      "Expected a regular, non-symlink database: {}",
      path.display()
    )));
  }
  Ok(())
}
fn directory(path: &Path) -> Result<()> {
  if !fs::symlink_metadata(path)?.file_type().is_dir() {
    return Err(invalid(format!("Expected a non-symlink directory: {}", path.display())));
  }
  Ok(())
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
  let mut name = path.as_os_str().to_os_string();
  name.push(suffix);
  PathBuf::from(name)
}
fn digest_file(path: &Path) -> Result<String> {
  let digest = Sha256::digest(fs::read(path)?);
  let mut hex = String::with_capacity(64);
  for byte in digest {
    write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
  }
  Ok(hex)
}
fn require_closed(path: &Path) -> Result<()> {
  for suffix in ["-wal", "-journal"] {
    if fs::symlink_metadata(sidecar(path, suffix)).is_ok() {
      return Err(invalid(format!(
        "Source must be stopped and checkpointed (found {suffix}): {}",
        path.display()
      )));
    }
  }
  Ok(())
}
fn verify_source_unchanged(source: &Path, sha256: &str) -> Result<()> {
  require_closed(source)?;
  if sha256 != digest_file(source)? {
    return Err(invalid(format!("Source changed while reading: {}", source.display())));
  }
  Ok(())
}
fn rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>> {
  let mut statement = conn.prepare(sql)?;
  let count = statement.column_count();
  let result = statement
    .query_map([], |row| (0..count).map(|index| row.get(index)).collect())?
    .collect::<rusqlite::Result<_>>()?;
  Ok(result)
}
fn columns(conn: &Connection, alias: &str, table: &str) -> Result<Vec<Column>> {
  let mut statement = conn.prepare(&format!("PRAGMA {}.table_info({})", quote(alias), quote(table)))?;
  let mut result = statement
    .query_map([], |row| {
      Ok(Column {
        name: row.get(1)?,
        data_type: row.get(2)?,
        not_null: row.get(3)?,
        default: row.get(4)?,
        primary_key: row.get(5)?,
      })
    })?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  result.sort();
  Ok(result)
}
fn version(conn: &Connection, alias: &str) -> Result<i64> {
  Ok(conn.query_row(
    &format!("SELECT max(version) FROM {}", qualified(alias, "schema_migrations")),
    [],
    |row| row.get(0),
  )?)
}
fn validate_tables(conn: &Connection, alias: &str, expected: &[&str]) -> Result<()> {
  let actual = rows(
    conn,
    &format!("SELECT name FROM {}.sqlite_master WHERE type='table'", quote(alias)),
  )?;
  let actual: BTreeSet<_> = actual
    .into_iter()
    .map(|row| match &row[0] {
      Value::Text(name) => name.clone(),
      _ => String::new(),
    })
    .collect();
  let expected: BTreeSet<_> = expected
    .iter()
    .copied()
    .chain(["schema_migrations"])
    .map(str::to_owned)
    .collect();
  if actual != expected {
    return Err(invalid(format!("Unexpected table set in {alias}")));
  }
  let triggers: i64 = conn.query_row(
    &format!(
      "SELECT count(*) FROM {}.sqlite_master WHERE type='trigger'",
      quote(alias)
    ),
    [],
    |row| row.get(0),
  )?;
  if triggers != 0 {
    return Err(invalid(format!("Import does not support database triggers in {alias}")));
  }
  Ok(())
}
fn load_capture(
  source: PathBuf,
  destination: PathBuf,
  expected: &[&str],
  expected_version: u32,
  is_day: bool,
) -> Result<Capture> {
  regular_file(&source)?;
  require_closed(&source)?;
  let sha256 = digest_file(&source)?;
  let conn = Connection::open_with_flags(
    &source,
    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
  )?;
  if rows(&conn, "PRAGMA integrity_check")? != vec![vec![Value::Text("ok".into())]] {
    return Err(invalid(format!("Source integrity check failed: {}", source.display())));
  }
  if !rows(&conn, "PRAGMA foreign_key_check")?.is_empty() {
    return Err(invalid(format!(
      "Source foreign key check failed: {}",
      source.display()
    )));
  }
  validate_tables(&conn, "main", expected)?;
  let version = version(&conn, "main")?;
  if version != i64::from(expected_version) {
    return Err(invalid(format!(
      "Source schema version {version} is not current ({expected_version}): {}",
      source.display()
    )));
  }
  let mut tables = Vec::new();
  for &name in expected {
    let schema = columns(&conn, "main", name)?;
    let is_usage = expected == ["requests"];
    let selected: Vec<_> = schema
      .iter()
      .filter(|column| !is_usage || column.name != "id")
      .map(|column| column.name.clone())
      .collect();
    let keys: Vec<_> = schema
      .iter()
      .filter(|column| {
        if is_usage {
          column.name == "request_id"
        } else {
          column.primary_key != 0
        }
      })
      .map(|column| &column.name)
      .collect();
    let key_indexes: Vec<_> = keys
      .iter()
      .map(|key| selected.iter().position(|column| column == *key).expect("selected key"))
      .collect();
    let selected_sql = selected
      .iter()
      .map(|column| quote(column))
      .collect::<Vec<_>>()
      .join(", ");
    let table_rows = rows(&conn, &format!("SELECT {selected_sql} FROM {}", quote(name)))?;
    if key_indexes.is_empty()
      || table_rows
        .iter()
        .any(|row| key_indexes.iter().any(|&index| row[index] == Value::Null))
    {
      return Err(invalid(format!(
        "Missing stable identity in {}/{name}",
        source.display()
      )));
    }
    tables.push(TableCapture {
      name: name.into(),
      schema,
      columns: selected,
      key_indexes,
      foreign_keys: rows(&conn, &format!("PRAGMA foreign_key_list({})", quote(name)))?,
      rows: table_rows,
    });
  }
  verify_source_unchanged(&source, &sha256)?;
  Ok(Capture {
    source,
    destination,
    sha256,
    version,
    tables,
    is_day,
  })
}

// Empty newly initialized day files are removed after rollback, including dry runs.
struct CreatedDays(Vec<(PathBuf, same_file::Handle)>);
impl Drop for CreatedDays {
  fn drop(&mut self) {
    for (path, identity) in &self.0 {
      if same_file::Handle::from_path(path).is_ok_and(|current| current == *identity) {
        let _ = fs::remove_file(path);
      }
    }
  }
}

/// Validate and insert a closed capture. With `commit = false`, every write is rolled back.
///
/// Usage and sessions destinations must already exist at the current schema version.
/// Existing rows are accepted only when all non-surrogate fields match exactly. A
/// conflicting row aborts the entire import. Sources must contain at most nine days.
pub fn import_history(source_root: &Path, destination: &DbPaths, commit: bool) -> Result<ImportReport> {
  directory(source_root)?;
  directory(&source_root.join("requests"))?;
  directory(&destination.requests_dir)?;
  let source_root = source_root.canonicalize()?;
  let mut sources = vec![
    (
      source_root.join("usage.db"),
      destination.usage_db.clone(),
      &["requests"][..],
      crate::usage::latest_version(),
      false,
    ),
    (
      source_root.join("sessions.db"),
      destination.sessions_db.clone(),
      SESSION_TABLES,
      crate::sessions::latest_version(),
      false,
    ),
  ];
  let mut days = fs::read_dir(source_root.join("requests"))?
    .map(|entry| entry.map(|entry| entry.path()))
    .collect::<std::io::Result<Vec<_>>>()?;
  days.sort();
  for source in days {
    regular_file(&source)?;
    let filename = source
      .file_name()
      .and_then(|value| value.to_str())
      .ok_or_else(|| invalid("Invalid source request filename"))?;
    if filename == ".tokn-requests-maintenance.lock" {
      continue;
    }
    if [".db.xz", ".db.zstd", ".db.zst", ".db.lzma"]
      .iter()
      .any(|suffix| filename.ends_with(suffix))
    {
      return Err(invalid(format!(
        "Archived source request days are not supported; export raw checkpointed databases: {}",
        source.display()
      )));
    }
    if ["-wal", "-journal", "-shm"]
      .iter()
      .any(|suffix| filename.ends_with(suffix))
    {
      return Err(invalid(format!(
        "Source must be stopped and checkpointed; unexpected request sidecar: {}",
        source.display()
      )));
    }
    if source.extension().is_none_or(|extension| extension != "db") {
      return Err(invalid(format!(
        "Unexpected source request entry (only day databases and the maintenance lock are supported): {}",
        source.display()
      )));
    }
    let day = source
      .file_stem()
      .and_then(|value| value.to_str())
      .ok_or_else(|| invalid("Invalid request day filename"))?;
    if !crate::is_valid_request_day(day) {
      return Err(invalid(format!("Invalid request day: {day}")));
    }
    let target = destination.requests_dir.join(source.file_name().expect("day filename"));
    sources.push((source, target, REQUEST_TABLES, crate::requests::latest_version(), true));
  }
  if sources.len() > 11 {
    return Err(invalid(
      "Atomic import supports at most nine request days; split captures before running tests",
    ));
  }
  let captures = sources
    .into_iter()
    .map(|(source, target, tables, version, is_day)| load_capture(source, target, tables, version, is_day))
    .collect::<Result<Vec<_>>>()?;
  // Reject aliases before creating the maintenance lock or any new day files.
  for capture in &captures {
    validate_target(capture, &captures, &source_root)?;
  }
  let _maintenance = RequestMaintenanceLock::acquire_writer(&destination.requests_dir)?;
  let mut created_days = CreatedDays(Vec::new());
  for capture in &captures {
    validate_target(capture, &captures, &source_root)?;
    if !capture.destination.exists() {
      if !capture.is_day {
        return Err(invalid(format!(
          "Destination must already exist: {}",
          capture.destination.display()
        )));
      }
      let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&capture.destination)?;
      created_days
        .0
        .push((capture.destination.clone(), same_file::Handle::from_file(file)?));
      drop(crate::requests::open_day_db(&capture.destination)?);
    }
  }
  let mut conn = Connection::open_with_flags(
    &destination.usage_db,
    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
  )?;
  conn.busy_timeout(Duration::from_secs(5))?;
  conn.pragma_update(None, "foreign_keys", true)?;
  let aliases: Vec<_> = (0..captures.len())
    .map(|index| {
      if index == 0 {
        "main".into()
      } else {
        format!("target_{index}")
      }
    })
    .collect();
  for (capture, alias) in captures.iter().zip(&aliases).skip(1) {
    let path = capture
      .destination
      .to_str()
      .ok_or_else(|| invalid("Destination path must be UTF-8"))?;
    conn.execute(&format!("ATTACH DATABASE ?1 AS {}", quote(alias)), [path])?;
  }
  for alias in &aliases {
    let mode: String = conn.query_row(&format!("PRAGMA {}.journal_mode", quote(alias)), [], |row| row.get(0))?;
    if mode != "delete" {
      return Err(invalid(format!(
        "Atomic import requires DELETE journal mode; {alias} uses {mode}"
      )));
    }
    conn.execute_batch(&format!("PRAGMA {}.synchronous=FULL", quote(alias)))?;
  }
  let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
  tx.pragma_update(None, "defer_foreign_keys", true)?;
  let mut report = ImportReport {
    committed: commit,
    inserted_total: 0,
    identical_skipped_total: 0,
    databases: Vec::new(),
  };
  for (capture, alias) in captures.iter().zip(&aliases) {
    if version(&tx, alias)? != capture.version {
      return Err(invalid(format!(
        "Schema version mismatch: {}",
        capture.destination.display()
      )));
    }
    let expected: Vec<_> = capture.tables.iter().map(|table| table.name.as_str()).collect();
    validate_tables(&tx, alias, &expected)?;
    let mut database = DatabaseImportReport {
      source: capture.source.clone(),
      destination: capture.destination.clone(),
      source_sha256: capture.sha256.clone(),
      created: created_days.0.iter().any(|(path, _)| path == &capture.destination),
      tables: Vec::new(),
    };
    for table in &capture.tables {
      let table_report = import_table(&tx, alias, table)?;
      report.inserted_total += table_report.inserted;
      report.identical_skipped_total += table_report.identical_skipped;
      database.tables.push(table_report);
    }
    report.databases.push(database);
  }
  // Check imported references explicitly: dry-run rollback would not evaluate the
  // deferred constraints, and scanning the host's entire history is unnecessary.
  for (capture, alias) in captures.iter().zip(&aliases) {
    for table in &capture.tables {
      validate_imported_foreign_keys(&tx, alias, table)?;
    }
  }
  if commit {
    tx.commit()?;
    created_days.0.clear();
  } else {
    tx.rollback()?;
  }
  Ok(report)
}

fn validate_target(capture: &Capture, captures: &[Capture], source_root: &Path) -> Result<()> {
  let target = &capture.destination;
  let parent = target
    .parent()
    .ok_or_else(|| invalid("Destination needs a parent directory"))?;
  directory(parent)?;
  if parent.canonicalize()?.starts_with(source_root) {
    return Err(invalid("Source and destination must be separate"));
  }
  if capture.is_day {
    for suffix in [".xz", ".zstd", ".zst", ".lzma"] {
      if fs::symlink_metadata(sidecar(target, suffix)).is_ok() {
        return Err(invalid(format!("Destination day has an archive: {}", target.display())));
      }
    }
  }
  if fs::symlink_metadata(target).is_ok() {
    regular_file(target)?;
    for other in captures {
      if same_file::is_same_file(target, &other.source)? {
        return Err(invalid("Source and destination database files alias each other"));
      }
      if target != &other.destination
        && other.destination.exists()
        && same_file::is_same_file(target, &other.destination)?
      {
        return Err(invalid("Destination database files alias each other"));
      }
    }
  }
  if captures.iter().filter(|other| &other.destination == target).count() > 1 {
    return Err(invalid("Destination database paths must be distinct"));
  }
  Ok(())
}

fn import_table(conn: &Connection, alias: &str, table: &TableCapture) -> Result<TableImportReport> {
  let name = &table.name;
  if columns(conn, alias, name)? != table.schema {
    return Err(invalid(format!("Column schema mismatch: {alias}/{name}")));
  }
  if rows(
    conn,
    &format!("PRAGMA {}.foreign_key_list({})", quote(alias), quote(name)),
  )? != table.foreign_keys
  {
    return Err(invalid(format!("Foreign key schema mismatch: {alias}/{name}")));
  }
  let selected = table
    .columns
    .iter()
    .map(|column| quote(column))
    .collect::<Vec<_>>()
    .join(", ");
  let predicate = table
    .key_indexes
    .iter()
    .map(|&index| format!("{} IS ?", quote(&table.columns[index])))
    .collect::<Vec<_>>()
    .join(" AND ");
  let qualified = qualified(alias, name);
  let mut find = conn.prepare(&format!("SELECT {selected} FROM {qualified} WHERE {predicate}"))?;
  let placeholders = vec!["?"; table.columns.len()].join(", ");
  let mut insert = conn.prepare(&format!("INSERT INTO {qualified} ({selected}) VALUES ({placeholders})"))?;
  let mut report = TableImportReport {
    table: name.clone(),
    inserted: 0,
    identical_skipped: 0,
  };
  for row in &table.rows {
    let keys = table.key_indexes.iter().map(|&index| &row[index]);
    let existing = find
      .query_row(params_from_iter(keys), |row| {
        (0..table.columns.len())
          .map(|index| row.get::<_, Value>(index))
          .collect::<rusqlite::Result<Vec<_>>>()
      })
      .optional()?;
    if let Some(existing) = existing {
      if existing != *row {
        return Err(invalid(format!(
          "Nonidentical collision: {alias}/{name}; no history imported"
        )));
      }
      report.identical_skipped += 1;
    } else {
      insert.execute(params_from_iter(row))?;
      report.inserted += 1;
    }
  }
  Ok(report)
}

fn validate_imported_foreign_keys(conn: &Connection, alias: &str, table: &TableCapture) -> Result<()> {
  let ids: BTreeSet<_> = table
    .foreign_keys
    .iter()
    .filter_map(|key| match key[0] {
      Value::Integer(id) => Some(id),
      _ => None,
    })
    .collect();
  for id in ids {
    let group: Vec<_> = table
      .foreign_keys
      .iter()
      .filter(|key| key[0] == Value::Integer(id))
      .collect();
    let text = |value: &Value| match value {
      Value::Text(text) => Ok(text.clone()),
      _ => Err(invalid("Unsupported implicit foreign key schema")),
    };
    let parent = text(&group[0][2])?;
    let mut indexes = Vec::new();
    let mut predicates = Vec::new();
    for key in &group {
      let column = text(&key[3])?;
      indexes.push(
        table
          .columns
          .iter()
          .position(|name| name == &column)
          .ok_or_else(|| invalid("Foreign key column missing"))?,
      );
      predicates.push(format!("{} IS ?", quote(&text(&key[4])?)));
    }
    let mut statement = conn.prepare(&format!(
      "SELECT 1 FROM {} WHERE {}",
      qualified(alias, &parent),
      predicates.join(" AND ")
    ))?;
    for row in &table.rows {
      if indexes.iter().any(|&index| row[index] == Value::Null) {
        continue;
      }
      let found = statement
        .query_row(params_from_iter(indexes.iter().map(|&index| &row[index])), |_| Ok(()))
        .optional()?;
      if found.is_none() {
        return Err(invalid(format!("Imported foreign key missing: {alias}/{}", table.name)));
      }
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests;
