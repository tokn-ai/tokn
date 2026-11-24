//! Generic, in-process SQLite migration runner.
//!
//! Each database (usage, sessions, every `requests/<day>.db`) embeds a
//! `&[Migration]` slice describing its full version history. On open we
//! ensure a `schema_migrations` table exists, take a `<path>.bak` snapshot
//! if any work needs doing, and apply pending migrations inside a single
//! transaction per file.
//!
//! Conventions enforced by `apply`:
//!   * Versions in the slice must be strictly increasing and start at 1.
//!   * Version 0 is reserved for the current snapshot SQL, applied transparently
//!     when opening a structurally empty database (no user tables) — it
//!     installs the canonical current schema in one shot and marks all
//!     known migration versions as applied.
//!   * `<path>.bak` is overwritten before *each* migration sequence so
//!     `migration --rollback` can restore the pre-sequence state. No
//!     backup is taken when there is nothing to do.
//!
//! See `services/persistence/schemas/{snapshot,migrations}/<db>/...` for the
//! authoritative schema definitions.

use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

use super::Result;

#[derive(Clone, Copy)]
pub struct Migration {
  pub version: u32,
  pub name: &'static str,
  pub sql: &'static str,
}

/// SQL that creates the canonical current schema on a fresh DB.
pub struct Bootstrap {
  pub sql: &'static str,
}

/// Apply pending migrations to `conn` (which must already be opened on
/// `path`). Takes a `<path>.bak` snapshot before any DDL runs, only when
/// at least one migration is pending. Idempotent.
pub fn apply(
  conn: &mut Connection,
  path: &Path,
  db: &str,
  bootstrap: Bootstrap,
  migrations: &[Migration],
) -> Result<()> {
  validate_sequence(migrations);
  ensure_schema_table(conn)?;

  let applied = current_version(conn)?;
  let head = migrations.last().map(|m| m.version).unwrap_or(0);

  if applied == head {
    return Ok(());
  }

  // Pre-flight: anything to do? Decide bootstrap vs incremental.
  let bootstrap_path = applied == 0 && !has_user_tables(conn)?;
  if !bootstrap_path && applied >= head {
    return Ok(());
  }

  // Bootstrap creates the schema for the first time on an effectively
  // empty file (only `schema_migrations` exists, possibly an empty SQLite
  // header), so there's nothing meaningful to back up. Incremental
  // migrations always snapshot first so `--rollback` can restore.
  if !bootstrap_path {
    backup(path)?;
  }

  let tx = conn.transaction()?;
  if bootstrap_path {
    tracing::info!(db, target_version = head, "applying bootstrap schema");
    tx.execute_batch(bootstrap.sql)?;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    for m in migrations {
      tx.execute(
        "INSERT INTO schema_migrations (version, name, applied_ts) VALUES (?1, ?2, ?3)",
        params![m.version, m.name, now],
      )?;
    }
  } else {
    for m in migrations {
      if m.version <= applied {
        continue;
      }
      tracing::info!(db, version = m.version, name = m.name, "applying migration");
      tx.execute_batch(m.sql)?;
      tx.execute(
        "INSERT INTO schema_migrations (version, name, applied_ts) VALUES (?1, ?2, ?3)",
        params![m.version, m.name, time::OffsetDateTime::now_utc().unix_timestamp()],
      )?;
    }
  }
  tx.commit()?;
  Ok(())
}

fn validate_sequence(migrations: &[Migration]) {
  let mut prev = 0u32;
  for m in migrations {
    assert!(
      m.version == prev + 1,
      "migration versions must be contiguous starting at 1; got {} after {}",
      m.version,
      prev
    );
    prev = m.version;
  }
}

pub fn latest_version(migrations: &[Migration]) -> u32 {
  validate_sequence(migrations);
  migrations.last().map(|m| m.version).unwrap_or(0)
}

pub fn read_current_version(conn: &Connection) -> Result<u32> {
  if !table_exists(conn, "schema_migrations")? {
    return Ok(0);
  }
  current_version(conn)
}

pub fn user_row_count(conn: &Connection) -> Result<u64> {
  let mut stmt = conn.prepare(
    "SELECT name FROM sqlite_master
     WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name <> 'schema_migrations'
     ORDER BY name",
  )?;
  let tables = stmt
    .query_map([], |r| r.get::<_, String>(0))?
    .collect::<rusqlite::Result<Vec<_>>>()?;
  let mut total = 0u64;
  for table in tables {
    let sql = format!("SELECT COUNT(*) FROM {}", quote_ident(&table));
    let count: i64 = conn.query_row(&sql, [], |r| r.get(0))?;
    total += count.max(0) as u64;
  }
  Ok(total)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
  Ok(
    conn
      .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1")?
      .exists(params![table])?,
  )
}

fn quote_ident(s: &str) -> String {
  format!("\"{}\"", s.replace('"', "\"\""))
}

fn ensure_schema_table(conn: &Connection) -> Result<()> {
  conn.execute_batch(
    r#"
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version    INTEGER PRIMARY KEY,
      name       TEXT    NOT NULL,
      applied_ts INTEGER NOT NULL
    );
    "#,
  )?;
  Ok(())
}

fn current_version(conn: &Connection) -> Result<u32> {
  let v: Option<i64> = conn
    .prepare("SELECT MAX(version) FROM schema_migrations")?
    .query_row([], |r| r.get(0))
    .unwrap_or(None);
  Ok(v.unwrap_or(0) as u32)
}

fn has_user_tables(conn: &Connection) -> Result<bool> {
  let n: i64 = conn
    .prepare(
      "SELECT COUNT(*) FROM sqlite_master
       WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name <> 'schema_migrations'",
    )?
    .query_row([], |r| r.get(0))?;
  Ok(n > 0)
}

/// Copy `path` to `path.bak`, truncating any existing backup. No-op if the
/// source file does not yet exist (fresh DB, nothing to preserve).
fn backup(path: &Path) -> Result<()> {
  if !path.exists() {
    return Ok(());
  }
  let bak = backup_path(path);
  // Use a temp file + rename so we never leave a half-written .bak.
  let tmp = bak.with_extension("bak.tmp");
  std::fs::copy(path, &tmp)?;
  if bak.exists() {
    std::fs::remove_file(&bak)?;
  }
  std::fs::rename(&tmp, &bak)?;
  tracing::debug!(src = %path.display(), bak = %bak.display(), "schema backup taken");
  Ok(())
}

pub fn backup_path(path: &Path) -> PathBuf {
  let mut s = path.as_os_str().to_owned();
  s.push(".bak");
  PathBuf::from(s)
}

/// Restore `path` from its `<path>.bak` snapshot. Returns `false` if no
/// backup exists.
pub fn rollback(path: &Path) -> Result<bool> {
  let bak = backup_path(path);
  if !bak.exists() {
    return Ok(false);
  }
  let tmp = path.with_extension("restore.tmp");
  std::fs::copy(&bak, &tmp)?;
  if path.exists() {
    std::fs::remove_file(path)?;
  }
  std::fs::rename(&tmp, path)?;
  std::fs::remove_file(&bak)?;
  tracing::info!(path = %path.display(), "schema restored from backup");
  Ok(true)
}

#[cfg(test)]
mod tests {
  use super::*;

  const BOOTSTRAP: &str = "CREATE TABLE foo (x INTEGER);";
  const M1: &str = "CREATE TABLE foo (x INTEGER);";
  const M2: &str = "ALTER TABLE foo ADD COLUMN y TEXT;";

  fn migs() -> Vec<Migration> {
    vec![
      Migration {
        version: 1,
        name: "initial",
        sql: M1,
      },
      Migration {
        version: 2,
        name: "add_y",
        sql: M2,
      },
    ]
  }

  #[test]
  fn fresh_db_takes_bootstrap_path_and_marks_all_versions_applied() {
    let dir = tempdir();
    let path = dir.join("t.db");
    let mut conn = Connection::open(&path).unwrap();
    apply(&mut conn, &path, "test", Bootstrap { sql: BOOTSTRAP }, &migs()).unwrap();
    let v = current_version(&conn).unwrap();
    assert_eq!(v, 2);
    // No backup taken on first init.
    assert!(!backup_path(&path).exists());
  }

  #[test]
  fn incremental_applies_only_pending_and_takes_backup() {
    let dir = tempdir();
    let path = dir.join("t.db");
    {
      let mut conn = Connection::open(&path).unwrap();
      let only_v1 = vec![Migration {
        version: 1,
        name: "initial",
        sql: M1,
      }];
      apply(&mut conn, &path, "test", Bootstrap { sql: BOOTSTRAP }, &only_v1).unwrap();
    }
    {
      let mut conn = Connection::open(&path).unwrap();
      apply(&mut conn, &path, "test", Bootstrap { sql: BOOTSTRAP }, &migs()).unwrap();
      assert_eq!(current_version(&conn).unwrap(), 2);
    }
    assert!(backup_path(&path).exists());
  }

  #[test]
  fn rollback_restores_backup() {
    let dir = tempdir();
    let path = dir.join("t.db");
    std::fs::write(&path, b"original").unwrap();
    std::fs::write(backup_path(&path), b"backup").unwrap();
    assert!(rollback(&path).unwrap());
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"backup");
    assert!(!backup_path(&path).exists());
  }

  fn tempdir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("tokn-router-mig-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&p).unwrap();
    p
  }
}
