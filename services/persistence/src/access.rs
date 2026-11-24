//! Persistent client API-key records.
//!
//! Authentication policy and secret verification live in `tokn-access`;
//! this module owns only SQLite lifecycle, schema migration, and row storage.

use crate::{migrate, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};

const BOOTSTRAP: &str = include_str!("../schemas/snapshot/access/v0.2.1.sql");
const MIGRATIONS: &[migrate::Migration] = &[migrate::Migration {
  version: 1,
  name: "initial",
  sql: include_str!("../schemas/snapshot/access/v0.2.1.sql"),
}];

pub fn latest_version() -> u32 {
  migrate::latest_version(MIGRATIONS)
}

pub struct NewApiKeyRecord<'a> {
  pub id: &'a str,
  pub name: &'a str,
  pub secret_hash: &'a [u8],
  pub allowed_providers: &'a str,
  pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyRecord {
  pub name: String,
  pub secret_hash: Vec<u8>,
  pub allowed_providers: String,
  pub revoked_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeySummaryRecord {
  pub id: String,
  pub name: String,
  pub allowed_providers: String,
  pub created_at: i64,
  pub revoked_at: Option<i64>,
}

pub struct AccessDb {
  path: PathBuf,
  conn: Connection,
}

impl AccessDb {
  pub fn open(path: impl AsRef<Path>) -> Result<Self> {
    let path = path.as_ref().to_path_buf();
    if let Some(parent) = path.parent() {
      std::fs::create_dir_all(parent)?;
      secure_dir(parent)?;
    }
    let conn = Connection::open(&path)?;
    Self::initialize(path, conn, true)
  }

  pub fn open_in_memory() -> Result<Self> {
    Self::initialize(PathBuf::from(":memory:"), Connection::open_in_memory()?, false)
  }

  fn initialize(path: PathBuf, mut conn: Connection, secure: bool) -> Result<Self> {
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")?;
    migrate::apply(
      &mut conn,
      &path,
      "access",
      migrate::Bootstrap { sql: BOOTSTRAP },
      MIGRATIONS,
    )?;
    if secure {
      secure_file(&path)?;
    }
    Ok(Self { path, conn })
  }

  pub fn path(&self) -> &Path {
    &self.path
  }

  pub fn insert_key(&self, record: &NewApiKeyRecord<'_>) -> Result<bool> {
    let changed = self.conn.execute(
      "INSERT OR IGNORE INTO api_keys (id, name, secret_hash, allowed_providers, created_at)
       VALUES (?1, ?2, ?3, ?4, ?5)",
      params![
        record.id,
        record.name,
        record.secret_hash,
        record.allowed_providers,
        record.created_at
      ],
    )?;
    Ok(changed == 1)
  }

  pub fn list_keys(&self) -> Result<Vec<ApiKeySummaryRecord>> {
    let mut statement = self.conn.prepare(
      "SELECT id, name, allowed_providers, created_at, revoked_at
       FROM api_keys ORDER BY created_at, id",
    )?;
    let records = statement
      .query_map([], |row| {
        Ok(ApiKeySummaryRecord {
          id: row.get(0)?,
          name: row.get(1)?,
          allowed_providers: row.get(2)?,
          created_at: row.get(3)?,
          revoked_at: row.get(4)?,
        })
      })?
      .collect::<rusqlite::Result<_>>()?;
    Ok(records)
  }

  pub fn revoke_key(&self, id: &str, revoked_at: i64) -> Result<bool> {
    let changed = self.conn.execute(
      "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, ?1) WHERE id = ?2",
      params![revoked_at, id],
    )?;
    Ok(changed == 1)
  }

  pub fn find_key(&self, id: &str) -> Result<Option<ApiKeyRecord>> {
    self
      .conn
      .query_row(
        "SELECT name, secret_hash, allowed_providers, revoked_at FROM api_keys WHERE id = ?1",
        [id],
        |row| {
          Ok(ApiKeyRecord {
            name: row.get(0)?,
            secret_hash: row.get(1)?,
            allowed_providers: row.get(2)?,
            revoked_at: row.get(3)?,
          })
        },
      )
      .optional()
      .map_err(Into::into)
  }
}

#[cfg(unix)]
fn secure_dir(path: &Path) -> Result<()> {
  use std::os::unix::fs::PermissionsExt;
  let mut permissions = std::fs::metadata(path)?.permissions();
  permissions.set_mode(0o700);
  std::fs::set_permissions(path, permissions)?;
  Ok(())
}

#[cfg(not(unix))]
fn secure_dir(_path: &Path) -> Result<()> {
  Ok(())
}

#[cfg(unix)]
fn secure_file(path: &Path) -> Result<()> {
  use std::os::unix::fs::PermissionsExt;
  let mut permissions = std::fs::metadata(path)?.permissions();
  permissions.set_mode(0o600);
  std::fs::set_permissions(path, permissions)?;
  Ok(())
}

#[cfg(not(unix))]
fn secure_file(_path: &Path) -> Result<()> {
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  fn record<'a>(hash: &'a [u8]) -> NewApiKeyRecord<'a> {
    NewApiKeyRecord {
      id: "key-id",
      name: "client",
      secret_hash: hash,
      allowed_providers: "[\"*\"]",
      created_at: 123,
    }
  }

  #[test]
  fn creates_v0_2_1_schema_and_round_trips_records() {
    let db = AccessDb::open_in_memory().unwrap();
    assert_eq!(latest_version(), 1);
    assert!(db.insert_key(&record(&[1; 32])).unwrap());
    assert!(!db.insert_key(&record(&[1; 32])).unwrap());

    let found = db.find_key("key-id").unwrap().unwrap();
    assert_eq!(found.name, "client");
    assert_eq!(found.secret_hash, vec![1; 32]);
    assert_eq!(db.list_keys().unwrap().len(), 1);
    assert!(db.revoke_key("key-id", 456).unwrap());
    assert_eq!(db.find_key("key-id").unwrap().unwrap().revoked_at, Some(456));
  }

  #[cfg(unix)]
  #[test]
  fn secures_the_database_directory_and_file() {
    use std::os::unix::fs::PermissionsExt;

    let directory = std::env::temp_dir().join(format!("tokn-access-permissions-{}", uuid::Uuid::new_v4()));
    let path = directory.join("access.db");
    let db = AccessDb::open(&path).unwrap();

    assert_eq!(
      std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
      0o700
    );
    assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    drop(db);
    std::fs::remove_dir_all(directory).unwrap();
  }
}
