use rusqlite::Connection;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;

use crate::days::DayFile;
use crate::Result;
use tokn_persistence::request_files::{begin_read, ArchiveFormat};

const READ_BUSY_TIMEOUT: Duration = Duration::from_millis(2_500);

pub(super) fn open_readonly(path: &Path) -> Result<Option<Connection>> {
  if !path.try_exists()? {
    return Ok(None);
  }
  Ok(Some(tokn_persistence::open_readonly(path, READ_BUSY_TIMEOUT)?))
}

/// Close SQLite before removing a private decompressed database, including on Windows.
pub(super) struct DayConnection {
  conn: Connection,
  _snapshot: Option<tempfile::TempDir>,
}

impl Deref for DayConnection {
  type Target = Connection;
  fn deref(&self) -> &Connection {
    &self.conn
  }
}

impl DayFile {
  pub(super) fn open(&self) -> Result<Option<DayConnection>> {
    self.open_with_timeout(READ_BUSY_TIMEOUT)
  }

  pub(super) fn open_with_timeout(&self, timeout: Duration) -> Result<Option<DayConnection>> {
    let mut error = match self.open_raw(timeout) {
      Ok(Some(conn)) => return Ok(Some(conn)),
      Ok(None) => None,
      Err(error) => {
        let busy = matches!(&error, crate::Error::Sqlite { source } if matches!(
          source.sqlite_error_code(), Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        ));
        if !busy && regular_file(&self.path)? {
          return Err(error);
        }
        Some(error)
      }
    };
    // The writer publishes an archive before pruning its raw database. Recheck
    // its actual filenames here even if enumeration saw only the raw file.
    for format in [ArchiveFormat::Zstd, ArchiveFormat::Xz] {
      let path = self.path.with_extension(format.extension());
      if !regular_file(&path)? {
        continue;
      }
      match open_archive(&path, format, timeout) {
        Ok(conn) => return Ok(Some(conn)),
        Err(err) => {
          error.get_or_insert(err);
        }
      }
    }
    match error {
      Some(err) => Err(err),
      None => Ok(None),
    }
  }

  fn open_raw(&self, timeout: Duration) -> Result<Option<DayConnection>> {
    if !regular_file(&self.path)? {
      return Ok(None);
    }
    let conn = tokn_persistence::open_readonly(&self.path, timeout)?;
    begin_read(&conn)?;
    Ok(Some(DayConnection { conn, _snapshot: None }))
  }
}

fn regular_file(path: &Path) -> std::io::Result<bool> {
  match path.symlink_metadata() {
    Ok(metadata) => Ok(metadata.is_file()),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
    Err(error) => Err(error),
  }
}

fn open_archive(path: &Path, format: ArchiveFormat, timeout: Duration) -> Result<DayConnection> {
  let input = BufReader::new(File::open(path)?);
  let mut builder = tempfile::Builder::new();
  builder.prefix("tokn-viewer-");
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    builder.permissions(std::fs::Permissions::from_mode(0o700));
  }
  let directory = builder.tempdir()?;
  let db = directory.path().join("requests.db");
  {
    let mut output = BufWriter::new(File::create_new(&db)?);
    format.decode(input, &mut output)?;
    output.flush()?;
  }
  let conn = tokn_persistence::open_readonly(&db, timeout)?;
  begin_read(&conn)?;
  Ok(DayConnection {
    conn,
    _snapshot: Some(directory),
  })
}
