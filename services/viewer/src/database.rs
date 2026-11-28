use rusqlite::Connection;
use std::fs::File;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;

use crate::days::DayFile;
use crate::Result;
use tokn_persistence::indexed_archive::{IndexedArchiveFormat, IndexedArchiveReader};
use tokn_persistence::request_archive::indexed::IndexedPackReader;
use tokn_persistence::request_files::{begin_read, ArchiveContents, ArchiveFormat, StableFile};

const READ_BUSY_TIMEOUT: Duration = Duration::from_millis(2_500);

type CompressedArchive = IndexedPackReader<IndexedArchiveReader<BufReader<File>>>;

pub(super) fn open_readonly(path: &Path) -> Result<Option<Connection>> {
  if !path.try_exists()? {
    return Ok(None);
  }
  Ok(Some(tokn_persistence::open_readonly(path, READ_BUSY_TIMEOUT)?))
}

pub(super) struct DayConnection(DayStorage);

enum DayStorage {
  Sqlite {
    // Drop the connection before removing its private snapshot on Windows.
    conn: Connection,
    snapshot: Option<(tempfile::TempDir, ArchiveContents)>,
  },
  Indexed(Box<CompressedArchive>),
}

impl DayConnection {
  pub(super) fn read_indexed_body(&self, request_id: &str, slot: u8) -> Result<Option<(Vec<u8>, u8)>> {
    if let DayStorage::Indexed(reader) = &self.0 {
      return Ok(reader.read_body(request_id, slot)?);
    }
    Ok(None)
  }

  pub(super) fn body_is_decoded(&self, request_id: &str, slot: u8) -> rusqlite::Result<bool> {
    if !matches!(
      self.0,
      DayStorage::Sqlite {
        snapshot: Some((_, ArchiveContents::RequestPack)),
        ..
      }
    ) {
      return Ok(false);
    }
    self.query_row(
      "SELECT EXISTS (SELECT 1 FROM tokn_archive_decoded_bodies WHERE request_id = ?1 AND slot = ?2)",
      (request_id, slot),
      |row| row.get(0),
    )
  }
}

impl Deref for DayConnection {
  type Target = Connection;
  fn deref(&self) -> &Connection {
    match &self.0 {
      DayStorage::Sqlite { conn, .. } => conn,
      DayStorage::Indexed(reader) => reader.connection(),
    }
  }
}

impl DayFile {
  pub(super) fn open(&self) -> Result<Option<DayConnection>> {
    self.open_with_timeout(READ_BUSY_TIMEOUT)
  }

  pub(super) fn open_with_timeout(&self, timeout: Duration) -> Result<Option<DayConnection>> {
    self.open_with_archive_scope(timeout, None)
  }

  pub(super) fn open_request(&self, request_id: &str) -> Result<Option<DayConnection>> {
    self.open_with_archive_scope(READ_BUSY_TIMEOUT, Some(request_id))
  }

  fn open_with_archive_scope(&self, timeout: Duration, request_id: Option<&str>) -> Result<Option<DayConnection>> {
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
    for &format in IndexedArchiveFormat::ALL {
      let path = format.path(&self.path);
      if !regular_file(&path)? {
        continue;
      }
      match open_indexed_archive(&path, format, timeout) {
        Ok(conn) => return Ok(Some(conn)),
        Err(err) => {
          error.get_or_insert(err);
        }
      }
    }
    for format in [ArchiveFormat::Zstd, ArchiveFormat::Xz] {
      let path = self.path.with_extension(format.extension());
      if !regular_file(&path)? {
        continue;
      }
      match open_archive(&path, format, timeout, request_id) {
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
    Ok(Some(DayConnection(DayStorage::Sqlite { conn, snapshot: None })))
  }
}

fn regular_file(path: &Path) -> std::io::Result<bool> {
  match path.symlink_metadata() {
    Ok(metadata) => Ok(metadata.is_file()),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
    Err(error) => Err(error),
  }
}

fn open_indexed_archive(path: &Path, format: IndexedArchiveFormat, timeout: Duration) -> Result<DayConnection> {
  let file = StableFile::open(path)?;
  let input = format.open(BufReader::new(file.file().try_clone()?))?;
  let reader = IndexedPackReader::open(input, timeout)?;
  Ok(DayConnection(DayStorage::Indexed(Box::new(reader))))
}

fn open_archive(
  path: &Path,
  format: ArchiveFormat,
  timeout: Duration,
  request_id: Option<&str>,
) -> Result<DayConnection> {
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
  let contents = match request_id {
    Some(request_id) => format.restore_request(input, &db, request_id)?,
    None => format.restore_metadata(input, &db)?,
  };
  let conn = tokn_persistence::open_readonly(&db, timeout)?;
  begin_read(&conn)?;
  Ok(DayConnection(DayStorage::Sqlite {
    conn,
    snapshot: Some((directory, contents)),
  }))
}
