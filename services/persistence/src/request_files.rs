//! Storage-level access to live and archived daily request databases.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

pub const MAINTENANCE_LOCK_FILE: &str = ".tokn-requests-maintenance.lock";
pub struct RequestMaintenanceLock {
  _file: File,
}

pub struct StableFile {
  path: PathBuf,
  file: File,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveFormat {
  Zstd,
  Xz,
}

impl ArchiveFormat {
  pub fn from_path(path: &Path) -> Option<Self> {
    let name = path.file_name()?.to_str()?;
    if name.ends_with(".db.zstd") {
      Some(Self::Zstd)
    } else if name.ends_with(".db.xz") {
      Some(Self::Xz)
    } else {
      None
    }
  }

  /// Decode a database snapshot into the caller's private destination.
  pub fn decode(&self, reader: impl Read, mut writer: impl Write) -> io::Result<u64> {
    match self {
      Self::Zstd => io::copy(&mut zstd::Decoder::new(reader)?, &mut writer),
      #[cfg(feature = "lzma")]
      Self::Xz => io::copy(&mut lzma_rust2::XzReader::new(reader, false), &mut writer),
      #[cfg(not(feature = "lzma"))]
      Self::Xz => Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "xz archive decompression requires the lzma feature",
      )),
    }
  }
  pub fn extension(self) -> &'static str {
    match self {
      Self::Zstd => "db.zstd",
      Self::Xz => "db.xz",
    }
  }
}

impl RequestMaintenanceLock {
  pub fn acquire(dir: &Path) -> io::Result<Self> {
    let lock_path = dir.join(MAINTENANCE_LOCK_FILE);
    let file = open_maintenance_lock(&lock_path)?;
    match file.try_lock() {
      Ok(()) => {
        revalidate_opened_file(&lock_path, &file, true)?;
        Ok(Self { _file: file })
      }
      Err(fs::TryLockError::WouldBlock) => Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        format!(
          "request database maintenance is already running for {}; retry the command",
          dir.display()
        ),
      )),
      Err(fs::TryLockError::Error(error)) => Err(error),
    }
  }

  pub(crate) fn acquire_writer(dir: &Path) -> io::Result<Self> {
    let lock_path = dir.join(MAINTENANCE_LOCK_FILE);
    let file = open_maintenance_lock(&lock_path)?;
    file.lock()?;
    revalidate_opened_file(&lock_path, &file, true)?;
    Ok(Self { _file: file })
  }
}

impl StableFile {
  pub fn open(path: &Path) -> io::Result<Self> {
    validate_regular_file_path(path, false)?;
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options);
    let file = options.open(path)?;
    revalidate_opened_file(path, &file, true)?;
    Ok(Self {
      path: path.to_path_buf(),
      file,
    })
  }

  pub fn revalidate(&self) -> io::Result<()> {
    self.revalidate_at(&self.path)
  }

  pub fn revalidate_at(&self, path: &Path) -> io::Result<()> {
    revalidate_opened_file(path, &self.file, true)
  }

  pub fn file(&self) -> &File {
    &self.file
  }
  pub fn file_mut(&mut self) -> &mut File {
    &mut self.file
  }
  pub fn path(&self) -> &Path {
    &self.path
  }
}

fn open_maintenance_lock(path: &Path) -> io::Result<File> {
  validate_regular_file_path(path, true)?;
  let mut options = OpenOptions::new();
  options.create(true).truncate(false).read(true).write(true);
  configure_no_follow(&mut options);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
  }
  let file = options.open(path)?;
  revalidate_opened_file(path, &file, true)?;
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
  }
  Ok(file)
}

#[cfg(unix)]
fn configure_no_follow(options: &mut OpenOptions) {
  use std::os::unix::fs::OpenOptionsExt;
  options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_no_follow(options: &mut OpenOptions) {
  use std::os::windows::fs::OpenOptionsExt;
  const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
  options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_no_follow(_options: &mut OpenOptions) {}

fn validate_regular_file_path(path: &Path, allow_missing: bool) -> io::Result<()> {
  match fs::symlink_metadata(path) {
    Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_file(path, "must not be a symlink")),
    Ok(metadata) if !metadata.is_file() => Err(invalid_file(path, "must be a regular file")),
    Ok(_) => Ok(()),
    Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => Ok(()),
    Err(error) => Err(error),
  }
}

fn revalidate_opened_file(path: &Path, file: &File, require_single_link: bool) -> io::Result<()> {
  validate_regular_file_path(path, false)?;
  #[cfg(unix)]
  {
    use std::os::unix::fs::MetadataExt;
    let opened = file.metadata()?;
    let linked = fs::symlink_metadata(path)?;
    if (opened.dev(), opened.ino()) != (linked.dev(), linked.ino()) {
      return Err(invalid_file(path, "changed while it was being opened or used"));
    }
  }
  #[cfg(windows)]
  {
    let opened = same_file::Handle::from_file(file.try_clone()?)?;
    let linked = same_file::Handle::from_path(path)?;
    if opened != linked {
      return Err(invalid_file(path, "changed while it was being opened or used"));
    }
  }
  #[cfg(any(unix, windows))]
  if require_single_link && file_link_count(file)? != 1 {
    return Err(invalid_file(path, "must not have multiple hard links"));
  }
  #[cfg(not(any(unix, windows)))]
  let _ = require_single_link;
  Ok(())
}

#[cfg(unix)]
fn file_link_count(file: &File) -> io::Result<u64> {
  use std::os::unix::fs::MetadataExt;
  Ok(file.metadata()?.nlink())
}

#[cfg(windows)]
fn file_link_count(file: &File) -> io::Result<u64> {
  use std::os::windows::io::AsRawHandle;
  use windows_sys::Win32::Storage::FileSystem::{GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION};

  let mut info = BY_HANDLE_FILE_INFORMATION::default();
  // SAFETY: the raw handle remains owned by `file` and `info` is writable for the duration of the call.
  if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
    return Err(io::Error::last_os_error());
  }
  Ok(u64::from(info.nNumberOfLinks))
}

fn invalid_file(path: &Path, reason: &str) -> io::Error {
  io::Error::new(io::ErrorKind::InvalidInput, format!("{} {reason}", path.display()))
}

/// Establish the read snapshot before a viewer starts querying a daily database.
/// SQLite releases this read transaction when the connection closes.
pub fn begin_read(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
  conn.execute_batch("BEGIN DEFERRED; SELECT rootpage FROM sqlite_schema LIMIT 1;")
}

/// Keep this daily file idle through verification and deletion. SQLite's normal
/// read transactions participate even when another process opened the database.
/// Windows SQLite opens the main file without delete sharing, so rename itself
/// provides exclusion there.
pub struct PruneGuard {
  #[cfg(not(windows))]
  _connection: Option<rusqlite::Connection>,
}

impl PruneGuard {
  /// An exclusive transaction on a previously quiescent WAL database can
  /// create its own empty WAL. It contains no frames that require checkpointing.
  pub fn holds_exclusive_transaction(&self) -> bool {
    #[cfg(not(windows))]
    {
      self._connection.is_some()
    }
    #[cfg(windows)]
    {
      false
    }
  }

  pub fn try_acquire(path: &Path) -> io::Result<Self> {
    validate_regular_file_path(path, false)?;
    #[cfg(not(windows))]
    {
      use rusqlite::{Connection, ErrorCode, OpenFlags};
      let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW;
      // SQLite NOFOLLOW also rejects ancestor symlinks. Resolve the directory,
      // keeping the verified leaf unchanged so replacement symlinks stay rejected.
      let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
      let filename = path
        .file_name()
        .ok_or_else(|| invalid_file(path, "must name a database"))?;
      let conn = Connection::open_with_flags(dir.canonicalize()?.join(filename), flags).map_err(io::Error::other)?;
      conn.busy_timeout(std::time::Duration::ZERO).map_err(io::Error::other)?;
      // Exclusive locking mode also excludes other clients of a WAL database.
      // The transaction makes no changes to the verified database.
      match conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;") {
        Ok(()) => {}
        // Existing byte-for-byte archives may also contain damaged or legacy
        // files that SQLite cannot query. Their full digest is still checked.
        Err(error) if error.sqlite_error_code() == Some(ErrorCode::NotADatabase) => {
          return Ok(Self { _connection: None });
        }
        Err(error)
          if matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
          ) =>
        {
          return Err(io::Error::new(io::ErrorKind::WouldBlock, "request database is in use"));
        }
        Err(error) => return Err(io::Error::other(error)),
      }
      Ok(Self {
        _connection: Some(conn),
      })
    }
    #[cfg(windows)]
    Ok(Self {})
  }
}
