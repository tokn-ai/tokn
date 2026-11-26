//! Archive, verify and prune rotated request databases.

mod worker;
pub use tokn_persistence::request_files::ArchiveFormat;
use tokn_persistence::request_files::{RequestMaintenanceLock, StableFile};
pub use worker::*;

use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use time::{Date, Duration, Month, OffsetDateTime};

const SECONDS_PER_DAY: i64 = 86_400;
static PRUNE_QUARANTINE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArchiveStats {
  pub archived: usize,
  pub skipped_existing: usize,
  pub failed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PruneReport {
  pub cutoff: Date,
  pub entries: Vec<PruneEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PruneEntry {
  pub path: PathBuf,
  pub archive: PathBuf,
  pub outcome: PruneOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PruneOutcome {
  Verified {
    sha256: String,
  },
  Deleted {
    sha256: String,
  },
  MissingArchive,
  InUse,
  HashMismatch {
    source_sha256: String,
    archived_sha256: String,
  },
  Failed {
    error: String,
  },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PruneProgressEvent {
  Started {
    files_total: usize,
  },
  FileStarted {
    path: PathBuf,
    file_index: usize,
    files_total: usize,
    bytes_total: u64,
  },
  FileProgress {
    bytes_processed: u64,
    bytes_total: u64,
  },
  FileFinished {
    path: PathBuf,
    file_index: usize,
    files_total: usize,
  },
  Finished {
    files_total: usize,
  },
}

fn resolve_format(configured_extension: Option<&str>) -> ArchiveFormat {
  let Some(value) = configured_extension.map(str::trim).filter(|value| !value.is_empty()) else {
    return if cfg!(feature = "lzma") {
      ArchiveFormat::Xz
    } else {
      ArchiveFormat::Zstd
    };
  };
  let normalized = value.trim_start_matches('.').to_ascii_lowercase();
  match normalized.as_str() {
    "zstd" | "db.zstd" => ArchiveFormat::Zstd,
    "xz" | "db.xz" | "lzma" | "db.lzma" if cfg!(feature = "lzma") => ArchiveFormat::Xz,
    "xz" | "db.xz" | "lzma" | "db.lzma" => {
      tracing::warn!(
        archive_extension = value,
        "LZMA archive extension requested without lzma feature; falling back to zstd"
      );
      ArchiveFormat::Zstd
    }
    _ => {
      tracing::warn!(
        archive_extension = value,
        "unsupported archive extension; falling back to zstd"
      );
      ArchiveFormat::Zstd
    }
  }
}

/// Verify old request database archives and optionally remove their source
/// `.db` files. Archive bytes are decompressed before hashing so the digest is
/// compared against the original database content.
pub fn prune_request_dbs(
  dir: &Path,
  archive_extension: Option<&str>,
  prune_after_days: i64,
  commit: bool,
) -> io::Result<PruneReport> {
  prune_request_dbs_with_progress(dir, archive_extension, prune_after_days, commit, |_| {})
}

pub fn prune_request_dbs_with_progress(
  dir: &Path,
  archive_extension: Option<&str>,
  prune_after_days: i64,
  commit: bool,
  mut progress: impl FnMut(PruneProgressEvent),
) -> io::Result<PruneReport> {
  prune_request_dbs_once_with_progress(
    dir,
    OffsetDateTime::now_utc().date(),
    prune_after_days,
    resolve_format(archive_extension),
    commit,
    &mut progress,
  )
}

#[cfg(test)]
fn prune_request_dbs_once(
  dir: &Path,
  today: Date,
  prune_after_days: i64,
  format: ArchiveFormat,
  commit: bool,
) -> io::Result<PruneReport> {
  prune_request_dbs_once_with_progress(
    dir,
    today,
    prune_after_days,
    format,
    commit,
    &mut |_: PruneProgressEvent| {},
  )
}

fn prune_request_dbs_once_with_progress(
  dir: &Path,
  today: Date,
  prune_after_days: i64,
  format: ArchiveFormat,
  commit: bool,
  progress: &mut impl FnMut(PruneProgressEvent),
) -> io::Result<PruneReport> {
  let cutoff = retention_cutoff(today, prune_after_days, "prune")?;
  let mut report = PruneReport {
    cutoff,
    entries: Vec::new(),
  };
  let _lock = match RequestMaintenanceLock::acquire(dir) {
    Ok(lock) => lock,
    Err(error) if error.kind() == io::ErrorKind::NotFound => {
      progress(PruneProgressEvent::Started { files_total: 0 });
      progress(PruneProgressEvent::Finished { files_total: 0 });
      return Ok(report);
    }
    Err(error) => return Err(error),
  };
  let mut paths = Vec::new();
  for entry in fs::read_dir(dir)? {
    let path = entry?.path();
    if is_archivable_request_db(&path, cutoff) {
      paths.push(path);
    }
  }
  paths.sort();

  let files_total = paths.len();
  progress(PruneProgressEvent::Started { files_total });
  for (file_index, path) in paths.into_iter().enumerate() {
    let archive = archive_path(&path, format);
    let bytes_per_pass = fs::symlink_metadata(&path).map(|metadata| metadata.len()).unwrap_or(0);
    let bytes_total = bytes_per_pass.saturating_mul(if commit { 4 } else { 2 });
    progress(PruneProgressEvent::FileStarted {
      path: path.clone(),
      file_index,
      files_total,
      bytes_total,
    });
    let mut bytes_processed = 0u64;
    let mut report_bytes = |bytes: u64| {
      bytes_processed = bytes_processed.saturating_add(bytes);
      progress(PruneProgressEvent::FileProgress {
        bytes_processed,
        bytes_total,
      });
    };
    let outcome = prune_request_db(&path, &archive, format, commit, None, &mut report_bytes);
    progress(PruneProgressEvent::FileFinished {
      path: path.clone(),
      file_index,
      files_total,
    });
    report.entries.push(PruneEntry { path, archive, outcome });
  }
  progress(PruneProgressEvent::Finished { files_total });
  Ok(report)
}

fn prune_request_db(
  db: &Path,
  archive: &Path,
  format: ArchiveFormat,
  commit: bool,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> PruneOutcome {
  let mut archive_file = match StableFile::open(archive) {
    Ok(file) => file,
    Err(error) if error.kind() == io::ErrorKind::NotFound => return PruneOutcome::MissingArchive,
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("open archive {}: {error}", archive.display()),
      };
    }
  };
  if let Err(error) = check_request_db_sidecars(db, false) {
    return PruneOutcome::Failed {
      error: format!("verify request database {}: {error}", db.display()),
    };
  }

  let idle = if commit {
    match tokn_persistence::request_files::PruneGuard::try_acquire(db) {
      Ok(guard) => Some(guard),
      Err(error) if error.kind() == io::ErrorKind::WouldBlock => return PruneOutcome::InUse,
      Err(error) => {
        return PruneOutcome::Failed {
          error: format!("lock request database {}: {error}", db.display()),
        }
      }
    }
  } else {
    None
  };

  // Exclusive-mode SQLite may create a zero-byte WAL while acquiring the guard.
  // Keep its identity so we can retire only our own file before unlinking the main DB.
  let owned_wal = if idle.as_ref().is_some_and(|guard| guard.holds_exclusive_transaction()) {
    match StableFile::open(&sidecar_path(db, "-wal")) {
      Ok(wal) => Some(wal),
      Err(error) if error.kind() == io::ErrorKind::NotFound => None,
      Err(error) => {
        return PruneOutcome::Failed {
          error: format!("check exclusive SQLite WAL: {error}"),
        }
      }
    }
  } else {
    None
  };

  let mut source_file = match StableFile::open(db) {
    Ok(file) => file,
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("open request database {}: {error}", db.display()),
      };
    }
  };
  let archived_digest = match hash_archive(&mut archive_file, format, cancelled, progress) {
    Ok(digest) => digest,
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("verify archive {}: {error}", archive.display()),
      };
    }
  };
  let source_digest = match hash_file(&mut source_file, cancelled, progress) {
    Ok(digest) => digest,
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("hash request database {}: {error}", db.display()),
      };
    }
  };
  let source_sha256 = hex_digest(&source_digest);
  if source_digest != archived_digest {
    return PruneOutcome::HashMismatch {
      source_sha256,
      archived_sha256: hex_digest(&archived_digest),
    };
  }
  if !commit {
    return PruneOutcome::Verified { sha256: source_sha256 };
  }

  match hash_archive(&mut archive_file, format, cancelled, progress) {
    Ok(current_digest) if current_digest == archived_digest => {}
    Ok(_) => {
      return PruneOutcome::Failed {
        error: format!("archive {} changed during verification", archive.display()),
      };
    }
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("recheck archive {}: {error}", archive.display()),
      };
    }
  }
  match hash_file(&mut source_file, cancelled, progress) {
    Ok(current_digest) if current_digest == source_digest => {}
    Ok(_) => {
      return PruneOutcome::Failed {
        error: format!("request database {} changed during verification", db.display()),
      };
    }
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("recheck request database {}: {error}", db.display()),
      };
    }
  }
  let result = sync_archive_before_prune(&archive_file)
    .and_then(|()| check_request_db_sidecars(db, owned_wal.is_some()))
    .and_then(|()| check_cancelled(cancelled))
    .and_then(|()| archive_file.revalidate())
    .and_then(|()| {
      if let Some(wal) = owned_wal {
        wal.revalidate()?;
        if wal.file().metadata()?.len() != 0 {
          return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "exclusive SQLite WAL contains data",
          ));
        }
        remove_stable_file(wal)?;
      }
      remove_stable_file(source_file)
    });
  match result {
    Ok(()) => PruneOutcome::Deleted { sha256: source_sha256 },
    #[cfg(windows)]
    Err(error) if error.raw_os_error() == Some(32) => PruneOutcome::InUse, // ERROR_SHARING_VIOLATION
    Err(error) => PruneOutcome::Failed {
      error: format!("delete verified request database {}: {error}", db.display()),
    },
  }
}

fn hash_file(
  file: &mut StableFile,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  file.file_mut().rewind()?;
  let digest = hash_reader(file.file_mut(), cancelled, progress)?;
  file.revalidate()?;
  Ok(digest)
}

fn hash_archive(
  file: &mut StableFile,
  format: ArchiveFormat,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  file.file_mut().rewind()?;
  let digest = match format {
    ArchiveFormat::Zstd => hash_reader(zstd::stream::read::Decoder::new(file.file_mut())?, cancelled, progress),
    ArchiveFormat::Xz => hash_xz_archive(file.file_mut(), cancelled, progress),
  }?;
  file.revalidate()?;
  Ok(digest)
}

fn sync_archive_before_prune(archive: &StableFile) -> io::Result<()> {
  // Sync the archive and its directory entry before removing the source on Unix.
  #[cfg(unix)]
  {
    archive.file().sync_all()?;
    let parent = archive.path().parent().filter(|path| !path.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()?;
  }
  #[cfg(not(unix))]
  let _ = archive;
  Ok(())
}

fn sidecar_path(db: &Path, suffix: &str) -> PathBuf {
  let mut path = db.as_os_str().to_owned();
  path.push(suffix);
  PathBuf::from(path)
}

fn check_request_db_sidecars(db: &Path, allow_empty_wal: bool) -> io::Result<()> {
  for suffix in ["-wal", "-journal", "-shm"] {
    match fs::symlink_metadata(sidecar_path(db, suffix)) {
      Ok(metadata) if allow_empty_wal && suffix == "-wal" && metadata.is_file() && metadata.len() == 0 => {}
      Ok(_) => {
        return Err(io::Error::new(
          io::ErrorKind::InvalidInput,
          format!("found SQLite {suffix} sidecar"),
        ));
      }
      Err(error) if error.kind() == io::ErrorKind::NotFound => {}
      Err(error) => return Err(error),
    }
  }
  Ok(())
}

fn remove_stable_file(file: StableFile) -> io::Result<()> {
  file.revalidate()?;
  let quarantine = unused_quarantine_path(file.path())?;
  fs::rename(file.path(), &quarantine)?;
  if let Err(error) = file.revalidate_at(&quarantine) {
    return Err(restore_quarantined_file(&quarantine, file.path(), error));
  }
  if let Err(error) = fs::remove_file(&quarantine) {
    return Err(restore_quarantined_file(&quarantine, file.path(), error));
  }
  Ok(())
}

fn unused_quarantine_path(path: &Path) -> io::Result<PathBuf> {
  let parent = path.parent().unwrap_or_else(|| Path::new("."));
  let name = path
    .file_name()
    .ok_or_else(|| {
      io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{} must name a file", path.display()),
      )
    })?
    .to_string_lossy();
  loop {
    let sequence = PRUNE_QUARANTINE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let candidate = parent.join(format!(".{name}.prune-{}-{sequence}.tmp", std::process::id()));
    match fs::symlink_metadata(&candidate) {
      Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(candidate),
      Err(error) => return Err(error),
      Ok(_) => {}
    }
  }
}

fn restore_quarantined_file(quarantine: &Path, original: &Path, cause: io::Error) -> io::Error {
  match fs::symlink_metadata(original) {
    Ok(_) => {
      return io::Error::new(
        cause.kind(),
        format!(
          "{cause}; verified data was retained at {} because {} was recreated",
          quarantine.display(),
          original.display()
        ),
      );
    }
    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
    Err(error) => {
      return io::Error::new(
        cause.kind(),
        format!(
          "{cause}; verified data was retained at {} because inspecting {} failed: {error}",
          quarantine.display(),
          original.display()
        ),
      );
    }
  }
  match fs::rename(quarantine, original) {
    Ok(()) => cause,
    Err(restore_error) => io::Error::new(
      cause.kind(),
      format!(
        "{cause}; verified data was retained at {} because restoring {} failed: {restore_error}",
        quarantine.display(),
        original.display()
      ),
    ),
  }
}

fn hash_reader(
  mut reader: impl Read,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  let mut digest = Sha256::new();
  let mut buffer = [0u8; 64 * 1024];
  loop {
    check_cancelled(cancelled)?;
    let read = reader.read(&mut buffer)?;
    if read == 0 {
      break;
    }
    digest.update(&buffer[..read]);
    progress(read as u64);
  }
  Ok(digest.finalize().into())
}

fn hex_digest(digest: &[u8; 32]) -> String {
  let mut encoded = String::with_capacity(digest.len() * 2);
  for byte in digest {
    write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
  }
  encoded
}

fn retention_cutoff(today: Date, retention_days: i64, operation: &str) -> io::Result<Date> {
  if retention_days <= 0 {
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("{operation}_after_days must be greater than zero"),
    ));
  }
  let seconds = retention_days.checked_mul(SECONDS_PER_DAY).ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("{operation}_after_days is too large for a time duration"),
    )
  })?;
  today.checked_sub(Duration::seconds(seconds)).ok_or_else(|| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("{operation} retention cutoff is outside the supported date range"),
    )
  })
}

#[cfg(feature = "lzma")]
fn hash_xz_archive(
  file: impl Read,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  hash_reader(lzma_rust2::XzReader::new(file, false), cancelled, progress)
}

#[cfg(not(feature = "lzma"))]
fn hash_xz_archive(
  _file: impl Read,
  _cancelled: Option<&AtomicBool>,
  _progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  Err(io::Error::new(
    io::ErrorKind::Unsupported,
    "xz archive decompression requires the lzma feature",
  ))
}

/// Archive eligible daily databases without pruning their sources.
pub fn archive_requests_once(
  dir: &Path,
  today: Date,
  archive_after_days: i64,
  format: ArchiveFormat,
) -> io::Result<ArchiveStats> {
  archive_requests_once_with_events(dir, today, archive_after_days, None, format, None, None)
}

fn archive_requests_once_with_events(
  dir: &Path,
  today: Date,
  archive_after_days: i64,
  prune_after_days: Option<i64>,
  format: ArchiveFormat,
  events: Option<&dyn ArchiveEmitter>,
  cancelled: Option<&AtomicBool>,
) -> io::Result<ArchiveStats> {
  let cutoff = retention_cutoff(today, archive_after_days, "archive")?;
  let prune_cutoff = prune_after_days.and_then(|days| match retention_cutoff(today, days, "prune") {
    Ok(cutoff) => Some(cutoff),
    Err(error) => {
      tracing::warn!(path = %dir.display(), error = %error, "request db prune skipped");
      None
    }
  });
  let _lock = match RequestMaintenanceLock::acquire(dir) {
    Ok(lock) => lock,
    Err(error) if error.kind() == io::ErrorKind::NotFound => return empty_archive_scan(dir, events),
    Err(error) => return Err(error),
  };
  archive_requests_once_locked(dir, cutoff, prune_cutoff, format, events, cancelled)
}

fn empty_archive_scan(dir: &Path, events: Option<&dyn ArchiveEmitter>) -> io::Result<ArchiveStats> {
  let stats = ArchiveStats::default();
  emit(events, ArchiveEvent::ScanStarted { dir: dir.to_path_buf() });
  emit(
    events,
    ArchiveEvent::ScanCompleted {
      dir: dir.to_path_buf(),
      stats: stats.clone(),
    },
  );
  Ok(stats)
}

fn archive_requests_once_locked(
  dir: &Path,
  cutoff: Date,
  prune_cutoff: Option<Date>,
  format: ArchiveFormat,
  events: Option<&dyn ArchiveEmitter>,
  cancelled: Option<&AtomicBool>,
) -> io::Result<ArchiveStats> {
  let mut stats = ArchiveStats::default();
  emit(events, ArchiveEvent::ScanStarted { dir: dir.to_path_buf() });
  for entry in fs::read_dir(dir)? {
    check_cancelled(cancelled)?;
    let entry = entry?;
    let path = entry.path();
    let Some(day) = request_db_day(&path).filter(|day| *day <= cutoff) else {
      continue;
    };

    let archive = archive_path(&path, format);
    if archive.exists() {
      stats.skipped_existing += 1;
      emit(
        events,
        ArchiveEvent::FileSkipped {
          path: path.clone(),
          archive: archive.clone(),
        },
      );
    } else {
      let id = archive_id(&path);
      match compress_db(&path, &archive, format, events, cancelled, &id) {
        Ok(()) => stats.archived += 1,
        Err(err) if err.kind() == io::ErrorKind::Interrupted => return Err(err),
        Err(err) => {
          stats.failed += 1;
          tracing::warn!(path = %path.display(), archive = %archive.display(), error = %err, "request db archive failed");
          emit(
            events,
            ArchiveEvent::FileFailed {
              id,
              path,
              archive,
              error: err.to_string(),
            },
          );
          continue;
        }
      }
    }

    if prune_cutoff.is_some_and(|cutoff| day <= cutoff) {
      check_cancelled(cancelled)?;
      let outcome = prune_request_db(&path, &archive, format, true, cancelled, &mut |_| {});
      check_cancelled(cancelled)?;
      match outcome {
        PruneOutcome::Deleted { sha256 } => tracing::info!(
          path = %path.display(), archive = %archive.display(), sha256, "pruned verified request db"
        ),
        PruneOutcome::InUse => {
          tracing::debug!(path = %path.display(), "request database is in use; retaining until the next scan")
        }
        outcome => tracing::warn!(
          path = %path.display(), archive = %archive.display(), ?outcome, "request db retained during prune"
        ),
      }
    }
  }

  emit(
    events,
    ArchiveEvent::ScanCompleted {
      dir: dir.to_path_buf(),
      stats: stats.clone(),
    },
  );
  Ok(stats)
}

fn is_archivable_request_db(path: &Path, cutoff: Date) -> bool {
  request_db_day(path).is_some_and(|day| day <= cutoff)
}

fn request_db_day(path: &Path) -> Option<Date> {
  if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
    || path.extension().and_then(|v| v.to_str()) != Some("db")
  {
    return None;
  }
  parse_day(path.file_stem()?.to_str()?)
}

fn archive_path(path: &Path, format: ArchiveFormat) -> PathBuf {
  path.with_extension(format.extension())
}

fn temp_archive_path(archive: &Path) -> PathBuf {
  let mut name = archive
    .file_name()
    .and_then(|v| v.to_str())
    .unwrap_or("archive.db.zstd")
    .to_string();
  name.push_str(".tmp");
  archive.with_file_name(name)
}

fn compress_db(
  db: &Path,
  archive: &Path,
  format: ArchiveFormat,
  events: Option<&dyn ArchiveEmitter>,
  cancelled: Option<&AtomicBool>,
  id: &str,
) -> io::Result<()> {
  let temp = temp_archive_path(archive);
  if temp.exists() {
    fs::remove_file(&temp)?;
  }

  check_request_db_sidecars(db, false)?;
  let mut source = StableFile::open(db)?;
  let total_bytes = source.file().metadata()?.len();
  emit(
    events,
    ArchiveEvent::FileStarted {
      id: id.to_string(),
      path: db.to_path_buf(),
      archive: archive.to_path_buf(),
      total_bytes,
    },
  );

  let result = (|| {
    let mut input = BufReader::new(source.file_mut());
    let mut output = BufWriter::new(File::create_new(&temp)?);
    let mut buf = [0u8; 64 * 1024];
    let mut bytes_read = 0u64;
    encode_archive(&mut output, format, |encoder| {
      loop {
        check_cancelled(cancelled)?;
        let n = input.read(&mut buf)?;
        if n == 0 {
          break;
        }
        encoder.write_all(&buf[..n])?;
        bytes_read = bytes_read.saturating_add(n as u64);
        check_cancelled(cancelled)?;
        emit(
          events,
          ArchiveEvent::FileProgress {
            id: id.to_string(),
            bytes_read,
            total_bytes,
          },
        );
      }
      Ok(())
    })?;
    if fs::metadata(&temp)?.len() == 0 {
      return Err(io::Error::new(io::ErrorKind::WriteZero, "empty archive"));
    }
    output.get_ref().sync_all()?;
    drop(output);
    source.revalidate()?;
    check_request_db_sidecars(db, false)?;
    fs::rename(&temp, archive)
  })();

  if result.is_err() {
    let _ = fs::remove_file(&temp);
  } else {
    let bytes_out = fs::metadata(archive)?.len();
    emit(
      events,
      ArchiveEvent::FileCompleted {
        id: id.to_string(),
        path: db.to_path_buf(),
        archive: archive.to_path_buf(),
        bytes_in: total_bytes,
        bytes_out,
      },
    );
  }
  result
}

fn encode_archive<W, F>(output: W, format: ArchiveFormat, write_body: F) -> io::Result<()>
where
  W: Write,
  F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
  match format {
    ArchiveFormat::Zstd => encode_zstd(output, write_body),
    ArchiveFormat::Xz => encode_xz(output, write_body),
  }
}

fn encode_zstd<W, F>(output: W, write_body: F) -> io::Result<()>
where
  W: Write,
  F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
  let mut encoder = zstd::Encoder::new(output, 0)?;
  write_body(&mut encoder)?;
  let mut output = encoder.finish()?;
  output.flush()
}

#[cfg(feature = "lzma")]
fn encode_xz<W, F>(output: W, write_body: F) -> io::Result<()>
where
  W: Write,
  F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
  let options = lzma_rust2::XzOptions::with_preset(6);
  let mut encoder = lzma_rust2::XzWriter::new(output, options)?;
  write_body(&mut encoder)?;
  let mut output = encoder.finish()?;
  output.flush()
}

#[cfg(not(feature = "lzma"))]
fn encode_xz<W, F>(output: W, write_body: F) -> io::Result<()>
where
  W: Write,
  F: FnOnce(&mut dyn Write) -> io::Result<()>,
{
  let _ = output;
  let _ = write_body;
  Err(io::Error::new(
    io::ErrorKind::Unsupported,
    "xz archive compression requires the lzma feature",
  ))
}

fn archive_id(path: &Path) -> String {
  path.to_string_lossy().into_owned()
}

fn emit(events: Option<&dyn ArchiveEmitter>, event: ArchiveEvent) {
  if let Some(events) = events {
    events.emit(event);
  }
}

fn check_cancelled(cancelled: Option<&AtomicBool>) -> io::Result<()> {
  if cancelled.is_some_and(|cancelled| cancelled.load(Ordering::Relaxed)) {
    Err(io::Error::new(
      io::ErrorKind::Interrupted,
      "request database maintenance cancelled",
    ))
  } else {
    Ok(())
  }
}

fn parse_day(value: &str) -> Option<Date> {
  let mut parts = value.split('-');
  let year = parts.next()?.parse::<i32>().ok()?;
  let month = parts.next()?.parse::<u8>().ok()?;
  let day = parts.next()?.parse::<u8>().ok()?;
  if parts.next().is_some() {
    return None;
  }
  Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()
}

#[cfg(test)]
mod tests;
