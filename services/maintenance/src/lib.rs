//! Archive, verify and prune rotated request databases.

mod pack;
mod worker;
use tokn_persistence::indexed_archive::IndexedArchiveFormat;
use tokn_persistence::request_archive::RequestPack;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArchivePreference {
  indexed: Option<IndexedArchiveFormat>,
  snapshot_format: ArchiveFormat,
}

impl From<ArchiveFormat> for ArchivePreference {
  fn from(snapshot_format: ArchiveFormat) -> Self {
    let indexed = match snapshot_format {
      #[cfg(feature = "lzma")]
      ArchiveFormat::Xz => Some(IndexedArchiveFormat::Xz),
      _ => None,
    };
    Self {
      indexed,
      snapshot_format,
    }
  }
}

fn resolve_format(configured_extension: Option<&str>) -> ArchivePreference {
  let Some(value) = configured_extension.map(str::trim).filter(|value| !value.is_empty()) else {
    return if cfg!(feature = "lzma") {
      ArchiveFormat::Xz.into()
    } else {
      ArchiveFormat::Zstd.into()
    };
  };
  let normalized = value.trim_start_matches('.').to_ascii_lowercase();
  match normalized.as_str() {
    "zst" | "pack.zst" | "pack.zstd" => ArchivePreference {
      indexed: Some(IndexedArchiveFormat::Zstd),
      snapshot_format: ArchiveFormat::Zstd,
    },
    "zstd" | "db.zstd" => ArchiveFormat::Zstd.into(),
    "xz" | "db.xz" | "pack.xz" | "lzma" | "db.lzma" if cfg!(feature = "lzma") => ArchiveFormat::Xz.into(),
    "xz" | "db.xz" | "pack.xz" | "lzma" | "db.lzma" => {
      tracing::warn!(
        archive_extension = value,
        "LZMA archive extension requested without lzma feature; falling back to zstd"
      );
      ArchiveFormat::Zstd.into()
    }
    _ => {
      tracing::warn!(
        archive_extension = value,
        "unsupported archive extension; falling back to zstd"
      );
      ArchiveFormat::Zstd.into()
    }
  }
}

/// Verify old request database archives and optionally remove their source
/// `.db` files. Compare raw snapshots byte for byte, and verify the metadata
/// snapshot, descriptors and body contents of packed archives.
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
    format.into(),
    commit,
    &mut |_: PruneProgressEvent| {},
  )
}

fn prune_request_dbs_once_with_progress(
  dir: &Path,
  today: Date,
  prune_after_days: i64,
  preference: ArchivePreference,
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
    let archive = existing_archive_path(&path, preference)?;
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
      bytes_processed = bytes_processed.saturating_add(bytes).min(bytes_total);
      progress(PruneProgressEvent::FileProgress {
        bytes_processed,
        bytes_total,
      });
    };
    let outcome = prune_request_db(&path, &archive, commit, None, &mut report_bytes);
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
  let contents = match ArchiveContents::from_path(archive_file.path()) {
    Ok(contents) => contents,
    Err(error) => {
      return PruneOutcome::Failed {
        error: format!("inspect archive {}: {error}", archive.display()),
      };
    }
  };
  let archived_digest = match archive_digest(&mut archive_file, contents, cancelled, progress) {
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
  if matches!(contents, ArchiveContents::Snapshot(_)) && source_digest != archived_digest {
    return PruneOutcome::HashMismatch {
      source_sha256,
      archived_sha256: hex_digest(&archived_digest),
    };
  }
  if let ArchiveContents::IndexedPack(format) = contents {
    let verified = RequestPack::capture(&source_file, &mut |bytes| {
      check_cancelled(cancelled)?;
      progress(bytes);
      Ok(())
    })
    .and_then(|pack| {
      pack.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "source cannot match a request pack"))
    })
    .and_then(|snapshot| pack::verify_indexed(format, &mut archive_file, &snapshot, cancelled, progress));
    if let Err(error) = verified {
      return PruneOutcome::Failed {
        error: format!("verify request pack {}: {error}", archive.display()),
      };
    }
  }
  if !commit {
    return PruneOutcome::Verified { sha256: source_sha256 };
  }

  match archive_digest(&mut archive_file, contents, cancelled, progress) {
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

#[derive(Clone, Copy)]
enum ArchiveContents {
  Snapshot(ArchiveFormat),
  IndexedPack(IndexedArchiveFormat),
}

impl ArchiveContents {
  fn from_path(path: &Path) -> io::Result<Self> {
    if let Some(format) = IndexedArchiveFormat::from_path(path) {
      return Ok(Self::IndexedPack(format));
    }
    ArchiveFormat::from_path(path)
      .map(Self::Snapshot)
      .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unrecognized request archive extension"))
  }
}

fn archive_digest(
  file: &mut StableFile,
  contents: ArchiveContents,
  cancelled: Option<&AtomicBool>,
  progress: &mut impl FnMut(u64),
) -> io::Result<[u8; 32]> {
  match contents {
    // Packs are compared logically. Recheck their complete compressed bytes
    // before deletion to detect changes after that comparison.
    ArchiveContents::IndexedPack(_) => hash_file(file, cancelled, progress),
    ArchiveContents::Snapshot(format) => hash_archive(file, format, cancelled, progress),
  }
}

fn sync_archive_before_prune(archive: &StableFile) -> io::Result<()> {
  // Sync the archive and its directory entry before removing the source on Unix.
  #[cfg(unix)]
  {
    archive.file().sync_all()?;
    let parent = archive.path().parent().filter(|path| !path.as_os_str().is_empty());
    sync_archive_directory(parent.unwrap_or_else(|| Path::new(".")))?;
  }
  #[cfg(not(unix))]
  let _ = archive;
  Ok(())
}

#[cfg(any(unix, feature = "lzma"))]
fn sync_archive_directory(directory: &Path) -> io::Result<()> {
  #[cfg(unix)]
  File::open(directory)?.sync_all()?;
  #[cfg(not(unix))]
  let _ = directory;
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
  archive_requests_once_with_events(dir, today, archive_after_days, None, format.into(), None, None)
}

fn archive_requests_once_with_events(
  dir: &Path,
  today: Date,
  archive_after_days: i64,
  prune_after_days: Option<i64>,
  preference: ArchivePreference,
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
  archive_requests_once_locked(dir, cutoff, prune_cutoff, preference, events, cancelled)
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
  preference: ArchivePreference,
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

    let mut archive = existing_archive_path(&path, preference)?;
    if archive.symlink_metadata().is_ok() {
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
      match compress_db(&path, &archive, preference, events, cancelled, &id) {
        Ok(published) => {
          archive = published;
          stats.archived += 1;
        }
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
      let outcome = prune_request_db(&path, &archive, true, cancelled, &mut |_| {});
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

fn existing_archive_path(path: &Path, preference: ArchivePreference) -> io::Result<PathBuf> {
  let snapshot_path = archive_path(path, preference.snapshot_format);
  let candidates = preference
    .indexed
    .into_iter()
    .map(|format| format.path(path))
    .chain(std::iter::once(snapshot_path.clone()))
    .chain(
      IndexedArchiveFormat::ALL
        .iter()
        .filter(|&&format| Some(format) != preference.indexed)
        .map(|format| format.path(path)),
    )
    .chain(
      [
        ArchiveFormat::Zstd,
        #[cfg(feature = "lzma")]
        ArchiveFormat::Xz,
      ]
      .into_iter()
      .filter(|&format| format != preference.snapshot_format)
      .map(|format| archive_path(path, format)),
    );
  for archive in candidates {
    match fs::symlink_metadata(&archive) {
      Ok(_) => return Ok(archive),
      Err(error) if error.kind() == io::ErrorKind::NotFound => {}
      Err(error) => return Err(error),
    }
  }
  Ok(snapshot_path)
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
  snapshot_path: &Path,
  preference: ArchivePreference,
  events: Option<&dyn ArchiveEmitter>,
  cancelled: Option<&AtomicBool>,
  id: &str,
) -> io::Result<PathBuf> {
  let temp = temp_archive_path(snapshot_path);
  if temp.exists() {
    fs::remove_file(&temp)?;
  }

  check_request_db_sidecars(db, false)?;
  let mut source = StableFile::open(db)?;
  let source_metadata = source.file().metadata()?;
  let total_bytes = source_metadata.len();
  let planned_archive = preference
    .indexed
    .map_or_else(|| snapshot_path.to_path_buf(), |format| format.path(db));
  emit(
    events,
    ArchiveEvent::FileStarted {
      id: id.to_string(),
      path: db.to_path_buf(),
      archive: planned_archive.clone(),
      total_bytes,
    },
  );

  let result = (|| {
    let mut bytes_read = 0u64;
    let mut update = |bytes: u64| {
      check_cancelled(cancelled)?;
      if bytes != 0 {
        bytes_read = bytes_read.saturating_add(bytes).min(total_bytes);
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
    };

    if let Some(format) = preference.indexed {
      if let Some(request_pack) = RequestPack::capture(&source, &mut update)? {
        let bytes_out = pack::publish_indexed(format, &request_pack, &planned_archive, &mut update, || {
          recheck_archive_source(&source, &source_metadata, cancelled)
        })
        .map_err(|error| check_cancelled(cancelled).err().unwrap_or(error))?;
        return Ok((planned_archive, bytes_out));
      }
    }

    let mut output = BufWriter::new(create_archive_temp(&temp)?);
    encode_snapshot(&mut source, &mut output, preference.snapshot_format, &mut update)?;
    let bytes_out = output.get_ref().metadata()?.len();
    if bytes_out == 0 {
      return Err(io::Error::new(io::ErrorKind::WriteZero, "empty archive"));
    }
    output.get_ref().sync_all()?;
    drop(output);
    recheck_archive_source(&source, &source_metadata, cancelled)?;
    fs::rename(&temp, snapshot_path)?;
    Ok((snapshot_path.to_path_buf(), bytes_out))
  })();

  match result {
    Ok((archive, bytes_out)) => {
      emit(
        events,
        ArchiveEvent::FileCompleted {
          id: id.to_string(),
          path: db.to_path_buf(),
          archive: archive.clone(),
          bytes_in: total_bytes,
          bytes_out,
        },
      );
      Ok(archive)
    }
    Err(error) => {
      let _ = fs::remove_file(&temp);
      Err(error)
    }
  }
}

fn recheck_archive_source(
  source: &StableFile,
  before: &fs::Metadata,
  cancelled: Option<&AtomicBool>,
) -> io::Result<()> {
  source.revalidate()?;
  let after = source.file().metadata()?;
  if after.len() != before.len() || after.modified()? != before.modified()? {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "request database changed while it was being archived",
    ));
  }
  check_request_db_sidecars(source.path(), false)?;
  check_cancelled(cancelled)
}

fn create_archive_temp(path: &Path) -> io::Result<File> {
  let mut options = fs::OpenOptions::new();
  options.write(true).create_new(true);
  #[cfg(unix)]
  {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
  }
  options.open(path)
}

fn encode_snapshot(
  source: &mut StableFile,
  output: &mut impl Write,
  format: ArchiveFormat,
  update: &mut impl FnMut(u64) -> io::Result<()>,
) -> io::Result<()> {
  source.file_mut().rewind()?;
  let mut input = BufReader::new(source.file_mut());
  let mut buf = [0u8; 64 * 1024];
  encode_archive(output, format, |encoder| {
    loop {
      update(0)?;
      let n = input.read(&mut buf)?;
      if n == 0 {
        break;
      }
      encoder.write_all(&buf[..n])?;
      update(n as u64)?;
    }
    Ok(())
  })
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
