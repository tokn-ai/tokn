//! Verify and migrate previously archived request days to the configured format.

use crate::archive_content::{self, ArchiveInput, ContentProgress, PayloadStats};
use crate::{
  archive_days, configured_format, hash_reader, hex_digest, sync_archive_directory, ArchiveContents, ArchiveSet,
  ArchiveSetError,
};
use std::io;
use std::path::{Path, PathBuf};
use time::Date;
use tokn_persistence::request_files::{RequestDayLock, RequestMaintenanceLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReport {
  pub target_extension: &'static str,
  pub entries: Vec<MigrationEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationEntry {
  pub day: Date,
  pub sources: Vec<PathBuf>,
  pub destination: PathBuf,
  pub outcome: MigrationOutcome,
  pub files: Vec<MigrationFileStats>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationFileStats {
  pub path: PathBuf,
  pub compressed_bytes: u64,
  pub sha256: String,
  pub uncompressed: Option<MigrationPayloadSize>,
  /// SHA-256 of the complete decoded SQLite snapshot, when available.
  pub sqlite_sha256: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationPayloadSize {
  Sqlite(u64),
  Pack(u64),
}

impl MigrationPayloadSize {
  fn from_format(format: ArchiveContents, bytes: u64) -> Self {
    match format {
      ArchiveContents::Snapshot(_) => Self::Sqlite(bytes),
      ArchiveContents::IndexedPack(_) => Self::Pack(bytes),
    }
  }
}

pub enum MigrationProgressEvent<'a> {
  Started {
    target_extension: &'static str,
  },
  DayStarted {
    day: Date,
    index: usize,
    total: usize,
  },
  StageStarted {
    label: &'static str,
    bytes_total: Option<u64>,
  },
  BytesProcessed {
    bytes: u64,
  },
  DayFinished(&'a MigrationEntry),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationOutcome {
  WouldMigrate { size: MigrationPayloadSize },
  Migrated,
  Verified,
  InUse,
  Conflict { first: PathBuf, other: PathBuf },
  Failed { error: String },
}

/// Verify existing archives and measure uncompressed targets or publish them. Sources are retained.
pub fn migrate_request_archives(
  dir: &Path,
  archive_extension: Option<&str>,
  commit: bool,
) -> io::Result<MigrationReport> {
  migrate_request_archives_with_progress(dir, archive_extension, commit, |_| {})
}

/// Like `migrate_request_archives`, with live progress and results as each day finishes.
/// Day positions are one-based. Byte events report increments in the current stage.
pub fn migrate_request_archives_with_progress(
  dir: &Path,
  archive_extension: Option<&str>,
  commit: bool,
  mut progress: impl FnMut(MigrationProgressEvent<'_>),
) -> io::Result<MigrationReport> {
  let target = configured_format(archive_extension)?.target();
  let mut report = MigrationReport {
    target_extension: target.extension(),
    entries: Vec::new(),
  };
  progress(MigrationProgressEvent::Started {
    target_extension: report.target_extension,
  });
  let _lock = match RequestMaintenanceLock::acquire(dir) {
    Ok(lock) => lock,
    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(report),
    Err(error) => return Err(error),
  };
  let eligible: Vec<_> = archive_days(dir)?
    .into_iter()
    .filter_map(|(day, files)| {
      let destination = target.path(&files.db);
      files
        .archives
        .iter()
        .any(|path| path != &destination)
        .then_some((day, files.db, files.archives, destination))
    })
    .collect();
  let total = eligible.len();
  for (index, (day, db, archives, destination)) in eligible.into_iter().enumerate() {
    progress(MigrationProgressEvent::DayStarted {
      day,
      index: index + 1,
      total,
    });
    let (outcome, files) = match RequestDayLock::try_acquire(&db) {
      Ok(_day_lock) => {
        let mut pending_bytes = 0;
        // Pack objects can emit many small writes. Batch progress updates
        // while preserving each stage's exact byte total.
        let result = migrate_day(&archives, &destination, target, commit, &mut |event| match event {
          ContentProgress::Stage(label, bytes_total) => {
            flush_progress_bytes(&mut pending_bytes, &mut progress);
            progress(MigrationProgressEvent::StageStarted { label, bytes_total });
          }
          ContentProgress::Bytes(bytes) => {
            pending_bytes += bytes;
            if pending_bytes >= 256 * 1024 {
              flush_progress_bytes(&mut pending_bytes, &mut progress);
            }
          }
        });
        flush_progress_bytes(&mut pending_bytes, &mut progress);
        result
      }
      Err(error) if error.kind() == io::ErrorKind::WouldBlock => (MigrationOutcome::InUse, Vec::new()),
      Err(error) => (
        MigrationOutcome::Failed {
          error: error.to_string(),
        },
        Vec::new(),
      ),
    };
    let sources = archives.into_iter().filter(|path| path != &destination).collect();
    let entry = MigrationEntry {
      day,
      sources,
      destination,
      outcome,
      files,
    };
    progress(MigrationProgressEvent::DayFinished(&entry));
    report.entries.push(entry);
  }
  Ok(report)
}

fn flush_progress_bytes(pending: &mut u64, progress: &mut impl FnMut(MigrationProgressEvent<'_>)) {
  let bytes = std::mem::take(pending);
  if bytes > 0 {
    progress(MigrationProgressEvent::BytesProcessed { bytes });
  }
}

fn migrate_day(
  paths: &[PathBuf],
  destination: &Path,
  target: ArchiveContents,
  commit: bool,
  progress: &mut impl FnMut(ContentProgress),
) -> (MigrationOutcome, Vec<MigrationFileStats>) {
  let mut set = match ArchiveSet::open(paths, progress) {
    Ok(set) => set,
    Err(error) => return (verification_outcome(error), Vec::new()),
  };
  let (outcome, output) = match set.verify_contents(progress) {
    Ok(()) => migrate_verified_day(&mut set, destination, target, commit, progress)
      .unwrap_or_else(|error| (verification_outcome(error.into()), None)),
    Err(error) => (verification_outcome(error), None),
  };
  let mut files: Vec<_> = set
    .archives
    .iter()
    .map(|archive| {
      file_stats(
        archive.file.path(),
        archive.format,
        archive.compressed_bytes,
        &archive.digest,
        archive.payload,
      )
    })
    .collect();
  files.extend(output);
  (outcome, files)
}

fn verification_outcome(error: ArchiveSetError) -> MigrationOutcome {
  match error {
    ArchiveSetError::Conflict { first, other } => MigrationOutcome::Conflict { first, other },
    ArchiveSetError::Io(error) => MigrationOutcome::Failed {
      error: error.to_string(),
    },
  }
}

fn file_stats(
  path: &Path,
  format: ArchiveContents,
  compressed_bytes: u64,
  digest: &[u8; 32],
  payload: Option<PayloadStats>,
) -> MigrationFileStats {
  MigrationFileStats {
    path: path.to_path_buf(),
    compressed_bytes,
    sha256: hex_digest(digest),
    uncompressed: payload.map(|stats| MigrationPayloadSize::from_format(format, stats.bytes)),
    sqlite_sha256: payload.and_then(|stats| stats.sqlite_sha256).as_ref().map(hex_digest),
  }
}

fn migrate_verified_day(
  set: &mut ArchiveSet,
  destination: &Path,
  target: ArchiveContents,
  commit: bool,
  progress: &mut impl FnMut(ContentProgress),
) -> io::Result<(MigrationOutcome, Option<MigrationFileStats>)> {
  if set.archives.iter().any(|archive| archive.file.path() == destination) {
    return Ok((MigrationOutcome::Verified, None));
  }

  // Keep the original representation when available. For example, transcode a
  // packed payload directly instead of rebuilding it from a SQLite archive.
  let source_index = set
    .archives
    .iter()
    .position(|archive| match target {
      ArchiveContents::Snapshot(_) => matches!(archive.format, ArchiveContents::Snapshot(_)),
      ArchiveContents::IndexedPack(_) => matches!(archive.format, ArchiveContents::IndexedPack(_)),
    })
    .unwrap_or(0);
  let source = &mut set.archives[source_index];
  if !commit {
    let target_bytes = archive_content::measure_conversion(source.input(), target, progress)?;
    set.recheck_with_progress(progress)?;
    return Ok((
      MigrationOutcome::WouldMigrate {
        size: MigrationPayloadSize::from_format(target, target_bytes),
      },
      None,
    ));
  }

  let parent = destination
    .parent()
    .filter(|path| !path.as_os_str().is_empty())
    .unwrap_or_else(|| Path::new("."));
  let mut staging = tempfile::Builder::new()
    .prefix(".tokn-migration-")
    .tempfile_in(parent)?;
  let mut converted = archive_content::convert(source.input(), staging.as_file_mut(), target, progress)?;
  staging.as_file().sync_all()?;

  let compressed_bytes = staging.as_file().metadata()?.len();
  let mut staged = staging.reopen()?;
  progress(ContentProgress::Stage("Output hash", Some(compressed_bytes)));
  let digest = hash_reader(&mut staged, None, &mut |bytes| progress(ContentProgress::Bytes(bytes)))?;
  for (index, archive) in set.archives.iter_mut().enumerate() {
    let capture = if index == source_index {
      converted.source_capture.take()
    } else {
      None
    };
    let equivalent = if let Some((capture, format)) = capture {
      let (equivalent, payload) = archive_content::verify_captured_pack(&capture, &mut staged, format, progress)?;
      equivalent && payload.bytes == converted.target.bytes
    } else {
      let mut staged_payload = None;
      archive_content::verify_equivalent(
        archive.input(),
        ArchiveInput {
          file: &mut staged,
          format: target,
          payload: &mut staged_payload,
        },
        progress,
      )?
    };
    if !equivalent {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("staged archive differs from {}", archive.file.path().display()),
      ));
    }
  }
  set.recheck_with_progress(progress)?;
  progress(ContentProgress::Stage("Recheck output", Some(compressed_bytes)));
  if hash_reader(staging.reopen()?, None, &mut |bytes| {
    progress(ContentProgress::Bytes(bytes))
  })?
    != digest
  {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "staged archive changed while being verified",
    ));
  }
  drop(staged);
  staging.persist_noclobber(destination).map_err(|error| error.error)?;
  sync_archive_directory(parent)?;
  Ok((
    MigrationOutcome::Migrated,
    Some(file_stats(
      destination,
      target,
      compressed_bytes,
      &digest,
      Some(converted.target),
    )),
  ))
}

#[cfg(test)]
mod tests {
  use super::*;
  use sha2::{Digest, Sha256};
  use std::fs::{self, File};
  use time::macros::date;
  use tokn_persistence::requests::open_day_db;

  fn write_requests_db(db: &Path, body: &[u8]) -> io::Result<()> {
    let conn = open_day_db(db).map_err(io::Error::other)?;
    conn
      .execute("INSERT INTO request_connection (request_id, ts) VALUES ('test', 1)", [])
      .map_err(io::Error::other)?;
    conn
      .execute(
        "INSERT INTO request_downstream (request_id, inbound_req_body) VALUES ('test', ?1)",
        [body],
      )
      .map_err(io::Error::other)?;
    Ok(())
  }

  fn snapshot(dir: &Path, day: Date, body: &[u8]) -> io::Result<PathBuf> {
    let temporary = tempfile::tempdir()?;
    let db = temporary.path().join("source.db");
    write_requests_db(&db, body)?;
    let archive = dir.join(format!("{day}.db.zstd"));
    zstd::stream::copy_encode(File::open(db)?, File::create(&archive)?, 0)?;
    Ok(archive)
  }

  #[test]
  fn migrates_archived_only_days_without_replacing_the_source() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let day = date!(2026 - 05 - 01);
    let source = snapshot(dir.path(), day, b"request body")?;
    let destination = dir.path().join(format!("{day}.pack.zst"));
    let original = fs::read(&source)?;
    let sqlite = zstd::stream::decode_all(original.as_slice())?;

    let source_stats = MigrationFileStats {
      path: source.clone(),
      compressed_bytes: original.len() as u64,
      sha256: hex_digest(&Sha256::digest(&original).into()),
      uncompressed: Some(MigrationPayloadSize::Sqlite(sqlite.len() as u64)),
      sqlite_sha256: Some(hex_digest(&Sha256::digest(&sqlite).into())),
    };
    let mut finished = Vec::new();
    let mut stages = Vec::new();
    let preview = migrate_request_archives_with_progress(dir.path(), Some("pack.zst"), false, |event| match event {
      MigrationProgressEvent::BytesProcessed { bytes } => {
        let (_, _, processed) = stages.last_mut().expect("bytes follow a stage start");
        *processed += bytes;
      }
      MigrationProgressEvent::DayFinished(entry) => finished.push(entry.clone()),
      MigrationProgressEvent::StageStarted { label, bytes_total } => stages.push((label, bytes_total, 0)),
      _ => (),
    })?;
    assert_eq!(preview.target_extension, "pack.zst");
    assert_eq!(finished, preview.entries);
    let input_total = Some(source_stats.compressed_bytes);
    assert_eq!(
      stages
        .iter()
        .map(|&(label, total, _)| (label, total))
        .collect::<Vec<_>>(),
      [
        ("Input hash", input_total),
        ("Unpack DB", input_total),
        ("Capture DB", None),
        ("Build pack", Some(b"request body".len() as u64)),
        ("Index pack", None),
        ("Recheck input", input_total),
      ]
    );
    for (label, total, bytes) in stages {
      if let Some(total) = total {
        assert_eq!(bytes, total, "{label}");
      }
    }
    let [entry] = preview.entries.as_slice() else {
      panic!("expected one candidate");
    };
    assert_eq!(entry.day, day);
    assert_eq!(entry.sources.as_slice(), std::slice::from_ref(&source));
    assert_eq!(entry.destination, destination);
    let MigrationOutcome::WouldMigrate { size } = entry.outcome else {
      panic!("expected an uncompressed pack preview");
    };
    assert_eq!(entry.files, std::slice::from_ref(&source_stats));
    assert!(!destination.exists());

    let mut commit_stages = Vec::new();
    let commit = migrate_request_archives_with_progress(dir.path(), Some("pack.zst"), true, |event| {
      if let MigrationProgressEvent::StageStarted { label, bytes_total } = event {
        commit_stages.push((label, bytes_total));
      }
    })?;
    for label in [
      "Unpack DB",
      "Capture DB",
      "Output hash",
      "Recheck input",
      "Recheck output",
    ] {
      assert_eq!(
        commit_stages.iter().filter(|&&(stage, _)| stage == label).count(),
        1,
        "{commit_stages:?}"
      );
    }
    let migrated = fs::read(&destination)?;
    let pack_bytes = zstd::stream::decode_all(migrated.as_slice())?.len() as u64;
    assert!(commit_stages.contains(&("Verify pack", Some(pack_bytes))));
    let final_stats = MigrationFileStats {
      path: destination.clone(),
      compressed_bytes: migrated.len() as u64,
      sha256: hex_digest(&Sha256::digest(&migrated).into()),
      uncompressed: Some(MigrationPayloadSize::Pack(pack_bytes)),
      sqlite_sha256: None,
    };
    assert_eq!(commit.entries[0].outcome, MigrationOutcome::Migrated);
    assert_eq!(Some(size), final_stats.uncompressed);
    assert_eq!(commit.entries[0].files, [source_stats, final_stats]);
    let existing = migrate_request_archives(dir.path(), Some("pack.zst"), true)?;
    assert_eq!(existing.entries[0].outcome, MigrationOutcome::Verified);
    assert_eq!(existing.entries[0].files, commit.entries[0].files);
    assert_eq!(fs::read(&destination)?, migrated);
    assert_eq!(fs::read(&source)?, original);
    assert!(!dir.path().join(format!("{day}.db")).exists());
    fs::remove_file(source)?;
    assert!(migrate_request_archives(dir.path(), Some("pack.zst"), true)?
      .entries
      .is_empty());
    Ok(())
  }

  #[cfg(feature = "lzma")]
  #[test]
  fn transcodes_an_indexed_pack_without_changing_its_payload() -> io::Result<()> {
    use std::io::{Read, Write};
    use tokn_persistence::indexed_archive::IndexedArchiveFormat;
    use tokn_persistence::request_archive::{indexed, RequestPack};
    use tokn_persistence::request_files::StableFile;

    let dir = tempfile::tempdir()?;
    let temporary = tempfile::tempdir()?;
    let db = temporary.path().join("source.db");
    write_requests_db(&db, b"request body")?;
    let capture = RequestPack::capture(&StableFile::open(&db)?, &mut |_| Ok(()))?.expect("packable fixture");
    let mut original = Vec::new();
    indexed::write(&capture, &mut original, &mut |_| Ok(()))?;

    let day = date!(2026 - 05 - 01);
    let source = dir.path().join(format!("{day}.pack.xz"));
    let mut encoder = IndexedArchiveFormat::Xz.writer(File::create(&source)?)?;
    encoder.write_all(&original)?;
    encoder.finish()?;

    let preview = migrate_request_archives(dir.path(), Some("pack.zst"), false)?;
    let [entry] = preview.entries.as_slice() else {
      panic!("expected one previewed archive");
    };
    let size = MigrationPayloadSize::Pack(original.len() as u64);
    assert_eq!(entry.outcome, MigrationOutcome::WouldMigrate { size });
    assert_eq!(entry.files.len(), 1);
    assert_eq!(entry.files[0].uncompressed, Some(size));
    assert!(!entry.destination.exists());

    let report = migrate_request_archives(dir.path(), Some("pack.zst"), true)?;
    let [entry] = report.entries.as_slice() else {
      panic!("expected one migrated archive");
    };
    assert_eq!(entry.outcome, MigrationOutcome::Migrated);
    assert!(source.exists());
    let mut converted = Vec::new();
    IndexedArchiveFormat::Zstd
      .open(File::open(&entry.destination)?)?
      .read_to_end(&mut converted)?;
    assert_eq!(converted, original);
    Ok(())
  }

  #[test]
  fn reports_decoded_snapshot_hash_when_its_schema_cannot_be_packed() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let scratch = tempfile::tempdir()?;
    let db = scratch.path().join("old.db");
    let conn = rusqlite::Connection::open(&db).map_err(io::Error::other)?;
    conn
      .execute_batch("CREATE TABLE requests (id INTEGER PRIMARY KEY);")
      .map_err(io::Error::other)?;
    drop(conn);
    let sqlite = fs::read(db)?;
    let day = date!(2026 - 05 - 01);
    let archive = dir.path().join(format!("{day}.db.zstd"));
    zstd::stream::copy_encode(sqlite.as_slice(), File::create(&archive)?, 0)?;
    let target = dir.path().join(format!("{day}.pack.zst"));
    snapshot(scratch.path(), day, b"packable fixture")?;
    let finished = migrate_request_archives(scratch.path(), None, true)?;

    for target_exists in [false, true] {
      if target_exists {
        fs::copy(&finished.entries[0].destination, &target)?;
      }
      let report = migrate_request_archives(dir.path(), None, false)?;
      let [entry] = report.entries.as_slice() else {
        panic!("expected one blocked day");
      };
      assert!(matches!(entry.outcome, MigrationOutcome::Failed { .. }));
      let source = entry.files.iter().find(|file| file.path == archive).unwrap();
      assert_eq!(
        source.uncompressed,
        Some(MigrationPayloadSize::Sqlite(sqlite.len() as u64))
      );
      assert_eq!(source.sqlite_sha256, Some(hex_digest(&Sha256::digest(&sqlite).into())));
      assert_eq!(entry.destination.exists(), target_exists);
    }
    Ok(())
  }

  #[test]
  fn preserves_existing_target_on_conflict_and_continues_after_errors() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let conflict = date!(2026 - 05 - 01);
    let malformed = date!(2026 - 05 - 02);
    let eligible = date!(2026 - 05 - 03);
    snapshot(dir.path(), conflict, b"original body")?;
    let target = Some("pack.zst");
    assert_eq!(
      migrate_request_archives(dir.path(), target, true)?.entries[0].outcome,
      MigrationOutcome::Migrated
    );
    let conflict_target = dir.path().join(format!("{conflict}.pack.zst"));
    let previous = fs::read(&conflict_target)?;
    let conflict_source = snapshot(dir.path(), conflict, b"different body")?;
    let malformed_source = dir.path().join(format!("{malformed}.db.zstd"));
    fs::write(&malformed_source, b"not an archive")?;
    let eligible_source = snapshot(dir.path(), eligible, b"eligible body")?;

    let mut dates = Vec::new();
    let report = migrate_request_archives_with_progress(dir.path(), target, true, |event| match event {
      MigrationProgressEvent::DayStarted { day, .. } => dates.push((day, false)),
      MigrationProgressEvent::DayFinished(entry) => dates.push((entry.day, true)),
      _ => (),
    })?;
    assert_eq!(
      dates,
      [
        (conflict, false),
        (conflict, true),
        (malformed, false),
        (malformed, true),
        (eligible, false),
        (eligible, true)
      ]
    );
    let [conflict, malformed, eligible] = report.entries.as_slice() else {
      panic!("expected an outcome for each archive day");
    };
    let MigrationOutcome::Conflict { first, other } = &conflict.outcome else {
      panic!("expected archives to conflict, got {:?}", conflict.outcome);
    };
    assert!([first, other].contains(&&conflict_target));
    assert!([first, other].contains(&&conflict_source));
    assert_eq!(fs::read(conflict_target)?, previous);
    assert!(matches!(malformed.outcome, MigrationOutcome::Failed { .. }));
    assert!(!malformed.destination.exists());
    assert_eq!(eligible.outcome, MigrationOutcome::Migrated);
    assert!(conflict_source.exists() && malformed_source.exists() && eligible_source.exists());
    Ok(())
  }
}
