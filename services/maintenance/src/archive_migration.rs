//! Verify and migrate previously archived request days to the configured format.

use crate::{
  archive_content, archive_days, configured_format, hash_reader, sync_archive_directory, ArchiveContents, ArchiveSet,
  ArchiveSetError,
};
use std::io;
use std::path::{Path, PathBuf};
use time::Date;
use tokn_persistence::request_files::RequestMaintenanceLock;

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MigrationOutcome {
  WouldMigrate,
  Migrated,
  Verified,
  Conflict { first: PathBuf, other: PathBuf },
  Failed { error: String },
}

/// Verify existing archives and optionally publish missing destinations. Sources are retained.
pub fn migrate_request_archives(
  dir: &Path,
  archive_extension: Option<&str>,
  commit: bool,
) -> io::Result<MigrationReport> {
  migrate_request_archives_with_progress(dir, archive_extension, commit, |_, _, _| {})
}

/// Like `migrate_request_archives`, with a callback for each day as it starts.
/// The callback receives the date, one-based position, and total number of days.
pub fn migrate_request_archives_with_progress(
  dir: &Path,
  archive_extension: Option<&str>,
  commit: bool,
  mut day_started: impl FnMut(Date, usize, usize),
) -> io::Result<MigrationReport> {
  let target = configured_format(archive_extension)?.target();
  let mut report = MigrationReport {
    target_extension: target.extension(),
    entries: Vec::new(),
  };
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
        .then_some((day, files.archives, destination))
    })
    .collect();
  let total = eligible.len();
  for (index, (day, archives, destination)) in eligible.into_iter().enumerate() {
    day_started(day, index + 1, total);
    let outcome = migrate_day(&archives, &destination, target, commit);
    let sources = archives.into_iter().filter(|path| path != &destination).collect();
    report.entries.push(MigrationEntry {
      day,
      sources,
      destination,
      outcome,
    });
  }
  Ok(report)
}

fn migrate_day(paths: &[PathBuf], destination: &Path, target: ArchiveContents, commit: bool) -> MigrationOutcome {
  let mut set = match ArchiveSet::verify(paths) {
    Ok(set) => set,
    Err(ArchiveSetError::Conflict { first, other }) => return MigrationOutcome::Conflict { first, other },
    Err(ArchiveSetError::Io(error)) => {
      return MigrationOutcome::Failed {
        error: error.to_string(),
      }
    }
  };
  migrate_verified_day(&mut set, destination, target, commit).unwrap_or_else(|error| MigrationOutcome::Failed {
    error: error.to_string(),
  })
}

fn migrate_verified_day(
  set: &mut ArchiveSet,
  destination: &Path,
  target: ArchiveContents,
  commit: bool,
) -> io::Result<MigrationOutcome> {
  if set.archives.iter().any(|archive| archive.file.path() == destination) {
    return Ok(MigrationOutcome::Verified);
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
    archive_content::validate(source.file.file_mut(), source.format, target)?;
    set.recheck()?;
    return Ok(MigrationOutcome::WouldMigrate);
  }

  let parent = destination
    .parent()
    .filter(|path| !path.as_os_str().is_empty())
    .unwrap_or_else(|| Path::new("."));
  let mut staging = tempfile::Builder::new()
    .prefix(".tokn-migration-")
    .tempfile_in(parent)?;
  archive_content::convert(source.file.file_mut(), source.format, staging.as_file_mut(), target)?;
  staging.as_file().sync_all()?;

  let mut staged = staging.reopen()?;
  let digest = hash_reader(&mut staged, None, &mut |_| {})?;
  for archive in &mut set.archives {
    if !archive_content::verify_equivalent(archive.file.file_mut(), archive.format, &mut staged, target)? {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("staged archive differs from {}", archive.file.path().display()),
      ));
    }
  }
  set.recheck()?;
  if hash_reader(staging.reopen()?, None, &mut |_| {})? != digest {
    return Err(io::Error::new(
      io::ErrorKind::InvalidData,
      "staged archive changed while being verified",
    ));
  }
  drop(staged);
  staging.persist_noclobber(destination).map_err(|error| error.error)?;
  sync_archive_directory(parent)?;
  Ok(MigrationOutcome::Migrated)
}

#[cfg(test)]
mod tests {
  use super::*;
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

    let expected = |outcome| MigrationReport {
      target_extension: "pack.zst",
      entries: vec![MigrationEntry {
        day,
        sources: vec![source.clone()],
        destination: destination.clone(),
        outcome,
      }],
    };
    assert_eq!(
      migrate_request_archives(dir.path(), Some("pack.zst"), false)?,
      expected(MigrationOutcome::WouldMigrate)
    );
    assert!(!destination.exists());
    assert_eq!(
      migrate_request_archives(dir.path(), Some("pack.zst"), true)?,
      expected(MigrationOutcome::Migrated)
    );
    let migrated = fs::read(&destination)?;
    assert_eq!(
      migrate_request_archives(dir.path(), Some("pack.zst"), true)?,
      expected(MigrationOutcome::Verified)
    );
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

    let report = migrate_request_archives(dir.path(), target, true)?;
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
