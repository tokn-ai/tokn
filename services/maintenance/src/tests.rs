use super::*;
use parking_lot::Mutex;
use std::io::{Read, Write};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration as ThreadDuration;
use time::macros::date;
use tokn_core::event::{Event, EventHandler};
use tokn_core::provider::Endpoint;
use tokn_core::request_event::{RequestEndpoint, RequestEvent, RequestEventPayload, StageEvent};
#[cfg(feature = "lzma")]
use tokn_persistence::request_files::indexed_xz_path;
use tokn_persistence::request_files::{RequestDayLock, MAINTENANCE_LOCK_FILE};
use tokn_persistence::RequestEventHandler;

#[derive(Default)]
struct CollectingEmitter {
  events: Mutex<Vec<ArchiveEvent>>,
}

impl ArchiveEmitter for CollectingEmitter {
  fn emit(&self, event: ArchiveEvent) {
    self.events.lock().push(event);
  }
}

fn tempdir() -> PathBuf {
  let p = std::env::temp_dir().join(format!("tokn-router-archive-{}", uuid::Uuid::new_v4()));
  fs::create_dir_all(&p).unwrap();
  p
}

fn write_db(dir: &Path, name: &str, body: &[u8]) -> PathBuf {
  let path = dir.join(name);
  fs::write(&path, body).unwrap();
  path
}

fn archive_format() -> ArchiveFormat {
  resolve_format(Some("xz")).snapshot_format
}

fn started_event(request_id: &str, ts: i64) -> Event {
  Event::Requests(RequestEvent {
    request_id: request_id.into(),
    attempt: 0,
    ts,
    payload: RequestEventPayload::Stage(StageEvent::Started {
      request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
    }),
  })
}

fn completed_event(request_id: &str, ts: i64) -> Event {
  Event::Requests(RequestEvent {
    request_id: request_id.into(),
    attempt: 0,
    ts,
    payload: RequestEventPayload::Stage(StageEvent::Completed {
      success: true,
      attempts: 1,
    }),
  })
}

#[test]
fn archives_eligible_daily_db_without_removing_original() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-02.db", b"sqlite-ish bytes");
  let format = archive_format();

  let stats = archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  assert_eq!(stats.archived, 1);
  assert!(db.exists());
  let archive = dir.join(format!("2026-05-02.{}", format.extension()));
  assert!(archive.exists());

  let mut decoded = Vec::new();
  decode_archive(&archive, format, &mut decoded);
  assert_eq!(decoded, b"sqlite-ish bytes");
}

fn write_packable_db(dir: &Path) -> PathBuf {
  let path = dir.join("2026-05-01.db");
  let conn = tokn_persistence::requests::open_day_db(&path).unwrap();
  conn.pragma_update(None, "journal_mode", "wal").unwrap();
  conn
    .execute_batch(
      r#"INSERT INTO request_connection (request_id, ts) VALUES ('request', 1);
       INSERT INTO request_downstream (request_id, inbound_req_headers, inbound_req_body)
       VALUES ('request', '{"content-type":"application/json"}', '{"input":"hello"}');"#,
    )
    .unwrap();
  path
}

#[cfg(feature = "lzma")]
#[test]
fn verified_request_pack_allows_pruning_a_checkpointed_database() {
  let dir = tempdir();
  let db = write_packable_db(&dir);
  let today = date!(2026 - 05 - 11);
  let format = ArchiveFormat::Xz;
  let archive = indexed_xz_path(&db);

  assert_eq!(archive_requests_once(&dir, today, 7, format).unwrap().archived, 1);
  assert!(archive.is_file());
  assert!(!archive_path(&db, format).exists());
  let report = prune_request_dbs_once(&dir, today, 10, format, true).unwrap();
  let [entry] = report.entries.as_slice() else {
    panic!("expected one request day");
  };
  assert_eq!(entry.archive, archive);
  assert!(matches!(entry.outcome, PruneOutcome::Deleted { .. }), "{entry:?}");
  assert!(!db.exists());
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn request_pack_does_not_allow_pruning_a_changed_body() {
  for format in [
    IndexedArchiveFormat::Zstd,
    #[cfg(feature = "lzma")]
    IndexedArchiveFormat::Xz,
  ] {
    let dir = tempfile::tempdir().unwrap();
    let db = write_packable_db(dir.path());
    let today = date!(2026 - 05 - 11);
    let preference = resolve_format(Some(format.extension()));
    let archive = format.path(&db);

    let stats = archive_requests_once_with_events(dir.path(), today, 7, None, preference, None, None).unwrap();
    assert_eq!(stats.archived, 1);
    let conn = tokn_persistence::requests::open_day_db(&db).unwrap();
    conn
      .execute("UPDATE request_downstream SET inbound_req_body = 'changed'", [])
      .unwrap();
    drop(conn);

    let outcome = prune_request_db(&db, &archive, true, None, &mut |_| {});
    assert!(matches!(outcome, PruneOutcome::Failed { .. }), "{outcome:?}");
    assert!(db.exists());
  }
}

#[test]
fn default_zstd_pack_can_be_pruned_after_switching_archive_format() {
  let dir = tempfile::tempdir().unwrap();
  let db = write_packable_db(dir.path());
  let archive = IndexedArchiveFormat::Zstd.path(&db);
  let today = date!(2026 - 05 - 11);
  let preference = resolve_format(None);

  let stats = archive_requests_once_with_events(dir.path(), today, 7, None, preference, None, None).unwrap();
  assert_eq!(stats.archived, 1);
  assert!(archive.is_file());
  assert!(!archive_path(&db, ArchiveFormat::Zstd).exists());

  let snapshot_format = ArchiveFormat::Zstd;
  let stats = archive_requests_once(dir.path(), today, 7, snapshot_format).unwrap();
  assert_eq!((stats.archived, stats.skipped_existing), (0, 1));
  let report = prune_request_dbs_once(dir.path(), today, 10, snapshot_format, true).unwrap();
  let [entry] = report.entries.as_slice() else {
    panic!("expected one request day");
  };
  assert_eq!(entry.archive, archive);
  assert!(matches!(entry.outcome, PruneOutcome::Deleted { .. }), "{entry:?}");
  assert!(!db.exists());
}

#[test]
fn prunes_a_verified_alternate_archive_without_a_live_database() {
  let dir = tempfile::tempdir().unwrap();
  let db = write_packable_db(dir.path());
  let old_archive = archive_path(&db, ArchiveFormat::Zstd);
  let keeper = IndexedArchiveFormat::Zstd.path(&db);
  let today = date!(2026 - 05 - 11);
  let preference = resolve_format(Some("pack.zst"));
  assert_eq!(
    archive_requests_once(dir.path(), today, 7, ArchiveFormat::Zstd)
      .unwrap()
      .archived,
    1
  );
  fs::remove_file(&db).unwrap();

  let prune =
    |commit| prune_request_dbs_once_with_progress(dir.path(), today, 10, preference, commit, &mut |_| {}).unwrap();
  let before_migration = prune(true);
  assert!(before_migration.entries.is_empty());
  assert_eq!(before_migration.archives[0].outcome, ArchivePruneOutcome::MissingTarget);
  assert!(old_archive.is_file());
  {
    let _day_lock = RequestDayLock::try_acquire(&db).unwrap();
    let migration = migrate_request_archives(dir.path(), Some("pack.zst"), true).unwrap();
    assert_eq!(migration.entries[0].outcome, MigrationOutcome::InUse);
    assert!(!keeper.exists());
  }
  assert_eq!(
    migrate_request_archives(dir.path(), Some("pack.zst"), true)
      .unwrap()
      .entries[0]
      .outcome,
    MigrationOutcome::Migrated
  );

  let preview = prune(false);
  assert_eq!(
    preview.archives,
    [ArchivePruneEntry {
      day: date!(2026 - 05 - 01),
      path: old_archive.clone(),
      keeper: keeper.clone(),
      outcome: ArchivePruneOutcome::WouldRemove,
    }]
  );
  assert!(old_archive.is_file());
  {
    let _day_lock = RequestDayLock::try_acquire(&db).unwrap();
    assert_eq!(prune(true).archives[0].outcome, ArchivePruneOutcome::InUse);
    assert!(old_archive.is_file());
  }
  assert_eq!(prune(true).archives[0].outcome, ArchivePruneOutcome::Removed);
  assert!(!old_archive.exists());
  assert!(keeper.is_file());
  assert!(prune(true).archives.is_empty());
}

#[test]
fn alternate_archive_conflict_retains_every_file_for_the_day() {
  let dir = tempfile::tempdir().unwrap();
  let db = write_packable_db(dir.path());
  let today = date!(2026 - 05 - 11);
  let preference = resolve_format(Some("pack.zst"));
  archive_requests_once_with_events(dir.path(), today, 7, None, preference, None, None).unwrap();
  let keeper = IndexedArchiveFormat::Zstd.path(&db);
  let keeper_bytes = fs::read(&keeper).unwrap();
  // Create an older-format snapshot with different request content.
  tokn_persistence::requests::open_day_db(&db)
    .unwrap()
    .execute("UPDATE request_downstream SET inbound_req_body = 'different body'", [])
    .unwrap();
  let old_archive = archive_path(&db, ArchiveFormat::Zstd);
  encode_snapshot(
    &mut StableFile::open(&db).unwrap(),
    &mut File::create_new(&old_archive).unwrap(),
    ArchiveFormat::Zstd,
    &mut |_| Ok(()),
  )
  .unwrap();

  let report = prune_request_dbs_once_with_progress(dir.path(), today, 10, preference, true, &mut |_| {}).unwrap();
  assert_eq!(report.entries[0].outcome, PruneOutcome::ArchiveConflict);
  let entry = &report.archives[0];
  let ArchivePruneOutcome::Conflict { first, other } = &entry.outcome else {
    panic!("expected conflict, got {entry:?}");
  };
  assert_eq!((first, other), (&old_archive, &keeper));
  assert!(db.is_file());
  assert!(old_archive.is_file());
  assert_eq!(fs::read(keeper).unwrap(), keeper_bytes);
}

#[cfg(feature = "lzma")]
#[test]
fn cancellation_interrupts_verifying_an_indexed_request_pack() {
  let dir = tempdir();
  let db = write_packable_db(&dir);
  archive_requests_once(&dir, date!(2026 - 05 - 11), 7, ArchiveFormat::Xz).unwrap();
  let archive = indexed_xz_path(&db);

  let source = StableFile::open(&db).unwrap();
  let capture = RequestPack::capture(&source, &mut |_| Ok(())).unwrap().unwrap();
  let cancelled = AtomicBool::new(false);
  let error = pack::verify_indexed(
    IndexedArchiveFormat::Xz,
    &mut StableFile::open(&archive).unwrap(),
    &capture,
    Some(&cancelled),
    &mut |_| cancelled.store(true, Ordering::Relaxed),
  )
  .unwrap_err();
  assert_eq!(error.kind(), io::ErrorKind::Interrupted);
  drop(source);
  drop(capture);
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn skips_and_prunes_existing_sqlite_snapshots() {
  let today = date!(2026 - 05 - 11);
  let preference = resolve_format(Some("pack.zst"));
  for format in [
    ArchiveFormat::Zstd,
    #[cfg(feature = "lzma")]
    ArchiveFormat::Xz,
  ] {
    let dir = tempfile::tempdir().unwrap();
    let db = write_packable_db(dir.path());
    let archive = archive_path(&db, format);
    encode_snapshot(
      &mut StableFile::open(&db).unwrap(),
      &mut File::create_new(&archive).unwrap(),
      format,
      &mut |_| Ok(()),
    )
    .unwrap();

    let stats = archive_requests_once_with_events(dir.path(), today, 7, None, preference, None, None).unwrap();
    assert_eq!((stats.archived, stats.skipped_existing), (0, 1));
    assert!(!IndexedArchiveFormat::Zstd.path(&db).exists());

    let report = prune_request_dbs_once_with_progress(dir.path(), today, 10, preference, true, &mut |_| {}).unwrap();
    let [entry] = report.entries.as_slice() else {
      panic!("expected one request day");
    };
    assert_eq!(entry.archive, archive);
    assert!(matches!(entry.outcome, PruneOutcome::Deleted { .. }), "{entry:?}");
    assert!(!db.exists());
  }
}

#[cfg(feature = "lzma")]
#[test]
fn missing_or_corrupt_indexed_archive_retains_source() {
  let dir = tempdir();
  let db = write_packable_db(&dir);
  let today = date!(2026 - 05 - 11);
  let format = ArchiveFormat::Xz;
  let archive = indexed_xz_path(&db);
  assert_eq!(archive_requests_once(&dir, today, 7, format).unwrap().archived, 1);
  fs::remove_file(&archive).unwrap();

  for corrupt in [false, true] {
    if corrupt {
      fs::write(&archive, b"corrupt pack").unwrap();
    }
    let report = prune_request_dbs_once(&dir, today, 10, format, true).unwrap();
    let entry = &report.entries[0];
    if corrupt {
      assert_eq!(entry.archive, archive);
      assert!(matches!(entry.outcome, PruneOutcome::Failed { .. }), "{entry:?}");
    } else {
      assert_eq!(entry.outcome, PruneOutcome::MissingArchive);
    }
    assert!(db.is_file());
  }
  fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "lzma")]
#[test]
fn scheduled_archival_retries_interrupted_publication_and_reports_archive_size() {
  let dir = tempdir();
  let db = write_packable_db(&dir);
  let today = date!(2026 - 05 - 11);
  let format = ArchiveFormat::Xz;
  let archive = indexed_xz_path(&db);
  {
    let source = StableFile::open(&db).unwrap();
    let capture = RequestPack::capture(&source, &mut |_| Ok(())).unwrap().unwrap();
    let error = pack::publish_indexed(IndexedArchiveFormat::Xz, &capture, &archive, &mut |_| Ok(()), || {
      Err(io::Error::new(
        io::ErrorKind::Interrupted,
        "test interrupted publication",
      ))
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
  }
  assert!(!archive.exists());
  let report = prune_request_dbs_once(&dir, today, 10, format, true).unwrap();
  assert!(matches!(report.entries[0].outcome, PruneOutcome::MissingArchive));
  assert!(db.is_file());

  let emitter = CollectingEmitter::default();
  let stats = archive_requests_once_with_events(&dir, today, 7, Some(10), format.into(), Some(&emitter), None).unwrap();
  assert_eq!(stats.archived, 1);
  assert!(!db.exists());
  let archive_bytes = fs::metadata(&archive).unwrap().len();
  assert!(emitter.events.lock().iter().any(|event| matches!(
    event,
    ArchiveEvent::FileCompleted { archive: path, bytes_out, .. } if path == &archive && *bytes_out == archive_bytes
  )));
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn maintenance_prunes_only_verified_databases_past_the_prune_cutoff() {
  let dir = tempdir();
  let format = ArchiveFormat::Zstd;
  let today = date!(2026 - 05 - 11);
  let existing = write_db(&dir, "2026-04-29.db", b"already archived");
  let changed = write_db(&dir, "2026-04-30.db", b"initial contents");
  archive_requests_once(&dir, today, 7, format).unwrap();
  fs::write(&changed, b"updated database").unwrap();

  let at_cutoff = write_db(&dir, "2026-05-01.db", b"archived this scan");
  let within_retention = write_db(&dir, "2026-05-02.db", b"archive without pruning");
  let journaled = write_db(&dir, "2026-04-28.db", b"database with a sidecar");
  write_db(&dir, "2026-04-28.db-wal", b"uncheckpointed writes");

  archive_requests_once_with_events(&dir, today, 7, Some(10), format.into(), None, None).unwrap();

  for (db, contents) in [
    (existing, b"already archived".as_slice()),
    (at_cutoff, b"archived this scan"),
  ] {
    assert!(!db.exists());
    let mut decoded = Vec::new();
    decode_archive(&archive_path(&db, format), format, &mut decoded);
    assert_eq!(decoded, contents);
  }
  assert_eq!(fs::read(changed).unwrap(), b"updated database");
  assert!(within_retention.exists());
  assert!(archive_path(&within_retention, format).exists());
  assert!(journaled.exists());
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn cancellation_during_prune_verification_retains_the_database() {
  let dir = tempdir();
  let format = archive_format();
  let db = write_db(&dir, "2026-05-01.db", b"verified request database");
  archive_requests_once(&dir, date!(2026 - 05 - 11), 7, format).unwrap();
  let archive = archive_path(&db, format);
  let cancelled = AtomicBool::new(false);

  let outcome = prune_request_db(&db, &archive, true, Some(&cancelled), &mut |_| {
    cancelled.store(true, Ordering::Relaxed);
  });

  assert!(matches!(outcome, PruneOutcome::Failed { .. }));
  assert!(db.exists());
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn skips_recent_non_daily_and_existing_archives() {
  let dir = tempdir();
  let format = archive_format();
  write_db(&dir, "2026-05-03.db", b"recent");
  write_db(&dir, "usage.db", b"not daily");
  write_db(&dir, &format!("2026-05-01.{}", format.extension()), b"already archived");
  write_db(&dir, "2026-05-01.db", b"old");

  let stats = archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  assert_eq!(stats.archived, 0);
  assert_eq!(stats.skipped_existing, 1);
  assert!(!dir.join(format!("usage.{}", format.extension())).exists());
  assert!(!dir.join(format!("2026-05-03.{}", format.extension())).exists());
}

#[test]
fn out_of_range_retention_cutoffs_return_errors() {
  let dir = tempdir();
  let format = archive_format();

  let archive_error = archive_requests_once(&dir, date!(2026 - 05 - 09), 1_000_000_000, format).unwrap_err();
  let prune_error = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 1_000_000_000, format, false).unwrap_err();

  assert_eq!(archive_error.kind(), io::ErrorKind::InvalidInput);
  assert_eq!(prune_error.kind(), io::ErrorKind::InvalidInput);
  assert_eq!(
    archive_error.to_string(),
    "archive retention cutoff is outside the supported date range"
  );
  assert_eq!(
    prune_error.to_string(),
    "prune retention cutoff is outside the supported date range"
  );

  let db = write_db(&dir, "2026-05-01.db", b"archive despite an invalid prune cutoff");
  let stats = archive_requests_once_with_events(
    &dir,
    date!(2026 - 05 - 09),
    7,
    Some(1_000_000_000),
    format.into(),
    None,
    None,
  )
  .unwrap();
  assert_eq!(stats.archived, 1);
  assert!(db.exists());
  fs::remove_dir_all(dir).unwrap();
}

#[test]
fn missing_request_directory_is_an_empty_locked_scan() {
  let dir = tempdir().join("missing");
  let format = archive_format();

  let archive = archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();
  let prune = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, false).unwrap();

  assert_eq!(archive, ArchiveStats::default());
  assert!(prune.entries.is_empty());
  assert!(!dir.exists());
}

#[test]
fn prune_dry_run_verifies_without_deleting_source() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-01.db", b"verified request database");
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  let report = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, false).unwrap();

  assert_eq!(report.cutoff, date!(2026 - 05 - 02));
  assert_eq!(report.entries.len(), 1);
  assert!(matches!(
    report.entries[0].outcome,
    PruneOutcome::Verified { ref sha256 } if sha256.len() == 64
  ));
  assert!(db.exists());
}

#[test]
fn prune_reports_file_and_byte_progress() {
  let dir = tempdir();
  let contents = b"verified request database";
  write_db(&dir, "2026-05-01.db", contents);
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();
  let mut events = Vec::new();

  let report =
    prune_request_dbs_once_with_progress(&dir, date!(2026 - 05 - 09), 7, format.into(), false, &mut |event| {
      events.push(event)
    })
    .unwrap();

  assert_eq!(report.entries.len(), 1);
  assert!(matches!(
    events.first(),
    Some(PruneProgressEvent::Started { files_total: 1 })
  ));
  assert!(matches!(
    events.get(1),
    Some(PruneProgressEvent::FileStarted {
      file_index: 0,
      files_total: 1,
      bytes_total,
      ..
    }) if *bytes_total == (contents.len() * 2) as u64
  ));
  assert!(events.iter().any(|event| matches!(
    event,
    PruneProgressEvent::FileProgress {
      bytes_processed,
      bytes_total,
      ..
    } if bytes_processed == bytes_total && *bytes_total == (contents.len() * 2) as u64
  )));
  assert!(matches!(
    events.get(events.len() - 2),
    Some(PruneProgressEvent::FileFinished {
      file_index: 0,
      files_total: 1,
      ..
    })
  ));
  assert!(matches!(
    events.last(),
    Some(PruneProgressEvent::Finished { files_total: 1 })
  ));
}

#[test]
fn prune_commit_deletes_only_sha256_verified_source() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-01.db", b"verified request database");
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  let report = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, true).unwrap();

  assert!(matches!(
    report.entries[0].outcome,
    PruneOutcome::Deleted { ref sha256 } if sha256.len() == 64
  ));
  assert!(!db.exists());
  assert!(report.entries[0].archive.exists());
}

#[test]
fn prune_preserves_replacement_source_file() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-01.db", b"verified request database");
  let original = dir.join("2026-05-01.db.original");
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();
  let mut replaced = false;

  let report =
    prune_request_dbs_once_with_progress(&dir, date!(2026 - 05 - 09), 7, format.into(), true, &mut |event| {
      if !replaced && matches!(event, PruneProgressEvent::FileProgress { .. }) {
        fs::rename(&db, &original).unwrap();
        fs::write(&db, b"replacement request database").unwrap();
        replaced = true;
      }
    })
    .unwrap();

  assert!(matches!(report.entries[0].outcome, PruneOutcome::Failed { .. }));
  assert_eq!(fs::read(&db).unwrap(), b"replacement request database");
  assert_eq!(fs::read(&original).unwrap(), b"verified request database");
}

#[test]
fn concurrent_request_write_waits_for_prune_and_is_preserved() {
  let dir = tempdir();
  let format = archive_format();
  let ts = date!(2026 - 05 - 01).midnight().assume_utc().unix_timestamp() * 1_000;
  let mut handler = RequestEventHandler::new(dir.clone()).unwrap();
  handler.handle(&started_event("old-request", ts));
  handler.handle(&completed_event("old-request", ts + 1));
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  let (started_tx, started_rx) = mpsc::channel();
  let (done_tx, done_rx) = mpsc::channel();
  let mut writer = Some(handler);
  let mut writer_thread = None;
  let mut launched = false;
  let report =
    prune_request_dbs_once_with_progress(&dir, date!(2026 - 05 - 09), 7, format.into(), true, &mut |event| {
      if !launched && matches!(event, PruneProgressEvent::FileProgress { .. }) {
        launched = true;
        let mut handler = writer.take().unwrap();
        let started_tx = started_tx.clone();
        let done_tx = done_tx.clone();
        writer_thread = Some(thread::spawn(move || {
          started_tx.send(()).unwrap();
          handler.handle(&started_event("concurrent-request", ts + 1));
          done_tx.send(()).unwrap();
        }));
        started_rx.recv_timeout(ThreadDuration::from_secs(1)).unwrap();
        assert!(done_rx.recv_timeout(ThreadDuration::from_millis(100)).is_err());
      }
    })
    .unwrap();

  assert!(matches!(report.entries[0].outcome, PruneOutcome::Deleted { .. }));
  done_rx.recv_timeout(ThreadDuration::from_secs(5)).unwrap();
  writer_thread.unwrap().join().unwrap();
  let db = dir.join("2026-05-01.db");
  assert!(db.exists());
  let connection = rusqlite::Connection::open(db).unwrap();
  let count: i64 = connection
    .query_row(
      "SELECT COUNT(*) FROM requests WHERE request_id = 'concurrent-request'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(count, 1);
}

#[test]
fn prune_skips_a_busy_day_and_allows_current_day_writes() {
  let dir = tempfile::tempdir().unwrap();
  let today = date!(2026 - 05 - 09);
  let busy = write_db(dir.path(), "2026-05-01.db", b"busy archived day");
  let ready = write_db(dir.path(), "2026-05-02.db", b"ready archived day");
  let format = ArchiveFormat::Zstd;
  archive_requests_once(dir.path(), today, 7, format).unwrap();
  let _busy_lock = RequestDayLock::try_acquire(&busy).unwrap();

  let mut writer_thread = None;
  let mut completed_during_prune = false;
  let report = prune_request_dbs_once_with_progress(dir.path(), today, 7, format.into(), true, &mut |event| {
    if writer_thread.is_none() && matches!(event, PruneProgressEvent::FileProgress { .. }) {
      let (done_tx, done_rx) = mpsc::channel();
      let requests = dir.path().to_path_buf();
      writer_thread = Some(thread::spawn(move || {
        let mut handler = RequestEventHandler::new(requests).unwrap();
        let ts = today.midnight().assume_utc().unix_timestamp() * 1_000;
        handler.handle(&started_event("current-request", ts));
        let _ = done_tx.send(());
      }));
      completed_during_prune = done_rx.recv_timeout(ThreadDuration::from_secs(5)).is_ok();
    }
  })
  .unwrap();

  writer_thread
    .expect("prune must progress on the available day")
    .join()
    .unwrap();
  assert!(completed_during_prune, "current-day write waited for old-day prune");
  assert_eq!(report.entries[0].outcome, PruneOutcome::InUse);
  assert!(busy.is_file());
  assert!(matches!(report.entries[1].outcome, PruneOutcome::Deleted { .. }));
  assert!(!ready.exists());
  let current = rusqlite::Connection::open(dir.path().join("2026-05-09.db")).unwrap();
  let count: i64 = current
    .query_row(
      "SELECT COUNT(*) FROM requests WHERE request_id = 'current-request'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(count, 1);
}

#[test]
fn prune_keeps_source_when_archive_content_does_not_match() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-01.db", b"original request database");
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();
  fs::write(&db, b"changed request database").unwrap();

  let report = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, true).unwrap();

  assert!(matches!(
    report.entries[0].outcome,
    PruneOutcome::HashMismatch {
      ref source_sha256,
      ref archived_sha256,
    } if source_sha256.len() == 64 && archived_sha256.len() == 64 && source_sha256 != archived_sha256
  ));
  assert!(db.exists());
}

#[test]
fn prune_keeps_source_when_archive_is_missing_or_corrupt() {
  let dir = tempdir();
  let missing = write_db(&dir, "2026-05-01.db", b"missing archive");
  let corrupt = write_db(&dir, "2026-05-02.db", b"corrupt archive");
  let format = archive_format();
  fs::write(archive_path(&corrupt, format), b"not an archive").unwrap();

  let report = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, true).unwrap();

  assert_eq!(report.entries.len(), 2);
  assert!(matches!(report.entries[0].outcome, PruneOutcome::MissingArchive));
  assert!(matches!(report.entries[1].outcome, PruneOutcome::Failed { .. }));
  assert!(missing.exists());
  assert!(corrupt.exists());
}

#[test]
fn maintenance_lock_prevents_overlapping_archive_and_prune_scans() {
  let dir = tempdir();
  write_db(&dir, "2026-05-01.db", b"old");
  let _lock = RequestMaintenanceLock::acquire(&dir).unwrap();

  let archive_error = archive_requests_once(&dir, date!(2026 - 05 - 09), 7, archive_format()).unwrap_err();
  let prune_error = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, archive_format(), false).unwrap_err();
  let migrate_error = migrate_request_archives(&dir, Some("pack.zst"), false).unwrap_err();

  assert_eq!(archive_error.kind(), io::ErrorKind::WouldBlock);
  assert_eq!(prune_error.kind(), io::ErrorKind::WouldBlock);
  assert_eq!(migrate_error.kind(), io::ErrorKind::WouldBlock);
}

#[cfg(unix)]
#[test]
fn maintenance_lock_rejects_symlinks_without_opening_the_target() {
  use std::os::unix::fs::symlink;

  let dir = tempdir();
  let target = dir.join("lock-target");
  fs::write(&target, b"unchanged").unwrap();
  symlink(&target, dir.join(MAINTENANCE_LOCK_FILE)).unwrap();

  let error = match RequestMaintenanceLock::acquire(&dir) {
    Ok(_) => panic!("symlinked maintenance lock was accepted"),
    Err(error) => error,
  };

  assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
  assert_eq!(fs::read(target).unwrap(), b"unchanged");
}

#[test]
fn prune_rejects_multiply_linked_source_files() {
  let dir = tempdir();
  let db = write_db(&dir, "2026-05-01.db", b"verified request database");
  let alias = dir.join("request-database-alias");
  let format = archive_format();
  archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();
  fs::hard_link(&db, &alias).unwrap();

  let report = prune_request_dbs_once(&dir, date!(2026 - 05 - 09), 7, format, true).unwrap();

  assert!(matches!(report.entries[0].outcome, PruneOutcome::Failed { .. }));
  assert!(db.exists());
  assert!(alias.exists());
}

#[test]
fn replaces_stale_temp_on_success() {
  let dir = tempdir();
  let format = archive_format();
  write_db(&dir, "2026-05-01.db", b"old");
  let tmp_path = dir.join(format!("2026-05-01.{}.tmp", format.extension()));
  let mut tmp = File::create(&tmp_path).unwrap();
  tmp.write_all(b"stale").unwrap();
  drop(tmp);

  let stats = archive_requests_once(&dir, date!(2026 - 05 - 09), 7, format).unwrap();

  assert_eq!(stats.archived, 1);
  assert!(!tmp_path.exists());
  assert!(dir.join(format!("2026-05-01.{}", format.extension())).exists());
}

#[test]
fn uses_selected_archive_extension() {
  assert_eq!(
    archive_path(Path::new("2026-05-01.db"), ArchiveFormat::Zstd),
    PathBuf::from("2026-05-01.db.zstd")
  );
  if cfg!(feature = "lzma") {
    assert_eq!(
      archive_path(Path::new("2026-05-01.db"), ArchiveFormat::Xz),
      PathBuf::from("2026-05-01.db.xz")
    );
  }
}

#[test]
fn resolves_configured_extension_or_falls_back_to_zstd() {
  let zstd_pack = ArchivePreference {
    indexed: Some(IndexedArchiveFormat::Zstd),
    snapshot_format: ArchiveFormat::Zstd,
  };
  assert_eq!(resolve_format(None), zstd_pack);
  assert_eq!(resolve_format(Some("  ")), zstd_pack);
  assert_eq!(resolve_format(Some(".pack.zst")), zstd_pack);
  assert_eq!(resolve_format(Some(".zstd")), ArchiveFormat::Zstd.into());
  assert_eq!(resolve_format(Some("db.zstd")), ArchiveFormat::Zstd.into());
  assert_eq!(resolve_format(Some("not-supported")), ArchiveFormat::Zstd.into());
  let expected_xz = if cfg!(feature = "lzma") {
    ArchiveFormat::Xz
  } else {
    ArchiveFormat::Zstd
  };
  assert_eq!(resolve_format(Some(".xz")), expected_xz.into());
  assert_eq!(resolve_format(Some(".pack.xz")), expected_xz.into());
  assert_eq!(resolve_format(Some("lzma")), expected_xz.into());
}

#[test]
fn emits_archive_progress_events() {
  let dir = tempdir();
  write_db(&dir, "2026-05-01.db", b"old enough to archive");
  let emitter = Arc::new(CollectingEmitter::default());

  let stats = archive_requests_once_with_events(
    &dir,
    date!(2026 - 05 - 09),
    7,
    None,
    archive_format().into(),
    Some(emitter.as_ref()),
    None,
  )
  .unwrap();

  assert_eq!(stats.archived, 1);
  let events = emitter.events.lock();
  assert!(events
    .iter()
    .any(|event| matches!(event, ArchiveEvent::ScanStarted { .. })));
  assert!(events
    .iter()
    .any(|event| matches!(event, ArchiveEvent::FileStarted { .. })));
  assert!(events
    .iter()
    .any(|event| matches!(event, ArchiveEvent::FileProgress { .. })));
  assert!(events
    .iter()
    .any(|event| matches!(event, ArchiveEvent::FileCompleted { .. })));
  assert!(events
    .iter()
    .any(|event| matches!(event, ArchiveEvent::ScanCompleted { .. })));
}

#[test]
fn cancellation_interrupts_compression_and_removes_temp_archive() {
  let dir = tempdir();
  write_db(&dir, "2026-05-01.db", &vec![b'x'; 1024 * 1024]);
  let cancelled = AtomicBool::new(true);

  let err = archive_requests_once_with_events(
    &dir,
    date!(2026 - 05 - 09),
    7,
    None,
    archive_format().into(),
    None,
    Some(&cancelled),
  )
  .unwrap_err();

  assert_eq!(err.kind(), io::ErrorKind::Interrupted);
  assert!(!dir
    .join(format!("2026-05-01.{}", archive_format().extension()))
    .exists());
  assert!(!dir
    .join(format!("2026-05-01.{}.tmp", archive_format().extension()))
    .exists());
}

fn decode_archive(path: &Path, format: ArchiveFormat, out: &mut Vec<u8>) {
  match format {
    ArchiveFormat::Zstd => {
      zstd::stream::read::Decoder::new(File::open(path).unwrap())
        .unwrap()
        .read_to_end(out)
        .unwrap();
    }
    ArchiveFormat::Xz => decode_xz_archive(path, out),
  }
}

#[cfg(feature = "lzma")]
fn decode_xz_archive(path: &Path, out: &mut Vec<u8>) {
  lzma_rust2::XzReader::new(File::open(path).unwrap(), false)
    .read_to_end(out)
    .unwrap();
}

#[cfg(not(feature = "lzma"))]
fn decode_xz_archive(_path: &Path, _out: &mut Vec<u8>) {
  panic!("xz decoding requires lzma feature");
}

#[test]
fn a_reader_in_another_process_retains_its_day_without_blocking_unrelated_pruning() {
  use rusqlite::{Connection, OpenFlags};
  use std::io::{BufRead, BufReader};
  use std::process::{Command, Stdio};

  // Exercise the SQLite locks used by a separate, offline viewer process.
  const READER_ROOT: &str = "TOKN_TEST_MAINTENANCE_READER_ROOT";
  const DAYS: [&str; 3] = ["2026-05-01.db", "2026-05-02.db", "2026-05-03.db"];
  if let Some(root) = std::env::var_os(READER_ROOT) {
    let readers = DAYS[..2]
      .iter()
      .map(|day| {
        let conn = Connection::open_with_flags(Path::new(&root).join(day), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        tokn_persistence::request_files::begin_read(&conn).unwrap();
        conn
      })
      .collect::<Vec<_>>();
    println!("reader snapshots established");
    io::stdout().flush().unwrap();
    io::stdin().read_exact(&mut [0; 1]).unwrap();
    for conn in readers {
      assert_eq!(
        conn
          .query_row("SELECT id FROM example", [], |row| row.get::<_, i64>(0))
          .unwrap(),
        1
      );
    }
    return;
  }

  let dir = tempdir();
  for (index, name) in DAYS.into_iter().enumerate() {
    let conn = Connection::open(dir.join(name)).unwrap();
    if index == 1 {
      conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    }
    conn
      .execute_batch("CREATE TABLE example(id); INSERT INTO example VALUES (1);")
      .unwrap();
  }
  let today = date!(2026 - 05 - 15);
  let format = ArchiveFormat::Zstd;
  archive_requests_once(&dir, today, 7, format).unwrap();
  let mut child = Command::new(std::env::current_exe().unwrap())
    .args([
      "--exact",
      "tests::a_reader_in_another_process_retains_its_day_without_blocking_unrelated_pruning",
      "--nocapture",
    ])
    .env(READER_ROOT, &dir)
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();
  let mut output = BufReader::new(child.stdout.take().unwrap());
  assert!((&mut output)
    .lines()
    .map_while(Result::ok)
    .any(|line| line.contains("reader snapshots established")));
  let result = prune_request_dbs_once(&dir, today, 7, format, true);
  child.stdin.take().unwrap().write_all(&[1]).unwrap();
  assert!(child.wait().unwrap().success());
  let report = result.unwrap();
  assert_eq!(report.entries[0].outcome, PruneOutcome::InUse);
  assert!(!matches!(report.entries[1].outcome, PruneOutcome::Deleted { .. }));
  assert!(matches!(report.entries[2].outcome, PruneOutcome::Deleted { .. }));
  assert!(dir.join(DAYS[0]).exists());
  assert!(dir.join(DAYS[1]).exists());
  assert!(!dir.join(DAYS[2]).exists());
  let report = prune_request_dbs_once(&dir, today, 7, format, true).unwrap();
  assert!(matches!(report.entries[0].outcome, PruneOutcome::Deleted { .. }));
}

#[cfg(unix)]
#[test]
fn prunes_a_checkpointed_wal_database_through_a_directory_alias() {
  let root = tempdir();
  let dir = root.join("history");
  fs::create_dir(&dir).unwrap();
  let alias = root.join("alias");
  std::os::unix::fs::symlink(&dir, &alias).unwrap();
  let db = dir.join("2026-05-01.db");
  let connection = tokn_persistence::requests::open_day_db(&db).unwrap();
  connection.pragma_update(None, "journal_mode", "WAL").unwrap();
  connection
    .execute(
      "INSERT INTO request_connection(request_id, ts, endpoint) VALUES ('wal-test', 0, 'responses')",
      [],
    )
    .unwrap();
  drop(connection);
  let today = date!(2026 - 05 - 15);
  archive_requests_once(&alias, today, 7, ArchiveFormat::Zstd).unwrap();
  let report = prune_request_dbs_once(&alias, today, 7, ArchiveFormat::Zstd, true).unwrap();
  assert!(
    matches!(report.entries[0].outcome, PruneOutcome::Deleted { .. }),
    "{report:?}"
  );
  assert!(!db.exists());
  assert!(!dir.join("2026-05-01.db-wal").exists());
  assert!(dir.join("2026-05-01.db.zstd").exists());
  fs::remove_dir_all(root).unwrap();
}
