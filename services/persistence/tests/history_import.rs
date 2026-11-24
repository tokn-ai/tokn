use rusqlite::{params, Connection};
use std::fs;
use std::path::{Path, PathBuf};
use tokn_persistence::{history_import::import_history, requests::open_day_db, sessions::SessionsDb, DbPaths, UsageDb};

const DAY: &str = "2026-09-21.db";

struct Fixture {
  root: PathBuf,
  source: PathBuf,
  destination: DbPaths,
}
impl Fixture {
  fn new() -> Self {
    let root = std::env::temp_dir().join(format!("tokn-history-import-{}", uuid::Uuid::new_v4()));
    let source = root.join("source");
    let destination = paths(&root.join("destination"));
    initialize(&paths(&source));
    initialize(&destination);
    let source_paths = paths(&source);
    Connection::open(&source_paths.usage_db)
      .unwrap()
      .execute(
        "INSERT INTO requests (id, ts, request_id, model) VALUES (1, 100, 'trial-request', 'trial-model')",
        [],
      )
      .unwrap();
    Connection::open(&source_paths.sessions_db).unwrap().execute_batch(
      "INSERT INTO sessions (id, first_seen_ts, last_seen_ts, source) VALUES ('trial-session', 100, 100, 'header');
       INSERT INTO part_blobs (hash, part_type, content) VALUES ('part-hash', 'text', X'68656c6c6f');
       INSERT INTO message_tree (id, depth, message_hash, role) VALUES (zeroblob(32), 1, zeroblob(32), 'user');
       INSERT INTO message_parts (message_id, part_index, part_hash) VALUES (zeroblob(32), 0, 'part-hash');
       INSERT INTO session_nodes (id, session_id, request_id, ts, endpoint, reduction_kind, parent_source, message_id)
         VALUES ('trial-node', 'trial-session', 'trial-request', 100, 'responses', 'message_tree', 'none', zeroblob(32));
       INSERT INTO session_heads (session_id, node_id, updated_ts) VALUES ('trial-session', 'trial-node', 100);",
    ).unwrap();
    open_day_db(&source_paths.requests_dir.join(DAY)).unwrap().execute_batch(
      "INSERT INTO request_connection (request_id, ts) VALUES ('trial-request', 100);
       INSERT INTO request_metadata (request_id, session_id, model) VALUES ('trial-request', 'trial-session', 'trial-model');",
    ).unwrap();
    Self {
      root,
      source,
      destination,
    }
  }
  fn error(&self, commit: bool) -> String {
    import_history(&self.source, &self.destination, commit)
      .unwrap_err()
      .to_string()
  }
  fn day(&self) -> PathBuf {
    self.destination.requests_dir.join(DAY)
  }
}
impl Drop for Fixture {
  fn drop(&mut self) {
    let _ = fs::remove_dir_all(&self.root);
  }
}
fn paths(root: &Path) -> DbPaths {
  DbPaths {
    usage_db: root.join("usage.db"),
    sessions_db: root.join("sessions.db"),
    requests_dir: root.join("requests"),
  }
}
fn initialize(paths: &DbPaths) {
  fs::create_dir_all(&paths.requests_dir).unwrap();
  drop(UsageDb::open(&paths.usage_db).unwrap());
  drop(SessionsDb::open(&paths.sessions_db).unwrap());
}
fn count(path: &Path, table: &str) -> i64 {
  Connection::open(path)
    .unwrap()
    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get(0))
    .unwrap()
}

#[test]
fn import_remaps_usage_ids_preserves_graph_and_is_idempotent() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.usage_db)
    .unwrap()
    .execute(
      "INSERT INTO requests (id, ts, request_id, model) VALUES (1, 99, 'host-request', 'host-model')",
      [],
    )
    .unwrap();
  let first = import_history(&fixture.source, &fixture.destination, true).unwrap();
  assert!(first.committed);
  assert_eq!(first.inserted_total, 9);
  assert_eq!(first.identical_skipped_total, 0);
  assert!(first.databases[2].created);
  let conn = Connection::open(&fixture.destination.usage_db).unwrap();
  let id: i64 = conn
    .query_row(
      "SELECT id FROM requests WHERE request_id = 'trial-request'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(id, 2);
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 2);
  let sessions = Connection::open(&fixture.destination.sessions_db).unwrap();
  let hash: Vec<u8> = sessions
    .query_row("SELECT message_id FROM session_nodes", [], |row| row.get(0))
    .unwrap();
  assert_eq!(hash, vec![0; 32]);
  assert!(!sessions
    .prepare("PRAGMA foreign_key_check")
    .unwrap()
    .exists([])
    .unwrap());
  let second = import_history(&fixture.source, &fixture.destination, true).unwrap();
  assert_eq!(second.inserted_total, 0);
  assert_eq!(second.identical_skipped_total, first.inserted_total);
  assert!(!second.databases[2].created);
}

#[test]
fn dry_run_checks_all_rows_and_removes_new_day() {
  let fixture = Fixture::new();
  let source_before = fs::read(fixture.source.join("usage.db")).unwrap();
  let report = import_history(&fixture.source, &fixture.destination, false).unwrap();
  assert!(!report.committed);
  assert_eq!(report.inserted_total, 9);
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert_eq!(count(&fixture.destination.sessions_db, "sessions"), 0);
  assert!(!fixture.day().exists());
  assert_eq!(fs::read(fixture.source.join("usage.db")).unwrap(), source_before);
}

#[test]
fn late_collision_rolls_back_every_database() {
  let fixture = Fixture::new();
  open_day_db(&fixture.day())
    .unwrap()
    .execute(
      "INSERT INTO request_connection (request_id, ts) VALUES ('trial-request', 999)",
      [],
    )
    .unwrap();
  assert!(fixture.error(true).contains("Nonidentical collision"));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert_eq!(count(&fixture.destination.sessions_db, "sessions"), 0);
  let ts: i64 = Connection::open(fixture.day())
    .unwrap()
    .query_row("SELECT ts FROM request_connection", [], |row| row.get(0))
    .unwrap();
  assert_eq!(ts, 999);
}

#[test]
fn collision_in_usage_dry_run_removes_initialized_day() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.usage_db)
    .unwrap()
    .execute(
      "INSERT INTO requests (ts, request_id, model) VALUES (100, 'trial-request', 'different-model')",
      [],
    )
    .unwrap();
  assert!(fixture.error(false).contains("Nonidentical collision"));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 1);
  assert!(!fixture.day().exists());
}

#[test]
fn schemas_compare_column_names_instead_of_ordinals() {
  let fixture = Fixture::new();
  // This mirrors a migrated database where ALTER TABLE appended `user`.
  let conn = Connection::open(&fixture.destination.usage_db).unwrap();
  conn.execute_batch("DROP INDEX idx_requests_user; ALTER TABLE requests DROP COLUMN user; ALTER TABLE requests ADD COLUMN user TEXT; CREATE INDEX idx_requests_user ON requests(user);").unwrap();
  let report = import_history(&fixture.source, &fixture.destination, true).unwrap();
  assert_eq!(report.inserted_total, 9);
}

#[test]
fn source_with_dangling_foreign_key_is_rejected_even_for_dry_run() {
  let fixture = Fixture::new();
  let conn = Connection::open(fixture.source.join("sessions.db")).unwrap();
  conn.pragma_update(None, "foreign_keys", false).unwrap();
  conn
    .execute(
      "INSERT INTO message_parts (message_id, part_index, part_hash) VALUES (?1, 0, 'missing-part')",
      params![vec![1u8; 32]],
    )
    .unwrap();
  assert!(fixture.error(false).contains("Source foreign key check failed"));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert!(!fixture.day().exists());
}

#[test]
fn destination_schema_mismatch_rolls_back_and_cleans_new_day() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute(
      "DELETE FROM schema_migrations WHERE version = ?1",
      [tokn_persistence::sessions::latest_version()],
    )
    .unwrap();
  assert!(fixture.error(true).contains("Schema version mismatch"));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert!(!fixture.day().exists());
}

#[test]
fn archived_destination_day_is_never_recreated() {
  let fixture = Fixture::new();
  fs::write(fixture.destination.requests_dir.join(format!("{DAY}.xz")), b"archive").unwrap();
  assert!(fixture.error(true).contains("has an archive"));
  assert!(!fixture.day().exists());
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
}

#[test]
fn active_source_sidecars_are_rejected() {
  let fixture = Fixture::new();
  for suffix in ["-wal", "-journal"] {
    let sidecar = fixture.source.join(format!("usage.db{suffix}"));
    fs::write(&sidecar, []).unwrap();
    assert!(fixture.error(true).contains("stopped and checkpointed"));
    fs::remove_file(sidecar).unwrap();
  }
}

#[test]
fn wal_destination_mode_is_rejected_without_changing_it() {
  let fixture = Fixture::new();
  let conn = Connection::open(&fixture.destination.usage_db).unwrap();
  conn.pragma_update(None, "journal_mode", "wal").unwrap();
  assert!(fixture.error(true).contains("requires DELETE journal mode"));
  let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0)).unwrap();
  assert_eq!(mode, "wal");
  assert!(!fixture.day().exists());
}

#[test]
fn hardlinked_source_and_destination_are_rejected() {
  let fixture = Fixture::new();
  fs::remove_file(&fixture.destination.usage_db).unwrap();
  fs::hard_link(fixture.source.join("usage.db"), &fixture.destination.usage_db).unwrap();
  assert!(fixture.error(true).contains("alias each other"));
  assert!(!fixture.day().exists());
}

#[cfg(unix)]
#[test]
fn symlinked_source_or_destination_is_rejected() {
  use std::os::unix::fs::symlink;
  let fixture = Fixture::new();
  let original = fixture.source.join("original.db");
  fs::rename(fixture.source.join("usage.db"), &original).unwrap();
  symlink(&original, fixture.source.join("usage.db")).unwrap();
  assert!(fixture.error(true).contains("non-symlink database"));
  fs::remove_file(fixture.source.join("usage.db")).unwrap();
  fs::rename(original, fixture.source.join("usage.db")).unwrap();
  let original = fixture.root.join("original-target.db");
  fs::rename(&fixture.destination.usage_db, &original).unwrap();
  symlink(&original, &fixture.destination.usage_db).unwrap();
  assert!(fixture.error(true).contains("non-symlink database"));
}

#[test]
fn archived_source_days_cannot_be_silently_omitted() {
  let fixture = Fixture::new();
  for extension in ["xz", "zstd", "zst", "lzma"] {
    // A different day simulates an older archived day alongside the current raw day.
    let archive = fixture.source.join(format!("requests/2026-09-20.db.{extension}"));
    fs::write(&archive, b"archive").unwrap();
    assert!(fixture
      .error(true)
      .contains("Archived source request days are not supported"));
    assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
    assert!(!fixture.day().exists());
    fs::remove_file(archive).unwrap();
  }
}

#[test]
fn orphan_source_request_sidecars_are_rejected() {
  let fixture = Fixture::new();
  for suffix in ["-wal", "-journal", "-shm"] {
    let sidecar = fixture.source.join(format!("requests/2026-09-20.db{suffix}"));
    fs::write(&sidecar, []).unwrap();
    assert!(fixture.error(true).contains("unexpected request sidecar"));
    assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
    assert!(!fixture.day().exists());
    fs::remove_file(sidecar).unwrap();
  }
}

#[test]
fn source_request_scan_ignores_only_the_regular_maintenance_lock() {
  let fixture = Fixture::new();
  fs::write(fixture.source.join("requests/.tokn-requests-maintenance.lock"), []).unwrap();
  assert_eq!(
    import_history(&fixture.source, &fixture.destination, false)
      .unwrap()
      .inserted_total,
    9
  );
  let unexpected = fixture.source.join("requests/scratch.txt");
  fs::write(&unexpected, b"scratch").unwrap();
  assert!(fixture.error(true).contains("Unexpected source request entry"));
  fs::remove_file(unexpected).unwrap();
  fs::create_dir(fixture.source.join("requests/nested-capture")).unwrap();
  assert!(fixture.error(true).contains("regular, non-symlink database"));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert!(!fixture.day().exists());
}

#[cfg(unix)]
#[test]
fn source_request_symlinks_are_rejected_regardless_of_filename() {
  let fixture = Fixture::new();
  for name in ["ignored.txt", ".tokn-requests-maintenance.lock", "2026-09-20.db.xz"] {
    let link = fixture.source.join("requests").join(name);
    std::os::unix::fs::symlink(fixture.source.join("usage.db"), &link).unwrap();
    assert!(fixture.error(true).contains("regular, non-symlink database"));
    fs::remove_file(link).unwrap();
  }
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert!(!fixture.day().exists());
}

fn assert_no_imported_history(fixture: &Fixture) {
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert_eq!(count(&fixture.destination.sessions_db, "sessions"), 0);
  assert!(!fixture.day().exists());
}

#[test]
fn unexpected_source_tables_are_rejected_before_creating_destination_files() {
  let fixture = Fixture::new();
  Connection::open(fixture.source.join("usage.db"))
    .unwrap()
    .execute_batch("CREATE TABLE unexpected_capture_data (id INTEGER PRIMARY KEY);")
    .unwrap();
  assert!(fixture.error(true).contains("Unexpected table set in main"));
  assert_no_imported_history(&fixture);
  assert!(!fixture
    .destination
    .requests_dir
    .join(".tokn-requests-maintenance.lock")
    .exists());
}

#[test]
fn source_triggers_are_rejected_without_running_them() {
  let fixture = Fixture::new();
  Connection::open(fixture.source.join("usage.db"))
    .unwrap()
    .execute_batch("CREATE TRIGGER capture_trigger AFTER INSERT ON requests BEGIN DELETE FROM requests; END;")
    .unwrap();
  assert!(fixture
    .error(true)
    .contains("does not support database triggers in main"));
  assert_eq!(count(&fixture.source.join("usage.db"), "requests"), 1);
  assert_no_imported_history(&fixture);
}

#[test]
fn incompatible_source_versions_are_rejected_without_migrating_the_capture() {
  for version_delta in [-1, 1] {
    let fixture = Fixture::new();
    let source = fixture.source.join("usage.db");
    let conn = Connection::open(&source).unwrap();
    let version = i64::from(tokn_persistence::usage::latest_version()) + version_delta;
    conn.execute("DELETE FROM schema_migrations", []).unwrap();
    conn
      .execute(
        "INSERT INTO schema_migrations (version, name, applied_ts) VALUES (?1, 'capture-version', 0)",
        [version],
      )
      .unwrap();
    drop(conn);
    let before = fs::read(&source).unwrap();
    let error = fixture.error(true);
    assert!(
      error.contains(&format!("Source schema version {version} is not current")),
      "{error}"
    );
    assert_eq!(fs::read(&source).unwrap(), before);
    assert_no_imported_history(&fixture);
  }
}

#[test]
fn source_usage_requires_nonnull_request_identity() {
  let fixture = Fixture::new();
  Connection::open(fixture.source.join("usage.db"))
    .unwrap()
    .execute(
      "INSERT INTO requests (ts, model) VALUES (101, 'uncorrelated-model')",
      [],
    )
    .unwrap();
  assert!(fixture.error(false).contains("Missing stable identity"));
  assert_no_imported_history(&fixture);
}

#[test]
fn source_usage_without_request_id_column_is_rejected() {
  let fixture = Fixture::new();
  Connection::open(fixture.source.join("usage.db"))
    .unwrap()
    .execute_batch("DROP INDEX idx_requests_request; ALTER TABLE requests DROP COLUMN request_id;")
    .unwrap();
  assert!(fixture.error(true).contains("Missing stable identity"));
  assert_no_imported_history(&fixture);
}

#[test]
fn source_request_tables_require_a_primary_key_even_when_empty() {
  let fixture = Fixture::new();
  Connection::open(fixture.source.join("requests").join(DAY))
    .unwrap()
    .execute_batch(
      "DROP VIEW requests;
       ALTER TABLE request_upstream RENAME TO old_request_upstream;
       CREATE TABLE request_upstream AS SELECT * FROM old_request_upstream;
       DROP TABLE old_request_upstream;",
    )
    .unwrap();
  assert!(fixture.error(true).contains("Missing stable identity"));
  assert_no_imported_history(&fixture);
}

#[test]
fn source_index_corruption_fails_integrity_validation() {
  let fixture = Fixture::new();
  // Simulate an index whose persisted entries no longer match its schema. A
  // read-only integrity check detects this before any destination is opened.
  Connection::open(fixture.source.join("usage.db"))
    .unwrap()
    .execute_batch(
      "PRAGMA writable_schema=ON;
       UPDATE sqlite_master SET sql='CREATE INDEX idx_requests_ts ON requests(model)'
         WHERE type='index' AND name='idx_requests_ts';
       PRAGMA writable_schema=OFF;",
    )
    .unwrap();
  let error = fixture.error(true);
  assert!(error.contains("Source integrity check failed"), "{error}");
  assert_no_imported_history(&fixture);
}

#[test]
fn damaged_source_database_is_reported_without_touching_destination_history() {
  let fixture = Fixture::new();
  fs::write(fixture.source.join("usage.db"), b"not a SQLite database").unwrap();
  assert!(matches!(
    import_history(&fixture.source, &fixture.destination, true),
    Err(tokn_persistence::history_import::ImportError::Sqlite { .. })
  ));
  assert_no_imported_history(&fixture);
}

#[test]
fn destination_column_mismatch_rolls_back_rows_from_prior_tables_and_databases() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute_batch("ALTER TABLE part_blobs ADD COLUMN import_extra TEXT;")
    .unwrap();
  assert!(fixture
    .error(true)
    .contains("Column schema mismatch: target_1/part_blobs"));
  assert_no_imported_history(&fixture);
  assert_eq!(count(&fixture.destination.sessions_db, "part_blobs"), 0);
}

#[test]
fn destination_foreign_key_mismatch_rolls_back_imported_graph() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute_batch(
      "DROP TABLE session_relations;
       CREATE TABLE session_relations (
         parent_session_id TEXT NOT NULL REFERENCES sessions(id),
         child_session_id TEXT NOT NULL,
         relation_kind TEXT NOT NULL,
         first_seen_ts INTEGER NOT NULL,
         last_seen_ts INTEGER NOT NULL,
         source TEXT NOT NULL,
         PRIMARY KEY(parent_session_id, child_session_id, relation_kind)
       );",
    )
    .unwrap();
  assert!(fixture
    .error(true)
    .contains("Foreign key schema mismatch: target_1/session_relations"));
  assert_no_imported_history(&fixture);
  for table in ["part_blobs", "message_tree", "message_parts", "session_nodes"] {
    assert_eq!(count(&fixture.destination.sessions_db, table), 0);
  }
}

#[test]
fn destination_triggers_are_rejected_before_they_can_change_imported_rows() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute_batch("CREATE TRIGGER reject_session AFTER INSERT ON sessions BEGIN DELETE FROM sessions; END;")
    .unwrap();
  assert!(fixture
    .error(true)
    .contains("does not support database triggers in target_1"));
  assert_no_imported_history(&fixture);
}

#[test]
fn destination_extra_tables_are_preserved_while_import_rolls_back() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute_batch("CREATE TABLE local_notes (note TEXT); INSERT INTO local_notes VALUES ('keep existing note');")
    .unwrap();
  assert!(fixture.error(true).contains("Unexpected table set in target_1"));
  assert_no_imported_history(&fixture);
  let note: String = Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .query_row("SELECT note FROM local_notes", [], |row| row.get(0))
    .unwrap();
  assert_eq!(note, "keep existing note");
}

#[test]
fn destination_constraint_errors_preserve_existing_rows_and_rollback_other_databases() {
  let fixture = Fixture::new();
  Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .execute_batch(
      "INSERT INTO sessions (id, first_seen_ts, last_seen_ts, source) VALUES ('host-session', 99, 99, 'header');
       CREATE UNIQUE INDEX one_session_per_source ON sessions(source);",
    )
    .unwrap();
  assert!(matches!(
    import_history(&fixture.source, &fixture.destination, true),
    Err(tokn_persistence::history_import::ImportError::Sqlite { .. })
  ));
  assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
  assert_eq!(count(&fixture.destination.sessions_db, "sessions"), 1);
  let session_id: String = Connection::open(&fixture.destination.sessions_db)
    .unwrap()
    .query_row("SELECT id FROM sessions", [], |row| row.get(0))
    .unwrap();
  assert_eq!(session_id, "host-session");
  assert!(!fixture.day().exists());
}

#[test]
fn source_root_must_be_an_existing_directory() {
  let fixture = Fixture::new();
  let error = import_history(&fixture.source.join("usage.db"), &fixture.destination, true)
    .unwrap_err()
    .to_string();
  assert!(error.contains("Expected a non-symlink directory"));
  assert!(matches!(
    import_history(&fixture.root.join("missing-capture"), &fixture.destination, true),
    Err(tokn_persistence::history_import::ImportError::Io { .. })
  ));
  assert_no_imported_history(&fixture);
}

#[test]
fn request_directories_cannot_be_regular_files() {
  for replace_source in [true, false] {
    let fixture = Fixture::new();
    let requests = if replace_source {
      fixture.source.join("requests")
    } else {
      fixture.destination.requests_dir.clone()
    };
    fs::remove_dir_all(&requests).unwrap();
    fs::write(&requests, b"not a directory").unwrap();
    assert!(fixture.error(true).contains("Expected a non-symlink directory"));
    assert_no_imported_history(&fixture);
  }
}

#[test]
fn missing_usage_or_sessions_destination_is_not_created_by_import() {
  for remove_usage in [true, false] {
    let fixture = Fixture::new();
    let missing = if remove_usage {
      &fixture.destination.usage_db
    } else {
      &fixture.destination.sessions_db
    };
    fs::remove_file(missing).unwrap();
    assert!(fixture.error(true).contains("Destination must already exist"));
    assert!(!missing.exists());
    if remove_usage {
      assert_eq!(count(&fixture.destination.sessions_db, "sessions"), 0);
    } else {
      assert_eq!(count(&fixture.destination.usage_db, "requests"), 0);
    }
    assert!(!fixture.day().exists());
  }
}

#[test]
fn identical_source_and_destination_roots_are_rejected_before_lock_creation() {
  let fixture = Fixture::new();
  let source_paths = paths(&fixture.source);
  let before = fs::read(&source_paths.usage_db).unwrap();
  let error = import_history(&fixture.source, &source_paths, true)
    .unwrap_err()
    .to_string();
  assert!(error.contains("Source and destination must be separate"));
  assert_eq!(fs::read(&source_paths.usage_db).unwrap(), before);
  assert!(!source_paths
    .requests_dir
    .join(".tokn-requests-maintenance.lock")
    .exists());
  assert_no_imported_history(&fixture);
}

#[test]
fn destination_database_aliases_are_rejected_before_opening_them_for_writing() {
  let fixture = Fixture::new();
  fs::remove_file(&fixture.destination.sessions_db).unwrap();
  fs::hard_link(&fixture.destination.usage_db, &fixture.destination.sessions_db).unwrap();
  let before = fs::read(&fixture.destination.usage_db).unwrap();
  assert!(fixture
    .error(true)
    .contains("Destination database files alias each other"));
  assert_eq!(fs::read(&fixture.destination.usage_db).unwrap(), before);
  assert!(!fixture.day().exists());
}

#[test]
fn duplicate_destination_paths_are_rejected_before_opening_them_for_writing() {
  let mut fixture = Fixture::new();
  fixture.destination.sessions_db = fixture.destination.usage_db.clone();
  let before = fs::read(&fixture.destination.usage_db).unwrap();
  assert!(fixture
    .error(true)
    .contains("Destination database paths must be distinct"));
  assert_eq!(fs::read(&fixture.destination.usage_db).unwrap(), before);
  assert!(!fixture.day().exists());
}

#[test]
fn request_day_names_must_represent_calendar_dates() {
  for filename in ["not-a-day.db", "2026-02-30.db", "2026-13-01.db"] {
    let fixture = Fixture::new();
    fs::write(fixture.source.join("requests").join(filename), []).unwrap();
    assert!(fixture.error(true).contains("Invalid request day"));
    assert_no_imported_history(&fixture);
  }
}

#[test]
fn nine_request_days_fit_atomic_import_and_ten_are_rejected_without_partial_history() {
  let fixture = Fixture::new();
  for day in 1..=8 {
    drop(open_day_db(&fixture.source.join(format!("requests/2026-09-{day:02}.db"))).unwrap());
  }
  let report = import_history(&fixture.source, &fixture.destination, false).unwrap();
  assert_eq!(report.databases.len(), 11);
  assert_eq!(report.databases.iter().filter(|database| database.created).count(), 9);
  assert_eq!(report.inserted_total, 9);
  assert_no_imported_history(&fixture);
  for day in 1..=8 {
    assert!(!fixture
      .destination
      .requests_dir
      .join(format!("2026-09-{day:02}.db"))
      .exists());
  }
  drop(open_day_db(&fixture.source.join("requests/2026-09-09.db")).unwrap());
  assert!(fixture.error(true).contains("at most nine request days"));
  assert_no_imported_history(&fixture);
  for day in 1..=9 {
    assert!(!fixture
      .destination
      .requests_dir
      .join(format!("2026-09-{day:02}.db"))
      .exists());
  }
}

// Linux permits invalid UTF-8 filenames; macOS filesystems reject their creation.
#[cfg(target_os = "linux")]
#[test]
fn non_utf8_source_request_filenames_are_rejected() {
  use std::os::unix::ffi::OsStringExt;
  let fixture = Fixture::new();
  let name = std::ffi::OsString::from_vec(b"2026-09-20\xff.db".to_vec());
  fs::write(fixture.source.join("requests").join(name), []).unwrap();
  assert!(fixture.error(true).contains("Invalid source request filename"));
  assert_no_imported_history(&fixture);
}

#[cfg(target_os = "linux")]
#[test]
fn non_utf8_attached_destination_paths_fail_and_remove_initialized_request_days() {
  use std::os::unix::ffi::OsStringExt;
  let mut fixture = Fixture::new();
  let name = std::ffi::OsString::from_vec(b"sessions-\xff.db".to_vec());
  let destination = fixture.destination.sessions_db.parent().unwrap().join(name);
  fs::rename(&fixture.destination.sessions_db, &destination).unwrap();
  fixture.destination.sessions_db = destination;
  assert!(fixture.error(true).contains("Destination path must be UTF-8"));
  assert_no_imported_history(&fixture);
}

#[cfg(unix)]
#[test]
fn symlinked_request_directories_are_rejected_without_following_them() {
  let fixture = Fixture::new();
  let actual = fixture.root.join("actual-requests");
  fs::rename(&fixture.destination.requests_dir, &actual).unwrap();
  std::os::unix::fs::symlink(&actual, &fixture.destination.requests_dir).unwrap();
  assert!(fixture.error(true).contains("Expected a non-symlink directory"));
  assert!(fs::read_dir(actual).unwrap().next().is_none());
  assert_no_imported_history(&fixture);
}
