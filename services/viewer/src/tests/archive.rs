use super::support::write_request;
use crate::days::request_day_files;
use crate::{
  get_request, get_request_llm_summary, get_request_payload, get_session, list_latest_requests, list_request_days,
  list_request_url_paths, list_requests, RequestDayState, RequestListOptions, RequestPayloadField,
};
use std::{
  fs,
  path::{Path, PathBuf},
};
use time::macros::date;
use tokn_maintenance::{archive_requests_once, prune_request_dbs, ArchiveFormat};
use tokn_persistence::indexed_archive::IndexedArchiveFormat;

const DAY: &str = "2000-01-01";

fn archive_fixture(dir: &Path, format: ArchiveFormat) {
  write_request(dir, DAY, "archived", 946_684_800_000, Some("session"), Some("provider"));
  let report = archive_requests_once(dir, date!(2000 - 01 - 10), 1, format).unwrap();
  assert_eq!(report.archived, 1);
}

fn archive_file(dir: &Path, format: ArchiveFormat) -> PathBuf {
  let db = dir.join(format!("{DAY}.db"));
  #[cfg(feature = "lzma")]
  if format == ArchiveFormat::Xz {
    return IndexedArchiveFormat::Xz.path(&db);
  }
  db.with_extension(format.extension())
}

fn check_archived_history(format: ArchiveFormat) {
  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  archive_fixture(source, format);
  let archive = archive_file(source, format);
  let original_archive = fs::read(&archive).unwrap();
  #[cfg(feature = "lzma")]
  if format == ArchiveFormat::Xz {
    assert!(!source.join(format!("{DAY}.meta.db")).exists());
    assert!(!source.join(format!("{DAY}.db.xz")).exists());
  }

  // The raw database can contain later writes. Present the day once and
  // prefer those rows until its raw file actually disappears.
  write_request(
    source,
    DAY,
    "latest",
    946_684_801_000,
    Some("session"),
    Some("provider"),
  );
  assert_eq!(list_request_days(source).unwrap().len(), 1);
  assert_eq!(list_latest_requests(source, None, None).unwrap().requests.len(), 2);
  let observed = request_day_files(source).unwrap().pop().unwrap();
  fs::remove_file(source.join("2000-01-01.db")).unwrap();
  let db = observed.open().unwrap().unwrap();
  let snapshot = Path::new(db.path().unwrap()).to_owned();
  assert!(!snapshot.starts_with(source));
  assert!(db.execute_batch("CREATE TABLE unexpected (id INTEGER)").is_err());
  let private_dir = (format == ArchiveFormat::Zstd).then(|| snapshot.parent().unwrap());
  #[cfg(unix)]
  if let Some(private_dir) = private_dir {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(fs::metadata(private_dir).unwrap().permissions().mode() & 0o077, 0);
  }
  drop(db);
  assert!(!snapshot.exists());
  if let Some(private_dir) = private_dir {
    assert!(!private_dir.exists());
  }

  assert_eq!(list_request_days(source).unwrap()[0].state, RequestDayState::Available);
  let rows = list_requests(source, &RequestListOptions::default()).unwrap().requests;
  assert_eq!(rows.len(), 1);
  assert_eq!(rows[0].request_id, "archived");
  assert!(get_request(source, DAY, "archived", Some(rows[0].row_id))
    .unwrap()
    .is_some());
  let payload = get_request_payload(source, DAY, "archived", None, RequestPayloadField::InboundReqBody)
    .unwrap()
    .unwrap();
  assert_eq!(payload.value["input"], "hello");
  assert!(get_request_llm_summary(source, DAY, "archived", None)
    .unwrap()
    .is_some());
  assert_eq!(
    list_request_url_paths(source, DAY).unwrap()[0].url_path,
    "/v1/responses"
  );
  assert!(get_session(source, "session", None).unwrap().is_some());
  assert!(!source.join("2000-01-01.db").exists());
  assert_eq!(fs::read(archive).unwrap(), original_archive);
}

#[test]
fn browses_zstd_archives_offline_and_prefers_the_live_database() {
  check_archived_history(ArchiveFormat::Zstd);
}

#[test]
fn browses_indexed_zstd_history_with_live_priority_and_legacy_fallback() {
  use tokn_persistence::request_archive::{indexed, RequestPack};
  use tokn_persistence::request_files::StableFile;

  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  archive_fixture(source, ArchiveFormat::Zstd);
  write_request(source, DAY, "packed", 946_684_801_000, None, None);
  let db = source.join(format!("{DAY}.db"));
  let format = IndexedArchiveFormat::Zstd;
  let archive = format.path(&db);
  {
    let file = StableFile::open(&db).unwrap();
    let snapshot = RequestPack::capture(&file, &mut |_| Ok(())).unwrap().unwrap();
    let mut encoder = format.writer(fs::File::create_new(&archive).unwrap()).unwrap();
    indexed::write(&snapshot, &mut encoder, &mut |_| Ok(())).unwrap();
    encoder.finish().unwrap();
  }

  write_request(source, DAY, "live", 946_684_802_000, None, None);
  assert_eq!(list_latest_requests(source, None, None).unwrap().requests.len(), 3);
  fs::remove_file(db).unwrap();

  let days = list_request_days(source).unwrap();
  assert_eq!(days.len(), 1);
  assert_eq!(days[0].day, DAY);
  assert_eq!(days[0].state, RequestDayState::Available);
  assert_eq!(
    list_requests(source, &RequestListOptions::default())
      .unwrap()
      .requests
      .len(),
    2
  );
  let request = get_request(source, DAY, "packed", None).unwrap().unwrap();
  let payload = get_request_payload(
    source,
    DAY,
    "packed",
    Some(request.row_id),
    RequestPayloadField::InboundReqBody,
  )
  .unwrap()
  .unwrap();
  assert_eq!(payload.value["input"], "hello");

  fs::write(&archive, b"damaged").unwrap();
  assert_eq!(list_request_days(source).unwrap()[0].state, RequestDayState::Available);
  assert!(get_request(source, DAY, "packed", None).unwrap().is_none());
  assert!(get_request(source, DAY, "archived", None).unwrap().is_some());
}

#[cfg(feature = "lzma")]
#[test]
fn browses_indexed_archives_offline_and_prefers_the_live_database() {
  check_archived_history(ArchiveFormat::Xz);
}

#[cfg(feature = "lzma")]
#[test]
fn reads_decoded_pack_payloads_and_keeps_the_captured_headers() {
  use std::io::Write;

  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  let request_id = "archived";
  let db = source.join(format!("{DAY}.db"));
  write_request(source, DAY, request_id, 946_684_800_000, None, None);
  write_request(source, DAY, "other", 946_684_801_000, None, None);
  let request = serde_json::json!({"messages": [{"role": "user", "content": "archived input"}]});
  let inbound_response =
    serde_json::json!({"choices": [{"message": {"role": "assistant", "content": "archived output"}}]});
  let request_bytes = serde_json::to_vec(&request).unwrap();
  let wire_request = zstd::stream::encode_all(request_bytes.as_slice(), 0).unwrap();
  let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
  gzip.write_all(&serde_json::to_vec(&inbound_response).unwrap()).unwrap();
  let wire_response = gzip.finish().unwrap();
  let request_headers = serde_json::json!({
    "content-encoding": "zstd",
    "content-length": wire_request.len().to_string()
  });
  let response_headers = serde_json::json!({
    "content-encoding": "gzip",
    "content-length": wire_response.len().to_string()
  });
  let conn = tokn_persistence::requests::open_day_db(&db).unwrap();
  conn
    .execute(
      "UPDATE request_downstream
       SET inbound_req_body = NULL, inbound_resp_headers = ?2, inbound_resp_body = ?3
       WHERE request_id = ?1",
      rusqlite::params![request_id, response_headers.to_string(), wire_response],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_upstream (request_id, outbound_req_headers, outbound_req_body) VALUES (?1, ?2, ?3)",
      rusqlite::params![request_id, request_headers.to_string(), wire_request],
    )
    .unwrap();
  drop(conn);

  let report = archive_requests_once(source, date!(2000 - 01 - 10), 1, ArchiveFormat::Xz).unwrap();
  assert_eq!(report.archived, 1);
  fs::remove_file(db).unwrap();

  let day = request_day_files(source).unwrap().pop().unwrap();
  let metadata_snapshot = day.open().unwrap().unwrap();
  let restored_bodies: i64 = metadata_snapshot
    .query_row(
      "SELECT count(inbound_req_body) + count(outbound_req_body) + count(inbound_resp_body) FROM requests",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(restored_bodies, 0);
  drop(metadata_snapshot);
  let selected = day.open_request(request_id).unwrap().unwrap();
  let snapshot_body_count: i64 = selected
    .query_row(
      "SELECT count(inbound_req_body) + count(outbound_req_body) + count(inbound_resp_body) FROM requests",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(snapshot_body_count, 0);
  let (packed_body, flags) = selected
    .read_indexed_body(request_id, tokn_persistence::request_archive::OUTBOUND_REQUEST_SLOT)
    .unwrap()
    .unwrap();
  assert_eq!(packed_body, request_bytes);
  assert_ne!(flags & tokn_persistence::request_archive::BODY_IS_DECODED, 0);
  drop(selected);

  for (field, expected) in [
    (RequestPayloadField::OutboundReqHeaders, request_headers),
    (RequestPayloadField::OutboundReqBody, request),
    (RequestPayloadField::InboundRespHeaders, response_headers),
    (RequestPayloadField::InboundRespBody, inbound_response),
  ] {
    let payload = get_request_payload(source, DAY, request_id, None, field)
      .unwrap()
      .unwrap();
    assert_eq!(payload.value, expected, "{}", field.as_str());
  }
  let summary = get_request_llm_summary(source, DAY, request_id, None).unwrap().unwrap();
  assert!(summary.warning.is_none());
  assert_eq!(summary.messages.len(), 2);
  assert_eq!(summary.messages[0].preview.as_deref(), Some("archived input"));
  assert_eq!(summary.messages[1].preview.as_deref(), Some("archived output"));
}

#[cfg(feature = "lzma")]
#[test]
fn reads_a_legacy_sqlite_xz_archive_without_migrating_it() {
  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  let db = source.join(format!("{DAY}.db"));
  let conn = rusqlite::Connection::open(&db).unwrap();
  for migration in &tokn_persistence::requests::MIGRATIONS[..7] {
    conn.execute_batch(migration.sql).unwrap();
  }
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_ts INTEGER NOT NULL);
       INSERT INTO schema_migrations VALUES (7, 'split_requests', 0);
       INSERT INTO request_connection (request_id, ts, endpoint) VALUES ('legacy', 946684800, 'responses');",
    )
    .unwrap();
  drop(conn);

  let report = archive_requests_once(source, date!(2000 - 01 - 10), 1, ArchiveFormat::Xz).unwrap();
  assert_eq!(report.archived, 1);
  fs::remove_file(db).unwrap();

  let detail = get_request(source, DAY, "legacy", None).unwrap().unwrap();
  assert_eq!(detail.request["ts"], 946_684_800_000_i64);
  let opened = request_day_files(source)
    .unwrap()
    .pop()
    .unwrap()
    .open()
    .unwrap()
    .unwrap();
  let version: i64 = opened
    .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
    .unwrap();
  assert_eq!(version, 7);
}

#[cfg(feature = "lzma")]
#[test]
fn reads_decoded_bodies_in_a_legacy_combined_pack() {
  use tokn_persistence::archive_xz::BlockWriter;
  use tokn_persistence::request_archive::RequestPack;
  use tokn_persistence::request_files::StableFile;

  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  write_request(source, DAY, "archived", 946_684_800_000, None, None);
  let db = source.join(format!("{DAY}.db"));
  let request = serde_json::json!({"messages":[{"role":"user","content":"legacy pack input"}]});
  let wire = zstd::stream::encode_all(request.to_string().as_bytes(), 0).unwrap();
  let headers = serde_json::json!({
    "content-encoding": "zstd",
    "content-length": wire.len().to_string()
  });
  let conn = tokn_persistence::requests::open_day_db(&db).unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_req_headers = ?1, inbound_req_body = ?2 WHERE request_id = 'archived'",
      rusqlite::params![headers.to_string(), wire],
    )
    .unwrap();
  drop(conn);

  let stable_db = StableFile::open(&db).unwrap();
  let snapshot = RequestPack::capture(&stable_db, &mut |_| Ok(())).unwrap().unwrap();
  let archive = fs::File::create_new(db.with_extension(ArchiveFormat::Xz.extension())).unwrap();
  let mut encoder = BlockWriter::new(archive);
  snapshot.write(&mut encoder).unwrap();
  encoder.finish().unwrap();
  drop(stable_db);
  fs::remove_file(db).unwrap();

  let summary = get_request_llm_summary(source, DAY, "archived", None).unwrap().unwrap();
  assert!(summary.warning.is_none());
  assert_eq!(summary.messages[0].preview.as_deref(), Some("legacy pack input"));
  let payload = get_request_payload(source, DAY, "archived", None, RequestPayloadField::InboundReqBody)
    .unwrap()
    .unwrap();
  assert_eq!(payload.value, request);
}

#[cfg(feature = "lzma")]
#[test]
fn tries_a_legacy_archive_when_the_indexed_archive_is_missing_or_corrupt() {
  let dir = tempfile::tempdir().unwrap();
  archive_fixture(dir.path(), ArchiveFormat::Xz);
  let legacy = tempfile::tempdir().unwrap();
  write_request(legacy.path(), DAY, "legacy", 946_684_800_000, None, None);
  let legacy_archive = format!("{DAY}.db.zstd");
  let report = archive_requests_once(legacy.path(), date!(2000 - 01 - 10), 1, ArchiveFormat::Zstd).unwrap();
  assert_eq!(report.archived, 1);
  fs::copy(legacy.path().join(&legacy_archive), dir.path().join(legacy_archive)).unwrap();
  let db = dir.path().join(format!("{DAY}.db"));
  fs::write(IndexedArchiveFormat::Zstd.path(&db), b"damaged").unwrap();
  fs::remove_file(&db).unwrap();

  let archive = archive_file(dir.path(), ArchiveFormat::Xz);
  let parked_pack = dir.path().join("parked-pack");
  fs::rename(&archive, &parked_pack).unwrap();
  assert!(get_request(dir.path(), DAY, "legacy", None).unwrap().is_some());
  fs::rename(parked_pack, &archive).unwrap();

  assert!(get_request(dir.path(), DAY, "archived", None).unwrap().is_some());
  assert!(get_request(dir.path(), DAY, "legacy", None).unwrap().is_none());
  fs::write(archive, b"damaged").unwrap();
  assert_eq!(
    list_request_days(dir.path()).unwrap()[0].state,
    RequestDayState::Available
  );
  assert!(get_request(dir.path(), DAY, "legacy", None).unwrap().is_some());
}

#[cfg(feature = "lzma")]
#[test]
fn indexed_payload_queries_require_matching_request_row_identity() {
  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  archive_fixture(source, ArchiveFormat::Xz);
  let db = source.join(format!("{DAY}.db"));
  fs::remove_file(db).unwrap();

  let request = get_request(source, DAY, "archived", None).unwrap().unwrap();
  let field = RequestPayloadField::InboundReqBody;
  assert!(
    get_request_payload(source, DAY, "archived", Some(request.row_id + 1), field)
      .unwrap()
      .is_none()
  );
  let payload = get_request_payload(source, DAY, "archived", Some(request.row_id), field)
    .unwrap()
    .unwrap();
  assert_eq!(payload.value["input"], "hello");
}

#[test]
fn pruning_respects_a_viewer_read_transaction() {
  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  archive_fixture(source, ArchiveFormat::Zstd);
  let day = request_day_files(source).unwrap().pop().unwrap();
  let reader = day.open().unwrap().unwrap();
  prune_request_dbs(source, Some("zstd"), 1, true).unwrap();
  assert!(day.path.exists());
  assert_eq!(
    reader
      .query_row("SELECT count(*) FROM requests", [], |row| row.get::<_, i64>(0))
      .unwrap(),
    1
  );
  drop(reader);
  prune_request_dbs(source, Some("zstd"), 1, true).unwrap();
  assert!(!day.path.exists());
  assert!(get_request(source, DAY, "archived", None).unwrap().is_some());
}

#[test]
fn a_corrupt_archive_is_unavailable_and_other_days_remain_readable() {
  let dir = tempfile::tempdir().unwrap();
  fs::write(dir.path().join("2000-01-02.db.zstd"), b"damaged").unwrap();
  write_request(dir.path(), DAY, "live", 946_684_800_000, None, None);
  assert_eq!(
    list_request_days(dir.path()).unwrap()[0].state,
    RequestDayState::Unavailable
  );
  assert_eq!(
    list_latest_requests(dir.path(), None, None).unwrap().requests[0].request_id,
    "live"
  );
  assert!(get_request(dir.path(), "2000-01-02", "damaged", None).is_err());
}
