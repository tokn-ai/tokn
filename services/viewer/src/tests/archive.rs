use super::support::write_request;
use crate::days::request_day_files;
use crate::{
  get_request, get_request_llm_summary, get_request_payload, get_session, list_latest_requests, list_request_days,
  list_request_url_paths, list_requests, RequestDayState, RequestListOptions, RequestPayloadField,
};
use std::{fs, path::Path};
use time::macros::date;
use tokn_maintenance::{archive_requests_once, prune_request_dbs, ArchiveFormat};

const DAY: &str = "2000-01-01";

fn archive_fixture(dir: &Path, format: ArchiveFormat) {
  write_request(dir, DAY, "archived", 946_684_800_000, Some("session"), Some("provider"));
  let report = archive_requests_once(dir, date!(2000 - 01 - 10), 1, format).unwrap();
  assert_eq!(report.archived, 1);
}

fn check_archived_history(format: ArchiveFormat) {
  let dir = tempfile::tempdir().unwrap();
  let source = dir.path();
  archive_fixture(source, format);
  let file = source.join(DAY).with_extension(format.extension());
  let original_archive = fs::read(&file).unwrap();

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
  let private_dir = snapshot.parent().unwrap().to_owned();
  assert!(!snapshot.starts_with(source));
  assert!(db.execute_batch("CREATE TABLE unexpected (id INTEGER)").is_err());
  #[cfg(unix)]
  {
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(fs::metadata(&private_dir).unwrap().permissions().mode() & 0o077, 0);
  }
  drop(db);
  assert!(!private_dir.exists());

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
  assert_eq!(fs::read(file).unwrap(), original_archive);
}

#[test]
fn browses_zstd_archives_offline_and_prefers_the_live_database() {
  check_archived_history(ArchiveFormat::Zstd);
}

#[cfg(feature = "lzma")]
#[test]
fn browses_xz_archives_offline_and_prefers_the_live_database() {
  check_archived_history(ArchiveFormat::Xz);
}

#[cfg(feature = "lzma")]
#[test]
fn tries_another_archive_when_the_first_format_is_corrupt() {
  let dir = tempfile::tempdir().unwrap();
  archive_fixture(dir.path(), ArchiveFormat::Xz);
  fs::remove_file(dir.path().join("2000-01-01.db")).unwrap();
  fs::write(dir.path().join("2000-01-01.db.zstd"), b"damaged").unwrap();
  assert_eq!(
    list_request_days(dir.path()).unwrap()[0].state,
    RequestDayState::Available
  );
  assert!(get_request(dir.path(), DAY, "archived", None).unwrap().is_some());
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
