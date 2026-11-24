use rusqlite::params;

use super::support::tempdir;
use crate::usage::UsageDb;
use crate::viewer::get_session_usage;

#[test]
fn missing_usage_database_has_no_session_usage_and_is_not_created() {
  let dir = tempdir();
  let usage_db = dir.join("usage.db");

  assert_eq!(get_session_usage(&usage_db, "session-1").unwrap(), None);
  assert!(!usage_db.exists());
}

#[test]
fn session_usage_aggregates_only_matching_usage_database_rows() {
  let dir = tempdir();
  let usage_db = dir.join("usage.db");
  drop(UsageDb::open(&usage_db).unwrap());
  let conn = rusqlite::Connection::open(&usage_db).unwrap();
  for (request_id, session_id, usage_json) in [
    (
      "request-1",
      "session-1",
      Some(r#"{"input":100,"output":20,"total":120,"cache_read":80,"reasoning":5}"#),
    ),
    (
      "request-2",
      "session-1",
      Some(r#"{"input":200,"output":30,"cache_read":120,"cache_write":10,"reasoning":7}"#),
    ),
    ("request-3", "session-1", None),
    ("request-other", "session-2", Some(r#"{"input":999}"#)),
  ] {
    conn
      .execute(
        "INSERT INTO requests (ts, session_id, request_id, model, usage_json)
         VALUES (1784444800000, ?1, ?2, 'gpt-test', ?3)",
        params![session_id, request_id, usage_json],
      )
      .unwrap();
  }
  drop(conn);

  let usage = get_session_usage(&usage_db, "session-1").unwrap().unwrap();
  assert_eq!(usage.request_count, 3);
  assert_eq!(usage.requests_with_usage, 2);
  assert_eq!(usage.input_tokens, Some(300));
  assert_eq!(usage.output_tokens, Some(50));
  assert_eq!(usage.total_tokens, Some(350));
  assert_eq!(usage.cache_read_tokens, Some(200));
  assert_eq!(usage.cache_write_tokens, Some(10));
  assert_eq!(usage.reasoning_tokens, Some(12));
  assert_eq!(usage.requests.len(), 3);
  let request_1 = usage
    .requests
    .iter()
    .find(|request| request.request_id == "request-1")
    .unwrap();
  assert_eq!(request_1.context_tokens, Some(100));
  assert_eq!(request_1.input_delta_tokens, Some(20));
  assert_eq!(request_1.output_tokens, Some(20));
  let request_2 = usage
    .requests
    .iter()
    .find(|request| request.request_id == "request-2")
    .unwrap();
  assert_eq!(request_2.context_tokens, Some(200));
  assert_eq!(request_2.input_delta_tokens, Some(80));
  assert_eq!(request_2.output_tokens, Some(30));
  assert_eq!(get_session_usage(&usage_db, "missing").unwrap(), None);
}
