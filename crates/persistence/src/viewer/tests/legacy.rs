use rusqlite::{params, Connection};

use super::super::{
  get_request, get_request_payload, list_request_days, list_requests, list_sessions, RequestCursor, RequestDay,
  RequestDayState, RequestListOptions, RequestPayloadField,
};
use super::support::{request_ids, tempdir};

const PAYLOAD_FIELDS: &[&str] = &[
  "inbound_req_headers",
  "inbound_req_body",
  "inbound_resp_headers",
  "inbound_resp_body",
  "outbound_req_headers",
  "outbound_req_body",
  "outbound_resp_headers",
  "outbound_resp_body",
];

#[test]
fn split_v7_requests_have_no_classification_and_remain_unmigrated() {
  let dir = tempdir();
  let day = "2026-07-14";
  let path = dir.join(format!("{day}.db"));
  let conn = Connection::open(&path).unwrap();
  for sql in [
    include_str!("../../../schemas/snapshot/requests/v0.0.0.sql"),
    include_str!("../../../schemas/migrations/requests/0002_add_correlation_and_error.sql"),
    include_str!("../../../schemas/migrations/requests/0003_add_usage_breakdown.sql"),
    include_str!("../../../schemas/migrations/requests/0004_add_response_header_latency.sql"),
    include_str!("../../../schemas/migrations/requests/0005_add_source_and_method.sql"),
    include_str!("../../../schemas/migrations/requests/0006_add_context_and_metrics.sql"),
    include_str!("../../../schemas/migrations/requests/0007_split_requests.sql"),
  ] {
    conn.execute_batch(sql).unwrap();
  }
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_ts INTEGER NOT NULL);
       INSERT INTO schema_migrations VALUES (7, 'split_requests', 0);
       INSERT INTO request_connection (request_id, ts, endpoint) VALUES ('split-request', 1784444800, 'responses');
       INSERT INTO request_metadata (request_id, model) VALUES ('split-request', 'gpt-test');",
    )
    .unwrap();
  drop(conn);

  let page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some(day.to_string()),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(page.requests.len(), 1);
  assert!(page.requests[0].request_classification.is_none());
  assert_eq!(page.requests[0].ts, 1_784_444_800_000);
  let conn = Connection::open(path).unwrap();
  let version: u32 = conn
    .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
    .unwrap();
  assert_eq!(version, 7);
}

#[test]
fn legacy_pagination_uses_numeric_row_id_with_duplicate_and_null_request_ids() {
  let dir = tempdir();
  let path = dir.join("2026-07-14.db");
  let conn = Connection::open(&path).unwrap();
  conn
    .execute_batch(include_str!("../../../schemas/snapshot/requests/v0.0.0.sql"))
    .unwrap();
  conn
    .execute_batch(include_str!(
      "../../../schemas/migrations/requests/0002_add_correlation_and_error.sql"
    ))
    .unwrap();
  for (id, request_id, model) in [
    (9_i64, None, "model-9"),
    (10_i64, None, "model-10"),
    (11_i64, Some("duplicate"), "model-11"),
    (12_i64, Some("duplicate"), "model-12"),
  ] {
    let body = format!("{{\"row_id\":{id}}}");
    conn
      .execute(
        "INSERT INTO requests (
           id, ts, session_id, endpoint, account_id, provider_id, model, initiator, status, stream,
           latency_ms, inbound_req_headers, inbound_req_body, request_id
         ) VALUES (?1, 1784444800, 'session', 'responses', 'account', 'openai', ?2, 'test', 200, 0,
                   1, '{}', ?3, ?4)",
        params![id, model, body, request_id],
      )
      .unwrap();
  }
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (
         version INTEGER PRIMARY KEY,
         name TEXT NOT NULL,
         applied_ts INTEGER NOT NULL
       );
       INSERT INTO schema_migrations (version, name, applied_ts) VALUES
         (1, 'initial', 0),
         (2, 'correlation_and_error', 0);",
    )
    .unwrap();
  drop(conn);

  let mut options = RequestListOptions {
    day: Some("2026-07-14".to_string()),
    limit: Some(1),
    ..RequestListOptions::default()
  };
  let mut request_ids = Vec::new();
  let mut models = Vec::new();
  loop {
    let page = list_requests(&dir, &options).unwrap();
    request_ids.extend(page.requests.iter().map(|request| request.request_id.clone()));
    models.extend(page.requests.iter().map(|request| request.model.clone().unwrap()));
    let Some(cursor) = page.next_cursor else {
      break;
    };
    options.cursor = Some(RequestCursor::decode(&cursor).unwrap());
  }

  assert_eq!(models, ["model-12", "model-11", "model-10", "model-9"]);
  assert_eq!(request_ids, ["duplicate", "duplicate", "legacy:10", "legacy:9"]);

  let detail = get_request(&dir, "2026-07-14", "duplicate", Some(11)).unwrap().unwrap();
  assert_eq!(detail.row_id, 11);
  assert_eq!(detail.request["model"], "model-11");
  assert_eq!(serde_json::to_value(&detail).unwrap()["row_id"], "11");

  let payload = get_request_payload(
    &dir,
    "2026-07-14",
    "duplicate",
    Some(12),
    RequestPayloadField::InboundReqBody,
  )
  .unwrap()
  .unwrap();
  assert_eq!(payload.value, serde_json::json!({"row_id": 12}));
  assert!(get_request(&dir, "2026-07-14", "duplicate", Some(10))
    .unwrap()
    .is_none());

  let page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      limit: Some(1),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(serde_json::to_value(&page.requests[0]).unwrap()["row_id"], "12");
}

#[test]
fn reads_legacy_request_days_without_migrating_them() {
  let dir = tempdir();
  let path = dir.join("2026-07-14.db");
  let conn = Connection::open(&path).unwrap();
  conn
    .execute_batch(include_str!("../../../schemas/snapshot/requests/v0.0.0.sql"))
    .unwrap();
  conn
    .execute(
      "INSERT INTO requests (
         ts, session_id, endpoint, account_id, provider_id, model, initiator,
         status, stream, latency_ms, inbound_req_headers, inbound_req_body
       ) VALUES (
         ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
       )",
      params![
        1_784_444_800_i64,
        "legacy-session",
        "chat.completions",
        "account-1",
        "openai",
        "gpt-legacy",
        "test",
        200_i64,
        0_i64,
        12_i64,
        br#"{"content-type":"application/json"}"#,
        br#"{"messages":["hello"]}"#,
      ],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO requests (
         ts, session_id, endpoint, account_id, provider_id, model, initiator,
         status, stream, latency_ms, inbound_req_headers, inbound_req_body
       ) VALUES (
         ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
       )",
      params![
        1_784_444_800_i64,
        "legacy-session",
        "chat.completions",
        "account-1",
        "openai",
        "gpt-legacy",
        "test",
        200_i64,
        0_i64,
        12_i64,
        br#"{"content-type":"application/json"}"#,
        br#"{"messages":["second"]}"#,
      ],
    )
    .unwrap();
  conn
    .execute("UPDATE requests SET outbound_resp_status = 502 WHERE id = 2", [])
    .unwrap();
  conn
    .execute(
      "UPDATE requests SET outbound_resp_status = 202, inbound_resp_status = 201 WHERE id = 1",
      [],
    )
    .unwrap();
  conn
    .execute_batch(
      "CREATE TABLE schema_migrations (
         version INTEGER PRIMARY KEY,
         name TEXT NOT NULL,
         applied_ts INTEGER NOT NULL
       );
       INSERT INTO schema_migrations (version, name, applied_ts) VALUES (1, 'initial', 0);",
    )
    .unwrap();
  drop(conn);

  let mut options = RequestListOptions {
    day: Some("2026-07-14".to_string()),
    limit: Some(1),
    ..RequestListOptions::default()
  };
  let page = list_requests(&dir, &options).unwrap();
  assert_eq!(request_ids(&page.requests), ["legacy:2"]);
  assert!(page.requests[0].request_classification.is_none());
  assert_eq!(page.requests[0].ts, 1_784_444_800_000);
  options.cursor = Some(RequestCursor::decode(page.next_cursor.as_deref().unwrap()).unwrap());
  let next_page = list_requests(&dir, &options).unwrap();
  assert_eq!(request_ids(&next_page.requests), ["legacy:1"]);
  assert!(next_page.next_cursor.is_none());
  let error_page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      errors_only: true,
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(request_ids(&error_page.requests), ["legacy:2"]);
  let downstream_status_page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      status: Some(201),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert_eq!(request_ids(&downstream_status_page.requests), ["legacy:1"]);
  let shadowed_upstream_status_page = list_requests(
    &dir,
    &RequestListOptions {
      day: Some("2026-07-14".to_string()),
      status: Some(202),
      ..RequestListOptions::default()
    },
  )
  .unwrap();
  assert!(shadowed_upstream_status_page.requests.is_empty());
  assert_eq!(
    list_request_days(&dir).unwrap(),
    vec![RequestDay {
      day: "2026-07-14".to_string(),
      state: RequestDayState::Available,
    }]
  );

  let detail = get_request(&dir, "2026-07-14", "legacy:1", None).unwrap().unwrap();
  assert_eq!(detail.request["request_id"], "legacy:1");
  for field in PAYLOAD_FIELDS {
    assert!(!detail.request.contains_key(*field));
  }
  let payload = get_request_payload(
    &dir,
    "2026-07-14",
    "legacy:1",
    None,
    RequestPayloadField::InboundReqBody,
  )
  .unwrap()
  .unwrap();
  assert_eq!(payload.value, serde_json::json!({"messages": ["hello"]}));

  let sessions = list_sessions(&dir, None).unwrap();
  assert_eq!(sessions[0].session_id, "legacy-session");

  let conn = Connection::open(path).unwrap();
  let version: i64 = conn
    .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| row.get(0))
    .unwrap();
  assert_eq!(version, 1);
  let split_table_count: i64 = conn
    .query_row(
      "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'request_connection'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(split_table_count, 0);
}
