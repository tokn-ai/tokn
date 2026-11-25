use super::*;
use rusqlite::params;
use tokn_persistence::requests::open_day_db;
use tokn_persistence::sessions::{SessionsDb, TreeRequestRecord};
use tokn_persistence::{MessageRecord, UsageDb};
fn write_request(dir: &std::path::Path, day: &str, request_id: &str, session_id: &str) {
  let conn = open_day_db(&dir.join(format!("{day}.db"))).unwrap();
  conn
    .execute(
      "INSERT INTO request_connection (request_id, ts, endpoint, status)
         VALUES (?1, 1784444800000, 'responses', 200)",
      params![request_id],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_metadata (request_id, session_id, account_id, provider_id, model)
         VALUES (?1, ?2, 'account-1', 'openai', 'gpt-test')",
      params![request_id, session_id],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_downstream (request_id, inbound_req_body) VALUES (?1, '{\"input\":\"hello\"}')",
      params![request_id],
    )
    .unwrap();
}

fn write_session(sessions_db: &std::path::Path, session_id: &str, request_id: &str) {
  let mut sessions = SessionsDb::open(sessions_db).unwrap();
  sessions
    .record_tree(&TreeRequestRecord {
      ts: 1_783_987_200_000,
      session_id: session_id.to_string(),
      thread_id: None,
      parent_thread_id: None,
      parent_session_id: None,
      request_id: request_id.to_string(),
      endpoint: "responses".to_string(),
      status: Some(200),
      account_id: Some("account-1".to_string()),
      provider_id: Some("openai".to_string()),
      model: Some("gpt-test".to_string()),
      request_messages: vec![MessageRecord {
        role: "user".to_string(),
        status: None,
        parts: Vec::new(),
      }],
      response_messages: Vec::new(),
    })
    .unwrap();
}

fn write_usage(usage_db: &std::path::Path, session_id: &str, request_id: &str, usage_json: &str) {
  drop(UsageDb::open(usage_db).unwrap());
  let conn = rusqlite::Connection::open(usage_db).unwrap();
  conn
    .execute(
      "INSERT INTO requests (ts, session_id, request_id, model, usage_json)
         VALUES (1784444800000, ?1, ?2, 'gpt-test', ?3)",
      params![session_id, request_id, usage_json],
    )
    .unwrap();
}

struct Response {
  status: u16,
  body: String,
}
impl Response {
  fn status(&self) -> u16 {
    self.status
  }
}
async fn get_response(requests_dir: &std::path::Path, sessions_db: &std::path::Path, uri: &str) -> Response {
  let url = url::Url::parse(&format!("http://fixture{uri}")).unwrap();
  let kind = url
    .path()
    .strip_prefix("/api/")
    .unwrap()
    .replace("requests/latest", "latest_requests")
    .replace('-', "_");
  let mut value = serde_json::json!({"kind": kind});
  for (key, text) in url.query_pairs() {
    value[key.as_ref()] = match key.as_ref() {
      "limit" | "index" | "status" => text
        .parse::<u64>()
        .map(serde_json::Value::from)
        .unwrap_or_else(|_| text.to_string().into()),
      "errors_only" => (text == "true").into(),
      _ => text.to_string().into(),
    };
  }
  let query = match serde_json::from_value(value) {
    Ok(query) => query,
    Err(error) => {
      return Response {
        status: 400,
        body: error.to_string(),
      }
    }
  };
  let result = dispatch(
    DbPaths {
      requests_dir: requests_dir.into(),
      sessions_db: sessions_db.into(),
      usage_db: sessions_db.with_file_name("usage.db"),
    },
    query,
  )
  .await;
  match result {
    Ok(body) => Response {
      status: 200,
      body: body.to_string(),
    },
    Err(error) => Response {
      status: error.status,
      body: serde_json::to_string(&error).unwrap(),
    },
  }
}
async fn response_body(response: Response) -> String {
  response.body
}

fn json_string_field(body: &str, field: &str) -> Option<String> {
  let prefix = format!(r#""{field}":""#);
  let value = body.split_once(&prefix)?.1.split_once('"')?.0;
  Some(value.to_string())
}

#[tokio::test]
async fn history_endpoints_preserve_success_and_not_found_statuses() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  write_request(tempdir.path(), "2026-07-14", "request/one", "session/one");
  write_request(tempdir.path(), "2026-07-13", "request/two", "session/two");
  drop(open_day_db(&tempdir.path().join("2026-07-15.db")).unwrap());
  std::fs::write(tempdir.path().join("2026-07-16.db"), b"not a sqlite database").unwrap();

  let requests_response = get_response(tempdir.path(), &sessions_db, "/api/requests").await;
  assert_eq!(requests_response.status(), 200);

  let day_requests_response = get_response(tempdir.path(), &sessions_db, "/api/requests?day=2026-07-14").await;
  assert_eq!(day_requests_response.status(), 200);
  let day_requests_body = response_body(day_requests_response).await;
  assert!(day_requests_body.contains("request/one"));
  assert!(!day_requests_body.contains("request/two"));

  let request_days_response = get_response(tempdir.path(), &sessions_db, "/api/request-days").await;
  assert_eq!(request_days_response.status(), 200);
  let request_days_body = response_body(request_days_response).await;
  assert!(request_days_body.contains(r#"{"day":"2026-07-14","state":"available"}"#));
  assert!(request_days_body.contains(r#"{"day":"2026-07-15","state":"empty"}"#));
  assert!(request_days_body.contains(r#"{"day":"2026-07-16","state":"unavailable"}"#));

  let latest_requests_response = get_response(tempdir.path(), &sessions_db, "/api/requests/latest?limit=1").await;
  assert_eq!(latest_requests_response.status(), 200);
  let latest_requests_body = response_body(latest_requests_response).await;
  assert!(latest_requests_body.contains("2026-07-14"));
  assert!(latest_requests_body.contains("request/one"));

  let request_response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=request%2Fone",
  )
  .await;
  assert_eq!(request_response.status(), 200);
  assert!(!response_body(request_response).await.contains("inbound_req_body"));

  let sessions_response = get_response(tempdir.path(), &sessions_db, "/api/sessions").await;
  assert_eq!(sessions_response.status(), 200);
  assert_eq!(response_body(sessions_response).await, "[]");

  let session_response = get_response(tempdir.path(), &sessions_db, "/api/session?session_id=session%2Fone").await;
  assert_eq!(session_response.status(), 404);

  let missing_request_response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=request%2Fmissing",
  )
  .await;
  assert_eq!(missing_request_response.status(), 404);

  let invalid_day_response = get_response(tempdir.path(), &sessions_db, "/api/requests?day=not-a-day").await;
  assert_eq!(invalid_day_response.status(), 400);

  let unavailable_day_response = get_response(tempdir.path(), &sessions_db, "/api/requests?day=2026-07-16").await;
  assert_eq!(unavailable_day_response.status(), 503);

  let invalid_detail_day_response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-02-30&request_id=request%2Fone",
  )
  .await;
  assert_eq!(invalid_detail_day_response.status(), 400);

  let missing_session_response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/session?session_id=session%2Fmissing",
  )
  .await;
  assert_eq!(missing_session_response.status(), 404);
}

#[tokio::test]
async fn request_endpoints_page_without_duplicates_and_validate_cursors() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  for request_id in ["request-a", "request-b", "request-c"] {
    write_request(tempdir.path(), "2026-07-14", request_id, "session/one");
  }

  let first_response = get_response(tempdir.path(), &sessions_db, "/api/requests?day=2026-07-14&limit=2").await;
  assert_eq!(first_response.status(), 200);
  let first_body = response_body(first_response).await;
  assert!(serde_json::from_str::<serde_json::Value>(&first_body).unwrap()["requests"].is_array());
  assert!(first_body.contains("request-c"));
  assert!(first_body.contains("request-b"));
  assert!(!first_body.contains("request-a"));
  assert!(first_body.contains(r#""row_id":"3""#));
  let cursor = json_string_field(&first_body, "next_cursor").unwrap();

  let second_response = get_response(
    tempdir.path(),
    &sessions_db,
    &format!("/api/requests?day=2026-07-14&limit=2&cursor={cursor}"),
  )
  .await;
  assert_eq!(second_response.status(), 200);
  let second_body = response_body(second_response).await;
  assert!(second_body.contains("request-a"));
  assert!(!second_body.contains("request-b"));
  assert!(!second_body.contains("request-c"));
  assert!(second_body.contains(r#""next_cursor":null"#));

  let latest_response = get_response(tempdir.path(), &sessions_db, "/api/requests/latest?limit=2").await;
  assert_eq!(latest_response.status(), 200);
  let latest_body = response_body(latest_response).await;
  assert!(latest_body.contains(r#""day":"2026-07-14""#));
  let latest_cursor = json_string_field(&latest_body, "next_cursor").unwrap();
  let latest_next_response = get_response(
    tempdir.path(),
    &sessions_db,
    &format!("/api/requests/latest?limit=2&cursor={latest_cursor}"),
  )
  .await;
  assert_eq!(latest_next_response.status(), 200);
  let latest_next_body = response_body(latest_next_response).await;
  assert!(latest_next_body.contains("request-a"));
  assert!(!latest_next_body.contains("request-b"));

  let malformed_response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/requests?day=2026-07-14&cursor=not-a-cursor",
  )
  .await;
  assert_eq!(malformed_response.status(), 400);

  let wrong_day_response = get_response(
    tempdir.path(),
    &sessions_db,
    &format!("/api/requests?day=2026-07-13&cursor={cursor}"),
  )
  .await;
  assert_eq!(wrong_day_response.status(), 400);
}

#[tokio::test]
async fn request_payload_endpoint_is_lazy_strict_and_base64_safe() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  write_request(tempdir.path(), "2026-07-14", "request/one", "session/one");
  let conn = open_day_db(&tempdir.path().join("2026-07-14.db")).unwrap();
  conn
      .execute(
        "UPDATE request_downstream
         SET inbound_req_body = '{\"input\":[{\"role\":\"user\",\"content\":\"hello\"},{\"type\":\"function_call\",\"name\":\"lookup\",\"arguments\":\"{}\"}],\"tools\":[{\"type\":\"function\",\"name\":\"lookup\",\"description\":\"Find a record\",\"parameters\":{\"type\":\"object\"}}]}'
         WHERE request_id = ?1",
        params!["request/one"],
      )
      .unwrap();
  conn
    .execute(
      "INSERT INTO request_upstream (request_id, outbound_resp_body) VALUES (?1, ?2)",
      params!["request/one", &[0xff_u8, 0x00]],
    )
    .unwrap();

  let overview = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=request%2Fone&row_id=1",
  )
  .await;
  assert_eq!(overview.status(), 200);
  let overview_body = response_body(overview).await;
  assert!(overview_body.contains("request/one"));
  assert!(overview_body.contains(r#""row_id":"1""#));
  assert!(!overview_body.contains("inbound_req_body"));
  assert!(!overview_body.contains("outbound_resp_body"));

  let json_payload = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-payload?day=2026-07-14&request_id=request%2Fone&row_id=1&field=inbound_req_body",
  )
  .await;
  assert_eq!(json_payload.status(), 200);
  let json_payload_body = response_body(json_payload).await;
  assert!(json_payload_body.contains(r#""field":"inbound_req_body""#));
  assert!(json_payload_body.contains(r#""role":"user""#));

  let llm_summary = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-llm-summary?day=2026-07-14&request_id=request%2Fone&row_id=1",
  )
  .await;
  assert_eq!(llm_summary.status(), 200);
  let llm_summary_body = response_body(llm_summary).await;
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&llm_summary_body).unwrap()["messages"][0]["index"],
    0
  );
  assert!(llm_summary_body.contains(r#""kind":"function_call""#));
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&llm_summary_body).unwrap()["tool_definitions"][0]["index"],
    0
  );
  assert!(llm_summary_body.contains(r#""name":"lookup""#));
  assert!(llm_summary_body.contains(r#""description":"Find a record""#));

  let llm_message = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-llm-message?day=2026-07-14&request_id=request%2Fone&row_id=1&index=0",
  )
  .await;
  assert_eq!(llm_message.status(), 200);
  assert!(response_body(llm_message).await.contains(r#""content":"hello""#));

  let llm_tool = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-llm-tool-definition?day=2026-07-14&request_id=request%2Fone&row_id=1&index=0",
  )
  .await;
  assert_eq!(llm_tool.status(), 200);
  assert!(response_body(llm_tool)
    .await
    .contains(r#""parameters":{"type":"object"}"#));

  let missing_llm_message = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-llm-message?day=2026-07-14&request_id=request%2Fone&row_id=1&index=2",
  )
  .await;
  assert_eq!(missing_llm_message.status(), 404);

  let binary_payload = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-payload?day=2026-07-14&request_id=request%2Fone&row_id=1&field=outbound_resp_body",
  )
  .await;
  assert_eq!(binary_payload.status(), 200);
  let binary_payload_body = response_body(binary_payload).await;
  assert!(binary_payload_body.contains(r#""encoding":"base64""#));
  assert!(binary_payload_body.contains(r#""data":"/wA=""#));

  let invalid_field = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-payload?day=2026-07-14&request_id=request%2Fone&field=endpoint",
  )
  .await;
  assert_eq!(invalid_field.status(), 400);

  let missing_request = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-payload?day=2026-07-14&request_id=missing&field=inbound_req_body",
  )
  .await;
  assert_eq!(missing_request.status(), 404);

  let missing_llm_summary = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-llm-summary?day=2026-07-14&request_id=missing",
  )
  .await;
  assert_eq!(missing_llm_summary.status(), 404);

  let mismatched_identity = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=request%2Fone&row_id=2",
  )
  .await;
  assert_eq!(mismatched_identity.status(), 404);
}

#[tokio::test]
async fn row_id_disambiguates_duplicate_legacy_request_ids() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  let requests_db = tempdir.path().join("2026-07-14.db");
  let conn = rusqlite::Connection::open(requests_db).unwrap();
  conn
    .execute_batch(
      "CREATE TABLE requests (
           id INTEGER PRIMARY KEY,
           ts INTEGER NOT NULL,
           request_id TEXT,
           model TEXT,
           inbound_req_body BLOB
         );
         CREATE TABLE schema_migrations (
           version INTEGER PRIMARY KEY,
           name TEXT NOT NULL,
           applied_ts INTEGER NOT NULL
         );
         INSERT INTO schema_migrations (version, name, applied_ts) VALUES
           (1, 'initial', 0),
           (2, 'correlation_and_error', 0);
         INSERT INTO requests (id, ts, request_id, model, inbound_req_body) VALUES
           (11, 1784444800, 'duplicate', 'model-11', '{\"row_id\":11}'),
           (12, 1784444800, 'duplicate', 'model-12', '{\"row_id\":12}');",
    )
    .unwrap();
  drop(conn);

  let overview = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=duplicate&row_id=11",
  )
  .await;
  assert_eq!(overview.status(), 200);
  let overview_body = response_body(overview).await;
  assert!(overview_body.contains(r#""row_id":"11""#));
  assert!(overview_body.contains("model-11"));
  assert!(!overview_body.contains("model-12"));

  let payload = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request-payload?day=2026-07-14&request_id=duplicate&row_id=12&field=inbound_req_body",
  )
  .await;
  assert_eq!(payload.status(), 200);
  let payload_body = response_body(payload).await;
  assert!(payload_body.contains(r#""row_id":12"#));

  let fallback = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/request?day=2026-07-14&request_id=duplicate",
  )
  .await;
  assert_eq!(fallback.status(), 200);
}

#[tokio::test]
async fn requests_endpoint_filters_errors_only() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  write_request(tempdir.path(), "2026-07-14", "healthy", "session/one");
  write_request(tempdir.path(), "2026-07-14", "failed", "session/one");
  let conn = open_day_db(&tempdir.path().join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_connection SET request_error = 'failed' WHERE request_id = 'failed'",
      [],
    )
    .unwrap();

  let response = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/requests?day=2026-07-14&errors_only=true",
  )
  .await;
  assert_eq!(response.status(), 200);
  let body = response_body(response).await;
  assert!(body.contains("failed"));
  assert!(!body.contains("healthy"));
}

#[tokio::test]
async fn request_url_paths_endpoint_drives_exact_path_filtering() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  write_request(tempdir.path(), "2026-07-14", "search", "session/one");
  write_request(tempdir.path(), "2026-07-14", "responses", "session/one");
  let conn = open_day_db(&tempdir.path().join("2026-07-14.db")).unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_req_url = ?2 WHERE request_id = ?1",
      ["search", "/backend-api/codex/alpha/search?client_version=1"],
    )
    .unwrap();
  conn
    .execute(
      "UPDATE request_downstream SET inbound_req_url = ?2 WHERE request_id = ?1",
      ["responses", "/backend-api/codex/responses"],
    )
    .unwrap();

  let choices = get_response(tempdir.path(), &sessions_db, "/api/request-url-paths?day=2026-07-14").await;
  assert_eq!(choices.status(), 200);
  let choices = response_body(choices).await;
  let choices: Vec<serde_json::Value> = serde_json::from_str(&choices).unwrap();
  assert!(choices.contains(&serde_json::json!({"url_path":"/backend-api/codex/alpha/search","request_count":1})));
  assert!(choices.contains(&serde_json::json!({"url_path":"/backend-api/codex/responses","request_count":1})));

  let filtered = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/requests?day=2026-07-14&url_path=%2Fbackend-api%2Fcodex%2Falpha%2Fsearch",
  )
  .await;
  assert_eq!(filtered.status(), 200);
  let filtered = response_body(filtered).await;
  assert!(filtered.contains(r#""request_id":"search""#));
  assert!(!filtered.contains(r#""request_id":"responses""#));
}

#[tokio::test]
async fn sessions_endpoint_reads_only_the_sessions_database() {
  let tempdir = tempfile::tempdir().unwrap();
  let requests_dir = tempdir.path().join("requests");
  std::fs::create_dir(&requests_dir).unwrap();
  write_request(&requests_dir, "2026-07-14", "request-only", "request-only-session");
  let sessions_db = tempdir.path().join("sessions.db");
  write_session(&sessions_db, "stored-session", "stored-request");

  let response = get_response(&requests_dir, &sessions_db, "/api/sessions").await;
  assert_eq!(response.status(), 200);
  let body = response_body(response).await;
  assert!(body.contains("stored-session"));
  assert!(!body.contains("request-only-session"));
}

#[tokio::test]
async fn sessions_endpoint_succeeds_without_request_history() {
  let tempdir = tempfile::tempdir().unwrap();
  let requests_dir = tempdir.path().join("not-a-directory");
  std::fs::write(&requests_dir, b"session endpoints must not read this path").unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  write_session(&sessions_db, "stored-session", "stored-request");

  let response = get_response(&requests_dir, &sessions_db, "/api/sessions").await;
  assert_eq!(response.status(), 200);
  assert!(response_body(response).await.contains("stored-session"));

  let detail = get_response(
    &requests_dir,
    &sessions_db,
    "/api/session?session_id=stored-session&limit=20",
  )
  .await;
  assert_eq!(detail.status(), 200);
  let detail = response_body(detail).await;
  assert!(detail.contains(r#""head_node_id":"stored-request""#));
  assert!(detail.contains(r#""node_id":"stored-request""#));
  assert!(detail.contains(r#""nodes_truncated":false"#));

  let node = get_response(
    &requests_dir,
    &sessions_db,
    "/api/session-node?session_id=stored-session&node_id=stored-request",
  )
  .await;
  assert_eq!(node.status(), 200);
  let node = response_body(node).await;
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&node).unwrap()["request_messages"][0]["role"],
    "user"
  );
  assert!(node.contains(r#""response_messages":[]"#));
  assert!(node.contains(r#""parts_total":0"#));
  assert!(node.contains(r#""messages_total":1"#));
  assert!(node.contains(r#""messages_returned":1"#));
  assert!(node.contains(r#""parts_omitted":0"#));
}

#[tokio::test]
async fn session_usage_endpoint_reads_only_the_usage_database() {
  let tempdir = tempfile::tempdir().unwrap();
  let requests_dir = tempdir.path().join("not-a-directory");
  std::fs::write(&requests_dir, b"session usage must not read request history").unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  let usage_db = tempdir.path().join("usage.db");
  write_session(&sessions_db, "stored-session", "stored-request");
  write_usage(
    &usage_db,
    "stored-session",
    "stored-request",
    r#"{"input":120,"output":30,"total":150,"cache_read":80,"reasoning":5}"#,
  );

  let response = get_response(
    &requests_dir,
    &sessions_db,
    "/api/session-usage?session_id=stored-session",
  )
  .await;
  assert_eq!(response.status(), 200);
  let body = response_body(response).await;
  assert!(body.contains(r#""session_id":"stored-session""#));
  assert!(body.contains(r#""input_tokens":120"#));
  assert!(body.contains(r#""output_tokens":30"#));
  assert!(body.contains(r#""cache_read_tokens":80"#));
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&body).unwrap()["requests"],
    serde_json::json!([{"request_id":"stored-request","context_tokens":120,"input_delta_tokens":40,"output_tokens":30}])
  );

  let missing = get_response(&requests_dir, &sessions_db, "/api/session-usage?session_id=missing").await;
  assert_eq!(missing.status(), 200);
  assert_eq!(response_body(missing).await, "null");
}

#[tokio::test]
async fn stored_session_endpoints_preserve_missing_session_and_node_statuses() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");

  let missing_database_session = get_response(tempdir.path(), &sessions_db, "/api/session?session_id=missing").await;
  assert_eq!(missing_database_session.status(), 404);
  let missing_database_node = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/session-node?session_id=missing&node_id=missing-node",
  )
  .await;
  assert_eq!(missing_database_node.status(), 404);
  assert!(!sessions_db.exists());

  write_session(&sessions_db, "stored-session", "stored-request");
  let missing_session = get_response(tempdir.path(), &sessions_db, "/api/session?session_id=missing").await;
  assert_eq!(missing_session.status(), 404);
  let missing_node = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/session-node?session_id=stored-session&node_id=missing-node",
  )
  .await;
  assert_eq!(missing_node.status(), 404);
  assert!(response_body(missing_node).await.contains("session node not found"));
}

#[tokio::test]
async fn sessions_endpoint_reports_an_unavailable_sessions_database() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  std::fs::write(&sessions_db, b"not a sqlite database").unwrap();

  let response = get_response(tempdir.path(), &sessions_db, "/api/sessions").await;
  assert_eq!(response.status(), 503);

  let detail = get_response(tempdir.path(), &sessions_db, "/api/session?session_id=stored").await;
  assert_eq!(detail.status(), 503);
  assert!(response_body(detail).await.contains("session database unavailable"));
}

#[tokio::test]
async fn sessions_endpoint_explains_when_the_database_needs_migration() {
  let tempdir = tempfile::tempdir().unwrap();
  let sessions_db = tempdir.path().join("sessions.db");
  let conn = rusqlite::Connection::open(&sessions_db).unwrap();
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

  let response = get_response(tempdir.path(), &sessions_db, "/api/sessions").await;
  assert_eq!(response.status(), 503);
  assert!(response_body(response).await.contains("migration"));

  let detail = get_response(tempdir.path(), &sessions_db, "/api/session?session_id=stored").await;
  assert_eq!(detail.status(), 503);
  assert!(response_body(detail).await.contains("migration"));

  let node = get_response(
    tempdir.path(),
    &sessions_db,
    "/api/session-node?session_id=stored&node_id=node",
  )
  .await;
  assert_eq!(node.status(), 503);
  assert!(response_body(node).await.contains("migration"));
}

#[test]
fn native_queries_keep_large_row_ids_exact_and_reject_unsupported_input() {
  let query: InspectQuery = serde_json::from_value(
    serde_json::json!({"kind":"request","day":"2026-09-25","request_id":"request","row_id":"9223372036854775807"}),
  )
  .unwrap();
  assert!(matches!(
    query,
    InspectQuery::Request(RequestDetailQuery {
      row_id: Some(i64::MAX),
      ..
    })
  ));
  for value in [
    serde_json::json!({"kind":"unknown"}),
    serde_json::json!({"kind":"sessions","sessions_db":"/arbitrary.db"}),
    serde_json::json!({"kind":"request","day":"2026-09-25","request_id":"request","row_id":9223372036854775807_i64}),
  ] {
    assert!(serde_json::from_value::<InspectQuery>(value).is_err());
  }
}

#[tokio::test]
async fn opening_empty_inspector_does_not_create_databases() {
  let dir = tempfile::tempdir().unwrap();
  let requests = dir.path().join("requests");
  let sessions = dir.path().join("sessions.db");
  for locator in [
    "/api/info",
    "/api/request-days",
    "/api/requests/latest",
    "/api/sessions",
    "/api/session-usage?session_id=missing",
  ] {
    let response = get_response(&requests, &sessions, locator).await;
    assert_eq!(response.status(), 200, "{locator}: {}", response.body);
  }
  assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}
