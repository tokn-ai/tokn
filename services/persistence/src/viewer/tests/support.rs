use bytes::Bytes;
use rusqlite::params;
use std::path::{Path, PathBuf};

use crate::requests::open_day_db;
use crate::sessions::{SessionsDb, TreeRequestRecord};
use crate::{MessageRecord, PartRecord};

pub(super) fn tempdir() -> PathBuf {
  let path = std::env::temp_dir().join(format!("tokn-router-viewer-{}", uuid::Uuid::new_v4()));
  std::fs::create_dir_all(&path).unwrap();
  path
}

pub(super) fn write_request(
  dir: &Path,
  day: &str,
  request_id: &str,
  ts: i64,
  session_id: Option<&str>,
  provider_id: Option<&str>,
) {
  let conn = open_day_db(&dir.join(format!("{day}.db"))).unwrap();
  conn
    .execute(
      "INSERT INTO request_connection (request_id, ts, endpoint, status, request_error, ctx_json)
       VALUES (?1, ?2, 'responses', 200, NULL, '{\"route\":\"default\"}')",
      params![request_id, ts],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_metadata (request_id, session_id, account_id, provider_id, model, params_json, usage_json)
       VALUES (?1, ?2, 'account-1', ?3, 'gpt-test', '{\"stream\":false}', '{\"input\":1}')",
      params![request_id, session_id, provider_id],
    )
    .unwrap();
  conn
    .execute(
      "INSERT INTO request_downstream (request_id, inbound_req_method, inbound_req_url, inbound_req_body)
       VALUES (?1, 'POST', '/v1/responses', '{\"input\":\"hello\"}')",
      params![request_id],
    )
    .unwrap();
}

pub(super) fn request_ids(requests: &[super::super::RequestSummary]) -> Vec<&str> {
  requests.iter().map(|request| request.request_id.as_str()).collect()
}

pub(super) fn write_session(
  sessions_db: &Path,
  session_id: &str,
  request_id: &str,
  ts: i64,
  provider_id: &str,
  model: &str,
) {
  let mut sessions = SessionsDb::open(sessions_db).unwrap();
  sessions
    .record_tree(&TreeRequestRecord {
      ts,
      session_id: session_id.to_string(),
      thread_id: None,
      parent_thread_id: None,
      parent_session_id: None,
      request_id: request_id.to_string(),
      endpoint: "responses".to_string(),
      status: Some(200),
      account_id: Some("account-1".to_string()),
      provider_id: Some(provider_id.to_string()),
      model: Some(model.to_string()),
      request_messages: vec![MessageRecord {
        role: "user".to_string(),
        status: None,
        parts: vec![PartRecord {
          part_type: "text".to_string(),
          content: Bytes::from_static(b"hello"),
        }],
      }],
      response_messages: Vec::new(),
    })
    .unwrap();
}
