#![allow(dead_code)]

use rusqlite::OptionalExtension;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use tokn_config::{Config, ModelFamily, RouteMode};
use tokn_core::account::{AccountConfig, AccountTier, AuthType, Secret};
use tokn_core::event::{spawn_event_loop, EventBus};
use tokn_persistence::{read_request_row, RequestEventHandler, UsageEventHandler};

pub struct RequestsHarness {
  _tmp: TempDir,
  pub requests_dir: PathBuf,
  pub usage_db: PathBuf,
  pub events: Arc<EventBus>,
  event_thread: Option<std::thread::JoinHandle<()>>,
}

impl RequestsHarness {
  pub fn new() -> Self {
    let tmp = tempfile::tempdir().expect("create temp db dir");
    let requests_dir = tmp.path().join("requests");
    let usage_db = tmp.path().join("usage.db");
    let events = Arc::new(EventBus::new(1024));
    let receiver = events.subscribe();
    let request_handler = RequestEventHandler::new(requests_dir.clone()).expect("create requests event handler");
    let usage_handler = UsageEventHandler::new(usage_db.clone()).expect("create usage event handler");
    let event_thread = spawn_event_loop(receiver, vec![Box::new(request_handler), Box::new(usage_handler)]);
    Self {
      _tmp: tmp,
      requests_dir,
      usage_db,
      events,
      event_thread: Some(event_thread),
    }
  }

  pub async fn row(&self, request_id: &str) -> Map<String, Value> {
    for _ in 0..100 {
      if let Some(row) = read_request_row(&self.requests_dir, request_id).expect("read request row") {
        if ctx(&row).get("latency_ms").and_then(Value::as_i64).is_some() {
          return row;
        }
      }
      sleep(Duration::from_millis(20)).await;
    }
    panic!("completed request row was not written for {request_id}");
  }

  pub async fn shutdown(&mut self) {
    self.events.shutdown().await;
    if let Some(thread) = self.event_thread.take() {
      thread.join().expect("join event loop");
    }
  }

  pub fn usage_session_id(&self, request_id: &str) -> Option<String> {
    rusqlite::Connection::open(&self.usage_db)
      .expect("open usage database")
      .query_row(
        "SELECT session_id FROM requests WHERE request_id = ?1",
        [request_id],
        |row| row.get(0),
      )
      .optional()
      .expect("read usage session id")
      .flatten()
  }
}

pub fn cfg_for(requests_dir: &Path, route_mode: RouteMode) -> Config {
  let mut cfg = Config::default();
  cfg.server.route_mode = route_mode;
  cfg.db.enabled = true;
  cfg.db.requests_dir = Some(requests_dir.to_path_buf());
  cfg.db.archive_extension = None;
  cfg.model_families = vec![ModelFamily {
    name: "glm-family".into(),
    members: vec!["glm-4.7".into(), "glm-5.1".into()],
  }];
  cfg
}

pub fn zai_account(base_url: &str) -> AccountConfig {
  AccountConfig {
    id: "zai-test-acct".into(),
    provider: "zai-coding-plan".into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: Vec::new(),
    label: None,
    base_url: Some(base_url.to_string()),
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: Some(Secret::new("sk-router-test".into())),
    api_key_expires_at: None,
    access_token: None,
    access_token_expires_at: None,
    id_token: None,
    refresh_token: None,
    provider_account_id: None,
    extra: Default::default(),
    refresh_url: None,
    last_refresh: None,
    settings: Default::default(),
  }
}

pub fn text(row: &Map<String, Value>, key: &str) -> Option<String> {
  row.get(key).and_then(Value::as_str).map(ToOwned::to_owned)
}

pub fn int(row: &Map<String, Value>, key: &str) -> Option<i64> {
  row.get(key).and_then(Value::as_i64)
}

pub fn ctx(row: &Map<String, Value>) -> Map<String, Value> {
  row
    .get("ctx_json")
    .and_then(Value::as_object)
    .cloned()
    .unwrap_or_default()
}

pub fn json_obj(row: &Map<String, Value>, key: &str) -> Map<String, Value> {
  row.get(key).and_then(Value::as_object).cloned().unwrap_or_default()
}

pub fn body_json(row: &Map<String, Value>, key: &str) -> Value {
  let Some(value) = row.get(key) else {
    panic!("{key} missing");
  };
  match value {
    Value::String(body) => serde_json::from_str(body).unwrap_or_else(|err| panic!("{key} is not JSON: {err}: {body}")),
    other => other.clone(),
  }
}

pub fn missing_or_null(row: &Map<String, Value>, key: &str) -> bool {
  row.get(key).map(Value::is_null).unwrap_or(true)
}

pub fn body_text(row: &Map<String, Value>, key: &str) -> String {
  let Some(value) = row.get(key) else {
    panic!("{key} missing");
  };
  match value {
    Value::String(body) => body.clone(),
    other => panic!("{key} is not text: {other:?}"),
  }
}
