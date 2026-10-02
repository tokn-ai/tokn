use super::{migrate, Result, Usage};
use rusqlite::{params, Connection};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokn_core::event::{Event, EventHandler};
use tokn_core::provider::Endpoint;
use tokn_core::request_event::stage::{
  ConvertedResponseSummary, ExtractedSummary, ResolvedSummary, SentSummary, Stage, StageEvent,
};
use tokn_core::request_event::{RecordEvent, RequestEndpoint, RequestEventPayload};

#[derive(Default)]
struct PendingUsageRecord {
  ts: i64,
  session_id: Option<String>,
  request_id: String,
  project_id: Option<String>,
  ver: Option<String>,
  request_error: Option<String>,
  user: Option<String>,
  endpoint: Option<String>,
  account_id: Option<String>,
  provider_id: Option<String>,
  model: Option<String>,
  params_json: Map<String, Value>,
  usage: Option<Usage>,
  usage_json: Option<String>,
  ctx_json: Map<String, Value>,
  status: Option<u16>,
  usage_seen: bool,
  completed: bool,
}

pub struct UsageRecord<'a> {
  pub ts: i64,
  pub session_id: Option<&'a str>,
  pub request_id: &'a str,
  pub project_id: Option<&'a str>,
  pub ver: Option<&'a str>,
  pub request_error: Option<&'a str>,
  pub user: Option<&'a str>,
  pub endpoint: &'a RequestEndpoint,
  pub account_id: Option<&'a str>,
  pub provider_id: Option<&'a str>,
  pub model: &'a str,
  pub params_json: Option<&'a str>,
  pub usage_json: Option<&'a str>,
  pub ctx_json: Option<&'a str>,
  pub status: Option<u16>,
}

struct InboundConnectionRecord<'a> {
  user: Option<&'a str>,
  api_key_id: Option<&'a str>,
  local_addr: Option<&'a str>,
  peer_addr: Option<&'a str>,
  mode: &'a str,
  method: &'a str,
}

const BOOTSTRAP: &str = include_str!("../schemas/snapshot/usage/v0.2.1.sql");
const MIGRATIONS: &[migrate::Migration] = &[
  migrate::Migration {
    version: 1,
    name: "initial",
    sql: include_str!("../schemas/snapshot/usage/v0.0.0.sql"),
  },
  migrate::Migration {
    version: 2,
    name: "add_correlation_ids",
    sql: include_str!("../schemas/migrations/usage/0002_add_correlation_ids.sql"),
  },
  migrate::Migration {
    version: 3,
    name: "lifecycle_columns",
    sql: include_str!("../schemas/migrations/usage/0003_lifecycle_columns.sql"),
  },
  migrate::Migration {
    version: 4,
    name: "add_usage_breakdown",
    sql: include_str!("../schemas/migrations/usage/0004_add_usage_breakdown.sql"),
  },
  migrate::Migration {
    version: 5,
    name: "request_metadata",
    sql: include_str!("../schemas/migrations/usage/0005_request_metadata.sql"),
  },
  migrate::Migration {
    version: 6,
    name: "add_user",
    sql: include_str!("../schemas/migrations/usage/0006_add_user.sql"),
  },
];

pub fn latest_version() -> u32 {
  migrate::latest_version(MIGRATIONS)
}

pub struct UsageDb {
  conn: Connection,
}

impl UsageDb {
  /// Open an existing usage database without migrations or writes.
  /// Summary queries require the current usage schema; older schemas report
  /// an error rather than being upgraded by a read-only viewer.
  pub fn open_readonly(path: &Path) -> Result<Self> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(std::time::Duration::from_millis(2_500))?;
    conn.execute_batch("PRAGMA query_only = ON;")?;
    Ok(Self { conn })
  }

  /// Open `usage.db` at `path`, applying any pending migrations. Pass the
  /// canonical filesystem path so `migrate::apply` can stage a `.bak`.
  pub fn open(path: &Path) -> Result<Self> {
    if let Some(parent) = path.parent() {
      std::fs::create_dir_all(parent)?;
    }
    let mut conn = Connection::open(path)?;
    migrate::apply(
      &mut conn,
      path,
      "usage",
      migrate::Bootstrap { sql: BOOTSTRAP },
      MIGRATIONS,
    )?;
    Ok(Self { conn })
  }

  pub fn record(&mut self, r: &UsageRecord<'_>) -> Result<()> {
    self.conn.execute(
      "INSERT OR REPLACE INTO requests (
         ts,
         session_id,
         request_id,
         project_id,
         ver,
         request_error,
         user,
         endpoint,
         account_id,
         provider_id,
         model,
         params_json,
         usage_json,
         ctx_json,
         status
       )
       VALUES (
         ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
       )",
      params![
        r.ts,
        r.session_id,
        r.request_id,
        r.project_id,
        r.ver,
        r.request_error,
        r.user,
        r.endpoint.as_str(),
        r.account_id,
        r.provider_id,
        r.model,
        r.params_json,
        r.usage_json,
        r.ctx_json,
        r.status.map(|v| v as i64),
      ],
    )?;
    Ok(())
  }

  /// Summarize requests since a Unix timestamp in milliseconds.
  pub fn summary(&self, since_ts: i64, account: Option<&str>, provider: Option<&str>) -> Result<Vec<RowSummary>> {
    let mut sql = String::from(
      "SELECT account_id, provider_id, model,
              json_extract(params_json, '$.initiator') AS initiator,
              COUNT(*) AS n,
              COALESCE(SUM(COALESCE(json_extract(usage_json, '$.input'), 0)),0),
              COALESCE(SUM(COALESCE(json_extract(usage_json, '$.output'), 0)),0),
              COALESCE(SUM(COALESCE(json_extract(usage_json, '$.cache_read'), 0)),0),
              COALESCE(SUM(COALESCE(json_extract(usage_json, '$.reasoning'), 0)),0),
              COALESCE(AVG(COALESCE(json_extract(ctx_json, '$.latency_ms'), 0)),0)
       FROM requests
       WHERE ts >= ?1",
    );
    let mut bind_account = false;
    let mut bind_provider = false;
    if account.is_some() {
      bind_account = true;
      sql.push_str(" AND account_id = ?2");
    }
    if provider.is_some() {
      bind_provider = true;
      sql.push_str(if bind_account {
        " AND provider_id = ?3"
      } else {
        " AND provider_id = ?2"
      });
    }
    sql.push_str(
      " GROUP BY account_id, provider_id, model,
               json_extract(params_json, '$.initiator')
        ORDER BY n DESC",
    );

    let mut stmt = self.conn.prepare(&sql)?;
    let map_row = |row: &rusqlite::Row<'_>| {
      Ok(RowSummary {
        account: row.get::<_, Option<String>>(0)?,
        provider: row.get::<_, Option<String>>(1)?,
        model: row.get::<_, String>(2)?,
        initiator: row.get::<_, Option<String>>(3)?,
        count: row.get::<_, i64>(4)? as u64,
        input_tokens: row.get::<_, i64>(5)? as u64,
        output_tokens: row.get::<_, i64>(6)? as u64,
        cached_tokens: row.get::<_, i64>(7)? as u64,
        reasoning_tokens: row.get::<_, i64>(8)? as u64,
        avg_latency_ms: row.get::<_, f64>(9)?,
      })
    };

    let rows = match (bind_account, bind_provider) {
      (true, true) => stmt
        .query_map(params![since_ts, account.unwrap(), provider.unwrap()], map_row)?
        .collect::<rusqlite::Result<_>>()?,
      (true, false) => stmt
        .query_map(params![since_ts, account.unwrap()], map_row)?
        .collect::<rusqlite::Result<_>>()?,
      (false, true) => stmt
        .query_map(params![since_ts, provider.unwrap()], map_row)?
        .collect::<rusqlite::Result<_>>()?,
      (false, false) => stmt
        .query_map(params![since_ts], map_row)?
        .collect::<rusqlite::Result<_>>()?,
    };
    Ok(rows)
  }
}

pub struct UsageEventHandler {
  db: UsageDb,
  pending: HashMap<String, PendingUsageRecord>,
}

impl UsageEventHandler {
  pub fn new(usage_db: PathBuf) -> Result<Self> {
    Ok(Self {
      db: UsageDb::open(&usage_db)?,
      pending: HashMap::new(),
    })
  }

  fn ensure_pending(&mut self, request_id: &str, attempt: u32, ts: i64) -> &mut PendingUsageRecord {
    let key = composite_request_id(request_id, attempt);
    self.pending.entry(key.clone()).or_insert_with(|| PendingUsageRecord {
      ts,
      request_id: key,
      ..PendingUsageRecord::default()
    })
  }

  fn persist_if_ready(&mut self, request_id: &str, attempt: u32) -> Result<()> {
    let key = composite_request_id(request_id, attempt);
    let should_persist = self
      .pending
      .get(&key)
      .map(|pending| pending.completed && model_is_known(pending.model.as_deref()))
      .unwrap_or(false);
    if !should_persist {
      return Ok(());
    }
    {
      let pending = self.pending.get(&key).expect("persistable pending usage record");
      let params_json = json_text(&pending.params_json);
      let ctx_json = json_text(&pending.ctx_json);
      let usage_json = pending
        .usage_json
        .clone()
        .or_else(|| pending.usage.as_ref().and_then(usage_json));
      let endpoint = request_endpoint_from_str(pending.endpoint.as_deref());
      let model = pending.model.as_deref().unwrap_or("");
      self.db.record(&UsageRecord {
        ts: pending.ts,
        session_id: pending.session_id.as_deref(),
        request_id: &pending.request_id,
        project_id: pending.project_id.as_deref(),
        ver: pending.ver.as_deref(),
        request_error: pending.request_error.as_deref(),
        user: pending.user.as_deref(),
        endpoint: &endpoint,
        account_id: pending.account_id.as_deref(),
        provider_id: pending.provider_id.as_deref(),
        model,
        params_json: params_json.as_deref(),
        usage_json: usage_json.as_deref(),
        ctx_json: ctx_json.as_deref(),
        status: pending.status,
      })?;
    }
    let complete = self
      .pending
      .get(&key)
      .map(|pending| pending.completed && pending.usage_seen)
      .unwrap_or(false);
    if complete {
      self.pending.remove(&key);
    }
    Ok(())
  }

  fn on_started(&mut self, request_id: &str, attempt: u32, ts: i64, endpoint: Option<&RequestEndpoint>) {
    let pending = self.ensure_pending(request_id, attempt, ts);
    pending.ts = ts;
    pending.ver = Some(tokn_core::util::version::full().to_string());
    if let Some(endpoint) = endpoint {
      pending.endpoint = Some(endpoint.as_str().to_string());
    }
  }

  fn on_extract(&mut self, request_id: &str, attempt: u32, summary: &ExtractedSummary) {
    let pending = self.ensure_pending(request_id, attempt, 0);
    pending.model = Some(summary.model.to_string());
    pending.session_id = summary.session_id.as_deref().map(str::to_string);
    pending.project_id = summary.project_id.as_deref().map(str::to_string);
    if let Some(initiator) = summary.initiator.as_deref() {
      pending
        .params_json
        .insert("initiator".to_string(), Value::String(initiator.to_string()));
    }
    pending
      .params_json
      .insert("stream".to_string(), Value::Bool(summary.stream));
    if let Some(classification) = summary.request_classification {
      pending
        .params_json
        .insert("request_classification".to_string(), serde_json::json!(classification));
    }
  }

  fn on_resolve(&mut self, request_id: &str, attempt: u32, summary: &ResolvedSummary) {
    let pending = self.ensure_pending(request_id, attempt, 0);
    pending.account_id = Some(summary.account_id.to_string());
    pending.provider_id = Some(summary.provider_id.to_string());
  }

  fn on_send(&mut self, request_id: &str, attempt: u32, ts: i64, summary: &SentSummary) {
    let pending = self.ensure_pending(request_id, attempt, ts);
    pending.status.get_or_insert(summary.status);
    patch_latency_header_ms(pending, ts);
  }

  fn on_convert_response(&mut self, request_id: &str, attempt: u32, summary: &ConvertedResponseSummary) {
    let pending = self.ensure_pending(request_id, attempt, 0);
    pending.status = Some(summary.status);
  }

  fn on_error(&mut self, request_id: &str, attempt: u32, stage: Stage, message: &str) {
    let pending = self.ensure_pending(request_id, attempt, 0);
    pending.request_error = Some(format!("{}: {message}", stage.as_str()));
  }

  fn on_completed(&mut self, request_id: &str, attempt: u32, ts: i64) -> Result<()> {
    let pending = self.ensure_pending(request_id, attempt, ts);
    let latency_ms = ts.saturating_sub(pending.ts).max(0) as u64;
    pending
      .ctx_json
      .insert("latency_ms".to_string(), Value::from(latency_ms));
    pending.completed = true;
    self.persist_if_ready(request_id, attempt)
  }

  fn on_inbound_connection(&mut self, request_id: &str, attempt: u32, record: InboundConnectionRecord<'_>) {
    let pending = self.ensure_pending(request_id, attempt, 0);
    if pending.user.is_none() {
      pending.user = record.user.map(str::to_string);
    }
    if let Some(api_key_id) = record.api_key_id {
      pending
        .ctx_json
        .insert("api_key_id".to_string(), Value::String(api_key_id.to_string()));
    }
    if let Some(local_addr) = record.local_addr {
      pending
        .ctx_json
        .insert("local_addr".to_string(), Value::String(local_addr.to_string()));
    }
    if let Some(peer_addr) = record.peer_addr {
      pending
        .ctx_json
        .insert("peer_addr".to_string(), Value::String(peer_addr.to_string()));
    }
    pending
      .ctx_json
      .insert("mode".to_string(), Value::String(record.mode.to_string()));
    pending
      .ctx_json
      .insert("pipeline_id".to_string(), Value::String(record.method.to_string()));
  }

  fn on_upstream_req(&mut self, request_id: &str, attempt: u32, _method: &str, _url: &str) {
    self.ensure_pending(request_id, attempt, 0);
  }

  fn on_upstream_resp(&mut self, request_id: &str, attempt: u32, ts: i64, status: u16) {
    let pending = self.ensure_pending(request_id, attempt, ts);
    pending.status.get_or_insert(status);
    patch_latency_header_ms(pending, ts);
  }

  fn on_usage(&mut self, request_id: &str, attempt: u32, usage: &Usage) -> Result<()> {
    let pending = self.ensure_pending(request_id, attempt, 0);
    pending.usage = Some(usage.clone());
    pending.usage_json = usage_json(usage);
    pending.usage_seen = true;
    self.persist_if_ready(request_id, attempt)
  }
}

impl EventHandler for UsageEventHandler {
  fn handle(&mut self, event: &Event) {
    let Event::Requests(request) = event else {
      return;
    };
    let request_id = request.request_id.as_str();
    let attempt = request.attempt;
    let result = match &request.payload {
      RequestEventPayload::Custom(_) => Ok(()),
      RequestEventPayload::Stage(stage) => match stage {
        StageEvent::Started { request_endpoint } => {
          self.on_started(request_id, attempt, request.ts, Some(request_endpoint));
          Ok(())
        }
        StageEvent::Extract(summary) => {
          self.on_extract(request_id, attempt, summary);
          Ok(())
        }
        StageEvent::Resolve(summary) => {
          self.on_resolve(request_id, attempt, summary);
          Ok(())
        }
        StageEvent::BuildHeaders(_) => Ok(()),
        StageEvent::ConvertRequest(_) => Ok(()),
        StageEvent::Send(summary) => {
          self.on_send(request_id, attempt, request.ts, summary);
          Ok(())
        }
        StageEvent::ConvertResponse(summary) => {
          self.on_convert_response(request_id, attempt, summary);
          Ok(())
        }
        StageEvent::Error { stage, message, .. } => {
          self.on_error(request_id, attempt, *stage, message);
          Ok(())
        }
        StageEvent::Completed { .. } => self.on_completed(request_id, attempt, request.ts),
      },
      RequestEventPayload::Record(record) => match record {
        RecordEvent::InboundConnection {
          user,
          api_key_id,
          local_addr,
          peer_addr,
          mode,
          method,
          inbound_method,
          url,
        } => {
          self.on_inbound_connection(
            request_id,
            attempt,
            InboundConnectionRecord {
              user: user.as_deref(),
              api_key_id: api_key_id.as_deref(),
              local_addr: local_addr.as_deref(),
              peer_addr: peer_addr.as_deref(),
              mode,
              method,
            },
          );
          let _ = inbound_method;
          let _ = url;
          Ok(())
        }
        RecordEvent::UpstreamReq { method, url, .. } => {
          self.on_upstream_req(request_id, attempt, method, url);
          Ok(())
        }
        RecordEvent::UpstreamResp { status, .. } => {
          self.on_upstream_resp(request_id, attempt, request.ts, *status);
          Ok(())
        }
        RecordEvent::UpstreamBody { .. } => Ok(()),
        RecordEvent::ConvertedBody { .. } => Ok(()),
        RecordEvent::Usage(usage) => self.on_usage(request_id, attempt, usage),
      },
    };
    if let Err(error) = result {
      tracing::warn!(%error, request_id, attempt, "usage persistence write failed");
    }
  }
}

#[derive(Debug)]
pub struct RowSummary {
  pub account: Option<String>,
  pub provider: Option<String>,
  pub model: String,
  pub initiator: Option<String>,
  pub count: u64,
  pub input_tokens: u64,
  pub output_tokens: u64,
  pub cached_tokens: u64,
  pub reasoning_tokens: u64,
  pub avg_latency_ms: f64,
}

fn composite_request_id(request_id: &str, attempt: u32) -> String {
  if attempt == 0 {
    request_id.to_string()
  } else {
    format!("{request_id}:{attempt}")
  }
}

fn patch_latency_header_ms(pending: &mut PendingUsageRecord, ts: i64) {
  let latency_header_ms = ts.saturating_sub(pending.ts).max(0) as u64;
  pending
    .ctx_json
    .insert("latency_header_ms".to_string(), Value::from(latency_header_ms));
}

fn json_text(value: &Map<String, Value>) -> Option<String> {
  (!value.is_empty()).then(|| Value::Object(value.clone()).to_string())
}

fn request_endpoint_from_str(value: Option<&str>) -> RequestEndpoint {
  match value {
    Some("responses") => Endpoint::Responses.into(),
    Some("chat_completions") => Endpoint::ChatCompletions.into(),
    Some("messages") => Endpoint::Messages.into(),
    Some(other) => RequestEndpoint::custom(other),
    None => RequestEndpoint::custom("unknown"),
  }
}

fn model_is_known(model: Option<&str>) -> bool {
  matches!(model, Some(model) if !model.is_empty() && model != "unknown")
}

fn usage_json(usage: &Usage) -> Option<String> {
  let mut out = Map::new();
  if let Some(value) = usage.usage_type {
    out.insert("kind".to_string(), Value::from(value.as_str()));
  }
  if let Some(value) = usage.input_tokens {
    out.insert("input".to_string(), Value::from(value));
  }
  if let Some(value) = usage.output_tokens {
    out.insert("output".to_string(), Value::from(value));
  }
  if let Some(value) = usage.total_tokens {
    out.insert("total".to_string(), Value::from(value));
  }
  if let Some(value) = usage.details.cache_read {
    out.insert("cache_read".to_string(), Value::from(value));
  }
  if let Some(value) = usage.details.cache_write {
    out.insert("cache_write".to_string(), Value::from(value));
  }
  if let Some(value) = usage.details.reasoning {
    out.insert("reasoning".to_string(), Value::from(value));
  }
  (!out.is_empty()).then(|| Value::Object(out).to_string())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Usage;
  use bytes::Bytes;
  use rusqlite::params;
  use serde_json::json;
  use smol_str::SmolStr;
  use std::sync::Arc;
  use tokn_core::provider::Endpoint;
  use tokn_core::request_event::stage::{ConvertedResponseSummary, ExtractedSummary, ResolvedSummary, SentSummary};
  use tokn_core::request_event::{RecordEvent, RequestEvent, RequestEventPayload, StageEvent};
  use tokn_headers::HeaderMap;

  #[test]
  fn summary_handles_unresolved_accounts_and_filters_millisecond_timestamps() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE requests (account_id TEXT, provider_id TEXT, model TEXT, params_json TEXT, usage_json TEXT, ctx_json TEXT, ts INTEGER);
      INSERT INTO requests (model, ts, usage_json) VALUES ('recent', 1700000000000, '{\"input\":12}'), ('old', 1699900000000, '{\"input\":99}');").unwrap();
    let db = UsageDb { conn };
    let rows = db.summary(1700000000000 - 86_400_000, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].model, "recent");
    assert_eq!(rows[0].account, None);
    assert_eq!(rows[0].provider, None);
    assert_eq!(rows[0].input_tokens, 12);
  }

  #[test]
  fn readonly_usage_never_creates_migrates_or_writes() {
    let dir = std::env::temp_dir().join(format!("tokn-readonly-usage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    assert!(UsageDb::open_readonly(&path).is_err());
    assert!(!path.exists());

    drop(UsageDb::open(&path).unwrap());
    let reader = UsageDb::open_readonly(&path).unwrap();
    assert!(reader.summary(0, None, None).unwrap().is_empty());
    assert!(reader.conn.execute("DELETE FROM requests", []).is_err());
    drop(reader);

    let old_path = dir.join("old.db");
    let old = Connection::open(&old_path).unwrap();
    old
      .execute_batch("CREATE TABLE legacy (value TEXT); PRAGMA user_version = 1;")
      .unwrap();
    drop(old);
    let before = std::fs::read(&old_path).unwrap();
    let reader = UsageDb::open_readonly(&old_path).unwrap();
    assert!(reader.summary(0, None, None).is_err());
    drop(reader);
    assert_eq!(std::fs::read(old_path).unwrap(), before);
    std::fs::remove_dir_all(dir).unwrap();
  }

  #[test]
  fn fresh_usage_db_records_correlation_ids() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut db = UsageDb::open(&path).unwrap();

    db.record(&UsageRecord {
      ts: 100,
      session_id: Some("session-1"),
      request_id: "request-1",
      project_id: Some("project-1"),
      ver: Some("v1"),
      request_error: None,
      user: Some("client-a"),
      endpoint: &Endpoint::ChatCompletions.into(),
      account_id: Some("account"),
      provider_id: Some("provider"),
      model: "model",
      params_json: Some("{\"initiator\":\"user\",\"stream\":false}"),
      usage_json: Some("{\"input\":1}"),
      ctx_json: Some("{\"latency_ms\":1}"),
      status: Some(200),
    })
    .unwrap();

    let row: (String, String, String) = db
      .conn
      .query_row("SELECT session_id, request_id, project_id FROM requests", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
      })
      .unwrap();
    assert_eq!(row, ("session-1".into(), "request-1".into(), "project-1".into()));
  }

  #[test]
  fn usage_event_handler_writes_one_row_after_completion() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-handler-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    let request_id = "req-usage";

    handler.handle(&request_stage(
      request_id,
      0,
      1_000,
      StageEvent::Started {
        request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
      },
    ));
    handler.handle(&request_record(
      request_id,
      0,
      1_001,
      RecordEvent::InboundConnection {
        user: Some(SmolStr::new("client-a")),
        api_key_id: Some(SmolStr::new("key-a")),
        local_addr: Some(SmolStr::new("127.0.0.1:4141")),
        peer_addr: Some(SmolStr::new("127.0.0.1:9999")),
        mode: SmolStr::new("router"),
        method: SmolStr::new("request_pipeline"),
        inbound_method: SmolStr::new("POST"),
        url: Some(SmolStr::new("http://localhost/v1/responses")),
      },
    ));
    handler.handle(&request_stage(request_id, 0, 1_002, extract_summary()));
    handler.handle(&request_stage(request_id, 0, 1_003, resolve_summary()));
    handler.handle(&request_record(
      request_id,
      0,
      1_004,
      RecordEvent::UpstreamReq {
        method: SmolStr::new("POST"),
        url: SmolStr::new("https://example.test/v1/responses"),
        headers: HeaderMap::new(),
        body: Bytes::new(),
      },
    ));
    handler.handle(&request_stage(request_id, 0, 1_005, send_summary()));
    handler.handle(&request_stage(request_id, 0, 1_006, converted_response_summary()));
    handler.handle(&request_record(
      request_id,
      0,
      1_007,
      RecordEvent::Usage(Usage {
        input_tokens: Some(12),
        output_tokens: Some(5),
        total_tokens: Some(17),
        usage_type: Some(tokn_core::db::UsageType::Responses),
        details: tokn_core::db::UsageDetails {
          cache_read: Some(3),
          cache_write: None,
          reasoning: Some(2),
        },
      }),
    ));

    let conn = Connection::open(&path).unwrap();
    let count_before: i64 = conn
      .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
      .unwrap();
    assert_eq!(count_before, 0, "usage row should not be flushed before completion");
    drop(conn);

    handler.handle(&request_stage(
      request_id,
      0,
      1_010,
      StageEvent::Completed {
        success: true,
        attempts: 1,
      },
    ));

    let conn = Connection::open(&path).unwrap();
    let row = conn
      .query_row(
        "SELECT session_id, request_id, project_id, endpoint, account_id, provider_id, model,
                params_json, usage_json, ctx_json, status
         FROM requests
         WHERE request_id = ?1",
        params![request_id],
        |row| {
          Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<i64>>(10)?,
          ))
        },
      )
      .unwrap();

    assert_eq!(row.0.as_deref(), Some("sess-1"));
    assert_eq!(row.1, request_id);
    assert_eq!(row.2.as_deref(), Some("project-1"));
    assert_eq!(row.3.as_deref(), Some("responses"));
    assert_eq!(row.4.as_deref(), Some("acct-1"));
    assert_eq!(row.5.as_deref(), Some("prov-1"));
    assert_eq!(row.6, "client-model");
    assert_eq!(
      parse_json(row.7.as_deref()),
      Some(json!({"initiator": "user", "stream": true}))
    );
    assert_eq!(
      parse_json(row.8.as_deref()),
      Some(json!({
        "kind": "responses",
        "input": 12,
        "output": 5,
        "total": 17,
        "cache_read": 3,
        "reasoning": 2
      }))
    );
    assert_eq!(
      parse_json(row.9.as_deref()),
      Some(json!({
        "api_key_id": "key-a",
        "local_addr": "127.0.0.1:4141",
        "peer_addr": "127.0.0.1:9999",
        "mode": "router",
        "pipeline_id": "request_pipeline",
        "latency_header_ms": 5,
        "latency_ms": 10
      }))
    );
    assert_eq!(row.10, Some(200));
    let user: Option<String> = conn
      .query_row(
        "SELECT user FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| row.get(0),
      )
      .unwrap();
    assert_eq!(user.as_deref(), Some("client-a"));
  }

  #[test]
  fn usage_event_handler_updates_usage_after_completed_row_is_written() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-late-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    let request_id = "req-late-usage";

    handler.handle(&request_stage(
      request_id,
      0,
      1_000,
      StageEvent::Started {
        request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
      },
    ));
    handler.handle(&request_stage(request_id, 0, 1_002, extract_summary()));
    handler.handle(&request_stage(request_id, 0, 1_003, resolve_summary()));
    handler.handle(&request_stage(request_id, 0, 1_006, converted_response_summary()));
    handler.handle(&request_stage(
      request_id,
      0,
      1_010,
      StageEvent::Completed {
        success: true,
        attempts: 1,
      },
    ));

    let conn = Connection::open(&path).unwrap();
    let row_before: (Option<String>, Option<i64>) = conn
      .query_row(
        "SELECT usage_json, status FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
      .unwrap();
    assert_eq!(row_before.0, None);
    assert_eq!(row_before.1, Some(200));
    drop(conn);

    handler.handle(&request_record(
      request_id,
      0,
      1_011,
      RecordEvent::Usage(Usage {
        input_tokens: Some(12),
        output_tokens: Some(5),
        total_tokens: Some(17),
        usage_type: Some(tokn_core::db::UsageType::Responses),
        details: tokn_core::db::UsageDetails {
          cache_read: Some(3),
          cache_write: None,
          reasoning: Some(2),
        },
      }),
    ));

    let conn = Connection::open(&path).unwrap();
    let row_after: (Option<String>, i64) = conn
      .query_row(
        "SELECT usage_json, COUNT(*) FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
      .unwrap();
    assert_eq!(
      parse_json(row_after.0.as_deref()),
      Some(json!({
        "kind": "responses",
        "input": 12,
        "output": 5,
        "total": 17,
        "cache_read": 3,
        "reasoning": 2
      }))
    );
    assert_eq!(row_after.1, 1);
  }

  #[test]
  fn usage_event_handler_persists_upstream_resp_status_and_header_latency() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-upstream-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    let request_id = "req-upstream-resp";

    handler.handle(&request_stage(
      request_id,
      0,
      1_000,
      StageEvent::Started {
        request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
      },
    ));
    handler.handle(&request_stage(request_id, 0, 1_002, extract_summary()));
    handler.handle(&request_stage(request_id, 0, 1_003, resolve_summary()));
    handler.handle(&request_record(
      request_id,
      0,
      1_006,
      RecordEvent::UpstreamResp {
        status: 202,
        headers: HeaderMap::new(),
      },
    ));
    handler.handle(&request_stage(
      request_id,
      0,
      1_010,
      StageEvent::Completed {
        success: true,
        attempts: 1,
      },
    ));

    let conn = Connection::open(&path).unwrap();
    let row: (Option<String>, Option<i64>) = conn
      .query_row(
        "SELECT ctx_json, status FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
      .unwrap();
    assert_eq!(
      parse_json(row.0.as_deref()),
      Some(json!({
        "latency_header_ms": 6,
        "latency_ms": 10
      }))
    );
    assert_eq!(row.1, Some(202));
  }

  #[test]
  fn usage_event_handler_persists_error_message_after_completion() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-error-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    let request_id = "req-error";

    handler.handle(&request_stage(
      request_id,
      0,
      1_000,
      StageEvent::Started {
        request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
      },
    ));
    handler.handle(&request_stage(request_id, 0, 1_002, extract_summary()));
    handler.handle(&request_stage(request_id, 0, 1_003, resolve_summary()));
    handler.handle(&request_stage(
      request_id,
      0,
      1_007,
      StageEvent::Error {
        stage: Stage::Send,
        message: SmolStr::new("upstream timeout"),
        recoverable: false,
        stop: true,
      },
    ));
    handler.handle(&request_stage(
      request_id,
      0,
      1_010,
      StageEvent::Completed {
        success: false,
        attempts: 1,
      },
    ));

    let conn = Connection::open(&path).unwrap();
    let row: (Option<String>, Option<String>) = conn
      .query_row(
        "SELECT request_error, usage_json FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
      )
      .unwrap();
    assert_eq!(row.0.as_deref(), Some("send: upstream timeout"));
    assert_eq!(row.1, None);
  }

  fn request_stage(request_id: &str, attempt: u32, ts: i64, payload: StageEvent) -> Event {
    Event::Requests(RequestEvent {
      request_id: SmolStr::new(request_id),
      attempt,
      ts,
      payload: RequestEventPayload::Stage(payload),
    })
  }

  fn request_record(request_id: &str, attempt: u32, ts: i64, payload: RecordEvent) -> Event {
    Event::Requests(RequestEvent {
      request_id: SmolStr::new(request_id),
      attempt,
      ts,
      payload: RequestEventPayload::Record(payload),
    })
  }

  fn extract_summary() -> StageEvent {
    extract_summary_with_initiator(Some("user"))
  }

  #[test]
  fn usage_preserves_request_classification_for_each_attempt() {
    use tokn_core::request_classification::{RequestClassification, RequestClassificationSource, RequestPurpose};

    let dir = std::env::temp_dir().join(format!("tokn-router-usage-classification-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    for (attempt, source) in [
      (0, RequestClassificationSource::CodexPrompt),
      (1, RequestClassificationSource::Endpoint),
    ] {
      handler.handle(&request_stage(
        "compaction-request",
        attempt,
        1_000,
        StageEvent::Started {
          request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
        },
      ));
      let StageEvent::Extract(mut summary) = extract_summary() else {
        panic!("expected extract summary");
      };
      summary.request_classification = Some(RequestClassification {
        purpose: RequestPurpose::Compaction,
        source,
      });
      handler.handle(&request_stage(
        "compaction-request",
        attempt,
        1_002,
        StageEvent::Extract(summary),
      ));
      handler.handle(&request_stage(
        "compaction-request",
        attempt,
        1_010,
        StageEvent::Completed {
          success: true,
          attempts: attempt + 1,
        },
      ));
    }
    let conn = Connection::open(path).unwrap();
    for (request_id, source) in [
      ("compaction-request", "codex_prompt"),
      ("compaction-request:1", "endpoint"),
    ] {
      let params_json: Option<String> = conn
        .query_row(
          "SELECT params_json FROM requests WHERE request_id = ?1",
          [request_id],
          |row| row.get(0),
        )
        .unwrap();
      assert_eq!(
        parse_json(params_json.as_deref()),
        Some(json!({
          "initiator": "user",
          "stream": true,
          "request_classification": {"purpose": "compaction", "source": source}
        }))
      );
    }
  }

  fn extract_summary_with_initiator(initiator: Option<&str>) -> StageEvent {
    let mut headers = HeaderMap::new();
    headers.insert("x-test", "1");
    StageEvent::Extract(ExtractedSummary {
      agent_id: None,
      model: SmolStr::new("client-model"),
      stream: true,
      session_id: Some(SmolStr::new("sess-1")),
      project_id: Some(SmolStr::new("project-1")),
      initiator: initiator.map(SmolStr::new),
      header_initiator: None,
      route_mode_hint: None,
      request_classification: None,
      headers,
      raw_body: Bytes::new(),
      decoded_body: Bytes::new(),
      body_json: Arc::new(Value::Null),
    })
  }

  fn resolve_summary() -> StageEvent {
    StageEvent::Resolve(ResolvedSummary {
      agent_id: None,
      model: SmolStr::new("client-model"),
      resolved_endpoint: Some(Endpoint::Responses),
      upstream_model: SmolStr::new("upstream-model"),
      upstream_endpoint: Some(Endpoint::Responses),
      account_id: SmolStr::new("acct-1"),
      provider_id: SmolStr::new("prov-1"),
    })
  }

  fn send_summary() -> StageEvent {
    StageEvent::Send(SentSummary {
      status: 200,
      headers: HeaderMap::new(),
      upstream_endpoint: Some(Endpoint::Responses),
      stream: true,
    })
  }

  fn converted_response_summary() -> StageEvent {
    StageEvent::ConvertResponse(ConvertedResponseSummary {
      status: 200,
      headers: HeaderMap::new(),
      body: Some(Arc::new(Value::Null)),
    })
  }

  fn parse_json(value: Option<&str>) -> Option<Value> {
    value.and_then(|value| serde_json::from_str(value).ok())
  }

  #[test]
  fn usage_event_handler_omits_unknown_initiator() {
    let dir = std::env::temp_dir().join(format!("tokn-router-usage-null-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    let mut handler = UsageEventHandler::new(path.clone()).unwrap();
    let request_id = "req-null-initiator";

    handler.handle(&request_stage(
      request_id,
      0,
      1_000,
      StageEvent::Started {
        request_endpoint: RequestEndpoint::Known(Endpoint::Responses),
      },
    ));
    handler.handle(&request_stage(
      request_id,
      0,
      1_002,
      extract_summary_with_initiator(None),
    ));
    handler.handle(&request_stage(request_id, 0, 1_003, resolve_summary()));
    handler.handle(&request_stage(request_id, 0, 1_006, converted_response_summary()));
    handler.handle(&request_stage(
      request_id,
      0,
      1_010,
      StageEvent::Completed {
        success: true,
        attempts: 1,
      },
    ));

    let conn = Connection::open(&path).unwrap();
    let params_json: Option<String> = conn
      .query_row(
        "SELECT params_json FROM requests WHERE request_id = ?1",
        params![request_id],
        |row| row.get(0),
      )
      .unwrap();
    assert_eq!(parse_json(params_json.as_deref()), Some(json!({"stream": true})));
  }
}
