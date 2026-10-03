use super::semantic::{request_messages_from_json, response_messages_from_body};
use super::{SessionsDb, TreeRequestRecord};
use crate::requests::composite_request_id;
use crate::Result;
use bytes::Bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokn_core::event::{Event, EventHandler};
use tokn_core::request_event::{ExtractedSummary, RecordEvent, RequestEvent, RequestEventPayload, StageEvent};
use tokn_headers::inbound::inbound_correlation;
use tokn_headers::keys::X_PARENT_SESSION_ID;

const PENDING_RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;

/// Builds semantic session trees directly from live request events.
pub struct SessionEventHandler {
  db: SessionsDb,
  pending: HashMap<String, PendingSession>,
}

struct PendingSession {
  ts: i64,
  endpoint: Option<String>,
  session_id: Option<String>,
  thread_id: Option<String>,
  parent_thread_id: Option<String>,
  parent_session_id: Option<String>,
  account_id: Option<String>,
  provider_id: Option<String>,
  model: Option<String>,
  status: Option<u16>,
  request_body: Option<Arc<Value>>,
  buffered_response_body: Option<Bytes>,
  converted_response_body: Option<Bytes>,
  upstream_response_body: Option<Bytes>,
}

impl PendingSession {
  fn new(ts: i64) -> Self {
    Self {
      ts,
      endpoint: None,
      session_id: None,
      thread_id: None,
      parent_thread_id: None,
      parent_session_id: None,
      account_id: None,
      provider_id: None,
      model: None,
      status: None,
      request_body: None,
      buffered_response_body: None,
      converted_response_body: None,
      upstream_response_body: None,
    }
  }

  fn into_tree_record(self, request_id: String) -> Option<TreeRequestRecord> {
    let session_id = self.session_id?;
    let endpoint = self.endpoint?;
    let request_messages = self
      .request_body
      .as_deref()
      .map(|body| request_messages_from_json(&endpoint, body))
      .unwrap_or_default();
    let response_body = self
      .converted_response_body
      .or(self.buffered_response_body)
      .or(self.upstream_response_body)
      .unwrap_or_default();
    let response_messages = response_messages_from_body(&response_body);
    if request_messages.is_empty() && response_messages.is_empty() {
      return None;
    }

    Some(TreeRequestRecord {
      ts: self.ts,
      session_id,
      thread_id: self.thread_id,
      parent_thread_id: self.parent_thread_id,
      parent_session_id: self.parent_session_id,
      request_id,
      endpoint,
      status: self.status,
      account_id: self.account_id,
      provider_id: self.provider_id,
      model: self.model,
      request_messages,
      response_messages,
    })
  }
}

impl SessionEventHandler {
  pub fn new(path: PathBuf) -> Result<Self> {
    Ok(Self {
      db: SessionsDb::open(&path)?,
      pending: HashMap::new(),
    })
  }

  fn handle_request(&mut self, event: &RequestEvent) {
    if matches!(event.payload, RequestEventPayload::Custom(_)) {
      return;
    }

    let request_id = composite_request_id(event.request_id.as_str(), event.attempt);
    match &event.payload {
      RequestEventPayload::Stage(StageEvent::Started { request_endpoint }) => {
        self.prune_stale(event.ts);
        let mut pending = PendingSession::new(event.ts);
        pending.endpoint = Some(request_endpoint.as_str().to_string());
        self.pending.insert(request_id, pending);
      }
      RequestEventPayload::Stage(StageEvent::Extract(summary)) => {
        let correlation = inbound_correlation(&summary.headers);
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.session_id = summary
          .session_id
          .clone()
          .or(correlation.session_id)
          .map(|value| value.to_string());
        pending.thread_id = correlation.thread_id.map(|value| value.to_string());
        pending.parent_thread_id = correlation.parent_thread_id.map(|value| value.to_string());
        pending.parent_session_id = summary
          .headers
          .get(&X_PARENT_SESSION_ID)
          .map(|value| value.as_str().to_string());
        pending.model = Some(summary.model.to_string());
        pending.request_body = if pending.session_id.is_some() {
          session_request_body(summary)
        } else {
          None
        };
      }
      RequestEventPayload::Stage(StageEvent::Resolve(summary)) => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.account_id = Some(summary.account_id.to_string());
        pending.provider_id = Some(summary.provider_id.to_string());
      }
      RequestEventPayload::Stage(StageEvent::Send(summary)) => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.status = Some(summary.status);
      }
      RequestEventPayload::Stage(StageEvent::ConvertResponse(summary)) => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.status = Some(summary.status);
        pending.buffered_response_body = summary
          .body
          .as_deref()
          .and_then(|body| serde_json::to_vec(body).ok())
          .map(Bytes::from);
      }
      RequestEventPayload::Stage(StageEvent::Completed { success, .. }) => {
        self.complete(&request_id, *success);
      }
      RequestEventPayload::Record(RecordEvent::UpstreamResp { status, .. }) => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.status = Some(*status);
      }
      RequestEventPayload::Record(RecordEvent::UpstreamBody { body, error }) if error.is_none() => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.upstream_response_body = Some(body.clone());
      }
      RequestEventPayload::Record(RecordEvent::ConvertedBody { body, error }) if error.is_none() => {
        let pending = self
          .pending
          .entry(request_id)
          .or_insert_with(|| PendingSession::new(event.ts));
        pending.converted_response_body = Some(body.clone());
      }
      RequestEventPayload::Stage(
        StageEvent::BuildHeaders(_) | StageEvent::ConvertRequest(_) | StageEvent::Error { .. },
      )
      | RequestEventPayload::Record(
        RecordEvent::InboundConnection { .. }
        | RecordEvent::UpstreamReq { .. }
        | RecordEvent::UpstreamBody { .. }
        | RecordEvent::ConvertedBody { .. }
        | RecordEvent::Usage(_),
      )
      | RequestEventPayload::Custom(_) => {}
    }
  }

  fn complete(&mut self, request_id: &str, success: bool) {
    let Some(pending) = self.pending.remove(request_id) else {
      return;
    };
    if !success {
      return;
    }
    let Some(record) = pending.into_tree_record(request_id.to_string()) else {
      tracing::debug!(request_id, "live session capture had no semantic messages");
      return;
    };
    if let Err(error) = self.db.record_tree(&record) {
      tracing::warn!(error = %error, request_id, session_id = %record.session_id, "live session persistence write failed");
    }
  }

  fn prune_stale(&mut self, ts: i64) {
    let cutoff = ts.saturating_sub(PENDING_RETENTION_MS);
    self.pending.retain(|_, pending| pending.ts >= cutoff);
  }
}

/// Passthrough uses null to avoid reserializing the request; its decoded side-copy remains available for observers.
fn session_request_body(summary: &ExtractedSummary) -> Option<Arc<Value>> {
  if !summary.body_json.is_null() {
    return Some(summary.body_json.clone());
  }
  serde_json::from_slice(&summary.decoded_body).ok().map(Arc::new)
}

impl EventHandler for SessionEventHandler {
  fn handle(&mut self, event: &Event) {
    let Event::Requests(event) = event else {
      return;
    };
    self.handle_request(event);
  }

  fn flush(&mut self) {
    self.pending.clear();
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde_json::json;
  use smol_str::SmolStr;
  use tokn_core::request_event::{
    ConvertedResponseSummary, ExtractedSummary, RequestEndpoint, ResolvedSummary, SentSummary,
  };
  use tokn_headers::HeaderMap;

  #[test]
  fn records_successful_streaming_session_with_routing_metadata() {
    let path = temp_db_path();
    let mut handler = SessionEventHandler::new(path).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(&X_PARENT_SESSION_ID, "parent-session");
    headers.insert(
      "x-codex-turn-metadata",
      r#"{"session_id":"session-live","thread_id":"thread-live","parent_thread_id":"thread-parent","turn_id":"turn-live"}"#,
    );

    emit(
      &mut handler,
      "req-live",
      0,
      1_000,
      RequestEventPayload::Stage(StageEvent::Started {
        request_endpoint: RequestEndpoint::custom("responses"),
      }),
    );
    emit(
      &mut handler,
      "req-live",
      0,
      1_001,
      RequestEventPayload::Stage(StageEvent::Extract(extracted(
        Some("session-live"),
        headers,
        json!({
          "instructions": "be concise",
          "input": [{"role": "user", "content": "hello"}]
        }),
      ))),
    );
    emit(
      &mut handler,
      "req-live",
      0,
      1_002,
      RequestEventPayload::Stage(StageEvent::Resolve(ResolvedSummary {
        agent_id: None,
        model: SmolStr::new("gpt-test"),
        resolved_endpoint: None,
        upstream_model: SmolStr::new("upstream-test"),
        upstream_endpoint: None,
        account_id: SmolStr::new("account-test"),
        provider_id: SmolStr::new("provider-test"),
      })),
    );
    emit(
      &mut handler,
      "req-live",
      0,
      1_003,
      RequestEventPayload::Stage(StageEvent::Send(SentSummary {
        status: 200,
        headers: HeaderMap::new(),
        upstream_endpoint: None,
        stream: true,
      })),
    );
    emit(
      &mut handler,
      "req-live",
      0,
      1_004,
      RequestEventPayload::Record(RecordEvent::ConvertedBody {
        body: Bytes::from(
          "event: response.output_text.delta\ndata: {\"delta\":\"hi\"}\n\n\
           event: response.output_text.delta\ndata: {\"delta\":\" there\"}\n\n",
        ),
        error: None,
      }),
    );
    emit(
      &mut handler,
      "req-live",
      0,
      1_005,
      RequestEventPayload::Stage(StageEvent::Completed {
        success: true,
        attempts: 1,
      }),
    );

    let node: (String, String, i64, String, String, String, String) = handler
      .db
      .conn
      .query_row(
        "SELECT request_id, endpoint, status, account_id, provider_id, model, thread_id
         FROM session_nodes WHERE session_id = 'session-live'",
        [],
        |row| {
          Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
          ))
        },
      )
      .unwrap();
    assert_eq!(
      node,
      (
        "req-live".into(),
        "responses".into(),
        200,
        "account-test".into(),
        "provider-test".into(),
        "gpt-test".into(),
        "thread-live".into(),
      )
    );

    let thread: (String, Option<String>, String) = handler
      .db
      .conn
      .query_row(
        "SELECT thread_id, parent_thread_id, source
         FROM session_threads WHERE session_id = 'session-live'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
      )
      .unwrap();
    assert_eq!(
      thread,
      (
        "thread-live".into(),
        Some("thread-parent".into()),
        "thread-header".into(),
      )
    );

    let relation: String = handler
      .db
      .conn
      .query_row(
        "SELECT parent_session_id FROM session_relations WHERE child_session_id = 'session-live'",
        [],
        |row| row.get(0),
      )
      .unwrap();
    assert_eq!(relation, "parent-session");

    let response = handler.db.materialize_response_messages("req-live").unwrap();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].role, "assistant");
    assert_eq!(response[0].parts[0].content.as_ref(), b"hi there");
  }

  #[test]
  fn drops_failed_attempt_and_records_successful_retry_with_composite_id() {
    let path = temp_db_path();
    let mut handler = SessionEventHandler::new(path).unwrap();

    emit_attempt(&mut handler, "req-retry", 0, false, Some("session-retry"));
    emit_attempt(&mut handler, "req-retry", 1, true, Some("session-retry"));

    let request_ids = handler
      .db
      .conn
      .prepare("SELECT request_id FROM session_nodes ORDER BY request_id")
      .unwrap()
      .query_map([], |row| row.get::<_, String>(0))
      .unwrap()
      .collect::<rusqlite::Result<Vec<_>>>()
      .unwrap();
    assert_eq!(request_ids, vec!["req-retry:1"]);
    assert!(handler.pending.is_empty());
  }

  #[test]
  fn records_buffered_stage_body_and_skips_requests_without_session_id() {
    let path = temp_db_path();
    let mut handler = SessionEventHandler::new(path).unwrap();

    emit(
      &mut handler,
      "req-buffered",
      0,
      3_000,
      RequestEventPayload::Stage(StageEvent::Started {
        request_endpoint: RequestEndpoint::custom("chat_completions"),
      }),
    );
    emit(
      &mut handler,
      "req-buffered",
      0,
      3_001,
      RequestEventPayload::Stage(StageEvent::Extract(extracted(
        Some("session-buffered"),
        HeaderMap::new(),
        json!({"messages": [{"role": "user", "content": "hello"}]}),
      ))),
    );
    emit(
      &mut handler,
      "req-buffered",
      0,
      3_002,
      RequestEventPayload::Stage(StageEvent::ConvertResponse(ConvertedResponseSummary {
        status: 201,
        headers: HeaderMap::new(),
        body: Some(Arc::new(json!({
          "choices": [{"message": {"content": "buffered reply"}}]
        }))),
      })),
    );
    emit(
      &mut handler,
      "req-buffered",
      0,
      3_003,
      RequestEventPayload::Stage(StageEvent::Completed {
        success: true,
        attempts: 1,
      }),
    );
    emit_attempt(&mut handler, "req-no-session", 0, true, None);

    let node_count: i64 = handler
      .db
      .conn
      .query_row("SELECT COUNT(*) FROM session_nodes", [], |row| row.get(0))
      .unwrap();
    assert_eq!(node_count, 1);
    let response = handler.db.materialize_response_messages("req-buffered").unwrap();
    assert_eq!(response[0].role, "assistant");
    assert_eq!(response[0].parts[0].content.as_ref(), b"buffered reply");
  }

  #[test]
  fn decodes_passthrough_request_body_when_body_json_is_null() {
    let path = temp_db_path();
    let mut handler = SessionEventHandler::new(path).unwrap();
    let mut summary = extracted(
      Some("session-passthrough"),
      HeaderMap::new(),
      json!({"messages": [{"role": "user", "content": "from decoded body"}]}),
    );
    summary.raw_body = Bytes::from_static(b"non-json wire body");
    summary.body_json = Arc::new(Value::Null);

    emit(
      &mut handler,
      "req-passthrough",
      0,
      4_000,
      RequestEventPayload::Stage(StageEvent::Started {
        request_endpoint: RequestEndpoint::custom("chat_completions"),
      }),
    );
    emit(
      &mut handler,
      "req-passthrough",
      0,
      4_001,
      RequestEventPayload::Stage(StageEvent::Extract(summary)),
    );
    emit(
      &mut handler,
      "req-passthrough",
      0,
      4_002,
      RequestEventPayload::Stage(StageEvent::Completed {
        success: true,
        attempts: 1,
      }),
    );

    let request = handler.db.materialize_request_messages("req-passthrough").unwrap();
    assert_eq!(request.len(), 1);
    assert_eq!(request[0].role, "user");
    assert_eq!(request[0].parts[0].content.as_ref(), b"from decoded body");
  }

  fn emit_attempt(
    handler: &mut SessionEventHandler,
    request_id: &str,
    attempt: u32,
    success: bool,
    session_id: Option<&str>,
  ) {
    let ts = 2_000 + i64::from(attempt) * 10;
    emit(
      handler,
      request_id,
      attempt,
      ts,
      RequestEventPayload::Stage(StageEvent::Started {
        request_endpoint: RequestEndpoint::custom("responses"),
      }),
    );
    emit(
      handler,
      request_id,
      attempt,
      ts + 1,
      RequestEventPayload::Stage(StageEvent::Extract(extracted(
        session_id,
        HeaderMap::new(),
        json!({"input": "hello"}),
      ))),
    );
    emit(
      handler,
      request_id,
      attempt,
      ts + 2,
      RequestEventPayload::Stage(StageEvent::Completed {
        success,
        attempts: attempt + 1,
      }),
    );
  }

  fn extracted(session_id: Option<&str>, headers: HeaderMap, body: Value) -> ExtractedSummary {
    let body_bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
    ExtractedSummary {
      agent_id: None,
      model: SmolStr::new("gpt-test"),
      stream: false,
      session_id: session_id.map(SmolStr::new),
      project_id: None,
      initiator: None,
      header_initiator: None,
      route_mode_hint: None,
      request_classification: None,
      headers,
      raw_body: body_bytes.clone(),
      decoded_body: body_bytes,
      body_json: Arc::new(body),
    }
  }

  fn emit(handler: &mut SessionEventHandler, request_id: &str, attempt: u32, ts: i64, payload: RequestEventPayload) {
    handler.handle(&Event::Requests(RequestEvent {
      request_id: SmolStr::new(request_id),
      attempt,
      ts,
      payload,
    }));
  }

  fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("tokn-live-sessions-{}.db", uuid::Uuid::new_v4()))
  }
}
