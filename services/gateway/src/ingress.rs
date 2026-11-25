use anyhow::Result;
use tokn_config::v2::PersistencePlan;
use tokn_core::event::EventHandler;

/// Record requests rejected here. A dispatched request is recorded only by its worker.
pub(super) fn handlers(persistence: &PersistencePlan) -> Result<Vec<Box<dyn EventHandler>>> {
  let mut handlers: Vec<Box<dyn EventHandler>> = Vec::new();
  if persistence.enabled() {
    let paths = persistence.resolve_paths()?;
    handlers.push(Box::new(tokn_persistence::RequestEventHandler::with_options(
      paths.requests_dir,
      tokn_persistence::requests::RequestPersistenceOptions {
        record_request_bodies: false,
        body_max_bytes: 0,
      },
    )?));
  }
  Ok(handlers)
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::path::Path;
  use tokn_core::event::EventBus;

  fn ingress_persistence(root: &Path, enabled: bool) -> tokn_config::v2::PersistencePlan {
    let config = format!(
      r#"
schema_version = 2
[listeners.proxy]
kind = "forward_proxy"
bind = "127.0.0.1:8080"
client_auth = "none"
default_http_action = {{ kind = "reject" }}
default_connect = "reject"
[service.persistence]
enabled = {enabled}
usage_db_path = {:?}
sessions_db_path = {:?}
requests_dir = {:?}
"#,
      root.join("usage.db"),
      root.join("sessions.db"),
      root.join("requests"),
    );
    tokn_config::v2::parse_config(&config, Path::new("frontend-persistence.toml"))
      .unwrap()
      .service()
      .persistence()
      .clone()
  }

  #[tokio::test]
  async fn ingress_event_bus_persists_rejections_and_drains_without_usage_or_sessions() {
    use tokn_core::event::Event;
    use tokn_core::request_event::{IngressRejection, RecordEvent, RequestEndpoint, RequestEvent, RequestEventPayload};

    let root = tempfile::tempdir().unwrap();
    let persistence = ingress_persistence(root.path(), true);
    let handlers = handlers(&persistence).unwrap();
    let events = EventBus::new(256);
    let receiver = events.subscribe();
    assert_eq!(handlers.len(), 1);
    let worker = tokn_core::event::spawn_event_loop(receiver, handlers);
    let ts = tokn_core::util::now_unix_ms();
    let mut headers = tokn_headers::HeaderMap::new();
    headers.insert("authorization", "Bearer private-test-value");
    headers.insert("x-request-id", "req-frontend-persisted");
    events.emit(Event::Requests(RequestEvent {
      request_id: "req-frontend-persisted".into(),
      attempt: 0,
      ts,
      payload: RequestEventPayload::Record(RecordEvent::IngressRejected(IngressRejection {
        started_ts: ts,
        request_endpoint: RequestEndpoint::infer_from_path("/v1/responses"),
        status: 413,
        error: "request body exceeds the configured limit".into(),
        mode: "requests".into(),
        pipeline_id: "requests".into(),
        method: "POST".into(),
        url: Some("/v1/responses".into()),
        headers,
        user: None,
        api_key_id: None,
        local_addr: None,
        peer_addr: None,
      })),
    }));
    crate::finish_events(&events, None).await.unwrap();
    worker.join().unwrap();

    assert!(!root.path().join("usage.db").exists());
    assert!(!root.path().join("sessions.db").exists());
    let days = tokn_persistence::requests::RequestsDb::day_files(&root.path().join("requests")).unwrap();
    assert_eq!(days.len(), 1);
    let db = rusqlite::Connection::open(&days[0]).unwrap();
    let (status, error, header_bytes, body): (i64, String, Vec<u8>, Option<Vec<u8>>) = db
      .query_row(
        "SELECT c.status, c.request_error, d.inbound_req_headers, d.inbound_req_body
         FROM request_connection c JOIN request_downstream d USING (request_id)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
      )
      .unwrap();
    assert_eq!(status, 413);
    assert!(error.contains("request body exceeds the configured limit"));
    let headers: serde_json::Value = serde_json::from_slice(&header_bytes).unwrap();
    assert_eq!(headers["authorization"], "<redacted>");
    assert_eq!(headers["x-request-id"], "req-frontend-persisted");
    assert!(body.is_none());
  }

  #[tokio::test]
  async fn disabled_ingress_persistence_does_not_open_databases() {
    let root = tempfile::tempdir().unwrap();
    let persistence = ingress_persistence(root.path(), false);
    let handlers = handlers(&persistence).unwrap();
    let events = EventBus::new(256);
    let receiver = events.subscribe();
    assert!(handlers.is_empty());
    let worker = tokn_core::event::spawn_event_loop(receiver, handlers);
    crate::finish_events(&events, None).await.unwrap();
    worker.join().unwrap();
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
  }
}
