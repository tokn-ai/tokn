use super::*;
use axum::body::{to_bytes, Body};
use axum::http::HeaderValue;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use std::path::Path;
use tokn_core::request_event::{IngressRejection, StageEvent};
use tower::ServiceExt;

type EventReceiver = tokio::sync::broadcast::Receiver<Arc<CoreEvent>>;

const REQUEST_PATH: &str = "/v1/responses?source=ingress-test";
const MANAGED_ROUTE: &str = r#"
[routes.test]
kind = "managed"
providers = ["local"]
provider = { kind = "fixed", provider = "local" }
model = { kind = "capability" }
operation = "preserve"
"#;
const RELAY_ROUTE: &str = r#"
[routes.test]
kind = "relay"
providers = ["local"]
destination = { kind = "fixed_provider", provider = "local" }
credentials = { kind = "client" }
"#;

fn fixture(
  route: &str,
  client_auth: &str,
  max_wire_bytes: u64,
  max_decoded_bytes: u64,
  upstream: &str,
) -> (Router, EventReceiver) {
  let config = format!(
    r#"
schema_version = 2

[service.request_limits]
max_wire_bytes = {max_wire_bytes}
max_decoded_bytes = {max_decoded_bytes}

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "{client_auth}"

[profiles.test]
route = "test"
binding = {{ path = "/v1", endpoints = ["responses"] }}

{route}

[providers.local]
driver = "openai"
base_url = "{upstream}/v1"
"#
  );
  let (plan, service) = tokn_config::v2::parse_config(&config, Path::new("ingress-record-tests.toml"))
    .unwrap()
    .into_parts();
  let events = Arc::new(EventBus::new(128));
  let receiver = events.subscribe();
  let mut states = build_states_with_service(
    plan,
    service,
    &[],
    Arc::new(tokn_access::AccessStore::disabled()),
    events,
  )
  .unwrap();
  (router(states.pop().unwrap()), receiver)
}

fn post(body: Body) -> Request {
  axum::http::Request::post(REQUEST_PATH)
    .header("content-type", "application/json")
    .body(body)
    .unwrap()
}

fn drain_events(receiver: &mut EventReceiver) -> Vec<RequestEvent> {
  let mut requests = Vec::new();
  while let Ok(event) = receiver.try_recv() {
    if let CoreEvent::Requests(request) = &*event {
      requests.push(request.clone());
    }
  }
  requests
}

fn rejections(events: &[RequestEvent]) -> Vec<(&RequestEvent, &IngressRejection)> {
  events
    .iter()
    .filter_map(|event| match &event.payload {
      RequestEventPayload::Record(RecordEvent::IngressRejected(rejection)) => Some((event, rejection)),
      _ => None,
    })
    .collect()
}

#[tokio::test]
async fn local_admission_failures_record_once_without_starting_a_pipeline() {
  let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
  encoder
    .write_all(br#"{"model":"gpt-4o","input":"decoded body exceeds eight bytes"}"#)
    .unwrap();
  let compressed = encoder.finish().unwrap();
  let mut decoded_request = post(Body::from(compressed));
  decoded_request
    .headers_mut()
    .insert("content-encoding", HeaderValue::from_static("gzip"));
  let mut malformed_request = post(Body::from("{"));
  malformed_request
    .headers_mut()
    .insert(REQUEST_ID_HEADER, HeaderValue::from_static("client-invalid-json"));
  let broken_body = Body::from_stream(futures_util::stream::once(async {
    Err::<Bytes, _>(std::io::Error::other("fixture request body read failed"))
  }));

  let cases = [
    (
      "wire limit",
      8,
      1024,
      "none",
      post(Body::from(r#"{"model":"gpt-4o"}"#)),
      StatusCode::PAYLOAD_TOO_LARGE,
    ),
    (
      "decoded limit",
      1024,
      8,
      "none",
      decoded_request,
      StatusCode::PAYLOAD_TOO_LARGE,
    ),
    (
      "malformed JSON",
      1024,
      1024,
      "none",
      malformed_request,
      StatusCode::BAD_REQUEST,
    ),
    (
      "missing authentication",
      1024,
      1024,
      "local_keys",
      post(Body::from(r#"{"model":"gpt-4o"}"#)),
      StatusCode::UNAUTHORIZED,
    ),
    (
      "wrong method",
      1024,
      1024,
      "none",
      axum::http::Request::get(REQUEST_PATH).body(Body::empty()).unwrap(),
      StatusCode::METHOD_NOT_ALLOWED,
    ),
    (
      "body read failure",
      1024,
      1024,
      "none",
      post(broken_body),
      StatusCode::BAD_REQUEST,
    ),
  ];

  for (name, wire_limit, decoded_limit, auth, request, expected) in cases {
    let method = request.method().to_string();
    let supplied_id = request.headers().get(REQUEST_ID_HEADER).cloned();
    let (app, mut receiver) = fixture(MANAGED_ROUTE, auth, wire_limit, decoded_limit, "http://127.0.0.1:9");
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), expected, "{name}");
    let response_id = response.headers().get(REQUEST_ID_HEADER).unwrap().to_str().unwrap();
    if let Some(supplied_id) = supplied_id {
      assert_eq!(response_id, supplied_id.to_str().unwrap(), "{name}");
    } else {
      assert!(response_id.starts_with("req-"), "{name}");
    }
    let events = drain_events(&mut receiver);
    let failures = rejections(&events);
    assert_eq!(failures.len(), 1, "{name}");
    let (event, rejection) = failures[0];
    assert_eq!(event.request_id, response_id, "{name}");
    assert_eq!(event.attempt, 0, "{name}");
    assert!(event.ts >= rejection.started_ts, "{name}");
    assert_eq!(rejection.status, expected.as_u16(), "{name}");
    assert_eq!(
      rejection.request_endpoint.resolved(),
      Some(Endpoint::Responses),
      "{name}"
    );
    assert_eq!(rejection.method, method, "{name}");
    assert_eq!(rejection.url.as_deref(), Some(REQUEST_PATH), "{name}");
    assert!(!rejection.error.is_empty(), "{name}");
    assert!(
      !events
        .iter()
        .any(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Started { .. }))),
      "{name}: locally rejected requests must not enter the provider pipeline"
    );
  }
}

#[tokio::test]
async fn pipeline_failures_keep_their_pipeline_records_without_an_ingress_rejection() {
  let (app, mut receiver) = fixture(MANAGED_ROUTE, "none", 1024, 1024, "http://127.0.0.1:9");
  let response = app
    .oneshot(post(Body::from(r#"{"model":"gpt-4o","input":"fixture"}"#)))
    .await
    .unwrap();
  assert!(response.status().is_server_error());
  let events = drain_events(&mut receiver);
  assert!(rejections(&events).is_empty());
  assert_eq!(
    events
      .iter()
      .filter(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Started { .. })))
      .count(),
    1
  );
  assert!(events
    .iter()
    .any(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Error { .. }))));
}

#[tokio::test]
async fn upstream_failure_status_is_forwarded_without_an_ingress_rejection() {
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let address = listener.local_addr().unwrap();
  let upstream = Router::new().fallback(|| async { (StatusCode::SERVICE_UNAVAILABLE, "fixture upstream failure") });
  let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
  let (app, mut receiver) = fixture(RELAY_ROUTE, "none", 1024, 1024, &format!("http://{address}"));
  let response = app
    .oneshot(post(Body::from(r#"{"model":"gpt-4o","input":"fixture"}"#)))
    .await
    .unwrap();
  assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
  let body = to_bytes(response.into_body(), 1024).await.unwrap();
  assert_eq!(body.as_ref(), b"fixture upstream failure");
  let events = drain_events(&mut receiver);
  assert!(rejections(&events).is_empty());
  assert_eq!(
    events
      .iter()
      .filter(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Started { .. })))
      .count(),
    1
  );
  assert!(events.iter().any(|event| matches!(
    event.payload,
    RequestEventPayload::Record(RecordEvent::UpstreamResp { status: 503, .. })
  )));
  task.abort();
}
