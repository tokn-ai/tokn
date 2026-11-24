mod common;

use axum::body::to_bytes;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use common::{body_json, cfg_for, ctx, int, missing_or_null, text, RequestsHarness};
use serde_json::{json, Map, Value};
use tokn_config::RouteMode;
use tokn_mock_server::{MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};
use tokn_router::api::build_proxy_state;
use tokn_router::proxy::passthrough_pipeline::{proxy_passthrough_via_pipeline_inner, proxy_switch_via_pipeline_inner};

#[derive(Clone, Copy)]
struct ProxyCase {
  name: &'static str,
  upstream_status: StatusCode,
  expected_status: StatusCode,
  expected_error: Option<&'static str>,
}

fn assert_proxy_row(row: &Map<String, Value>, case: ProxyCase, inbound_body: &Bytes) {
  let ctx = ctx(row);
  assert_eq!(
    ctx.get("mode").and_then(Value::as_str),
    Some("passthrough"),
    "{}",
    case.name
  );
  assert_eq!(
    ctx.get("pipeline_id").and_then(Value::as_str),
    Some("proxy"),
    "{}",
    case.name
  );
  assert_eq!(
    text(row, "endpoint").as_deref(),
    Some("chat_completions"),
    "{}",
    case.name
  );
  assert_eq!(
    text(row, "inbound_req_method").as_deref(),
    Some("POST"),
    "{}",
    case.name
  );
  assert!(
    text(row, "inbound_req_url")
      .as_deref()
      .is_some_and(|url| url.starts_with("http://127.0.0.1:") && url.ends_with("/v1/chat/completions")),
    "{}",
    case.name
  );
  assert_eq!(text(row, "provider_id").as_deref(), Some("127.0.0.1"), "{}", case.name);
  assert_eq!(
    text(row, "session_id").as_deref(),
    Some("sess-proxy-1"),
    "{}",
    case.name
  );
  assert_eq!(
    int(row, "outbound_resp_status"),
    Some(case.upstream_status.as_u16() as i64)
  );

  if let Some(expected_error) = case.expected_error {
    let error = text(row, "request_error").unwrap_or_default();
    assert!(
      error.contains(expected_error),
      "{} persisted error {error:?} did not contain {expected_error:?}",
      case.name
    );
    assert!(
      int(row, "status").is_none(),
      "{} should not persist success status",
      case.name
    );
  } else {
    assert_eq!(int(row, "status"), Some(200), "{}", case.name);
    assert!(missing_or_null(row, "request_error"), "{}", case.name);
    assert_eq!(
      body_json(row, "outbound_req_body"),
      serde_json::from_slice::<Value>(inbound_body).unwrap()
    );
    assert_eq!(body_json(row, "inbound_resp_body"), json!({"proxy": true}));
  }
}

#[tokio::test]
async fn proxy_passthrough_modes_return_expected_results_and_persist_request_rows() {
  let mut harness = RequestsHarness::new();
  let cfg = cfg_for(&harness.requests_dir, RouteMode::Passthrough);
  let state = build_proxy_state(&cfg, &[], harness.events.clone()).unwrap();

  let cases = [
    ProxyCase {
      name: "proxy_passthrough_success",
      upstream_status: StatusCode::OK,
      expected_status: StatusCode::OK,
      expected_error: None,
    },
    ProxyCase {
      name: "proxy_passthrough_failure",
      upstream_status: StatusCode::INTERNAL_SERVER_ERROR,
      expected_status: StatusCode::INTERNAL_SERVER_ERROR,
      expected_error: Some("upstream 500"),
    },
  ];

  for case in cases {
    let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
      MockEndpoint::Custom {
        method: Method::POST,
        path: "/v1/chat/completions".into(),
      },
      MockResponse {
        status: case.upstream_status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: Bytes::from_static(br#"{"proxy":true}"#),
      },
    )))
    .await;
    let mock_addr = mock
      .base_url()
      .strip_prefix("http://")
      .expect("mock uses http")
      .to_string();
    let (host, port) = mock_addr.rsplit_once(':').expect("host:port");
    let port = port.parse::<u16>().expect("mock port");
    let request_id = case.name;
    let inbound_body = Bytes::from_static(br#"{"model":"glm-4.7","messages":[{"role":"user","content":"via proxy"}]}"#);
    let req = Request::builder()
      .method(Method::POST)
      .uri("/v1/chat/completions")
      .header("content-type", "application/json")
      .header("authorization", "Bearer client-token")
      .header(
        "x-codex-turn-metadata",
        r#"{"session_id":"sess-proxy-1","thread_id":"thread-proxy-1","turn_id":"turn-proxy-1"}"#,
      )
      .header("x-request-id", request_id)
      .body(())
      .unwrap();
    let (parts, ()) = req.into_parts();

    let response = proxy_passthrough_via_pipeline_inner(
      &state,
      host,
      port,
      "http",
      Some("127.0.0.1:10000".into()),
      Some("127.0.0.1:4142".into()),
      parts,
      inbound_body.clone(),
    )
    .await;

    assert_eq!(response.status(), case.expected_status, "{}", case.name);
    let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();

    mock.shutdown().await;
  }

  harness.shutdown().await;
  let inbound_body = Bytes::from_static(br#"{"model":"glm-4.7","messages":[{"role":"user","content":"via proxy"}]}"#);
  for case in cases {
    let row = harness.row(case.name).await;
    assert_proxy_row(&row, case, &inbound_body);
    assert_eq!(
      harness.usage_session_id(case.name).as_deref(),
      Some("sess-proxy-1"),
      "{}",
      case.name
    );
  }
}

#[tokio::test]
async fn proxy_switch_failure_returns_bad_request_and_persists_error_row() {
  let mut harness = RequestsHarness::new();
  let cfg = cfg_for(&harness.requests_dir, RouteMode::Passthrough);
  let state = build_proxy_state(&cfg, &[], harness.events.clone()).unwrap();
  let request_id = "proxy-switch-unrecognized";
  let req = Request::builder()
    .method(Method::POST)
    .uri("/v1/chat/completions")
    .header("content-type", "application/json")
    .header("x-request-id", request_id)
    .body(())
    .unwrap();
  let (parts, ()) = req.into_parts();

  let response = proxy_switch_via_pipeline_inner(
    &state,
    "unrecognized.local",
    443,
    "https",
    Some("127.0.0.1:10001".into()),
    Some("127.0.0.1:4142".into()),
    parts,
    Bytes::from_static(br#"{"model":"glm-4.7","messages":[]}"#),
  )
  .await;

  assert_eq!(response.status(), StatusCode::BAD_REQUEST);
  let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
  let body: Value = serde_json::from_slice(&body).unwrap();
  assert!(body["error"]["message"]
    .as_str()
    .unwrap_or_default()
    .contains("switch mode requires a recognized provider URL"));

  harness.shutdown().await;

  let row = harness.row(request_id).await;
  let ctx = ctx(&row);
  assert_eq!(ctx.get("mode").and_then(Value::as_str), Some("switch"));
  assert_eq!(ctx.get("pipeline_id").and_then(Value::as_str), Some("proxy"));
  assert_eq!(text(&row, "endpoint").as_deref(), Some("chat_completions"));
  assert_eq!(text(&row, "inbound_req_method").as_deref(), Some("POST"));
  assert_eq!(
    text(&row, "inbound_req_url").as_deref(),
    Some("https://unrecognized.local/v1/chat/completions")
  );
  assert_eq!(int(&row, "status"), Some(400));
  let error = text(&row, "request_error").unwrap_or_default();
  assert!(
    error.contains("switch mode requires a recognized provider URL"),
    "persisted error was {error:?}"
  );
}
