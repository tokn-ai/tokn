use crate::{HeaderExpectation, MockAuthConfig, MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};
use axum::body::Bytes;
use axum::http::Method;

#[tokio::test]
async fn rejects_unconfigured_endpoints() {
  let server = MockLlmServer::start(MockLlmConfig {
    routes: vec![MockRoute::models(["gpt-4o-mini"])],
    ..Default::default()
  })
  .await;

  let http = reqwest::Client::new();
  let ok = http.get(server.url("/models")).send().await.unwrap();
  let missing = http.post(server.url("/responses")).send().await.unwrap();

  assert_eq!(ok.status(), reqwest::StatusCode::OK);
  assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enforces_auth_and_header_rules() {
  let server = MockLlmServer::start(
    MockLlmConfig {
      routes: vec![MockRoute::chat_completions()],
      ..Default::default()
    }
    .with_auth(MockAuthConfig::bearer(["sk-good", "sk-backup"]))
    .require_header(HeaderExpectation::equals("content-type", "application/json"))
    .require_header(HeaderExpectation::present("x-trace-id"))
    .forbid_header("x-blocked"),
  )
  .await;

  let http = reqwest::Client::new();
  let unauthorized = http
    .post(server.url("/chat/completions"))
    .header("content-type", "application/json")
    .header("x-trace-id", "trace-1")
    .send()
    .await
    .unwrap();
  let blocked = http
    .post(server.url("/chat/completions"))
    .header("authorization", "Bearer sk-good")
    .header("content-type", "application/json")
    .header("x-trace-id", "trace-2")
    .header("x-blocked", "true")
    .send()
    .await
    .unwrap();
  let ok = http
    .post(server.url("/chat/completions"))
    .header("authorization", "Bearer sk-backup")
    .header("content-type", "application/json")
    .header("x-trace-id", "trace-3")
    .body(r#"{"model":"gpt-4o-mini"}"#)
    .send()
    .await
    .unwrap();

  assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);
  assert_eq!(blocked.status(), reqwest::StatusCode::BAD_REQUEST);
  assert_eq!(ok.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn captures_requests_for_assertions() {
  let server = MockLlmServer::start(MockLlmConfig {
    routes: vec![MockRoute::responses()],
    ..Default::default()
  })
  .await;

  let http = reqwest::Client::new();
  let response = http
    .post(server.url("/responses?view=full"))
    .header("x-test", "yes")
    .body(r#"{"input":"hello"}"#)
    .send()
    .await
    .unwrap();

  assert_eq!(response.status(), reqwest::StatusCode::OK);

  let captured = server.last_request().expect("captured request");
  assert_eq!(captured.method, Method::POST);
  assert_eq!(captured.path, "/responses");
  assert_eq!(captured.query.as_deref(), Some("view=full"));
  assert_eq!(captured.header("x-test"), Some("yes"));
  assert_eq!(captured.body, Bytes::from_static(br#"{"input":"hello"}"#));
}

#[tokio::test]
async fn rejects_bodies_over_limit() {
  let server = MockLlmServer::start(MockLlmConfig {
    routes: vec![MockRoute::chat_completions()],
    ..Default::default()
  })
  .await;

  let http = reqwest::Client::new();
  let response = http
    .post(server.url("/chat/completions"))
    .body(vec![b'x'; 1024 * 1024 + 1])
    .send()
    .await
    .unwrap();

  assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
  assert!(response.text().await.unwrap().contains("request body exceeds"));
}

#[tokio::test]
async fn last_configured_route_overrides_earlier_match() {
  let server = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
    MockEndpoint::Responses,
    MockResponse::json(serde_json::json!({"id": "resp-override"})),
  )))
  .await;

  let http = reqwest::Client::new();
  let response = http.post(server.url("/responses")).send().await.unwrap();

  assert_eq!(response.status(), reqwest::StatusCode::OK);
  let body: serde_json::Value = response.json().await.unwrap();
  assert_eq!(body["id"], "resp-override");
}
