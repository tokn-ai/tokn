use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{ConnectInfo, Extension, State};
use axum::http::{HeaderMap, Request, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use std::path::Path;
use std::sync::Arc;
use tokn_access::AccessStore;
use tokn_core::account::{AccountConfig, AccountTier, AuthType, Secret};
use tokn_core::event::{Event, EventBus};
use tokn_core::request_event::{RecordEvent, RequestEventPayload};
use tower::ServiceExt;

struct CapturedRequest {
  uri: Uri,
  headers: HeaderMap,
  body: Bytes,
}

#[tokio::test]
async fn custom_codex_client_relays_prioritize_compaction_on_api_and_proxy() {
  let (capture_tx, mut capture_rx) = tokio::sync::mpsc::channel(2);
  let upstream = Router::new()
    .route(
      "/{*path}",
      any(
        |State(capture_tx): State<tokio::sync::mpsc::Sender<CapturedRequest>>,
         uri: Uri,
         headers: HeaderMap,
         body: Bytes| async move {
          capture_tx.send(CapturedRequest { uri, headers, body }).await.unwrap();
          axum::Json(serde_json::json!({"id": "compacted", "output": []}))
        },
      ),
    )
    .with_state(capture_tx);
  let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = upstream_listener.local_addr().unwrap();
  let upstream_task = tokio::spawn(async move { axum::serve(upstream_listener, upstream).await.unwrap() });

  let proxy_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let proxy_addr = proxy_probe.local_addr().unwrap();
  drop(proxy_probe);
  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"

[listeners.proxy]
kind = "forward_proxy"
bind = "{proxy_addr}"
client_auth = "none"
default_http_action = {{ kind = "route", profile = "relay" }}
default_connect = "reject"

[profiles.relay]
route = "relay"
binding = {{ endpoints = ["responses"] }}

[routes.relay]
kind = "relay"
destination = {{ kind = "fixed_provider", provider = "work-codex" }}
credentials = {{ kind = "client" }}

[providers.work-codex]
driver = "codex"
base_url = "http://{upstream_addr}/backend-api/codex"
"#
  );
  let plan = tokn_config::v2::parse(&config, Path::new("custom-codex-client-relay.toml")).unwrap();
  let mut states =
    tokn_router::v2::build_runtime_states(plan, &[], Arc::new(AccessStore::disabled()), Arc::new(EventBus::noop()))
      .unwrap();
  let app = tokn_router::v2::router(states.llm_api.pop().unwrap());
  let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
  let proxy_task = tokio::spawn(tokn_router::v2::serve_forward_proxy(
    states.forward_proxy.pop().unwrap(),
    proxy_addr,
    async {
      let _ = shutdown_rx.await;
    },
  ));
  tokio::time::timeout(std::time::Duration::from_secs(5), async {
    loop {
      if tokio::net::TcpStream::connect(proxy_addr).await.is_ok() {
        break;
      }
      tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("proxy listener should start");

  let body = br#"{"model":"gpt-test","input":[{"type":"compaction_trigger"}],"service_tier":"auto"}"#;
  let response = app
    .clone()
    .oneshot(
      Request::post("/relay/v1/responses")
        .header("content-type", "application/json")
        .header("authorization", "Bearer client-key")
        .header(
          "x-codex-routing-hint",
          "model=stale-model;tier=default;region=test;tier=flex",
        )
        .body(Body::from(body.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  let api_request = capture_rx.recv().await.unwrap();
  assert!(api_request.uri.path().ends_with("/responses"));
  assert_eq!(api_request.headers["authorization"], "Bearer client-key");
  let api_body: serde_json::Value = serde_json::from_slice(&api_request.body).unwrap();
  assert_eq!(api_body["service_tier"], "priority");
  assert_priority_routing_hint(&api_request.headers, "gpt-test", &["region=test"]);

  let ordinary_body = br#"{ "model": "gpt-test", "input": [], "service_tier": "priority" }"#;
  let ordinary_hint = "model=stale-model;tier=default;region=test";
  let response = app
    .oneshot(
      Request::post("/relay/v1/responses")
        .header("content-type", "application/json")
        .header("authorization", "Bearer client-key")
        .header("x-codex-routing-hint", ordinary_hint)
        .body(Body::from(ordinary_body.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  let ordinary_request = capture_rx.recv().await.unwrap();
  assert_eq!(ordinary_request.headers["x-codex-routing-hint"], ordinary_hint);
  assert_eq!(ordinary_request.body.as_ref(), ordinary_body);

  let client = reqwest::Client::builder()
    .proxy(reqwest::Proxy::http(format!("http://{proxy_addr}")).unwrap())
    .build()
    .unwrap();
  let response = client
    .post("http://original.example/v1/responses/compact")
    .header("content-type", "application/json")
    .header("authorization", "Bearer client-key")
    .body(body.as_slice())
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  response.bytes().await.unwrap();
  let proxy_request = capture_rx.recv().await.unwrap();
  assert!(proxy_request.uri.path().ends_with("/responses/compact"));
  assert_eq!(proxy_request.headers["authorization"], "Bearer client-key");
  assert_eq!(
    proxy_request.headers["content-length"].to_str().unwrap(),
    proxy_request.body.len().to_string()
  );
  let proxy_body: serde_json::Value = serde_json::from_slice(&proxy_request.body).unwrap();
  assert_eq!(proxy_body["service_tier"], "priority");
  assert_priority_routing_hint(&proxy_request.headers, "gpt-test", &[]);

  drop(client);
  shutdown_tx.send(()).unwrap();
  proxy_task.await.unwrap().unwrap();
  upstream_task.abort();
}

fn assert_priority_routing_hint(headers: &HeaderMap, model: &str, unrelated: &[&str]) {
  let directives: Vec<_> = headers["x-codex-routing-hint"]
    .to_str()
    .unwrap()
    .split(';')
    .map(str::trim)
    .collect();
  let expected_model = format!("model={model}");
  assert_eq!(
    directives
      .iter()
      .copied()
      .filter(|directive| directive.starts_with("model="))
      .collect::<Vec<_>>(),
    [expected_model.as_str()]
  );
  assert_eq!(
    directives
      .iter()
      .copied()
      .filter(|directive| directive.starts_with("tier="))
      .collect::<Vec<_>>(),
    ["tier=priority"]
  );
  assert_eq!(
    directives
      .iter()
      .copied()
      .filter(|directive| !directive.starts_with("model=") && !directive.starts_with("tier="))
      .collect::<Vec<_>>(),
    unrelated
  );
}

#[tokio::test]
async fn explicit_managed_destinations_forward_unlisted_ids_and_keep_access_constraints() {
  let (capture_tx, mut capture_rx) = tokio::sync::mpsc::channel(8);
  let upstream = Router::new()
    .route(
      "/v1/chat/completions",
      any(
        |State(capture_tx): State<tokio::sync::mpsc::Sender<serde_json::Value>>, body: Bytes| async move {
          capture_tx.send(serde_json::from_slice(&body).unwrap()).await.unwrap();
          axum::Json(serde_json::json!({
            "id": "unlisted-model",
            "choices": [{"message": {"role": "assistant", "content": "ok"}}],
          }))
        },
      ),
    )
    .with_state(capture_tx);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = listener.local_addr().unwrap();
  let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
  let mut config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "local_keys"

[providers.local]
driver = "openai"
base_url = "http://{upstream_addr}/v1"
"#
  );
  for (name, provider, model) in [
    (
      "fixed",
      r#"{ kind = "fixed", provider = "local" }"#,
      r#"{ kind = "capability" }"#,
    ),
    (
      "provider",
      r#"{ kind = "any" }"#,
      r#"{ kind = "qualified", namespace = "provider" }"#,
    ),
    (
      "driver",
      r#"{ kind = "any" }"#,
      r#"{ kind = "qualified", namespace = "driver" }"#,
    ),
    ("automatic", r#"{ kind = "any" }"#, r#"{ kind = "capability" }"#),
    (
      "family",
      r#"{ kind = "fixed", provider = "local" }"#,
      r#"{ kind = "family", families = { smart = ["unlisted-first", "gpt-4o"] } }"#,
    ),
  ] {
    config.push_str(&format!(
      r#"
[profiles.{name}]
route = "{name}"
account_pool = {{ accounts = ["acct"] }}

[routes.{name}]
kind = "managed"
providers = ["local"]
provider = {provider}
model = {model}
operation = "preserve"
"#
    ));
  }
  let plan = tokn_config::v2::parse(&config, Path::new("unlisted-model-test.toml")).unwrap();
  let access = Arc::new(AccessStore::disabled());
  let allowed = access.create_key("local", vec!["local".into()]).unwrap();
  let denied = access.create_key("other provider", vec!["openai".into()]).unwrap();
  let states = tokn_router::v2::build_states(plan, &[account()], access, Arc::new(EventBus::noop())).unwrap();
  let app = tokn_router::v2::router(states.into_iter().next().unwrap());

  for (profile, model, upstream_model) in [
    ("fixed", "organization/custom-model", "organization/custom-model"),
    (
      "provider",
      "local/organization/custom-model",
      "organization/custom-model",
    ),
    (
      "driver",
      "openai/organization/custom-model",
      "organization/custom-model",
    ),
    ("automatic", "organization/custom-model", "organization/custom-model"),
    ("family", "organization/custom-model", "organization/custom-model"),
    ("family", "smart", "gpt-4o"),
  ] {
    let response = app
      .clone()
      .oneshot(
        Request::post(format!("/{profile}/v1/chat/completions"))
          .header("authorization", format!("Bearer {}", allowed.token))
          .header("content-type", "application/json")
          .body(Body::from(
            serde_json::json!({"model": model, "messages": []}).to_string(),
          ))
          .unwrap(),
      )
      .await
      .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status, StatusCode::OK, "{profile}: {}", String::from_utf8_lossy(&body));
    assert_eq!(capture_rx.recv().await.unwrap()["model"], upstream_model);
  }

  for (profile, model, token, expected) in [
    (
      "provider",
      "other/organization/custom-model",
      &allowed.token,
      StatusCode::NOT_IMPLEMENTED,
    ),
    (
      "driver",
      "deepseek/organization/custom-model",
      &allowed.token,
      StatusCode::NOT_IMPLEMENTED,
    ),
    (
      "fixed",
      "organization/custom-model",
      &denied.token,
      StatusCode::FORBIDDEN,
    ),
    (
      "provider",
      "local/organization/custom-model",
      &denied.token,
      StatusCode::FORBIDDEN,
    ),
    (
      "driver",
      "openai/organization/custom-model",
      &denied.token,
      StatusCode::FORBIDDEN,
    ),
  ] {
    let response = app
      .clone()
      .oneshot(
        Request::post(format!("/{profile}/v1/chat/completions"))
          .header("authorization", format!("Bearer {token}"))
          .header("content-type", "application/json")
          .body(Body::from(
            serde_json::json!({"model": model, "messages": []}).to_string(),
          ))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), expected, "{profile}: {model}");
  }
  assert!(capture_rx.try_recv().is_err());
  server.abort();
}

#[tokio::test]
async fn fixed_provider_client_relay_preserves_client_credentials_without_accounts() {
  let (capture_tx, mut capture_rx) = tokio::sync::mpsc::channel(1);
  let upstream = Router::new()
    .route(
      "/{*path}",
      any(
        |State(capture_tx): State<tokio::sync::mpsc::Sender<CapturedRequest>>,
         uri: Uri,
         headers: HeaderMap,
         body: Bytes| async move {
          capture_tx.send(CapturedRequest { uri, headers, body }).await.unwrap();
          Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(r#"{"client_relay":"unchanged"}"#))
            .unwrap()
        },
      ),
    )
    .with_state(capture_tx);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = listener.local_addr().unwrap();
  let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "local_keys"

[profiles.client-relay]
route = "client-relay"

[routes.client-relay]
kind = "relay"
destination = {{ kind = "fixed_provider", provider = "local" }}
credentials = {{ kind = "client" }}

[providers.local]
driver = "openai"
base_url = "http://{upstream_addr}/v1"
"#
  );
  let plan = tokn_config::v2::parse(&config, Path::new("v2-client-relay.toml")).unwrap();
  let events = Arc::new(EventBus::new(64));
  let mut event_rx = events.subscribe();
  let states = tokn_router::v2::build_states(plan, &[], Arc::new(AccessStore::disabled()), events).unwrap();
  let local_addr = "127.0.0.1:4141".parse::<std::net::SocketAddr>().unwrap();
  let peer_addr = "127.0.0.1:5151".parse::<std::net::SocketAddr>().unwrap();
  let app = tokn_router::v2::router(states.into_iter().next().unwrap()).layer(Extension(local_addr));
  let body = Bytes::from_static(br#"{"model":"gpt-4o","input":"hello","opaque":true}"#);

  let mut request = Request::post("/client-relay/v1/responses")
    .header("host", "gateway.example")
    .header("x-tokn-router-local-addr", "spoofed.example")
    .header("content-type", "application/json")
    .header("authorization", "Bearer client-secret")
    .header("x-api-key", "client-key")
    .body(Body::from(body.clone()))
    .unwrap();
  request.extensions_mut().insert(ConnectInfo(peer_addr));
  let response = app.oneshot(request).await.unwrap();
  let request_id = response
    .headers()
    .get("x-request-id")
    .expect("v2 response missing request id")
    .to_str()
    .unwrap()
    .to_string();
  let status = response.status();
  let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    status,
    StatusCode::OK,
    "client relay response: {}",
    String::from_utf8_lossy(&response_body)
  );
  assert_eq!(response_body.as_ref(), br#"{"client_relay":"unchanged"}"#);

  let captured = capture_rx.recv().await.unwrap();
  assert_eq!(captured.uri.path(), "/v1/responses");
  assert_eq!(captured.headers["authorization"], "Bearer client-secret");
  assert_eq!(captured.headers["x-api-key"], "client-key");
  assert_ne!(captured.headers["host"], "gateway.example");
  assert_eq!(captured.body, body);

  let inbound = std::iter::from_fn(|| event_rx.try_recv().ok()).find_map(|event| {
    let Event::Requests(request) = &*event else {
      return None;
    };
    match &request.payload {
      RequestEventPayload::Record(RecordEvent::InboundConnection {
        user,
        api_key_id,
        local_addr,
        peer_addr,
        mode,
        method,
        inbound_method,
        url,
      }) => Some((
        request.request_id.clone(),
        user.clone(),
        api_key_id.clone(),
        local_addr.clone(),
        peer_addr.clone(),
        mode.clone(),
        method.clone(),
        inbound_method.clone(),
        url.clone(),
      )),
      _ => None,
    }
  });
  let (event_request_id, user, api_key_id, local_addr, peer_addr, mode, pipeline_id, inbound_method, url) =
    inbound.expect("v2 API request did not emit an inbound connection event");
  assert_eq!(event_request_id.as_str(), request_id);
  assert!(user.is_none());
  assert!(api_key_id.is_none());
  assert_eq!(local_addr.as_deref(), Some("127.0.0.1:4141"));
  assert_eq!(peer_addr.as_deref(), Some("127.0.0.1:5151"));
  assert_eq!(mode.as_str(), "passthrough");
  assert_eq!(pipeline_id.as_str(), "requests");
  assert_eq!(inbound_method.as_str(), "POST");
  assert!(url.is_none());

  server.abort();
}

#[tokio::test]
async fn v2_listener_selects_managed_and_relay_six_stage_pipelines() {
  let (capture_tx, mut capture_rx) = tokio::sync::mpsc::channel(2);
  let upstream = Router::new()
    .route(
      "/{*path}",
      any(
        |State(capture_tx): State<tokio::sync::mpsc::Sender<CapturedRequest>>,
         uri: Uri,
         headers: HeaderMap,
         body: Bytes| async move {
          let is_responses = uri.path().ends_with("/responses");
          capture_tx.send(CapturedRequest { uri, headers, body }).await.unwrap();
          if is_responses {
            Response::builder()
              .header("content-type", "application/json")
              .body(Body::from(r#"{"relay":"unchanged"}"#))
              .unwrap()
          } else {
            Response::builder()
              .header("content-type", "application/json")
              .body(Body::from(
                r#"{"id":"chatcmpl-v2","choices":[{"message":{"role":"assistant","content":"hello"}}]}"#,
              ))
              .unwrap()
          }
        },
      ),
    )
    .with_state(capture_tx);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = listener.local_addr().unwrap();
  let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "local_keys"

[profiles.managed]
route = "managed"
binding = {{ path = "/v1", endpoints = ["chat_completions"] }}

[profiles.managed.account_pool]
accounts = ["acct"]

[profiles.relay]
route = "relay"
binding = {{ endpoints = ["responses"] }}

[profiles.relay.account_pool]
accounts = ["acct"]

[routes.managed]
kind = "managed"
providers = ["local"]
provider = {{ kind = "fixed", provider = "local" }}
model = {{ kind = "family", families = {{ smart = ["not-an-openai-model", "gpt-4o"] }} }}
operation = "preserve"

[routes.relay]
kind = "relay"
providers = ["local"]
destination = {{ kind = "fixed_provider", provider = "local" }}
credentials = {{ kind = "account_pool" }}

[providers.local]
driver = "openai"
base_url = "http://{upstream_addr}/v1"
"#
  );
  let plan = tokn_config::v2::parse(&config, Path::new("v2-test.toml")).unwrap();
  let access = Arc::new(AccessStore::disabled());
  let allowed_key = access.create_key("local provider", vec!["local".into()]).unwrap();
  let driver_only_key = access.create_key("driver only", vec!["openai".into()]).unwrap();
  let states = tokn_router::v2::build_states(plan, &[account()], access, Arc::new(EventBus::noop())).unwrap();
  let app = tokn_router::v2::router(states.into_iter().next().unwrap());

  let managed_body = br#"{"model":"smart","messages":[{"role":"user","content":"hi"}]}"#;
  let missing_key = app
    .clone()
    .oneshot(
      Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(managed_body.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(missing_key.status(), StatusCode::UNAUTHORIZED);

  let rejected = app
    .clone()
    .oneshot(
      Request::post("/v1/messages")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", allowed_key.token))
        .body(Body::from(br#"{"model":"gpt-4o","messages":[]}"#.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(rejected.status(), StatusCode::NOT_FOUND);

  let denied = app
    .clone()
    .oneshot(
      Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", driver_only_key.token))
        .body(Body::from(managed_body.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(denied.status(), StatusCode::FORBIDDEN);

  let managed = app
    .clone()
    .oneshot(
      Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", allowed_key.token))
        .body(Body::from(managed_body.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  let managed_request_id = managed
    .headers()
    .get("x-request-id")
    .expect("managed response missing generated request id")
    .to_str()
    .unwrap()
    .to_string();
  let managed_uuid = managed_request_id
    .strip_prefix("req-")
    .expect("managed request id missing req- prefix");
  assert!(uuid::Uuid::parse_str(managed_uuid).is_ok());
  let managed_status = managed.status();
  let managed_response = to_bytes(managed.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    managed_status,
    StatusCode::OK,
    "managed response: {}",
    String::from_utf8_lossy(&managed_response)
  );
  assert_eq!(
    serde_json::from_slice::<serde_json::Value>(&managed_response).unwrap()["id"],
    "chatcmpl-v2"
  );

  let captured_managed = capture_rx.recv().await.unwrap();
  assert_eq!(captured_managed.uri.path(), "/v1/chat/completions");
  assert_eq!(captured_managed.headers["authorization"], "Bearer sk-v2-test");
  assert_eq!(captured_managed.headers["x-request-id"], managed_request_id);
  assert_eq!(
    serde_json::from_slice::<serde_json::Value>(&captured_managed.body).unwrap()["model"],
    "gpt-4o"
  );

  let relay_body = Bytes::from_static(br#"{"input":"hi","model":"gpt-4o","unusual_order":true}"#);
  let relay = app
    .oneshot(
      Request::post("/relay/v1/responses")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", allowed_key.token))
        .header("x-request-id", "client-v2-request")
        .body(Body::from(relay_body.clone()))
        .unwrap(),
    )
    .await
    .unwrap();
  assert_eq!(relay.headers()["x-request-id"], "client-v2-request");
  let relay_status = relay.status();
  let relay_response = to_bytes(relay.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    relay_status,
    StatusCode::OK,
    "relay response: {}",
    String::from_utf8_lossy(&relay_response)
  );
  assert_eq!(relay_response.as_ref(), br#"{"relay":"unchanged"}"#);

  let captured_relay = capture_rx.recv().await.unwrap();
  assert_eq!(captured_relay.uri.path(), "/v1/responses");
  assert_eq!(captured_relay.headers["authorization"], "Bearer sk-v2-test");
  assert_eq!(captured_relay.headers["x-request-id"], "client-v2-request");
  assert_eq!(captured_relay.body, relay_body);

  server.abort();
}

#[tokio::test]
async fn managed_retry_reselects_after_a_recoverable_account_failure() {
  let (capture_tx, mut capture_rx) = tokio::sync::mpsc::channel(2);
  let upstream = Router::new()
    .route(
      "/{*path}",
      any(
        |State(capture_tx): State<tokio::sync::mpsc::Sender<String>>, headers: HeaderMap| async move {
          let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
          capture_tx.send(authorization.clone()).await.unwrap();
          if authorization == "Bearer sk-primary" {
            Response::builder()
              .status(StatusCode::SERVICE_UNAVAILABLE)
              .header("content-type", "application/json")
              .body(Body::from(r#"{"error":"try another account"}"#))
              .unwrap()
          } else {
            Response::builder()
              .header("content-type", "application/json")
              .body(Body::from(
                r#"{"id":"chatcmpl-failover","choices":[{"message":{"role":"assistant","content":"ok"}}]}"#,
              ))
              .unwrap()
          }
        },
      ),
    )
    .with_state(capture_tx);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let upstream_addr = listener.local_addr().unwrap();
  let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"

[profiles.managed]
route = "managed"
binding = {{ path = "/v1" }}

[profiles.managed.account_pool]
accounts = ["a-primary", "b-secondary"]
failure_cooldown_secs = 60

[routes.managed]
kind = "managed"
providers = ["local"]
provider = {{ kind = "fixed", provider = "local" }}
model = {{ kind = "capability" }}
operation = "preserve"
retry = {{ kind = "recoverable", policy = "failover" }}

[retry_policies.failover]
max_retries = 1
initial_backoff_ms = 0

[providers.local]
driver = "openai"
base_url = "http://{upstream_addr}/v1"
"#
  );
  let plan = tokn_config::v2::parse(&config, Path::new("v2-retry.toml")).unwrap();
  let accounts = [
    account_with_credentials("a-primary", "sk-primary"),
    account_with_credentials("b-secondary", "sk-secondary"),
  ];
  let states = tokn_router::v2::build_states(
    plan,
    &accounts,
    Arc::new(AccessStore::disabled()),
    Arc::new(EventBus::noop()),
  )
  .unwrap();
  let app = tokn_router::v2::router(states.into_iter().next().unwrap());

  let response = app
    .oneshot(
      Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(br#"{"model":"gpt-4o","messages":[]}"#.as_slice()))
        .unwrap(),
    )
    .await
    .unwrap();
  let status = response.status();
  let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
  assert_eq!(
    status,
    StatusCode::OK,
    "retry response: {}",
    String::from_utf8_lossy(&body)
  );
  assert_eq!(
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"],
    "chatcmpl-failover"
  );
  assert_eq!(capture_rx.recv().await.as_deref(), Some("Bearer sk-primary"));
  assert_eq!(capture_rx.recv().await.as_deref(), Some("Bearer sk-secondary"));

  server.abort();
}

#[derive(Clone)]
struct ControlledUpstreamState {
  label: &'static str,
  arrived: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
  release: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
}

struct ControlledUpstream {
  addr: std::net::SocketAddr,
  arrived: tokio::sync::oneshot::Receiver<()>,
  release: tokio::sync::oneshot::Sender<()>,
  server: tokio::task::JoinHandle<()>,
}

async fn controlled_upstream(label: &'static str) -> ControlledUpstream {
  let (arrived_tx, arrived) = tokio::sync::oneshot::channel();
  let (release, release_rx) = tokio::sync::oneshot::channel();
  let state = ControlledUpstreamState {
    label,
    arrived: Arc::new(tokio::sync::Mutex::new(Some(arrived_tx))),
    release: Arc::new(tokio::sync::Mutex::new(Some(release_rx))),
  };
  let upstream = Router::new()
    .route(
      "/{*path}",
      any(|State(state): State<ControlledUpstreamState>| async move {
        if let Some(arrived) = state.arrived.lock().await.take() {
          let _ = arrived.send(());
        }
        let release = state.release.lock().await.take();
        if let Some(release) = release {
          let _ = release.await;
        }
        Response::builder()
          .header("content-type", "application/json")
          .body(Body::from(
            serde_json::json!({
              "id": format!("chatcmpl-{}", state.label),
              "choices": [{"message": {"role": "assistant", "content": state.label}}],
            })
            .to_string(),
          ))
          .unwrap()
      }),
    )
    .with_state(state);
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
  ControlledUpstream {
    addr,
    arrived,
    release,
    server,
  }
}

fn reload_runtime_states(upstream: std::net::SocketAddr) -> tokn_router::v2::RuntimeStates {
  let config = format!(
    r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"

[profiles.managed]
route = "managed"
binding = {{ path = "/v1" }}

[profiles.managed.account_pool]
accounts = ["acct"]

[routes.managed]
kind = "managed"
providers = ["local"]
provider = {{ kind = "fixed", provider = "local" }}
model = {{ kind = "capability" }}
operation = "preserve"

[providers.local]
driver = "openai"
base_url = "http://{upstream}/v1"
"#
  );
  let plan = tokn_config::v2::parse(&config, Path::new("v2-reload.toml")).unwrap();
  tokn_router::v2::build_runtime_states(
    plan,
    &[account()],
    Arc::new(AccessStore::disabled()),
    Arc::new(EventBus::noop()),
  )
  .unwrap()
}

fn managed_chat_request() -> Request<Body> {
  Request::post("/v1/chat/completions")
    .header("content-type", "application/json")
    .body(Body::from(
      br#"{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}"#.as_slice(),
    ))
    .unwrap()
}

#[tokio::test]
async fn live_reload_pins_in_flight_api_requests_and_updates_new_requests() {
  let old = controlled_upstream("old").await;
  let new = controlled_upstream("new").await;
  let live = tokn_router::v2::LiveRuntime::new(reload_runtime_states(old.addr), 1);
  let app = tokn_router::v2::router_live(live.llm_api_listeners().pop().unwrap());

  let old_request = tokio::spawn({
    let app = app.clone();
    async move { app.oneshot(managed_chat_request()).await.unwrap() }
  });
  tokio::time::timeout(std::time::Duration::from_secs(2), old.arrived)
    .await
    .expect("old request should reach its upstream before reload")
    .unwrap();

  live.replace(reload_runtime_states(new.addr), 1).unwrap();
  let new_request = tokio::spawn({
    let app = app.clone();
    async move { app.oneshot(managed_chat_request()).await.unwrap() }
  });
  tokio::time::timeout(std::time::Duration::from_secs(2), new.arrived)
    .await
    .expect("new request should reach the reloaded upstream")
    .unwrap();

  new.release.send(()).unwrap();
  let response = new_request.await.unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  let body: serde_json::Value =
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
  assert_eq!(body["id"], "chatcmpl-new");

  old.release.send(()).unwrap();
  let response = old_request.await.unwrap();
  assert_eq!(response.status(), StatusCode::OK);
  let body: serde_json::Value =
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
  assert_eq!(body["id"], "chatcmpl-old");

  old.server.abort();
  new.server.abort();
}

fn account() -> AccountConfig {
  account_with_credentials("acct", "sk-v2-test")
}

fn account_with_credentials(id: &str, api_key: &str) -> AccountConfig {
  AccountConfig {
    id: id.into(),
    provider: "local".into(),
    enabled: true,
    tier: AccountTier::Active,
    tags: Vec::new(),
    label: None,
    base_url: None,
    headers: Default::default(),
    auth_type: Some(AuthType::Bearer),
    username: None,
    api_key: Some(Secret::new(api_key.into())),
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
