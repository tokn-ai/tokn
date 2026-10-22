//! Compare routing through the real v2 gateway and injected execution.
//!
//! Both paths compile the same configuration and receive the same account and
//! model inventory. Only HTTP delivery and the clock differ. Assertions inspect
//! upstream destinations, credentials, model names, and native operations.

use axum::{
  body::{to_bytes, Body, Bytes},
  extract::State,
  http::{HeaderMap, Request as HttpRequest, StatusCode, Uri},
  routing::any,
  Json, Router,
};
use serde_json::{json, Value};
use std::{
  cell::RefCell,
  collections::{BTreeMap, BTreeSet, VecDeque},
  convert::Infallible,
  path::Path,
  rc::Rc,
  sync::{Arc, Mutex},
  time::Duration,
};
use tokn_access::AccessStore;
use tokn_config::v2::{compile, compile_execution, RawConfig};
use tokn_core::{account::AccountConfig, event::EventBus, types::SmolStr, Endpoint};
use tokn_policy::ProviderId;
use tokn_requests::configured::{Injected, RequestOptions, Runtime};
use tokn_service::transport::{Request, ResponseHead, Transport};
use tower::ServiceExt;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Observation {
  provider: String,
  account: String,
  operation: String,
  model: String,
}

impl Observation {
  fn new(provider: &str, account: &str, operation: &str, model: &str) -> Self {
    Self {
      provider: provider.into(),
      account: account.into(),
      operation: operation.into(),
      model: model.into(),
    }
  }

  fn capture(path: &str, authorization: &str, body: &[u8]) -> Self {
    let (provider, operation) = path.trim_start_matches('/').split_once("/v1/").unwrap();
    let body: Value = serde_json::from_slice(body).unwrap();
    Self::new(
      provider,
      authorization.strip_prefix("Bearer key-").unwrap(),
      operation,
      body["model"].as_str().unwrap(),
    )
  }
}

struct Capture {
  requests: Vec<Observation>,
  statuses: VecDeque<u16>,
}

impl Capture {
  fn new(statuses: &[u16]) -> Self {
    Self {
      requests: Vec::new(),
      statuses: statuses.iter().copied().collect(),
    }
  }

  fn record(&mut self, request: Observation) -> u16 {
    self.requests.push(request);
    self.statuses.pop_front().unwrap_or(200)
  }
}

fn reply(status: u16) -> Value {
  if status != 200 {
    return json!({"error": {"message": "temporarily unavailable"}});
  }
  json!({
    "id": "chat-routing-parity",
    "object": "chat.completion",
    "model": "gpt-4o",
    "choices": [{
      "index": 0,
      "message": {"role": "assistant", "content": "ok"},
      "finish_reason": "stop"
    }],
    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
  })
}

async fn upstream(
  State(capture): State<Arc<Mutex<Capture>>>,
  uri: Uri,
  headers: HeaderMap,
  body: Bytes,
) -> (StatusCode, Json<Value>) {
  let request = Observation::capture(uri.path(), headers["authorization"].to_str().unwrap(), &body);
  let status = capture.lock().unwrap().record(request);
  (StatusCode::from_u16(status).unwrap(), Json(reply(status)))
}

struct CaptureTransport {
  capture: Rc<RefCell<Capture>>,
  reply: Vec<u8>,
  position: usize,
}

impl Transport for CaptureTransport {
  type Error = Infallible;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    let url = reqwest::Url::parse(&request.url).unwrap();
    let request = Observation::capture(
      url.path(),
      request.headers.get("authorization").unwrap().as_str(),
      &request.body,
    );
    let status = self.capture.borrow_mut().record(request);
    self.reply = serde_json::to_vec(&reply(status)).unwrap();
    self.position = 0;
    let mut headers = tokn_core::headers::HeaderMap::new();
    headers.insert("content-type", "application/json");
    Ok(ResponseHead { status, headers })
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    let count = buffer.len().min(self.reply.len() - self.position);
    buffer[..count].copy_from_slice(&self.reply[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }
}

#[derive(Default)]
struct TestRuntime(Duration);

impl Runtime for TestRuntime {
  fn now(&self) -> Duration {
    self.0
  }

  fn unix_seconds(&self) -> u64 {
    1_700_000_000
  }

  async fn sleep(&mut self, duration: Duration) {
    self.0 += duration;
  }
}

struct Fixture {
  app: Router,
  injected: Injected<CaptureTransport, TestRuntime>,
  standard_capture: Arc<Mutex<Capture>>,
  injected_capture: Rc<RefCell<Capture>>,
  upstream: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
  fn drop(&mut self) {
    self.upstream.abort();
  }
}

impl Fixture {
  async fn new(mut config: Value, accounts: Vec<AccountConfig>, statuses: &[u16]) -> Self {
    let standard_capture = Arc::new(Mutex::new(Capture::new(statuses)));
    let upstream_app = Router::new()
      .route("/{*path}", any(upstream))
      .with_state(standard_capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = tokio::spawn(async move { axum::serve(listener, upstream_app).await.unwrap() });
    for provider in ["alpha", "beta"] {
      config["providers"][provider] = json!({
        "driver": "openai",
        "base_url": format!("http://{address}/{provider}/v1")
      });
    }
    let raw: RawConfig = serde_json::from_value(config).unwrap();
    let gateway = compile(&raw, Path::new("routing-parity.toml")).unwrap();
    let execution = compile_execution(&raw).unwrap();
    assert_eq!(gateway.execution(), &execution);
    let state = tokn_router::v2::build_states(
      gateway,
      &accounts,
      Arc::new(AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap()
    .pop()
    .unwrap();
    // Standard OpenAI drivers initialize discovery from this same catalogue.
    // No model refresh task runs, so the two inventory snapshots stay equal.
    let models: BTreeSet<SmolStr> = tokn_catalogue::default_models_for("openai")
      .into_iter()
      .map(|model| SmolStr::new(model.id))
      .collect();
    assert!(models.contains("gpt-4o"));
    let models = BTreeMap::from([
      (ProviderId::new("alpha").unwrap(), models.clone()),
      (ProviderId::new("beta").unwrap(), models),
    ]);
    let injected_capture = Rc::new(RefCell::new(Capture::new(statuses)));
    let injected = Injected::with_models(
      execution,
      accounts,
      models,
      CaptureTransport {
        capture: injected_capture.clone(),
        reply: Vec::new(),
        position: 0,
      },
      TestRuntime::default(),
    )
    .unwrap();
    Self {
      app: tokn_router::v2::router(state),
      injected,
      standard_capture,
      injected_capture,
      upstream,
    }
  }

  async fn assert_request(
    &self,
    profile: &str,
    model: &str,
    endpoint: Endpoint,
    session: Option<&str>,
    expected: &[Observation],
  ) {
    let before = self.standard_capture.lock().unwrap().requests.len();
    assert_eq!(before, self.injected_capture.borrow().requests.len());
    let body = json!({
      "model": model,
      "messages": [{"role": "user", "content": "hello"}],
      "max_tokens": 16
    });
    let operation = match endpoint {
      Endpoint::ChatCompletions => "chat/completions",
      Endpoint::Messages => "messages",
      Endpoint::Responses => panic!("fixture sends Chat or Messages input"),
    };
    let mut request =
      HttpRequest::post(format!("/{profile}/v1/{operation}")).header("content-type", "application/json");
    let mut options = RequestOptions::default().with_profile(profile);
    if let Some(session) = session {
      request = request.header("x-session-id", session);
      options = options.with_session_id(session);
    }
    let standard = self
      .app
      .clone()
      .oneshot(request.body(Body::from(body.to_string())).unwrap())
      .await
      .unwrap();
    let standard_status = standard.status();
    let standard_body = to_bytes(standard.into_body(), usize::MAX).await.unwrap();
    let injected = self.injected.execute(endpoint, body, options).await;
    if expected.is_empty() {
      assert!(!standard_status.is_success(), "unsupported route must not send");
      let error = injected.err().expect("injected route must also reject before sending");
      assert_eq!(error.stage, "resolve");
    } else {
      assert_eq!(
        standard_status,
        StatusCode::OK,
        "gateway: {}",
        String::from_utf8_lossy(&standard_body)
      );
      let injected = injected.expect("injected route must succeed");
      assert_eq!(injected.status, 200);
      injected.into_json::<Value>().unwrap();
    }
    let standard = self.standard_capture.lock().unwrap();
    let injected = self.injected_capture.borrow();
    assert_eq!(
      &standard.requests[before..],
      expected,
      "gateway route {profile}: {model}"
    );
    assert_eq!(
      &injected.requests[before..],
      expected,
      "injected route {profile}: {model}"
    );
  }
}

fn account(id: &str, provider: &str, tier: &str) -> AccountConfig {
  serde_json::from_value(json!({
    "id": id,
    "provider": provider,
    "api_key": format!("key-{id}"),
    "tier": tier
  }))
  .unwrap()
}

fn config() -> Value {
  json!({
    "schema_version": 2,
    "listeners": {"api": {"kind": "llm_api", "bind": "127.0.0.1:4141", "client_auth": "none"}},
    "providers": {},
    "profiles": {},
    "routes": {}
  })
}

fn add_route(config: &mut Value, name: &str, model: Value, operation: &str) {
  config["profiles"][name] = json!({
    "route": name,
    "wire_identity": "none",
    "account_pool": {"failure_cooldown_secs": 3600, "session_ttl_secs": 3600}
  });
  config["routes"][name] = json!({
    "kind": "managed",
    "providers": ["alpha", "beta"],
    "provider": {"kind": "any"},
    "model": model,
    "operation": operation
  });
}

#[tokio::test]
async fn model_selection_scores_and_native_operation_fallback_match() {
  let mut config = config();
  for (name, model, operation) in [
    ("capability", json!({"kind": "capability"}), "translate_compatible"),
    (
      "qualified",
      json!({"kind": "qualified", "namespace": "provider"}),
      "preserve",
    ),
    (
      "driver",
      json!({"kind": "qualified", "namespace": "driver"}),
      "preserve",
    ),
    (
      "family",
      json!({"kind": "family", "families": {"smart": ["__not_in_catalogue__", "gpt-4o"]}}),
      "translate_compatible",
    ),
    ("preserve", json!({"kind": "capability"}), "preserve"),
    ("filtered", json!({"kind": "capability"}), "preserve"),
  ] {
    add_route(&mut config, name, model, operation);
  }
  config["model_scores"] = json!({"gpt-4o": {"beta": 10}});
  config["profiles"]["filtered"]["account_pool"]["accounts"] = json!(["alpha-1"]);
  let mut disabled = account("disabled", "alpha", "active");
  disabled.enabled = false;
  let fixture = Fixture::new(
    config,
    vec![
      disabled,
      account("alpha-1", "alpha", "active"),
      account("beta-1", "beta", "active"),
    ],
    &[],
  )
  .await;
  for (profile, model, provider, account, upstream_model, endpoint) in [
    (
      "capability",
      "gpt-4o",
      "beta",
      "beta-1",
      "gpt-4o",
      Endpoint::ChatCompletions,
    ),
    (
      "qualified",
      "alpha/gpt-4o",
      "alpha",
      "alpha-1",
      "gpt-4o",
      Endpoint::ChatCompletions,
    ),
    (
      "driver",
      "openai/gpt-4o",
      "alpha",
      "alpha-1",
      "gpt-4o",
      Endpoint::ChatCompletions,
    ),
    ("family", "smart", "beta", "beta-1", "gpt-4o", Endpoint::ChatCompletions),
    (
      "filtered",
      "gpt-4o",
      "alpha",
      "alpha-1",
      "gpt-4o",
      Endpoint::ChatCompletions,
    ),
    (
      "capability",
      "future-model",
      "beta",
      "beta-1",
      "future-model",
      Endpoint::ChatCompletions,
    ),
    ("capability", "gpt-4o", "beta", "beta-1", "gpt-4o", Endpoint::Messages),
  ] {
    fixture
      .assert_request(
        profile,
        model,
        endpoint,
        None,
        &[Observation::new(provider, account, "chat/completions", upstream_model)],
      )
      .await;
  }
  fixture
    .assert_request("preserve", "gpt-4o", Endpoint::Messages, None, &[])
    .await;
}

#[tokio::test]
async fn round_robin_and_session_affinity_match_across_provider_destinations() {
  let mut config = config();
  add_route(&mut config, "balanced", json!({"kind": "capability"}), "preserve");
  let fixture = Fixture::new(
    config,
    vec![
      account("alpha-1", "alpha", "active"),
      account("beta-1", "beta", "active"),
    ],
    &[],
  )
  .await;
  for (session, provider, account) in [
    (Some("first"), "alpha", "alpha-1"),
    (Some("second"), "beta", "beta-1"),
    (Some("first"), "alpha", "alpha-1"),
    (None, "alpha", "alpha-1"),
    (Some("second"), "beta", "beta-1"),
    (None, "beta", "beta-1"),
  ] {
    fixture
      .assert_request(
        "balanced",
        "gpt-4o",
        Endpoint::ChatCompletions,
        session,
        &[Observation::new(provider, account, "chat/completions", "gpt-4o")],
      )
      .await;
  }
}

#[tokio::test]
async fn fixed_provider_account_relay_keeps_unknown_model_and_account_filter() {
  let mut config = config();
  config["profiles"]["relay"] = json!({
    "route": "relay",
    "wire_identity": "none",
    "account_pool": {"accounts": ["beta-2"]}
  });
  config["routes"]["relay"] = json!({
    "kind": "relay",
    "destination": {"kind": "fixed_provider", "provider": "beta"},
    "credentials": {"kind": "account_pool"}
  });
  let fixture = Fixture::new(
    config,
    vec![
      account("alpha-1", "alpha", "active"),
      account("beta-1", "beta", "active"),
      account("beta-2", "beta", "active"),
    ],
    &[],
  )
  .await;
  fixture
    .assert_request(
      "relay",
      "organization/custom-model",
      Endpoint::ChatCompletions,
      None,
      &[Observation::new(
        "beta",
        "beta-2",
        "chat/completions",
        "organization/custom-model",
      )],
    )
    .await;
}

#[tokio::test]
async fn recoverable_failures_cool_accounts_and_activate_fallback_identically() {
  let mut config = config();
  add_route(&mut config, "retrying", json!({"kind": "capability"}), "preserve");
  config["retry_policies"] = json!({"once": {"max_retries": 1, "initial_backoff_ms": 1}});
  config["routes"]["retrying"]["retry"] = json!({"kind": "recoverable", "policy": "once"});
  let fixture = Fixture::new(
    config,
    vec![
      account("alpha-1", "alpha", "active"),
      account("alpha-2", "alpha", "active"),
      account("beta-fallback", "beta", "fallback"),
    ],
    &[503, 200, 503, 200, 200],
  )
  .await;
  for expected in [
    vec![
      Observation::new("alpha", "alpha-1", "chat/completions", "gpt-4o"),
      Observation::new("alpha", "alpha-2", "chat/completions", "gpt-4o"),
    ],
    vec![
      Observation::new("alpha", "alpha-2", "chat/completions", "gpt-4o"),
      Observation::new("beta", "beta-fallback", "chat/completions", "gpt-4o"),
    ],
    vec![Observation::new("beta", "beta-fallback", "chat/completions", "gpt-4o")],
  ] {
    fixture
      .assert_request(
        "retrying",
        "gpt-4o",
        Endpoint::ChatCompletions,
        Some("session"),
        &expected,
      )
      .await;
  }
}
