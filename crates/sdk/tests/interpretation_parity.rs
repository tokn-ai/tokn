//! Exercise request interpretation through both public SDK execution paths.
//!
//! The standard client sends HTTP to the fixture server; the injected client
//! receives the same upstream reply through a local transport. Assertions use
//! externally visible response modes, provider headers, and account affinity.

#![cfg(feature = "std")]

use serde_json::{json, Value};
use std::{cell::RefCell, convert::Infallible, fs, rc::Rc, time::Duration};
use tempfile::TempDir;
use tokn_mock_server::{MockLlmConfig, MockLlmServer, MockResponse, MockRoute};
use tokn_requests::configured::ResponseBody as InjectedBody;
use tokn_sdk::{
  account::AccountConfig,
  transport::{Request, ResponseHead, Transport},
  Client, Endpoint, Injected, RequestOptions, ResponseBody, Runtime,
};

struct CaptureTransport {
  requests: Rc<RefCell<Vec<Request>>>,
  reply: MockResponse,
  position: usize,
}

impl Transport for CaptureTransport {
  type Error = Infallible;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    self.requests.borrow_mut().push(request);
    self.position = 0;
    let mut headers = tokn_headers::HeaderMap::new();
    for (name, value) in &self.reply.headers {
      headers.insert(tokn_headers::HeaderName::new(name.as_str()), value.clone());
    }
    Ok(ResponseHead {
      status: self.reply.status.as_u16(),
      headers,
    })
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    let count = buffer.len().min(self.reply.body.len() - self.position);
    buffer[..count].copy_from_slice(&self.reply.body[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }
}

struct TestRuntime;

impl Runtime for TestRuntime {
  fn now(&self) -> Duration {
    Duration::ZERO
  }

  fn unix_seconds(&self) -> u64 {
    1_700_000_000
  }

  async fn sleep(&mut self, _: Duration) {
    panic!("interpretation fixtures must not retry");
  }
}

struct Fixture {
  _root: TempDir,
  server: MockLlmServer,
  standard: Client,
  injected: Client<Injected<CaptureTransport, TestRuntime>>,
  requests: Rc<RefCell<Vec<Request>>>,
  provider: &'static str,
}

#[derive(Debug)]
struct Observation {
  authorization: String,
  initiator: Option<String>,
  accept: Option<String>,
  body: Value,
  streaming: bool,
}

impl Fixture {
  async fn new(provider: &'static str, account_count: usize, route: MockRoute) -> Self {
    let reply = route.response.clone();
    let server = MockLlmServer::start(MockLlmConfig::default().with_route(route)).await;
    let root = tempfile::tempdir().expect("create parity fixture");
    let config_path = root.path().join("config.toml");
    let auth_path = root.path().join("auth.yaml");
    fs::write(&config_path, format!(
      "schema_version = 2\n[defaults]\nprovider = {{ kind = 'fixed', provider = '{provider}' }}\nmodel = {{ kind = 'qualified', namespace = 'provider' }}\noperation = 'preserve'\nwire_identity = 'none'\n[providers.'{provider}']\ndriver = '{provider}'\nbase_url = '{}'\n", server.base_url()
    )).unwrap();
    let accounts: Vec<Value> = (0..account_count)
      .map(|index| {
        json!({
          "id": format!("account-{index}"),
          "provider": provider,
          "api_key": format!("key-{index}"),
          "access_token": format!("key-{index}"),
          "access_token_expires_at": 4_102_444_800_i64,
          "refresh_token": "unused-refresh-token"
        })
      })
      .collect();
    // JSON is also valid YAML, keeping credentials identical in both fixtures.
    fs::write(
      &auth_path,
      serde_json::to_vec(&json!({"version": 1, "accounts": accounts})).unwrap(),
    )
    .unwrap();
    let standard = Client::builder()
      .config_path(&config_path)
      .auth_path(&auth_path)
      .build()
      .expect("build standard client");
    let raw = serde_json::from_value(json!({
      "schema_version": 2,
      "defaults": {
        "provider": {"kind": "fixed", "provider": provider},
        "model": {"kind": "qualified", "namespace": "provider"},
        "operation": "preserve",
        "wire_identity": "none"
      },
      "providers": {provider: {"driver": provider, "base_url": server.base_url()}}
    }))
    .unwrap();
    let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
    let accounts: Vec<AccountConfig> = accounts
      .into_iter()
      .map(|account| serde_json::from_value(account).unwrap())
      .collect();
    let requests = Rc::new(RefCell::new(Vec::new()));
    let injected = Client::from_platform(
      Injected::new(
        plan,
        accounts,
        CaptureTransport {
          requests: requests.clone(),
          reply,
          position: 0,
        },
        TestRuntime,
      )
      .unwrap(),
    );
    Self {
      _root: root,
      server,
      standard,
      injected,
      requests,
      provider,
    }
  }

  fn chat(&self) -> Value {
    json!({"model": format!("{}/gpt-4o", self.provider), "messages": [{"role": "user", "content": "hello"}]})
  }

  async fn execute(&self, body: Value, options: RequestOptions) -> (Observation, Observation) {
    let standard = self
      .standard
      .execute(Endpoint::ChatCompletions, body.clone(), options.clone())
      .await
      .expect("execute standard request");
    assert_eq!(standard.status, 200);
    let standard_streaming = matches!(standard.body, ResponseBody::Stream(_));
    if standard_streaming {
      standard
        .into_stream()
        .unwrap()
        .bytes()
        .await
        .expect("consume standard SSE");
    }
    let injected = self
      .injected
      .execute(Endpoint::ChatCompletions, body, options)
      .await
      .expect("execute injected request");
    assert_eq!(injected.status, 200);
    let injected_streaming = matches!(injected.body, InjectedBody::Stream(_));
    if let InjectedBody::Stream(mut stream) = injected.body {
      while stream.next_event().await.expect("consume injected SSE").is_some() {}
    }
    let standard = self.server.last_request().expect("standard upstream request");
    let requests = self.requests.borrow();
    let injected = requests.last().expect("injected upstream request");
    (
      Observation {
        authorization: standard.header("authorization").unwrap().to_string(),
        initiator: standard.header("x-initiator").map(str::to_string),
        accept: standard.header("accept").map(str::to_string),
        body: serde_json::from_slice(&standard.body).unwrap(),
        streaming: standard_streaming,
      },
      Observation {
        authorization: injected.headers.get("authorization").unwrap().as_str().to_string(),
        initiator: injected
          .headers
          .get("x-initiator")
          .map(|value| value.as_str().to_string()),
        accept: injected.headers.get("accept").map(|value| value.as_str().to_string()),
        body: serde_json::from_slice(&injected.body).unwrap(),
        streaming: injected_streaming,
      },
    )
  }
}

#[tokio::test]
async fn accept_stream_fallback_and_body_boolean_precedence_match() {
  let fixture = Fixture::new("openai", 1, MockRoute::chat_completions_stream()).await;
  for (stream, accept, expected) in [
    (None, "application/json, text/event-stream; charset=utf-8", true),
    (Some(json!(false)), "text/event-stream", false),
    (Some(json!(true)), "application/json", true),
    (Some(json!("invalid")), "text/event-stream", true),
    (None, "application/json", false),
  ] {
    let mut body = fixture.chat();
    if let Some(stream) = stream {
      body["stream"] = stream;
    }
    let (standard, injected) = fixture
      .execute(body, RequestOptions::default().with_header("Accept", accept))
      .await;
    assert_eq!(standard.streaming, expected, "standard: {standard:?}");
    assert_eq!(injected.streaming, expected, "injected: {injected:?}");
    assert_eq!(standard.accept, injected.accept);
    assert_eq!(standard.body, injected.body);
  }
}

#[tokio::test]
async fn initiator_body_inference_and_option_header_precedence_match() {
  let fixture = Fixture::new("github-copilot", 1, MockRoute::chat_completions()).await;
  for (role, options, expected) in [
    ("user", RequestOptions::default(), "user"),
    ("assistant", RequestOptions::default(), "agent"),
    ("user", RequestOptions::default().with_initiator("agent"), "agent"),
    (
      "user",
      RequestOptions::default().with_header("X-Initiator", "  AgEnT  "),
      "agent",
    ),
    (
      "user",
      RequestOptions::default()
        .with_initiator("user")
        .with_header("x-initiator", "agent"),
      "agent",
    ),
    (
      "assistant",
      RequestOptions::default()
        .with_initiator("user")
        .with_header("x-initiator", "invalid"),
      "agent",
    ),
    (
      "user",
      RequestOptions::default()
        .with_initiator("agent")
        .with_header("x-initiator", " "),
      "user",
    ),
  ] {
    let mut body = fixture.chat();
    body["messages"][0]["role"] = json!(role);
    let (standard, injected) = fixture.execute(body, options).await;
    assert_eq!(standard.initiator.as_deref(), Some(expected), "standard: {standard:?}");
    assert_eq!(injected.initiator, standard.initiator, "injected: {injected:?}");
  }
}

#[tokio::test]
async fn every_session_alias_and_structured_metadata_preserve_affinity() {
  for header in tokn_headers::inbound::SESSION_ID_HEADERS
    .iter()
    .copied()
    .chain(["x-codex-turn-metadata"])
  {
    let fixture = Fixture::new("openai", 2, MockRoute::chat_completions()).await;
    let value = if header == "x-codex-turn-metadata" {
      r#"{"session_id":"session-a"}"#
    } else {
      " session-a "
    };
    let options = RequestOptions::default().with_header(header, value);
    let (first_standard, first_injected) = fixture.execute(fixture.chat(), options.clone()).await;
    let (second_standard, second_injected) = fixture.execute(fixture.chat(), options).await;
    assert_eq!(
      first_standard.authorization, second_standard.authorization,
      "standard alias {header}"
    );
    assert_eq!(
      first_injected.authorization, second_injected.authorization,
      "injected alias {header}"
    );
    let (other_standard, other_injected) = fixture
      .execute(fixture.chat(), RequestOptions::default().with_session_id("session-b"))
      .await;
    assert_ne!(
      first_standard.authorization, other_standard.authorization,
      "standard must have two eligible accounts"
    );
    assert_ne!(
      first_injected.authorization, other_injected.authorization,
      "injected must have two eligible accounts"
    );
  }
}

#[tokio::test]
async fn session_options_and_explicit_headers_use_the_same_precedence() {
  let fixture = Fixture::new("openai", 2, MockRoute::chat_completions()).await;
  let (option_standard, option_injected) = fixture
    .execute(
      fixture.chat(),
      RequestOptions::default().with_session_id("option-session"),
    )
    .await;
  let (header_standard, header_injected) = fixture
    .execute(
      fixture.chat(),
      RequestOptions::default().with_session_id("header-session"),
    )
    .await;
  assert_ne!(option_standard.authorization, header_standard.authorization);
  assert_ne!(option_injected.authorization, header_injected.authorization);
  for (header, value, expect_header_session) in [
    ("X-Session-Id", "header-session", true),
    ("x-client-session-id", "header-session", false),
    ("x-session-id", "   ", true),
  ] {
    let options = RequestOptions::default()
      .with_session_id("option-session")
      .with_header("session-id", "header-session")
      .with_header(header, value);
    let (standard, injected) = fixture.execute(fixture.chat(), options).await;
    let (expected_standard, expected_injected) = if expect_header_session {
      (&header_standard, &header_injected)
    } else {
      (&option_standard, &option_injected)
    };
    assert_eq!(
      standard.authorization, expected_standard.authorization,
      "standard precedence for {header}={value}"
    );
    assert_eq!(
      injected.authorization, expected_injected.authorization,
      "injected precedence for {header}={value}"
    );
  }
}
