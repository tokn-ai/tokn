use futures_util::TryStreamExt;
use serde_json::json;
use std::fs;
use tempfile::TempDir;
use tokn_mock_server::{MockEndpoint, MockLlmConfig, MockLlmServer, MockResponse, MockRoute};
use tokn_sdk::chat_completions::{ChatRequest, ChatResponse};
use tokn_sdk::{
  Client, Error, Event, GenerateEvent, GenerateRequest, Message, ReasoningEffort, ReasoningSummary,
  RequestEventPayload, RequestOptions, StageEvent, Tool, ToolCall, ToolChoice,
};

struct Fixture {
  _root: TempDir,
  config_path: std::path::PathBuf,
  auth_path: std::path::PathBuf,
}

impl Fixture {
  fn new(base_url: &str) -> Self {
    Self::with_account(base_url, "llama-cpp", "")
  }

  fn openai(base_url: &str) -> Self {
    Self::with_account(base_url, "openai", "    api_key: test-key\n")
  }

  fn with_account(base_url: &str, provider: &str, credentials: &str) -> Self {
    let root = tempfile::tempdir().expect("create SDK fixture directory");
    let config_path = root.path().join("config.toml");
    let auth_path = root.path().join("auth.yaml");
    fs::write(
      &config_path,
      format!(
        "schema_version = 2\n[defaults]\nprovider = {{ kind = 'fixed', provider = '{provider}' }}\nmodel = {{ kind = 'qualified', namespace = 'provider' }}\nwire_identity = 'none'\n[providers.'{provider}']\ndriver = '{provider}'\nbase_url = '{base_url}'\n"
      ),
    )
    .expect("write SDK config");
    fs::write(
      &auth_path,
      format!("version: 1\naccounts:\n  - id: local\n    provider: {provider}\n{credentials}"),
    )
    .expect("write SDK credentials");
    Self {
      _root: root,
      config_path,
      auth_path,
    }
  }

  fn relay(base_url: &str, provider: &str, credentials: &str, credential_kind: &str) -> Self {
    let fixture = Self::with_account(base_url, provider, credentials);
    fs::write(&fixture.config_path, format!(
      "schema_version = 2\n[profiles.default]\nroute = 'relay'\nwire_identity = 'none'\n[routes.relay]\nkind = 'relay'\ndestination = {{ kind = 'fixed_provider', provider = '{provider}' }}\ncredentials = {{ kind = '{credential_kind}' }}\nretry = {{ kind = 'never' }}\n[providers.'{provider}']\ndriver = '{provider}'\nbase_url = '{base_url}'\n"
    )).unwrap();
    fixture
  }

  fn profiles(first_url: &str, second_url: &str) -> Self {
    let fixture = Self::new(first_url);
    fs::write(
      &fixture.config_path,
      format!(
        r#"
schema_version = 2
[profiles.default]
route = "first"
wire_identity = "none"
[profiles.other]
route = "second"
wire_identity = "none"
[routes.first]
kind = "managed"
provider = {{ kind = "fixed", provider = "first" }}
model = {{ kind = "qualified", namespace = "driver" }}
operation = "translate_compatible"
retry = {{ kind = "never" }}
[routes.second]
kind = "managed"
provider = {{ kind = "fixed", provider = "second" }}
model = {{ kind = "qualified", namespace = "driver" }}
operation = "translate_compatible"
retry = {{ kind = "never" }}
[providers.first]
driver = "llama-cpp"
base_url = "{first_url}"
[providers.second]
driver = "llama-cpp"
base_url = "{second_url}"
"#
      ),
    )
    .unwrap();
    fs::write(&fixture.auth_path, "version: 1\naccounts:\n  - id: first-account\n    provider: first\n  - id: second-account\n    provider: second\n").unwrap();
    fixture
  }

  fn client(&self) -> Client {
    Client::builder()
      .config_path(&self.config_path)
      .auth_path(&self.auth_path)
      .build()
      .expect("build SDK client")
  }
}

fn chat_request() -> ChatRequest {
  serde_json::from_value(json!({
    "model": "llama-cpp/mock-model",
    "messages": [{"role": "user", "content": "hello"}]
  }))
  .expect("deserialize chat request fixture")
}

#[tokio::test]
async fn lifecycle_subscription_survives_reload_and_serializes_request_events() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();
  let mut events = client.subscribe_events();
  client.reload().expect("reload SDK client");

  client
    .chat_completions()
    .create_with(
      &chat_request(),
      RequestOptions::default().with_request_id("sdk-lifecycle"),
    )
    .await
    .expect("execute lifecycle request");

  tokio::time::timeout(std::time::Duration::from_secs(5), async {
    loop {
      let event = events.recv().await.expect("receive lifecycle event");
      let Event::Requests(event) = event.as_ref() else {
        continue;
      };
      if event.request_id != "sdk-lifecycle" {
        continue;
      }
      let serialized = serde_json::to_value(event).expect("serialize request event");
      assert_eq!(serialized["request_id"], "sdk-lifecycle");
      if matches!(
        event.payload,
        RequestEventPayload::Stage(StageEvent::Completed { success: true, .. })
      ) {
        break;
      }
    }
  })
  .await
  .expect("lifecycle completion timed out");

  mock.shutdown().await;
}

#[tokio::test]
async fn concurrent_requests_share_one_credential_refresh_and_reuse_the_result() {
  let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
    MockEndpoint::Custom {
      method: "GET".parse().unwrap(),
      path: "/token".into(),
    },
    MockResponse::json(json!({"token": "fresh-api-token", "expires_at": 4_102_444_800_i64})),
  )))
  .await;
  let fixture = Fixture::with_account(
    mock.base_url(),
    "github-copilot",
    &format!(
      "    access_token: expired-api-token\n    access_token_expires_at: 1\n    refresh_token: github-credential\n    refresh_url: {}/token\n",
      mock.base_url()
    ),
  );
  let client = fixture.client();
  let request: ChatRequest = serde_json::from_value(json!({
    "model": "github-copilot/gpt-4o", "messages": [{"role": "user", "content": "hello"}]
  }))
  .unwrap();
  let endpoint = client.chat_completions();
  tokio::time::timeout(std::time::Duration::from_secs(5), async {
    let (first, second) = tokio::join!(endpoint.create(&request), endpoint.create(&request));
    assert_eq!(first.unwrap().data.id.as_deref(), Some("chatcmpl-mock"));
    assert_eq!(second.unwrap().data.id.as_deref(), Some("chatcmpl-mock"));
    endpoint
      .create(&request)
      .await
      .expect("later requests reuse the cached credential");
  })
  .await
  .expect("refresh waiters wake after the credential is published");
  let requests = mock.requests();
  let exchanges: Vec<_> = requests.iter().filter(|request| request.path == "/token").collect();
  assert_eq!(exchanges.len(), 1, "concurrent requests share the credential exchange");
  assert_eq!(exchanges[0].header("authorization"), Some("token github-credential"));
  let calls: Vec<_> = requests
    .iter()
    .filter(|request| request.path == "/chat/completions")
    .collect();
  assert_eq!(calls.len(), 3);
  assert!(calls
    .iter()
    .all(|request| request.header("authorization") == Some("Bearer fresh-api-token")));
}

#[tokio::test]
async fn client_credential_codex_relay_retains_compaction_policy() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::relay(mock.base_url(), "codex", "", "client");
  fs::write(&fixture.auth_path, "version: 1\naccounts: []\n").unwrap();
  let client = fixture.client();
  let response = client
    .execute(
      tokn_sdk::Endpoint::Responses,
      json!({
        "model": "gpt-5",
        "input": [{"type": "compaction_trigger"}],
        "service_tier": "auto",
        "max_output_tokens": 32
      }),
      RequestOptions::default()
        .with_header("authorization", "Bearer caller-token")
        .with_header("x-codex-routing-hint", "model=old;tier=default;region=west"),
    )
    .await
    .expect("client-owned relay credentials still receive shared compaction policy");
  assert_eq!(response.status, 200);
  let request = mock.last_request().unwrap();
  let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
  assert_eq!(body["service_tier"], "priority");
  assert_eq!(
    body["max_output_tokens"], 32,
    "relay must not apply managed native transformation"
  );
  assert_eq!(request.header("authorization"), Some("Bearer caller-token"));
  assert_eq!(
    request.header("x-codex-routing-hint"),
    Some("model=gpt-5;tier=priority;region=west")
  );
}

#[tokio::test]
async fn typed_request_uses_configured_provider_and_returns_typed_response() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let response = client
    .chat_completions()
    .create_with(
      &chat_request(),
      RequestOptions::default()
        .with_request_id("sdk-buffered")
        .with_session_id("session-1")
        .with_header("x-sdk-test", "buffered"),
    )
    .await
    .expect("execute typed SDK request");

  assert_eq!(response.status, 200);
  assert_eq!(response.data.id.as_deref(), Some("chatcmpl-mock"));
  let captured = mock.last_request().expect("upstream request captured");
  assert_eq!(captured.path, "/chat/completions");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["model"], "mock-model");
  assert_eq!(outbound["stream"], false);

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_request_uses_native_responses_provider_when_available() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::openai(mock.base_url());
  let client = fixture.client();

  let response = client
    .generate("openai/gpt-5")
    .prompt("use the native responses endpoint")
    .max_tokens(1024)
    .reasoning_effort(ReasoningEffort::High)
    .reasoning_summary(ReasoningSummary::Auto)
    .tool(
      Tool::function(
        "lookup",
        json!({"type": "object", "properties": {"query": {"type": "string"}}}),
      )
      .description("Look up a value"),
    )
    .tool_choice("lookup")
    .send()
    .await
    .expect("send native Responses request");

  assert_eq!(response.text, "mock response");
  let captured = mock.last_request().expect("upstream request captured");
  assert_eq!(captured.path, "/responses");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["model"], "gpt-5");
  assert_eq!(outbound["input"][0]["role"], "user");
  assert_eq!(outbound["max_output_tokens"], 1024);
  assert_eq!(outbound["reasoning"]["effort"], "high");
  assert_eq!(outbound["reasoning"]["summary"], "auto");
  assert_eq!(outbound["tool_choice"], json!({"type": "function", "name": "lookup"}));

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_reasoning_rejects_known_non_reasoning_model_locally() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::openai(mock.base_url());
  let client = fixture.client();

  let error = client
    .generate("openai/gpt-4o")
    .prompt("Do not send an unsupported reasoning control.")
    .reasoning_effort(ReasoningEffort::High)
    .send()
    .await
    .expect_err("known non-reasoning model should fail locally");

  assert!(
    matches!(error, Error::InvalidGenerateRequest { message } if message.contains("known not to support reasoning"))
  );
  assert!(mock.last_request().is_none());

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_unsupported_top_k_is_an_invalid_request() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::openai(mock.base_url());
  let client = fixture.client();

  let error = client
    .generate("openai/gpt-5")
    .prompt("Do not send unsupported top-k sampling.")
    .top_k(40)
    .send()
    .await
    .expect_err("unsupported top_k should fail locally");

  assert!(matches!(error, Error::InvalidGenerateRequest { message } if message.contains("top_k")));
  assert!(mock.last_request().is_none());

  mock.shutdown().await;
}

#[tokio::test]
async fn detached_generation_request_round_trips_transforms_and_uses_mock_provider() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let request = GenerateRequest::builder("llama-cpp/mock-model")
    .system("Answer briefly.")
    .prompt("hello from a detached request")
    .temperature(0.2)
    .request_id("sdk-detached")
    .build()
    .expect("build detached request");
  let serialized = serde_json::to_string(&request).expect("serialize detached request");
  let request: GenerateRequest = serde_json::from_str(&serialized).expect("deserialize detached request");
  let request = request
    .into_builder()
    .top_k(40)
    .max_output_tokens(64)
    .build()
    .expect("transform detached request");

  let response = client.send(&request).await.expect("send detached request");

  assert_eq!(response.http_status, 200);
  assert_eq!(response.text, "mock response");
  assert_eq!(response.status.as_deref(), Some("completed"));
  assert_eq!(response.finish_reason.as_deref(), Some("stop"));
  let captured = mock.last_request().expect("upstream request captured");
  assert_eq!(captured.path, "/chat/completions");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["model"], "mock-model");
  assert_eq!(
    outbound["messages"][0],
    json!({"role": "system", "content": "Answer briefly."})
  );
  assert_eq!(
    outbound["messages"][1],
    json!({"role": "user", "content": "hello from a detached request"})
  );
  assert_eq!(outbound["temperature"], 0.2);
  assert_eq!(outbound["top_k"], 40);
  assert_eq!(outbound["max_tokens"], 64);
  assert!(!outbound
    .get("stream")
    .and_then(serde_json::Value::as_bool)
    .unwrap_or(false));

  mock.shutdown().await;
}

#[tokio::test]
async fn typed_generation_controls_reject_relay_routes() {
  for credentials in ["account_pool", "client"] {
    let fixture = Fixture::relay("http://127.0.0.1:1", "llama-cpp", "", credentials);
    let client = fixture.client();

    let error = client
      .generate("llama-cpp/mock-model")
      .prompt("do not silently drop controls")
      .top_k(40)
      .send()
      .await
      .expect_err("relay route should reject typed generation controls");

    assert!(
      matches!(error, Error::InvalidGenerateRequest { message } if message.contains("relay")),
      "unexpected error for {credentials} relay"
    );
  }
}

#[tokio::test]
async fn max_tokens_is_already_wire_native_in_relay_routes() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;

  for credentials in ["account_pool", "client"] {
    let fixture = Fixture::relay(mock.base_url(), "openai", "    api_key: test-key\n", credentials);
    let client = fixture.client();

    let response = client
      .generate("gpt-5")
      .prompt("preserve this native Responses limit")
      .max_tokens(64)
      .send()
      .await
      .expect("wire-native output limit should not require lowering");

    assert_eq!(response.text, "mock response");
    let captured = mock.last_request().expect("upstream request captured");
    assert_eq!(captured.path, "/responses");
    let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
    assert_eq!(outbound["max_output_tokens"], 64);
  }

  mock.shutdown().await;
}

#[tokio::test]
async fn detached_request_preserves_assistant_tool_call_history() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let request = GenerateRequest::builder("llama-cpp/mock-model")
    .prompt("look up rust")
    .message(Message::assistant_with_tool_calls(
      "",
      [ToolCall::new("lookup", json!({"query": "rust"})).id("call_1")],
    ))
    .tool_result("call_1", "Rust is a systems language.")
    .build()
    .expect("build tool follow-up request");

  client.send(&request).await.expect("send tool follow-up request");

  let captured = mock.last_request().expect("upstream request captured");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(
    outbound["messages"][0],
    json!({"role": "user", "content": "look up rust"})
  );
  assert_eq!(outbound["messages"][1]["role"], "assistant");
  assert_eq!(outbound["messages"][1]["tool_calls"][0]["id"], "call_1");
  assert_eq!(outbound["messages"][1]["tool_calls"][0]["function"]["name"], "lookup");
  assert_eq!(
    outbound["messages"][1]["tool_calls"][0]["function"]["arguments"],
    r#"{"query":"rust"}"#
  );
  assert_eq!(
    outbound["messages"][2],
    json!({
      "role": "tool",
      "content": "Rust is a systems language.",
      "tool_call_id": "call_1"
    })
  );

  mock.shutdown().await;
}

#[tokio::test]
async fn client_bound_generation_builder_uses_same_owned_request_model() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let response = client
    .generate("llama-cpp/mock-model")
    .prompt("hello from a bound request")
    .tool(
      Tool::function(
        "lookup",
        json!({
          "type": "object",
          "properties": {"query": {"type": "string"}},
          "required": ["query"]
        }),
      )
      .description("Look up a value")
      .strict(true),
    )
    .tool_choice(ToolChoice::named("lookup"))
    .send()
    .await
    .expect("send client-bound request");

  assert_eq!(response.text, "mock response");
  let captured = mock.last_request().expect("upstream request captured");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["messages"][0]["content"], "hello from a bound request");
  assert_eq!(outbound["tools"][0]["type"], "function");
  assert_eq!(outbound["tools"][0]["function"]["name"], "lookup");
  assert_eq!(outbound["tools"][0]["function"]["description"], "Look up a value");
  assert_eq!(outbound["tools"][0]["function"]["strict"], true);
  assert_eq!(
    outbound["tool_choice"],
    json!({"type": "function", "function": {"name": "lookup"}})
  );

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_response_normalizes_reasoning_tools_and_usage_from_mock_provider() {
  let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
    MockEndpoint::ChatCompletions,
    MockResponse::json(json!({
      "id": "chatcmpl-friendly",
      "model": "mock-model",
      "choices": [{
        "index": 0,
        "message": {
          "role": "assistant",
          "content": "mock answer",
          "reasoning_content": "mock reasoning",
          "tool_calls": [{
            "id": "call_1",
            "type": "function",
            "function": {"name": "lookup", "arguments": "{\"query\":\"rust\"}"}
          }]
        },
        "finish_reason": "tool_calls"
      }],
      "usage": {"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8}
    })),
  )))
  .await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let response = client
    .generate("llama-cpp/mock-model")
    .prompt("use a tool")
    .send()
    .await
    .expect("send generation request");

  assert_eq!(response.text, "mock answer");
  assert_eq!(response.reasoning.as_deref(), Some("mock reasoning"));
  assert_eq!(response.tool_calls.len(), 1);
  assert_eq!(response.tool_calls[0].id.as_deref(), Some("call_1"));
  assert_eq!(response.tool_calls[0].name, "lookup");
  assert_eq!(response.tool_calls[0].arguments, json!({"query": "rust"}));
  assert_eq!(response.finish_reason.as_deref(), Some("tool_calls"));
  let usage = response.usage.expect("normalized usage");
  assert_eq!(usage.input_tokens, Some(3));
  assert_eq!(usage.output_tokens, Some(5));
  assert_eq!(usage.total_tokens, Some(8));

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_response_preserves_length_finish_reason() {
  let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(
    MockEndpoint::ChatCompletions,
    MockResponse::json(json!({
      "id": "chatcmpl-length",
      "model": "mock-model",
      "choices": [{
        "index": 0,
        "message": {"role": "assistant", "content": "partial"},
        "finish_reason": "length"
      }]
    })),
  )))
  .await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let response = client
    .generate("llama-cpp/mock-model")
    .prompt("write a long answer")
    .send()
    .await
    .expect("send generation request");

  assert_eq!(response.text, "partial");
  assert_eq!(response.status.as_deref(), Some("incomplete"));
  assert_eq!(response.finish_reason.as_deref(), Some("length"));
  assert_eq!(response.raw["incomplete_details"]["reason"], "max_output_tokens");

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_request_turns_non_success_status_into_sdk_error() {
  let mut failure = MockResponse::json(json!({"error": {"message": "mock failure"}}));
  failure.status = "502".parse().expect("valid mock status");
  let mock =
    MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::new(MockEndpoint::ChatCompletions, failure)))
      .await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let error = client
    .generate("llama-cpp/mock-model")
    .prompt("fail this request")
    .send()
    .await
    .expect_err("non-success status should fail");

  assert!(matches!(
    error,
    Error::GenerateResponseStatus { status: 502, body } if body.contains("mock failure")
  ));

  mock.shutdown().await;
}

#[tokio::test]
async fn detached_request_can_bind_to_client_before_execution() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let request = GenerateRequest::builder("llama-cpp/mock-model")
    .prompt("bind this request")
    .build()
    .expect("build detached request");
  let response = request
    .bind(&client)
    .send()
    .await
    .expect("send explicitly bound request");

  assert_eq!(response.text, "mock response");
  assert_eq!(
    mock.last_request().expect("upstream request captured").path,
    "/chat/completions"
  );

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_text_stream_parses_mock_provider_sse() {
  let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::chat_completions_stream())).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let text: Vec<String> = client
    .generate("llama-cpp/mock-model")
    .prompt("stream this request")
    .stream_text()
    .await
    .expect("start text stream")
    .try_collect()
    .await
    .expect("collect text stream");

  assert_eq!(text, vec!["hel", "lo"]);
  let captured = mock.last_request().expect("upstream request captured");
  assert_eq!(captured.path, "/chat/completions");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["stream"], true);

  mock.shutdown().await;
}

#[tokio::test]
async fn friendly_stream_preserves_tool_call_identity_and_finish_reason() {
  let mock = MockLlmServer::start(
    MockLlmConfig::default().with_route(MockRoute::new(
      MockEndpoint::ChatCompletions,
      MockResponse::sse_data([
        json!({
          "id": "chatcmpl-tool-stream",
          "model": "mock-model",
          "choices": [{
            "index": 0,
            "delta": {
              "role": "assistant",
              "tool_calls": [{
                "index": 0,
                "id": "call_stream",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{\"query\":"}
              }]
            },
            "finish_reason": null
          }]
        })
        .to_string(),
        json!({
          "id": "chatcmpl-tool-stream",
          "model": "mock-model",
          "choices": [{
            "index": 0,
            "delta": {
              "tool_calls": [{
                "index": 0,
                "function": {"arguments": "\"rust\"}"}
              }]
            },
            "finish_reason": null
          }]
        })
        .to_string(),
        json!({
          "id": "chatcmpl-tool-stream",
          "model": "mock-model",
          "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]
        })
        .to_string(),
        "[DONE]".into(),
      ]),
    )),
  )
  .await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let events: Vec<GenerateEvent> = client
    .generate("llama-cpp/mock-model")
    .prompt("use a tool")
    .stream()
    .await
    .expect("start semantic stream")
    .try_collect()
    .await
    .expect("collect semantic stream");

  let tool_events: Vec<_> = events
    .iter()
    .filter_map(|event| match event {
      GenerateEvent::ToolCallDelta {
        id,
        name,
        arguments_delta,
        ..
      } => Some((id.as_deref(), name.as_deref(), arguments_delta.as_str())),
      _ => None,
    })
    .collect();
  assert!(!tool_events.is_empty());
  assert!(tool_events
    .iter()
    .all(|(id, name, _)| *id == Some("call_stream") && *name == Some("lookup")));
  assert_eq!(
    tool_events.iter().map(|(_, _, delta)| *delta).collect::<String>(),
    r#"{"query":"rust"}"#
  );
  assert!(events.iter().any(|event| {
    matches!(
      event,
      GenerateEvent::Completed {
        finish_reason: Some(reason)
      } if reason == "tool_calls"
    )
  }));

  mock.shutdown().await;
}

#[tokio::test]
async fn raw_request_and_client_lifecycle_use_explicit_paths() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  assert_eq!(client.config_path(), fixture.config_path);
  assert_eq!(client.auth_path(), fixture.auth_path);
  client.reload().expect("reload SDK client");
  let _ = client.responses();
  let _ = client.messages();

  let options = RequestOptions::default()
    .with_profile("default")
    .with_request_id("sdk-raw")
    .with_session_id("session-raw")
    .with_project_id("/tmp/sdk-project")
    .with_initiator("sdk-test")
    .with_header("x-sdk-test", "raw");
  let response = client
    .execute(
      tokn_core::provider::Endpoint::ChatCompletions,
      serde_json::to_value(chat_request()).expect("serialize chat request"),
      options,
    )
    .await
    .expect("explicit default profile should execute");
  assert_eq!(response.status, 200);
  let error = client
    .execute(
      tokn_sdk::Endpoint::ChatCompletions,
      serde_json::to_value(chat_request()).unwrap(),
      RequestOptions::default().with_profile("missing"),
    )
    .await
    .expect_err("unknown request profile should fail");
  assert!(matches!(error, Error::UnknownProfile { profile } if profile == "missing"));

  mock.shutdown().await;
}

#[tokio::test]
async fn streaming_request_remains_a_live_byte_stream() {
  let mock = MockLlmServer::start(MockLlmConfig::default().with_route(MockRoute::chat_completions_stream())).await;
  let fixture = Fixture::new(mock.base_url());
  let client = fixture.client();

  let response = client
    .chat_completions()
    .stream(&chat_request())
    .await
    .expect("execute streaming SDK request");
  assert_eq!(response.status, 200);
  let body = response.bytes().await.expect("consume SDK stream");
  let body = std::str::from_utf8(&body).expect("stream is UTF-8");
  assert!(body.contains("\"content\":\"hel\""), "{body}");
  assert!(body.contains("data: [DONE]"), "{body}");

  let captured = mock.last_request().expect("upstream request captured");
  let outbound: serde_json::Value = serde_json::from_slice(&captured.body).expect("parse upstream body");
  assert_eq!(outbound["stream"], true);

  mock.shutdown().await;
}

#[test]
fn invalid_default_profile_is_rejected_during_construction() {
  let fixture = Fixture::new("http://127.0.0.1:1");
  let error = Client::builder()
    .config_path(&fixture.config_path)
    .auth_path(&fixture.auth_path)
    .profile("missing")
    .build()
    .err()
    .expect("unknown profile should fail");

  assert!(matches!(error, Error::UnknownProfile { profile } if profile == "missing"));
}

#[test]
fn legacy_and_missing_schema_are_rejected_instead_of_using_legacy_defaults() {
  let fixture = Fixture::new("http://127.0.0.1:1");
  for text in ["[defaults]\nmode = 'exact'\n", "", "schema_version = 1\n"] {
    fs::write(&fixture.config_path, text).unwrap();
    let error = Client::builder()
      .config_path(&fixture.config_path)
      .auth_path(&fixture.auth_path)
      .build()
      .err()
      .expect("SDK requires explicit v2 config");
    assert!(matches!(error, Error::LoadConfig { .. }));
    let mut chain = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(error) = source {
      chain.push_str(&error.to_string());
      source = error.source();
    }
    assert!(
      chain.contains("schema_version") || chain.contains("schema version"),
      "{chain}"
    );
  }
}

#[test]
fn listener_free_v2_requires_an_existing_default_or_explicit_profile() {
  let fixture = Fixture::profiles("http://127.0.0.1:1", "http://127.0.0.1:2");
  let config = fs::read_to_string(&fixture.config_path)
    .unwrap()
    .replace("[profiles.default]", "[profiles.named]");
  fs::write(&fixture.config_path, config).unwrap();
  let builder = Client::builder()
    .config_path(&fixture.config_path)
    .auth_path(&fixture.auth_path);
  let error = builder
    .clone()
    .build()
    .err()
    .expect("missing default profile must be explicit");
  assert!(matches!(error, Error::UnknownProfile { profile } if profile == "default"));
  builder
    .profile("named")
    .build()
    .expect("listener-free v2 accepts an explicit profile");
}

#[tokio::test]
async fn configured_provider_url_overrides_legacy_account_destination() {
  let configured = MockLlmServer::start(MockLlmConfig::default()).await;
  let account_destination = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(configured.base_url());
  fs::write(
    &fixture.auth_path,
    format!(
      "version: 1\naccounts:\n  - id: local\n    provider: llama-cpp\n    base_url: {}\n",
      account_destination.base_url()
    ),
  )
  .unwrap();
  fixture
    .client()
    .chat_completions()
    .create(&chat_request())
    .await
    .unwrap();
  assert_eq!(configured.requests().len(), 1);
  assert!(account_destination.requests().is_empty());
  configured.shutdown().await;
  account_destination.shutdown().await;
}

#[tokio::test]
async fn original_destination_profile_reports_its_sdk_limitation_before_sending() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let mut config = fs::read_to_string(&fixture.config_path).unwrap();
  config.push_str("\n[profiles.proxy]\nroute = 'original'\n[routes.original]\nkind = 'relay'\ndestination = { kind = 'original' }\ncredentials = { kind = 'client' }\n");
  fs::write(&fixture.config_path, config).unwrap();
  let builder = Client::builder()
    .config_path(&fixture.config_path)
    .auth_path(&fixture.auth_path);
  let error = builder
    .clone()
    .profile("proxy")
    .build()
    .err()
    .expect("SDK cannot supply an original destination");
  assert!(matches!(error, Error::BuildEngine { source } if source.to_string().contains("original proxy destination")));
  let client = builder.build().unwrap();
  let error = client
    .chat_completions()
    .create_with(&chat_request(), RequestOptions::default().with_profile("proxy"))
    .await
    .expect_err("request cannot select an original-destination profile");
  assert!(matches!(error, Error::BuildEngine { source } if source.to_string().contains("original proxy destination")));
  assert!(mock.requests().is_empty());
  mock.shutdown().await;
}

#[tokio::test]
async fn profile_endpoint_binding_restricts_sdk_operations() {
  let mock = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(mock.base_url());
  let config = fs::read_to_string(&fixture.config_path).unwrap().replace(
    "wire_identity = 'none'",
    "wire_identity = 'none'\nbinding = { endpoints = ['chat_completions'] }",
  );
  fs::write(&fixture.config_path, config).unwrap();
  let client = fixture.client();
  client
    .chat_completions()
    .create(&chat_request())
    .await
    .expect("allowed Chat operation executes");
  let error = client
    .generate("llama-cpp/mock-model")
    .prompt("blocked Responses operation")
    .send()
    .await
    .expect_err("generation cannot bypass the profile endpoint restriction");
  assert!(
    matches!(error, Error::BuildEngine { source } if source.to_string().contains("does not expose operation 'responses'"))
  );
  assert_eq!(mock.requests().len(), 1);
  mock.shutdown().await;
}

#[tokio::test]
async fn builder_and_request_profiles_select_independent_routes_concurrently() {
  let first = MockLlmServer::start(MockLlmConfig::default()).await;
  let second = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::profiles(first.base_url(), second.base_url());
  let client = Client::builder()
    .config_path(&fixture.config_path)
    .auth_path(&fixture.auth_path)
    .profile("other")
    .build()
    .unwrap();
  let endpoint = client.chat_completions();
  let request = chat_request();
  let (builder_default, request_override) = tokio::join!(
    endpoint.create(&request),
    endpoint.create_with(&request, RequestOptions::default().with_profile("default")),
  );
  builder_default.expect("builder profile selects second provider");
  request_override.expect("request profile overrides builder profile");
  assert_eq!(first.requests().len(), 1);
  assert_eq!(second.requests().len(), 1);
  let error = endpoint
    .create_with(&request, RequestOptions::default().with_profile("missing"))
    .await
    .expect_err("unknown per-request profile fails before sending");
  assert!(matches!(error, Error::UnknownProfile { profile } if profile == "missing"));
  assert_eq!(first.requests().len() + second.requests().len(), 2);
  first.shutdown().await;
  second.shutdown().await;
}

#[tokio::test]
async fn rejected_reload_keeps_the_last_snapshot_until_valid_replacement() {
  let first = MockLlmServer::start(MockLlmConfig::default()).await;
  let second = MockLlmServer::start(MockLlmConfig::default()).await;
  let fixture = Fixture::new(first.base_url());
  let original = fs::read_to_string(&fixture.config_path).unwrap();
  let credentials = fs::read_to_string(&fixture.auth_path).unwrap();
  let client = fixture.client();
  let mut events = client.subscribe_events();
  for invalid in [
    "[defaults]\nmode = 'exact'\n".to_string(),
    "schema_version = 2\n".to_string(),
    original.replace("driver = 'llama-cpp'", "driver = 'unknown-driver'"),
  ] {
    fs::write(&fixture.config_path, invalid).unwrap();
    client.reload().expect_err("invalid replacement must fail atomically");
    client
      .chat_completions()
      .create(&chat_request())
      .await
      .expect("previous snapshot remains usable");
  }
  fs::write(&fixture.config_path, &original).unwrap();
  fs::write(&fixture.auth_path, "accounts: [broken").unwrap();
  client
    .reload()
    .expect_err("invalid credentials must not replace the snapshot");
  client.chat_completions().create(&chat_request()).await.unwrap();
  assert_eq!(first.requests().len(), 4);
  assert!(second.requests().is_empty());

  fs::write(&fixture.auth_path, credentials).unwrap();
  fs::write(
    &fixture.config_path,
    original.replace(first.base_url(), second.base_url()),
  )
  .unwrap();
  client.reload().expect("valid replacement swaps snapshots");
  client
    .chat_completions()
    .create_with(
      &chat_request(),
      RequestOptions::default().with_request_id("after-reload"),
    )
    .await
    .unwrap();
  assert_eq!(first.requests().len(), 4);
  assert_eq!(second.requests().len(), 1);
  tokio::time::timeout(std::time::Duration::from_secs(5), async {
    loop {
      let event = events.recv().await.unwrap();
      if let Event::Requests(event) = event.as_ref() {
        if event.request_id == "after-reload"
          && matches!(
            event.payload,
            RequestEventPayload::Stage(StageEvent::Completed { success: true, .. })
          )
        {
          break;
        }
      }
    }
  })
  .await
  .expect("existing event subscription survives failed and successful reloads");
  first.shutdown().await;
  second.shutdown().await;
}

#[test]
fn response_types_remain_publicly_deserializable() {
  let response: ChatResponse = serde_json::from_value(json!({
    "id": "chatcmpl-test",
    "choices": []
  }))
  .expect("deserialize public SDK response type");
  assert_eq!(response.id.as_deref(), Some("chatcmpl-test"));
}
