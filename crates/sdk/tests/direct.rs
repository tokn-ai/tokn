use core::{
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use std::{collections::VecDeque, fmt, rc::Rc};

use serde_json::{json, Value};
use tokn_sdk::{
  chat_completions::ChatRequest,
  direct::{
    Anthropic, ConfigError, Endpoint, EndpointConfig, Error, Header, OpenAi, ProviderConfig, Request, Response,
    Transport, OPENAI_BASE_URL,
  },
  messages::{ContentBlock, MessagesRequest},
  responses::ResponsesRequest,
  DirectClient,
};

#[derive(Debug, PartialEq, Eq)]
struct TransportError(&'static str);

impl fmt::Display for TransportError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.0)
  }
}

impl std::error::Error for TransportError {}

// Rc intentionally makes this transport !Send: embedded callers must not need
// an atomic reference count, a thread-safe driver, or a multithreaded executor.
struct MockTransport {
  _local: Rc<()>,
  requests: Vec<Request>,
  responses: VecDeque<Result<Response, TransportError>>,
}

impl MockTransport {
  fn new(responses: impl IntoIterator<Item = Result<Response, TransportError>>) -> Self {
    Self {
      _local: Rc::new(()),
      requests: Vec::new(),
      responses: responses.into_iter().collect(),
    }
  }
}

impl Transport for MockTransport {
  type Error = TransportError;

  async fn send(&mut self, request: Request) -> Result<Response, Self::Error> {
    self.requests.push(request);
    self.responses.pop_front().expect("unexpected transport call")
  }
}

fn run_ready<F: Future>(future: F) -> F::Output {
  let mut future = pin!(future);
  let mut context = Context::from_waker(Waker::noop());
  match future.as_mut().poll(&mut context) {
    Poll::Ready(output) => output,
    Poll::Pending => panic!("mock transport should resolve immediately"),
  }
}

fn response(status: u16, body: &[u8]) -> Response {
  Response {
    status,
    headers: vec![Header::new("x-request-id", "request-123")],
    body: body.into(),
  }
}

fn client(responses: impl IntoIterator<Item = Result<Response, TransportError>>) -> DirectClient<MockTransport> {
  DirectClient::new(
    MockTransport::new(responses),
    OpenAi::new("sk-test-private-key").expect("valid config"),
  )
}

fn responses_request() -> ResponsesRequest {
  serde_json::from_value(json!({
    "model": "test-model",
    "input": "private prompt",
    "max_output_tokens": 64,
    "reasoning": {"effort": "high"},
    "metadata": {"client": "embedded"},
    "extra_wire_field": {"preserve": true}
  }))
  .expect("Responses fixture")
}

fn chat_request() -> ChatRequest {
  serde_json::from_value(json!({
    "model": "test-model",
    "messages": [{"role": "user", "content": "hello"}],
    "max_completion_tokens": 128,
    "temperature": 0.2,
    "tools": [{
      "type": "function",
      "function": {"name": "lookup", "parameters": {"type": "object"}}
    }]
  }))
  .expect("chat fixture")
}

#[test]
fn responses_sends_openai_auth_and_native_controls_and_preserves_metadata() {
  let mut client = client([Ok(response(
    200,
    br#"{"id":"resp_1","model":"test-model","status":"completed","output_text":"hello","extra_output":123}"#,
  ))]);
  let response = run_ready(client.responses(&responses_request())).expect("Responses response");
  assert_eq!(response.status, 200);
  assert_eq!(response.headers, [Header::new("x-request-id", "request-123")]);
  assert_eq!(response.data.id.as_deref(), Some("resp_1"));
  assert_eq!(response.data.output_text.as_deref(), Some("hello"));
  assert_eq!(response.data.extras["extra_output"], 123);

  let sent = &client.transport().requests[0];
  assert_eq!(sent.method, "POST");
  assert_eq!(sent.url, "https://api.openai.com/v1/responses");
  assert_eq!(
    sent.headers,
    [
      Header::new("authorization", "Bearer sk-test-private-key"),
      Header::new("content-type", "application/json"),
      Header::new("accept", "application/json"),
    ]
  );
  let wire: Value = serde_json::from_slice(&sent.body).expect("request JSON");
  assert_eq!(wire["model"], "test-model");
  assert_eq!(wire["input"], "private prompt");
  assert_eq!(wire["max_output_tokens"], 64);
  assert_eq!(wire["reasoning"], json!({"effort":"high"}));
  assert_eq!(wire["metadata"], json!({"client":"embedded"}));
  assert_eq!(wire["extra_wire_field"], json!({"preserve":true}));
  assert_eq!(wire["stream"], false);
}

#[test]
fn chat_completions_uses_explicit_base_path_and_decodes_typed_response() {
  let provider = OpenAi::new("test-key")
    .unwrap()
    .with_base_url("https://example.com:8443/custom/v1///")
    .unwrap();
  let mut client = DirectClient::new(
    MockTransport::new([Ok(response(
      201,
      br#"{"id":"chat_1","choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#,
    ))]),
    provider,
  );
  let response = run_ready(client.chat_completions(&chat_request())).unwrap();
  assert_eq!(response.status, 201);
  assert_eq!(response.data.id.as_deref(), Some("chat_1"));
  let decoded = serde_json::to_value(response.data).unwrap();
  assert_eq!(decoded["choices"][0]["message"]["content"], "hello");

  let transport = client.into_transport();
  assert_eq!(
    transport.requests[0].url,
    "https://example.com:8443/custom/v1/chat/completions"
  );
  let wire: Value = serde_json::from_slice(&transport.requests[0].body).unwrap();
  assert_eq!(wire["max_completion_tokens"], 128);
  assert_eq!(wire["temperature"], 0.2);
  assert_eq!(wire["tools"][0]["function"]["name"], "lookup");
  assert_eq!(wire["stream"], false);
}

#[test]
fn http_errors_preserve_bytes_and_metadata_before_attempting_json_decoding() {
  for status in [307, 401, 429, 503] {
    let mut client = client([Ok(response(status, b"not JSON"))]);
    let error = run_ready(client.responses(&responses_request())).unwrap_err();
    let Error::HttpStatus { response } = error else {
      panic!("expected HTTP status error");
    };
    assert_eq!(response.status, status);
    assert_eq!(response.body, b"not JSON");
    assert_eq!(response.headers[0], Header::new("x-request-id", "request-123"));
  }
}

#[test]
fn transport_and_decode_failures_remain_distinct() {
  let mut client = client([
    Err(TransportError("network unavailable")),
    Ok(response(200, b"invalid JSON")),
  ]);
  assert!(matches!(
    run_ready(client.responses(&responses_request())),
    Err(Error::Transport(TransportError("network unavailable")))
  ));
  assert!(matches!(
    run_ready(client.chat_completions(&chat_request())),
    Err(Error::Deserialize(_))
  ));
}

#[test]
fn streaming_requests_and_flattened_overrides_never_reach_the_transport() {
  let mut client = client([]);
  let mut responses = responses_request();
  responses.stream = Some(true);
  // Even a conflicting flattened false cannot hide an explicit typed true.
  responses.extras.insert("stream".into(), false.into());
  assert!(matches!(
    run_ready(client.responses(&responses)),
    Err(Error::StreamingUnsupported)
  ));
  responses.stream = Some(false);
  responses.extras.insert("stream".into(), true.into());
  assert!(matches!(
    run_ready(client.responses(&responses)),
    Err(Error::StreamingUnsupported)
  ));

  let mut chat = chat_request();
  chat.stream = Some(true);
  assert!(matches!(
    run_ready(client.chat_completions(&chat)),
    Err(Error::StreamingUnsupported)
  ));
  chat.stream = None;
  chat.extras.insert("stream".into(), true.into());
  assert!(matches!(
    run_ready(client.chat_completions(&chat)),
    Err(Error::StreamingUnsupported)
  ));
  assert!(client.transport().requests.is_empty());
}

#[test]
fn unexpected_event_stream_is_rejected() {
  let mut response = response(200, b"data: {\"type\":\"response.completed\"}\n\n");
  response
    .headers
    .push(Header::new("Content-Type", "Text/Event-Stream; charset=utf-8"));
  let mut client = client([Ok(response)]);
  assert!(matches!(
    run_ready(client.responses(&responses_request())),
    Err(Error::UnexpectedStream)
  ));
}

#[test]
fn config_rejects_invalid_keys_and_ambiguous_or_insecure_urls() {
  for key in ["", " ", "key\ninjected: header", "has space", "nonascii-é", "nul\0key"] {
    assert_eq!(OpenAi::new(key).unwrap_err(), ConfigError::InvalidApiKey);
  }
  for url in [
    "",
    "http://example.com/v1",
    "https://",
    "https:///v1",
    "https://user:password@example.com/v1",
    "https://example.com?key=secret",
    "https://example.com/v1?key=secret",
    "https://example.com/v1#fragment",
    "https://example.com\\@another.com/v1",
    "https://example.com/v1/../v2",
    "https://example.com/./v1",
    "https://example.com/%2e%2e/v1",
    "https://example.com/path with spaces",
    "https://example.com:\n8443/v1",
    "https://example.com:0/v1",
    "https://example.com:65536/v1",
    "https://example.com:/v1",
    "https://.example.com/v1",
    "https://example..com/v1",
    "https://-example.com/v1",
    "https://[invalid-ipv6]/v1",
    "https://[::1]evil.com/v1",
  ] {
    assert_eq!(
      OpenAi::new("test-key").unwrap().with_base_url(url).unwrap_err(),
      ConfigError::InvalidBaseUrl,
      "{url}"
    );
  }
  for url in [
    "https://api.openai.com/v1",
    "https://example.com",
    "https://localhost:8443/v1",
    "https://127.0.0.1:443/v1",
    "https://[::1]:8443/v1",
    "https://example.com./v1",
  ] {
    assert_eq!(
      OpenAi::new("test-key").unwrap().with_base_url(url).unwrap().base_url(),
      url
    );
  }
  assert_eq!(OpenAi::new("test-key").unwrap().base_url(), OPENAI_BASE_URL);
}

#[test]
fn debug_output_redacts_credentials_and_request_and_error_bodies() {
  let mut client = client([Ok(response(401, b"private response body"))]);
  let error = run_ready(client.responses(&responses_request())).unwrap_err();
  let debug = format!("{client:?} {:?} {error:?} {error}", client.transport().requests[0]);
  for private in ["sk-test-private-key", "private prompt", "private response body"] {
    assert!(!debug.contains(private), "debug output leaked {private}");
  }
  assert!(debug.contains("401"));
}

fn messages_request() -> MessagesRequest {
  serde_json::from_value(json!({
    "model": "messages-model",
    "messages": [{"role": "user", "content": "private message"}],
    "max_tokens": 128,
    "system": [{"type": "text", "text": "Be concise.", "cache_control": {"type": "ephemeral"}}],
    "tools": [{"name": "lookup", "input_schema": {"type": "object"}}],
    "thinking": {"type": "enabled", "budget_tokens": 64},
    "extra_wire_field": {"preserve": true}
  }))
  .expect("Messages fixture")
}

#[test]
fn messages_preserves_native_controls_content_usage_and_anthropic_headers() {
  let mut client = DirectClient::new(
    MockTransport::new([Ok(response(
      200,
      br#"{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"hello"},{"type":"tool_use","id":"tool_1","name":"lookup","input":{"query":"rust"}}],"stop_reason":"tool_use","usage":{"input_tokens":8,"output_tokens":12},"extra_output":123}"#,
    ))]),
    Anthropic::new("anthropic-private-key").unwrap(),
  );
  let response = run_ready(client.messages(&messages_request())).unwrap();
  assert_eq!(response.status, 200);
  assert_eq!(response.headers, [Header::new("x-request-id", "request-123")]);
  assert_eq!(response.data.id.as_deref(), Some("msg_1"));
  assert!(matches!(&response.data.content[0], ContentBlock::Text { text, .. } if text == "hello"));
  assert!(
    matches!(&response.data.content[1], ContentBlock::ToolUse { name, input, .. }
    if name == "lookup" && input["query"] == "rust")
  );
  let decoded = serde_json::to_value(response.data).unwrap();
  assert_eq!(decoded["stop_reason"], "tool_use");
  assert_eq!(decoded["usage"], json!({"input_tokens": 8, "output_tokens": 12}));
  assert_eq!(decoded["extra_output"], 123);

  let sent = &client.transport().requests[0];
  assert_eq!(sent.method, "POST");
  assert_eq!(sent.url, "https://api.anthropic.com/v1/messages");
  assert_eq!(
    sent.headers,
    [
      Header::new("x-api-key", "anthropic-private-key"),
      Header::new("anthropic-version", "2023-06-01"),
      Header::new("content-type", "application/json"),
      Header::new("accept", "application/json"),
    ]
  );
  let wire: Value = serde_json::from_slice(&sent.body).unwrap();
  assert_eq!(wire["model"], "messages-model");
  assert_eq!(wire["messages"][0]["content"], "private message");
  assert_eq!(wire["max_tokens"], 128);
  assert_eq!(wire["system"][0]["cache_control"], json!({"type": "ephemeral"}));
  assert_eq!(wire["tools"][0]["input_schema"], json!({"type": "object"}));
  assert_eq!(wire["thinking"], json!({"type": "enabled", "budget_tokens": 64}));
  assert_eq!(wire["extra_wire_field"], json!({"preserve": true}));
  assert_eq!(wire["stream"], false);
}

#[test]
fn messages_rejects_stream_overrides_and_unconfigured_endpoints_without_io() {
  let mut client = DirectClient::new(MockTransport::new([]), Anthropic::new("test-key").unwrap());
  let mut request = messages_request();
  request.stream = Some(true);
  request.extras.insert("stream".into(), false.into());
  assert!(matches!(
    run_ready(client.messages(&request)),
    Err(Error::StreamingUnsupported)
  ));
  request.stream = Some(false);
  request.extras.insert("stream".into(), true.into());
  assert!(matches!(
    run_ready(client.messages(&request)),
    Err(Error::StreamingUnsupported)
  ));
  assert!(matches!(
    run_ready(client.responses(&responses_request())),
    Err(Error::UnsupportedEndpoint(Endpoint::Responses))
  ));
  assert!(matches!(
    run_ready(client.chat_completions(&chat_request())),
    Err(Error::UnsupportedEndpoint(Endpoint::ChatCompletions))
  ));
  client.set_provider(OpenAi::new("test-key").unwrap());
  assert!(matches!(
    run_ready(client.messages(&messages_request())),
    Err(Error::UnsupportedEndpoint(Endpoint::Messages))
  ));
  assert!(client.transport().requests.is_empty());
}

#[test]
fn messages_retains_http_errors_and_distinguishes_transport_decode_and_sse_failures() {
  let mut sse_response = response(200, b"data: {}\n\n");
  sse_response
    .headers
    .push(Header::new("content-type", "text/event-stream"));
  let mut client = DirectClient::new(
    MockTransport::new([
      Ok(response(429, b"private rate limit body")),
      Err(TransportError("offline")),
      Ok(response(200, b"invalid JSON")),
      Ok(sse_response),
    ]),
    Anthropic::new("test-key").unwrap(),
  );
  let Error::HttpStatus { response } = run_ready(client.messages(&messages_request())).unwrap_err() else {
    panic!("expected HTTP status error");
  };
  assert_eq!(response.status, 429);
  assert_eq!(response.body, b"private rate limit body");
  assert_eq!(response.headers[0], Header::new("x-request-id", "request-123"));
  assert!(matches!(
    run_ready(client.messages(&messages_request())),
    Err(Error::Transport(TransportError("offline")))
  ));
  assert!(matches!(
    run_ready(client.messages(&messages_request())),
    Err(Error::Deserialize(_))
  ));
  assert!(matches!(
    run_ready(client.messages(&messages_request())),
    Err(Error::UnexpectedStream)
  ));
}

#[test]
fn custom_routes_keep_urls_and_credentials_scoped_to_the_selected_format() {
  let provider = ProviderConfig::new()
    .with_endpoint(
      Endpoint::Responses,
      EndpointConfig::new("https://responses.example/custom/generate")
        .unwrap()
        .with_bearer_auth("responses-token")
        .unwrap(),
    )
    .with_endpoint(
      Endpoint::Messages,
      EndpointConfig::new("https://messages.example/anthropic/v1/messages/")
        .unwrap()
        .with_header("x-api-key", "messages-token")
        .unwrap()
        .with_header("anthropic-version", "2023-06-01")
        .unwrap(),
    )
    .with_endpoint(
      Endpoint::ChatCompletions,
      EndpointConfig::new_insecure_http("http://127.0.0.1:8080/v1/chat/completions").unwrap(),
    );
  let mut client = DirectClient::new(
    MockTransport::new([
      Ok(response(200, br#"{"id":"resp_1"}"#)),
      Ok(response(200, br#"{"id":"msg_1"}"#)),
      Ok(response(200, br#"{"id":"chat_1","choices":[]}"#)),
    ]),
    provider,
  );
  run_ready(client.responses(&responses_request())).unwrap();
  run_ready(client.messages(&messages_request())).unwrap();
  run_ready(client.chat_completions(&chat_request())).unwrap();
  let requests = &client.transport().requests;
  assert_eq!(requests[0].url, "https://responses.example/custom/generate");
  assert_eq!(requests[1].url, "https://messages.example/anthropic/v1/messages/");
  assert_eq!(requests[2].url, "http://127.0.0.1:8080/v1/chat/completions");
  assert_eq!(
    requests[0].headers[0],
    Header::new("authorization", "Bearer responses-token")
  );
  assert_eq!(requests[1].headers[0], Header::new("x-api-key", "messages-token"));
  assert_eq!(requests[1].headers[1], Header::new("anthropic-version", "2023-06-01"));
  assert!(!requests[0].headers.iter().any(|header| header.name == "x-api-key"));
  assert!(!requests[1].headers.iter().any(|header| header.name == "authorization"));
  assert_eq!(
    requests[2].headers,
    [
      Header::new("content-type", "application/json"),
      Header::new("accept", "application/json")
    ]
  );
}

#[test]
fn endpoint_configuration_rejects_header_injection_reserved_headers_and_insecure_defaults() {
  let endpoint = || EndpointConfig::new("https://example.com/messages").unwrap();
  for name in [
    "",
    "bad name",
    "x-header:",
    "x-header\r\ninjected",
    "héader",
    ":authority",
  ] {
    assert_eq!(
      endpoint().with_header(name, "value").unwrap_err(),
      ConfigError::InvalidHeaderName
    );
  }
  for value in [
    "line\rbreak",
    "line\nbreak",
    "tab\tvalue",
    "nul\0value",
    "nonascii-é",
    "del\x7f",
  ] {
    assert_eq!(
      endpoint().with_header("x-header", value).unwrap_err(),
      ConfigError::InvalidHeaderValue
    );
  }
  for name in [
    "Accept",
    "CONTENT-TYPE",
    "Host",
    "Content-Length",
    "Transfer-Encoding",
    "Connection",
    "Keep-Alive",
    "Proxy-Authenticate",
    "Proxy-Authorization",
    "TE",
    "Trailer",
    "Upgrade",
  ] {
    assert_eq!(
      endpoint().with_header(name, "value").unwrap_err(),
      ConfigError::ReservedHeader
    );
  }
  for url in [
    "http://localhost/messages",
    "https://user:secret@example.com/messages",
    "https://example.com/messages?key=secret",
    "https://example.com/messages#fragment",
    "https://example.com/a/../messages",
    "https://example.com/%2e/messages",
  ] {
    assert_eq!(EndpointConfig::new(url).unwrap_err(), ConfigError::InvalidEndpointUrl);
  }
  assert_eq!(
    EndpointConfig::new_insecure_http("https://example.com/messages").unwrap_err(),
    ConfigError::InvalidEndpointUrl
  );
  for key in ["", "has space", "key\ninjected"] {
    assert_eq!(Anthropic::new(key).unwrap_err(), ConfigError::InvalidApiKey);
    assert_eq!(
      endpoint().with_bearer_auth(key).unwrap_err(),
      ConfigError::InvalidApiKey
    );
  }
  let endpoint = endpoint()
    .with_header("X-Api-Key", "old-key")
    .unwrap()
    .with_header("x-api-key", "new-key")
    .unwrap();
  assert_eq!(endpoint.headers(), [Header::new("x-api-key", "new-key")]);
  assert_eq!(endpoint.url(), "https://example.com/messages");
}

#[test]
fn replacing_provider_rotates_credentials_without_replacing_the_transport() {
  let mut client = DirectClient::new(
    MockTransport::new([Ok(response(200, b"{}")), Ok(response(200, b"{}"))]),
    Anthropic::new("old-private-key").unwrap(),
  );
  run_ready(client.messages(&messages_request())).unwrap();
  let replacement = Anthropic::new("new-private-key")
    .unwrap()
    .with_base_url("https://example.com/custom/v1///")
    .unwrap()
    .with_version("2099-01-01")
    .unwrap();
  client.set_provider(replacement);
  run_ready(client.messages(&messages_request())).unwrap();
  assert_eq!(client.transport().requests.len(), 2);
  assert_eq!(
    client.transport().requests[0].headers[0],
    Header::new("x-api-key", "old-private-key")
  );
  assert_eq!(
    client.transport().requests[1].headers[0],
    Header::new("x-api-key", "new-private-key")
  );
  assert_eq!(
    client.transport().requests[1].headers[1],
    Header::new("anthropic-version", "2099-01-01")
  );
  assert_eq!(
    client.provider().endpoint(Endpoint::Messages).unwrap().url(),
    "https://example.com/custom/v1/messages"
  );
  assert!(client.provider().endpoint(Endpoint::Responses).is_none());
  let debug = format!("{client:?} {:?}", client.transport().requests);
  for private in ["old-private-key", "new-private-key", "private message"] {
    assert!(!debug.contains(private), "debug output leaked {private}");
  }
}
