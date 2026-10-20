use core::{
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use std::{collections::VecDeque, fmt, rc::Rc};

use serde_json::{json, Value};
use tokn_sdk::{
  chat_completions::ChatRequest,
  direct::{ConfigError, Error, Header, OpenAi, Request, Response, Transport, OPENAI_BASE_URL},
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
