//! The same consumer scenarios run with std storage and an isolated alloc-only graph.

use alloc::{collections::VecDeque, rc::Rc, string::String, vec, vec::Vec};
use core::{
  cell::RefCell,
  future::{poll_fn, Future},
  pin::pin,
  task::{Context, Poll, Waker},
  time::Duration,
};
use serde_json::{json, Value};
use tokn_sdk::{
  account::AccountConfig,
  chat_completions::ChatRequest,
  messages::MessagesRequest,
  responses::ResponsesRequest,
  transport::{Request, ResponseHead, Transport},
  Client, Endpoint, Injected, Lifecycle, Limits, PlatformResponse, RequestOptions, Runtime,
};

#[path = "typed_events.rs"]
mod typed_events;
pub use typed_events::{
  buffered_usage_events, event_timestamps_and_local_sink, streaming_usage_events, typed_event_failures,
  typed_event_retry_attribution, typed_event_summaries,
};

const CHAT_JSON: &[u8] = br#"{"id":"chat-test","object":"chat.completion","model":"model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#;
const RESPONSES_JSON: &[u8] = br#"{"id":"resp-test","status":"completed","model":"model","output":[{"type":"message","id":"message-test","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}"#;
const MESSAGES_JSON: &[u8] = br#"{"id":"msg-test","type":"message","model":"model","role":"assistant","content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2}}"#;
const CHAT_SSE: &[u8] = concat!(
  "data: {\"id\":\"chat-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"héllo 🌍\"}}]}\r\n\r\n",
  "data: {\"id\":\"chat-test\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\n",
  "data: [DONE]\r\n\r\n"
)
.as_bytes();
const RESPONSES_SSE: &[u8] = concat!(
  "event: response.output_text.delta\r\n",
  "data: {\"type\":\"response.output_text.delta\",\"delta\":\"héllo 🌍\"}\r\n\r\n",
  "event: response.completed\r\n",
  "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"output\":[]}}\r\n\r\n"
).as_bytes();
const MESSAGES_SSE: &[u8] = concat!(
  "event: content_block_delta\r\n",
  "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"héllo 🌍\"}}\r\n\r\n",
  "event: message_stop\r\n",
  "data: {\"type\":\"message_stop\"}\r\n\r\n"
).as_bytes();

#[derive(Clone, Copy)]
struct Reply {
  status: u16,
  content_type: &'static str,
  bytes: &'static [u8],
}
impl Reply {
  fn json(bytes: &'static [u8]) -> Self {
    Self {
      status: 200,
      content_type: "application/json",
      bytes,
    }
  }
  fn sse(bytes: &'static [u8]) -> Self {
    Self {
      status: 200,
      content_type: "text/event-stream; charset=utf-8",
      bytes,
    }
  }
}

/// Borrows application state and owns non-atomic state. Every body read yields
/// before producing exactly one byte, splitting UTF-8 and CRLF across polls.
struct MemoryTransport<'a> {
  requests: &'a RefCell<Vec<Request>>,
  replies: VecDeque<Reply>,
  active: &'static [u8],
  position: usize,
  yield_next: bool,
  local: Rc<()>,
  encoding: &'static str,
  recoverable_errors: bool,
  expected_methods: VecDeque<&'static str>,
  read_failure_after: Option<usize>,
  trace: Rc<RefCell<TransportTrace>>,
}
#[derive(Default)]
struct TransportTrace {
  reads: usize,
  closed: usize,
}
impl<'a> MemoryTransport<'a> {
  fn with_trace(mut self, trace: Rc<RefCell<TransportTrace>>) -> Self {
    self.trace = trace;
    self
  }
  fn with_encoding(mut self, encoding: &'static str) -> Self {
    self.encoding = encoding;
    self
  }

  fn with_recoverable_errors(mut self, recoverable: bool) -> Self {
    self.recoverable_errors = recoverable;
    self
  }

  fn with_read_failure_after(mut self, bytes: usize) -> Self {
    self.read_failure_after = Some(bytes);
    self
  }

  fn with_methods(mut self, methods: impl IntoIterator<Item = &'static str>) -> Self {
    self.expected_methods = methods.into_iter().collect();
    self
  }

  fn new(requests: &'a RefCell<Vec<Request>>, replies: impl IntoIterator<Item = Reply>) -> Self {
    Self {
      requests,
      replies: replies.into_iter().collect(),
      active: &[],
      position: 0,
      yield_next: true,
      local: Rc::new(()),
      encoding: "identity",
      recoverable_errors: true,
      expected_methods: VecDeque::new(),
      read_failure_after: None,
      trace: Rc::default(),
    }
  }
}
#[derive(Debug)]
struct MemoryError;
impl core::fmt::Display for MemoryError {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("injected transport failure")
  }
}
impl Transport for MemoryTransport<'_> {
  type Error = MemoryError;
  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    assert_eq!(request.method, self.expected_methods.pop_front().unwrap_or("POST"));
    self.requests.borrow_mut().push(request);
    let reply = self
      .replies
      .pop_front()
      .expect("fixture has a reply for each sent request");
    if reply.status == 0 {
      return Err(MemoryError);
    }
    self.active = reply.bytes;
    self.position = 0;
    self.yield_next = true;
    let headers = Default::default();
    // The explicit response type below keeps HeaderMap out of the consumer's dependencies.
    let mut head = ResponseHead {
      status: reply.status,
      headers,
    };
    if !reply.content_type.is_empty() {
      head.headers.insert("content-type", reply.content_type);
    }
    if self.encoding != "identity" {
      head.headers.insert("content-encoding", self.encoding);
      head.headers.insert("content-length", "1");
    }
    Ok(head)
  }
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    self.trace.borrow_mut().reads += 1;
    poll_fn(|context| {
      if self.read_failure_after.is_some_and(|limit| self.position >= limit) {
        self.read_failure_after = None;
        return Poll::Ready(Err(MemoryError));
      }
      if self.yield_next {
        self.yield_next = false;
        context.waker().wake_by_ref();
        return Poll::Pending;
      }
      assert_eq!(Rc::strong_count(&self.local), 1);
      let count = buffer.len().min(1).min(self.active.len() - self.position);
      buffer[..count].copy_from_slice(&self.active[self.position..self.position + count]);
      self.position += count;
      self.yield_next = true;
      Poll::Ready(Ok(count))
    })
    .await
  }
  fn accept_encoding(&self) -> &str {
    self.encoding
  }
  fn recoverable(&self, _: &Self::Error) -> bool {
    self.recoverable_errors
  }
  fn close_response(&mut self) {
    self.trace.borrow_mut().closed += 1;
    self.active = &[];
    self.position = 0;
  }
}

#[derive(Clone, Debug)]
struct SeenEvent {
  request_id: String,
  attempt: u32,
  stage: &'static str,
  success: Option<bool>,
}
#[derive(Default)]
struct RuntimeState {
  elapsed: Duration,
  sleeps: Vec<Duration>,
  events: Vec<SeenEvent>,
}
#[derive(Clone, Default)]
struct TestRuntime(Rc<RefCell<RuntimeState>>);
impl Runtime for TestRuntime {
  fn now(&self) -> Duration {
    self.0.borrow().elapsed
  }
  fn unix_seconds(&self) -> u64 {
    1_700_000_000
  }
  async fn sleep(&mut self, duration: Duration) {
    let mut state = self.0.borrow_mut();
    state.elapsed = state.elapsed.saturating_add(duration);
    state.sleeps.push(duration);
  }
  fn observe(&mut self, event: Lifecycle<'_>) {
    self.0.borrow_mut().events.push(SeenEvent {
      request_id: event.request_id.into(),
      attempt: event.attempt,
      stage: event.stage,
      success: event.success,
    });
  }
}

type TestClient<'a> = Client<Injected<MemoryTransport<'a>, TestRuntime>>;
fn plan(driver: &str) -> tokn_sdk::policy::ExecutionRules {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none"},
    "providers":{"upstream":{"driver":driver,"base_url":"https://example.com/v1"}}
  }))
  .unwrap();
  tokn_sdk::config::v2::compile_execution(&raw).unwrap()
}
fn retry_plan(max_retries: u32, initial_backoff_ms: u64) -> tokn_sdk::policy::ExecutionRules {
  retry_plan_for(max_retries, initial_backoff_ms, "openai")
}
fn retry_plan_for(max_retries: u32, initial_backoff_ms: u64, driver: &str) -> tokn_sdk::policy::ExecutionRules {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none","retry":{"kind":"recoverable","policy":"retry"}},
    "retry_policies":{"retry":{"max_retries":max_retries,"initial_backoff_ms":initial_backoff_ms}},
    "providers":{"upstream":{"driver":driver,"base_url":"https://example.com/v1"}}
  })).unwrap();
  tokn_sdk::config::v2::compile_execution(&raw).unwrap()
}
fn account_config(id: &str, key: &str, tier: &str) -> AccountConfig {
  serde_json::from_value(json!({"id":id,"provider":"upstream","api_key":key,"tier":tier})).unwrap()
}
fn make_client<'a>(
  requests: &'a RefCell<Vec<Request>>,
  replies: impl IntoIterator<Item = Reply>,
  driver: &str,
) -> TestClient<'a> {
  Client::new(
    plan(driver),
    vec![account_config("primary", "test-key", "active")],
    Injected::new(MemoryTransport::new(requests, replies), TestRuntime::default()),
  )
  .unwrap()
}
fn chat() -> ChatRequest {
  serde_json::from_value(json!({"model":"model","messages":[{"role":"user","content":"hello"}]})).unwrap()
}
fn responses() -> ResponsesRequest {
  serde_json::from_value(json!({"model":"model","input":"hello"})).unwrap()
}
fn messages() -> MessagesRequest {
  serde_json::from_value(json!({"model":"model","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}))
    .unwrap()
}

fn run<F: Future>(future: F) -> F::Output {
  let mut future = pin!(future);
  let mut context = Context::from_waker(Waker::noop());
  for _ in 0..100_000 {
    if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
      return value;
    }
  }
  panic!("fixture exceeded its pending-poll budget");
}

/// An unrelated named profile must never become the implicit default merely
/// because it is first in the config's map.
pub fn explicit_profiles_do_not_imply_default() {
  let raw = serde_json::from_value(json!({
    "schema_version": 2,
    "profiles": {"named": {"route": "managed", "wire_identity": "none"}},
    "routes": {"managed": {
      "kind": "managed",
      "provider": {"kind": "fixed", "provider": "upstream"},
      "model": {"kind": "qualified", "namespace": "provider"},
      "operation": "preserve",
      "retry": {"kind": "never"}
    }},
    "providers": {"upstream": {"driver": "openai", "base_url": "https://example.com/v1"}}
  }))
  .unwrap();
  let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    plan,
    vec![account_config("primary", "test-key", "active")],
    Injected::new(
      MemoryTransport::new(&sent, [Reply::json(CHAT_JSON)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let body = json!({"model": "upstream/model", "messages": [{"role": "user", "content": "hello"}]});
  for (options, expected_profile) in [
    (RequestOptions::default(), "default"),
    (RequestOptions::default().with_profile("missing"), "missing"),
  ] {
    let error = match run(client.execute(Endpoint::ChatCompletions, body.clone(), options)) {
      Err(error) => error,
      Ok(_) => panic!("an absent profile must fail before transport I/O"),
    };
    assert!(error.message.contains(expected_profile), "{error}");
    assert!(sent.borrow().is_empty());
  }
  let response = run(client.execute(
    Endpoint::ChatCompletions,
    body,
    RequestOptions::default().with_profile("named"),
  ))
  .expect("an explicit existing profile is valid without a default profile");
  assert_eq!(response.status, 200);
  assert_eq!(sent.borrow().len(), 1);
}

/// Credential refresh is part of shared request preparation. The transport sees
/// both exchanges, and a successful refresh is reused by later requests.
pub fn copilot_refresh_uses_injected_transport_and_reuses_credentials() {
  const TOKEN: &[u8] = br#"{"token":"refreshed-api-token","expires_at":1700003600}"#;
  for fail_first_refresh in [false, true] {
    let sent = RefCell::new(Vec::new());
    let account = serde_json::from_value(json!({
      "id": "primary",
      "provider": "upstream",
      "access_token": "expired-api-token",
      "access_token_expires_at": 1_699_999_999_i64,
      "refresh_token": "github-credential",
      "refresh_url": "https://credentials.example/token",
      "settings": {"editor_version": "test-editor/1"}
    }))
    .unwrap();
    let mut replies = Vec::new();
    let mut methods = Vec::new();
    if fail_first_refresh {
      replies.push(Reply::json(b"invalid-token-response"));
      methods.push("GET");
    }
    replies.extend([Reply::json(TOKEN), Reply::json(CHAT_JSON), Reply::json(CHAT_JSON)]);
    methods.extend(["GET", "POST", "POST"]);
    let client = Client::new(
      plan("github-copilot"),
      vec![account],
      Injected::new(
        MemoryTransport::new(&sent, replies).with_methods(methods),
        TestRuntime::default(),
      ),
    )
    .unwrap();
    if fail_first_refresh {
      assert!(run(client.chat_completions().create(&chat())).is_err());
      assert_eq!(
        sent.borrow().len(),
        1,
        "invalid refreshed credentials must not reach LLM send"
      );
    }
    for _ in 0..2 {
      let response =
        run(client.chat_completions().create(&chat())).expect("valid refresh and later cached credential both execute");
      assert_eq!(response.data.id.as_deref(), Some("chat-test"));
    }
    let requests = sent.borrow();
    let refresh_count = 1 + usize::from(fail_first_refresh);
    assert_eq!(requests.len(), refresh_count + 2);
    for request in &requests[..refresh_count] {
      assert_eq!(request.method, "GET");
      assert_eq!(request.url, "https://credentials.example/token");
      assert!(request.body.is_empty());
      assert_eq!(
        request.headers.get("authorization").unwrap().as_str(),
        "token github-credential"
      );
      assert_eq!(request.headers.get("editor-version").unwrap().as_str(), "test-editor/1");
    }
    for request in &requests[refresh_count..] {
      assert_eq!(request.method, "POST");
      assert_eq!(request.url, "https://example.com/v1/chat/completions");
      assert_eq!(
        request.headers.get("authorization").unwrap().as_str(),
        "Bearer refreshed-api-token"
      );
    }
  }
}

/// A recoverable credential exchange failure is a send failure: cool down the
/// account, retry the full request, and refresh the newly selected account.
pub fn recoverable_refresh_failure_reselects_accounts() {
  let sent = RefCell::new(Vec::new());
  let accounts = [
    ("primary", "active", "github-primary"),
    ("backup", "fallback", "github-backup"),
  ]
  .map(|(id, tier, refresh)| {
    serde_json::from_value(json!({
      "id": id, "tier": tier, "provider": "upstream",
      "access_token": "expired-api-token",
      "access_token_expires_at": 1_699_999_999_i64,
      "refresh_token": refresh,
      "refresh_url": "https://credentials.example/token"
    }))
    .unwrap()
  });
  let runtime = TestRuntime::default();
  let client = Client::new(
    retry_plan_for(1, 2, "github-copilot"),
    accounts.into(),
    Injected::new(
      MemoryTransport::new(
        &sent,
        [
          Reply {
            status: 503,
            content_type: "application/json",
            bytes: br#"{"error":"try another account"}"#,
          },
          Reply::json(br#"{"token":"fallback-api-token","expires_at":1700003600}"#),
          Reply::json(CHAT_JSON),
        ],
      )
      .with_methods(["GET", "GET", "POST"]),
      runtime.clone(),
    ),
  )
  .unwrap();
  let response = run(
    client
      .chat_completions()
      .create_with(&chat(), RequestOptions::default().with_request_id("refresh-retry")),
  )
  .expect("recoverable refresh failure retries the full send with another account");
  assert_eq!(response.data.id.as_deref(), Some("chat-test"));
  let requests = sent.borrow();
  assert_eq!(requests.len(), 3);
  for (request, expected) in requests.iter().zip([
    "token github-primary",
    "token github-backup",
    "Bearer fallback-api-token",
  ]) {
    assert_eq!(request.headers.get("authorization").unwrap().as_str(), expected);
  }
  let state = runtime.0.borrow();
  assert_eq!(state.sleeps, [Duration::from_millis(2)]);
  let completed: Vec<_> = state
    .events
    .iter()
    .filter(|event| event.stage == "completed")
    .map(|event| (event.request_id.as_str(), event.attempt, event.success))
    .collect();
  assert_eq!(
    completed,
    [("refresh-retry", 0, Some(false)), ("refresh-retry", 1, Some(true))]
  );
}

/// All three native API formats use the configured Client and shared provider preparation.
pub fn buffered_round_trip() {
  let sent = RefCell::new(Vec::new());
  let client = make_client(
    &sent,
    [
      Reply::json(CHAT_JSON),
      Reply::json(RESPONSES_JSON),
      Reply::json(MESSAGES_JSON),
    ],
    "github-copilot",
  );
  assert_eq!(
    run(client.chat_completions().create(&chat()))
      .unwrap()
      .data
      .id
      .as_deref(),
    Some("chat-test")
  );
  assert_eq!(
    run(client.responses().create(&responses())).unwrap().data.id.as_deref(),
    Some("resp-test")
  );
  assert_eq!(
    run(client.messages().create(&messages())).unwrap().data.id.as_deref(),
    Some("msg-test")
  );
  let sent = sent.borrow();
  assert_eq!(sent.len(), 3);
  for (request, path) in sent.iter().zip(["chat/completions", "responses", "v1/messages"]) {
    assert_eq!(request.url, alloc::format!("https://example.com/v1/{path}"));
    assert_eq!(
      request.headers.get("authorization").unwrap().as_str(),
      "Bearer test-key"
    );
    assert_eq!(serde_json::from_slice::<Value>(&request.body).unwrap()["stream"], false);
  }
}

/// A local, borrowing transport streams each format across suspension points.
pub fn streaming_round_trip() {
  let sent = RefCell::new(Vec::new());
  let client = make_client(
    &sent,
    [
      Reply::sse(CHAT_SSE),
      Reply::sse(RESPONSES_SSE),
      Reply::sse(MESSAGES_SSE),
    ],
    "github-copilot",
  );
  {
    let endpoint = client.chat_completions();
    let request = chat();
    let mut events = run(endpoint.stream(&request)).unwrap();
    assert_eq!(events.status, 200);
    assert_eq!(
      events.headers.get("content-type").unwrap().as_str(),
      "text/event-stream"
    );
    let delta = run(events.next_event()).unwrap().unwrap();
    assert_eq!(delta.json.unwrap()["choices"][0]["delta"]["content"], "héllo 🌍");
    assert!(run(events.next_event()).unwrap().is_some());
    assert!(run(events.next_event()).unwrap().unwrap().is_done());
    assert!(run(events.next_event()).unwrap().is_none());
  }
  {
    let endpoint = client.responses();
    let request = responses();
    let mut events = run(endpoint.stream(&request)).unwrap();
    assert_eq!(events.status, 200);
    assert_eq!(
      events.headers.get("content-type").unwrap().as_str(),
      "text/event-stream"
    );
    assert_eq!(
      run(events.next_event()).unwrap().unwrap().json.unwrap()["delta"],
      "héllo 🌍"
    );
    assert_eq!(
      run(events.next_event()).unwrap().unwrap().event.as_deref(),
      Some("response.completed")
    );
    assert!(run(events.next_event()).unwrap().is_none());
  }
  {
    let endpoint = client.messages();
    let request = messages();
    let mut events = run(endpoint.stream(&request)).unwrap();
    assert_eq!(events.status, 200);
    assert_eq!(
      events.headers.get("content-type").unwrap().as_str(),
      "text/event-stream"
    );
    assert_eq!(
      run(events.next_event()).unwrap().unwrap().json.unwrap()["delta"]["text"],
      "héllo 🌍"
    );
    assert_eq!(
      run(events.next_event()).unwrap().unwrap().event.as_deref(),
      Some("message_stop")
    );
    assert!(run(events.next_event()).unwrap().is_none());
  }
  assert_eq!(sent.borrow().len(), 3);
}

/// A Messages caller can route through an OpenAI Chat upstream using the same converters.
pub fn cross_format_round_trip() {
  let sent = RefCell::new(Vec::new());
  let client = make_client(&sent, [Reply::json(CHAT_JSON), Reply::sse(CHAT_SSE)], "openai");
  let response = run(client.messages().create(&messages())).unwrap();
  let response = serde_json::to_value(response.data).unwrap();
  assert_eq!(response["content"][0]["text"], "hello");
  assert_eq!(response["stop_reason"], "end_turn");
  let endpoint = client.messages();
  let request = messages();
  let mut events = run(endpoint.stream(&request)).unwrap();
  let mut text = String::new();
  let mut terminal = false;
  while let Some(event) = run(events.next_event()).unwrap() {
    if let Some(value) = event.json {
      if let Some(delta) = value["delta"]["text"].as_str() {
        text.push_str(delta);
      }
      terminal |= value["type"] == "message_stop";
    }
  }
  assert_eq!(text, "héllo 🌍");
  assert!(terminal);
  for request in sent.borrow().iter() {
    assert!(request.url.ends_with("/chat/completions"));
    let body: Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["messages"][0]["content"], "hello");
  }
}

/// Buffered responses sniff SSE without relying on a correct Content-Type,
/// while empty bodies remain successful at the untyped execution boundary.
pub fn buffered_content_detection() {
  for content_type in ["", "application/json", "text/event-stream; charset=utf-8"] {
    for endpoint in [tokn_sdk::Endpoint::ChatCompletions, tokn_sdk::Endpoint::Messages] {
      let sent = RefCell::new(Vec::new());
      let client = make_client(
        &sent,
        [Reply {
          status: 200,
          content_type,
          bytes: CHAT_SSE,
        }],
        "openai",
      );
      let response = run(client.execute(
        endpoint,
        serde_json::to_value(messages()).unwrap(),
        RequestOptions::default(),
      ))
      .unwrap();
      let (_, headers, body) = response.into_json::<Value>().unwrap();
      assert_eq!(headers.get("content-type").unwrap().as_str(), "application/json");
      let text = match endpoint {
        tokn_sdk::Endpoint::ChatCompletions => &body["choices"][0]["message"]["content"],
        _ => &body["content"][0]["text"],
      };
      assert_eq!(text, "héllo 🌍");
    }
  }
  for content_type in ["", "application/json"] {
    let sent = RefCell::new(Vec::new());
    let client = make_client(
      &sent,
      [Reply {
        status: 200,
        content_type,
        bytes: &[],
      }],
      "openai",
    );
    let response = run(client.execute(
      tokn_sdk::Endpoint::ChatCompletions,
      serde_json::to_value(chat()).unwrap(),
      RequestOptions::default(),
    ))
    .unwrap();
    assert_eq!(response.status, 200);
    // Empty response bodies are accepted by execution, but have no typed JSON value.
    assert_eq!(response.into_json::<Value>().unwrap_err().stage, "response");
  }

  let sent = RefCell::new(Vec::new());
  let client = make_client(
    &sent,
    [Reply {
      status: 200,
      content_type: "text/plain",
      bytes: b"not JSON or SSE",
    }],
    "openai",
  );
  let error = run(client.execute(
    tokn_sdk::Endpoint::ChatCompletions,
    serde_json::to_value(chat()).unwrap(),
    RequestOptions::default(),
  ))
  .err()
  .expect("managed non-JSON bodies must fail normalization");
  assert_eq!(error.stage, "convert_response");
}

/// Buffered normalization enforces limits before and after conversion, as well
/// as the per-event limit when an SSE body arrives without its expected MIME.
pub fn buffered_response_limits() {
  const MINIMAL_CHAT: &[u8] = br#"{"choices":[{"message":{"role":"assistant","content":""}}]}"#;
  for (reply, endpoint, limits, succeeds) in [
    (
      Reply::json(CHAT_JSON),
      tokn_sdk::Endpoint::ChatCompletions,
      Limits {
        max_response_bytes: CHAT_JSON.len(),
        ..Limits::default()
      },
      true,
    ),
    (
      Reply::json(CHAT_JSON),
      tokn_sdk::Endpoint::ChatCompletions,
      Limits {
        max_response_bytes: CHAT_JSON.len() - 1,
        ..Limits::default()
      },
      false,
    ),
    (
      Reply::json(MINIMAL_CHAT),
      tokn_sdk::Endpoint::Messages,
      Limits::default(),
      true,
    ),
    (
      Reply::json(MINIMAL_CHAT),
      tokn_sdk::Endpoint::Messages,
      Limits {
        max_response_bytes: MINIMAL_CHAT.len(),
        ..Limits::default()
      },
      false,
    ),
    (
      Reply::json(CHAT_SSE),
      tokn_sdk::Endpoint::Messages,
      Limits {
        max_event_bytes: 16,
        ..Limits::default()
      },
      false,
    ),
  ] {
    let sent = RefCell::new(Vec::new());
    let client = make_client(&sent, [reply], "openai").with_limits(limits);
    let result = run(client.execute(
      endpoint,
      serde_json::to_value(messages()).unwrap(),
      RequestOptions::default(),
    ));
    match result {
      Ok(response) => {
        assert!(succeeds, "response limit must reject the body");
        response.into_json::<Value>().unwrap();
      }
      Err(error) => {
        assert!(!succeeds, "valid response was rejected: {error}");
        assert_eq!(error.stage, "convert_response");
        assert!(error.message.contains("limit"));
      }
    }
  }
}

/// Retry timing, fallback tiers and committed affinity all use the configured pool.
pub fn retry_fallback_affinity() {
  let sent = RefCell::new(Vec::new());
  let runtime = TestRuntime::default();
  let trace = runtime.clone();
  let replies = [
    Reply {
      status: 503,
      content_type: "application/json",
      bytes: br#"{"error":{"message":"retry"}}"#,
    },
    Reply::json(CHAT_JSON),
    Reply::json(CHAT_JSON),
  ];
  let client = Client::new(
    retry_plan(1, 2),
    vec![
      account_config("primary", "primary-key", "active"),
      account_config("backup", "backup-key", "fallback"),
    ],
    Injected::new(MemoryTransport::new(&sent, replies), runtime),
  )
  .unwrap();
  let options = RequestOptions::default()
    .with_request_id("retry-request")
    .with_session_id("session");
  run(client.chat_completions().create_with(&chat(), options)).unwrap();
  run(
    client
      .chat_completions()
      .create_with(&chat(), RequestOptions::default().with_session_id("session")),
  )
  .unwrap();
  let sent = sent.borrow();
  let credentials: Vec<_> = sent
    .iter()
    .map(|request| request.headers.get("authorization").unwrap().as_str())
    .collect();
  assert_eq!(
    credentials,
    ["Bearer primary-key", "Bearer backup-key", "Bearer backup-key"]
  );
  assert_eq!(sent[0].headers.get("x-request-id").unwrap().as_str(), "retry-request");
  assert_eq!(sent[1].headers.get("x-request-id").unwrap().as_str(), "retry-request");
  let trace = trace.0.borrow();
  assert_eq!(trace.sleeps, [Duration::from_millis(2)]);
  assert!(trace.events.iter().any(|event| event.request_id == "retry-request"
    && event.attempt == 1
    && event.stage == "completed"
    && event.success == Some(true)));
}

/// Recoverable status and transport failures use the same retry budget and
/// exponential schedule, including immediate retries without timer calls.
pub fn retry_schedule_and_exhaustion() {
  for initial_backoff_ms in [2, 0] {
    let sent = RefCell::new(Vec::new());
    let runtime = TestRuntime::default();
    let trace = runtime.clone();
    let replies = [503, 0, 502].map(|status| Reply {
      status,
      content_type: "application/json",
      bytes: br#"{"error":{"message":"retry exhausted"}}"#,
    });
    let client = Client::new(
      retry_plan(2, initial_backoff_ms),
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("secondary", "secondary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      Injected::new(MemoryTransport::new(&sent, replies), runtime),
    )
    .unwrap();
    let error = run(
      client
        .chat_completions()
        .create_with(&chat(), RequestOptions::default().with_request_id("exhausted-request")),
    )
    .unwrap_err();
    assert_eq!(error.stage, "send");
    assert!(error.recoverable);
    assert!(error.message.contains("upstream 502"));
    let sent = sent.borrow();
    let credentials: Vec<_> = sent
      .iter()
      .map(|request| request.headers.get("authorization").unwrap().as_str())
      .collect();
    assert_eq!(
      credentials,
      ["Bearer primary-key", "Bearer secondary-key", "Bearer backup-key"]
    );
    for request in sent.iter() {
      assert_eq!(request.url, "https://example.com/v1/chat/completions");
      assert_eq!(
        request.headers.get("x-request-id").unwrap().as_str(),
        "exhausted-request"
      );
      assert_eq!(request.body, sent[0].body);
    }
    let trace = trace.0.borrow();
    let expected_sleeps = if initial_backoff_ms == 0 {
      vec![]
    } else {
      vec![Duration::from_millis(2), Duration::from_millis(4)]
    };
    assert_eq!(trace.sleeps, expected_sleeps);
    let failed_attempts: Vec<_> = trace
      .events
      .iter()
      .filter(|event| event.stage == "completed")
      .map(|event| {
        assert_eq!(event.request_id, "exhausted-request");
        assert_eq!(event.success, Some(false));
        event.attempt
      })
      .collect();
    assert_eq!(failed_attempts, [0, 1, 2]);
    drop(trace);
    drop(sent);

    // The final failure still cools its account after consuming the retry
    // budget, leaving no eligible account for the next independent request.
    let error = run(client.chat_completions().create(&chat())).unwrap_err();
    assert_eq!(error.stage, "resolve");
  }
}

/// Transport classification controls cooldown independently of whether replay
/// is enabled or has any remaining budget.
pub fn transport_failure_account_feedback() {
  for (recoverable, retry_budget) in [(false, Some(2)), (true, None)] {
    let sent = RefCell::new(Vec::new());
    let runtime = TestRuntime::default();
    let trace = runtime.clone();
    let plan = retry_budget.map_or_else(|| plan("openai"), |max_retries| retry_plan(max_retries, 2));
    let client = Client::new(
      plan,
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      Injected::new(
        MemoryTransport::new(
          &sent,
          [
            Reply {
              status: 0,
              content_type: "",
              bytes: &[],
            },
            Reply::json(CHAT_JSON),
          ],
        )
        .with_recoverable_errors(recoverable),
        runtime,
      ),
    )
    .unwrap();
    let error = run(client.chat_completions().create(&chat())).unwrap_err();
    assert_eq!(error.stage, "send");
    assert_eq!(error.recoverable, recoverable);
    assert_eq!(sent.borrow().len(), 1);
    assert!(trace.0.borrow().sleeps.is_empty());

    run(client.chat_completions().create(&chat())).unwrap();
    let sent = sent.borrow();
    assert_eq!(sent.len(), 2);
    assert_eq!(
      sent[0].headers.get("authorization").unwrap().as_str(),
      "Bearer primary-key"
    );
    assert_eq!(
      sent[1].headers.get("authorization").unwrap().as_str(),
      if recoverable {
        "Bearer backup-key"
      } else {
        "Bearer primary-key"
      }
    );
  }
}

/// Successful response headers commit affinity and end replay eligibility,
/// even when subsequently reading or decoding the response body fails.
pub fn response_body_failure_does_not_retry() {
  for read_failure in [false, true] {
    let sent = RefCell::new(Vec::new());
    let runtime = TestRuntime::default();
    let trace = runtime.clone();
    let mut transport = MemoryTransport::new(
      &sent,
      [
        Reply::json(if read_failure { CHAT_JSON } else { b"invalid JSON" }),
        Reply::json(CHAT_JSON),
        Reply::json(CHAT_JSON),
      ],
    );
    if read_failure {
      transport = transport.with_read_failure_after(1);
    }
    let client = Client::new(
      retry_plan(2, 2),
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("secondary", "secondary-key", "active"),
      ],
      Injected::new(transport, runtime),
    )
    .unwrap();
    let options = RequestOptions::default().with_session_id("body-failure-session");
    let error = run(client.chat_completions().create_with(&chat(), options.clone())).unwrap_err();
    assert_eq!(error.stage, "convert_response");
    assert!(!error.recoverable);
    assert_eq!(sent.borrow().len(), 1);
    assert!(trace.0.borrow().sleeps.is_empty());

    // Body failure releases the transport and retains the successful account's
    // affinity. A request without that session advances to the other account.
    run(client.chat_completions().create_with(&chat(), options)).unwrap();
    run(client.chat_completions().create(&chat())).unwrap();
    let sent = sent.borrow();
    let credentials: Vec<_> = sent
      .iter()
      .map(|request| request.headers.get("authorization").unwrap().as_str())
      .collect();
    assert_eq!(
      credentials,
      ["Bearer primary-key", "Bearer primary-key", "Bearer secondary-key"]
    );
  }
}

/// EOF and event limits are errors, and dropping a stream releases the borrowed session.
pub fn stream_errors_and_cancellation() {
  // Content sniffing is only for buffered normalization. Live streaming still
  // requires an SSE Content-Type even if its bytes contain a valid SSE stream.
  for content_type in ["", "application/json"] {
    let sent = RefCell::new(Vec::new());
    let client = make_client(
      &sent,
      [Reply {
        status: 200,
        content_type,
        bytes: CHAT_SSE,
      }],
      "openai",
    );
    let error = run(client.chat_completions().stream(&chat()))
      .err()
      .expect("live SSE requires its MIME");
    assert_eq!(error.stage, "convert_response");
  }
  const TRUNCATED: &[u8] = b"data: {\"choices\":[]}\n\n";
  let sent = RefCell::new(Vec::new());
  let client = make_client(&sent, [Reply::sse(TRUNCATED)], "openai");
  let endpoint = client.chat_completions();
  let request = chat();
  let mut events = run(endpoint.stream(&request)).unwrap();
  assert!(run(events.next_event()).unwrap().is_some());
  assert!(run(events.next_event()).is_err());
  assert!(run(events.next_event()).unwrap().is_none());
  drop(events);

  let sent = RefCell::new(Vec::new());
  let bounded = Client::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    Injected::new(
      MemoryTransport::new(&sent, [Reply::sse(CHAT_SSE)]),
      TestRuntime::default(),
    ),
  )
  .unwrap()
  .with_limits(Limits {
    max_event_bytes: 16,
    ..Limits::default()
  });
  let endpoint = bounded.chat_completions();
  let mut events = run(endpoint.stream(&request)).unwrap();
  assert!(run(events.next_event()).is_err());
  drop(events);

  let sent = RefCell::new(Vec::new());
  let client = make_client(&sent, [Reply::sse(CHAT_SSE), Reply::json(CHAT_JSON)], "openai");
  let endpoint = client.chat_completions();
  let mut events = run(endpoint.stream(&request)).unwrap();
  assert!(run(events.next_event()).unwrap().is_some());
  assert!(run(client.chat_completions().create(&request)).is_err());
  drop(events);
  assert!(run(client.chat_completions().create(&request)).is_ok());
  assert_eq!(sent.borrow().len(), 2);
}

fn stream_request(endpoint: Endpoint) -> Value {
  let mut request = match endpoint {
    Endpoint::ChatCompletions => serde_json::to_value(chat()).unwrap(),
    Endpoint::Responses => serde_json::to_value(responses()).unwrap(),
    Endpoint::Messages => serde_json::to_value(messages()).unwrap(),
  };
  request["stream"] = Value::Bool(true);
  request
}

fn assert_completions(runtime: &TestRuntime, expected: &[bool]) {
  let state = runtime.0.borrow();
  let completed: Vec<_> = state
    .events
    .iter()
    .filter(|event| event.stage == "completed")
    .map(|event| event.success.expect("completion includes its outcome"))
    .collect();
  assert_eq!(completed, expected);
}

/// Completion follows delivery of the final event, without another transport
/// read. The same rules hold when one upstream event expands into many outputs.
pub fn streaming_terminal_lifecycle() {
  const CHAT_WITH_USAGE: &[u8] = concat!(
    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
  )
  .as_bytes();
  for (endpoint, driver, bytes) in [
    (Endpoint::ChatCompletions, "github-copilot", CHAT_WITH_USAGE),
    (Endpoint::Responses, "github-copilot", RESPONSES_SSE),
    (Endpoint::Messages, "github-copilot", MESSAGES_SSE),
    (Endpoint::Responses, "deepseek", CHAT_SSE),
    (Endpoint::Messages, "openai", CHAT_SSE),
  ] {
    let sent = RefCell::new(Vec::new());
    let runtime = TestRuntime::default();
    let transport = Rc::new(RefCell::new(TransportTrace::default()));
    let client = Client::new(
      plan(driver),
      vec![account_config("primary", "test-key", "active")],
      Injected::new(
        MemoryTransport::new(&sent, [Reply::sse(bytes)])
          .with_trace(transport.clone())
          .with_read_failure_after(bytes.len()),
        runtime.clone(),
      ),
    )
    .unwrap();
    let mut events = run(client.execute(endpoint, stream_request(endpoint), RequestOptions::default()))
      .unwrap()
      .into_stream()
      .unwrap();
    assert_completions(&runtime, &[]);
    let mut terminal_count = 0;
    let mut saw_usage_after_finish = false;
    while let Some(event) = run(events.next_event()).unwrap() {
      saw_usage_after_finish |= event
        .json
        .as_ref()
        .is_some_and(|value| value["usage"]["total_tokens"] == 3);
      let terminal = match endpoint {
        Endpoint::ChatCompletions => event.is_done(),
        Endpoint::Responses => event
          .json
          .as_ref()
          .is_some_and(|value| value["type"] == "response.completed"),
        Endpoint::Messages => event.json.as_ref().is_some_and(|value| value["type"] == "message_stop"),
      };
      if terminal {
        terminal_count += 1;
        assert_completions(&runtime, &[true]);
      } else {
        assert_completions(&runtime, &[]);
      }
    }
    assert_eq!(terminal_count, 1);
    if endpoint == Endpoint::ChatCompletions {
      assert!(
        saw_usage_after_finish,
        "finish_reason must not discard the later usage frame"
      );
    }
    assert!(run(events.next_event()).unwrap().is_none());
    drop(events);
    assert_completions(&runtime, &[true]);
    assert_eq!(transport.borrow().closed, 1);
    assert!(
      transport.borrow().reads <= bytes.len(),
      "terminal stops before the trailing read fault"
    );
  }
}

/// Body failures are terminal, never retried, and release the local session.
pub fn streaming_failure_lifecycle() {
  const TRUNCATED_CHAT: &[u8] = b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
  const UNTERMINATED: &[u8] = b"data: [DONE]\n";
  const ERROR: &[u8] = b"data: {\"error\":{\"message\":\"failed\"}}\n\n";
  const FAILED: &[u8] = b"event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n";
  const INCOMPLETE: &[u8] = b"event: response.incomplete\ndata: {\"type\":\"response.incomplete\"}\n\n";
  const CANCELLED: &[u8] = b"event: response.cancelled\ndata: {\"type\":\"response.cancelled\"}\n\n";
  const CONFLICT: &[u8] = b"event: content_block_delta\ndata: {\"type\":\"message_stop\"}\n\n";
  for (endpoint, driver, bytes, fail_after) in [
    (Endpoint::ChatCompletions, "openai", TRUNCATED_CHAT, None),
    (Endpoint::ChatCompletions, "openai", UNTERMINATED, None),
    (Endpoint::ChatCompletions, "openai", ERROR, None),
    (Endpoint::Responses, "github-copilot", FAILED, None),
    (Endpoint::Responses, "github-copilot", INCOMPLETE, None),
    (Endpoint::Responses, "github-copilot", CANCELLED, None),
    (Endpoint::Messages, "github-copilot", CONFLICT, None),
    (Endpoint::Messages, "openai", &b"data: {broken}\n\n"[..], None),
    (Endpoint::ChatCompletions, "openai", &b"data: \xff\n\n"[..], None),
    (Endpoint::ChatCompletions, "openai", CHAT_SSE, Some(8)),
  ] {
    let sent = RefCell::new(Vec::new());
    let runtime = TestRuntime::default();
    let trace = Rc::new(RefCell::new(TransportTrace::default()));
    let mut transport =
      MemoryTransport::new(&sent, [Reply::sse(bytes), Reply::json(CHAT_JSON)]).with_trace(trace.clone());
    transport.read_failure_after = fail_after;
    let client = Client::new(
      plan(driver),
      vec![account_config("primary", "test-key", "active")],
      Injected::new(transport, runtime.clone()),
    )
    .unwrap();
    let mut events = run(client.execute(endpoint, stream_request(endpoint), RequestOptions::default()))
      .unwrap()
      .into_stream()
      .unwrap();
    let error = loop {
      match run(events.next_event()) {
        Ok(Some(_)) => assert_completions(&runtime, &[]),
        Ok(None) => panic!("failed stream must surface an error"),
        Err(error) => break error,
      }
    };
    assert_eq!(error.stage, "stream");
    assert!(!error.recoverable);
    assert!(run(events.next_event()).unwrap().is_none());
    drop(events);
    assert_completions(&runtime, &[false]);
    assert_eq!(trace.borrow().closed, 1);
    assert_eq!(sent.borrow().len(), 1);
    assert!(runtime.0.borrow().sleeps.is_empty());
    run(client.chat_completions().create(&chat())).expect("failed stream releases its transport lease");
    assert_eq!(trace.borrow().closed, 2);
  }
}

/// Cancelling one pending pull is resumable. Cancelling or dropping its owning
/// stream discards pending outputs and reports one failed completion.
pub fn streaming_cancellation_lifecycle() {
  for cancel_explicitly in [false, true] {
    for stop_at in 0..4 {
      let sent = RefCell::new(Vec::new());
      let runtime = TestRuntime::default();
      let trace = Rc::new(RefCell::new(TransportTrace::default()));
      let (endpoint, bytes) = if stop_at == 2 {
        (Endpoint::Messages, &b"data: [DONE]\n\n"[..])
      } else {
        (Endpoint::ChatCompletions, CHAT_SSE)
      };
      let client = Client::new(
        plan("openai"),
        vec![account_config("primary", "test-key", "active")],
        Injected::new(
          MemoryTransport::new(&sent, [Reply::sse(bytes), Reply::json(CHAT_JSON)]).with_trace(trace.clone()),
          runtime.clone(),
        ),
      )
      .unwrap();
      let mut events = run(client.execute(endpoint, stream_request(endpoint), RequestOptions::default()))
        .unwrap()
        .into_stream()
        .unwrap();
      match stop_at {
        0 => {}
        1 | 2 => {
          let first = run(events.next_event()).unwrap().unwrap();
          assert!(!first.is_done());
          assert!(first.json.is_some_and(|value| value["type"] != "message_stop"));
        }
        _ => {
          let mut pull = pin!(events.next_event());
          for _ in 0..3 {
            assert!(pull.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
          }
        }
      }
      assert_completions(&runtime, &[]);
      if cancel_explicitly {
        events.cancel();
        events.cancel();
        assert!(run(events.next_event()).unwrap().is_none());
        assert_completions(&runtime, &[false]);
      }
      drop(events);
      assert_completions(&runtime, &[false]);
      assert_eq!(trace.borrow().closed, 1);
      run(client.chat_completions().create(&chat())).expect("cancelled stream releases its transport lease");
      assert_eq!(trace.borrow().closed, 2);
    }
  }

  let sent = RefCell::new(Vec::new());
  let runtime = TestRuntime::default();
  let trace = Rc::new(RefCell::new(TransportTrace::default()));
  let client = Client::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    Injected::new(
      MemoryTransport::new(&sent, [Reply::sse(CHAT_SSE)]).with_trace(trace.clone()),
      runtime.clone(),
    ),
  )
  .unwrap();
  let endpoint = client.chat_completions();
  let request = chat();
  let mut events = run(endpoint.stream(&request)).unwrap();
  {
    let mut pull = pin!(events.next_event());
    for _ in 0..3 {
      assert!(pull.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
  }
  assert_completions(&runtime, &[]);
  assert_eq!(trace.borrow().closed, 0);
  let first = run(events.next_event()).unwrap().unwrap();
  assert_eq!(first.json.unwrap()["choices"][0]["delta"]["content"], "héllo 🌍");
  while run(events.next_event()).unwrap().is_some() {}
  drop(events);
  assert_completions(&runtime, &[true]);
  assert_eq!(trace.borrow().closed, 1);
}

/// Provider defaults, per-model operation rules and explicit wire identities
/// retain the same meaning with an injected platform.
pub fn configured_provider_policies() {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"github-copilot"}}
  }))
  .unwrap();
  let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
  let mut account = account_config("primary", "test-key", "active");
  account.provider = "github-copilot".into();
  account.base_url = Some("https://wrong-account-destination.example/v1".into());
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    plan,
    vec![account],
    Injected::new(
      MemoryTransport::new(&sent, [Reply::json(RESPONSES_JSON)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let request =
    serde_json::from_value(json!({"model":"gpt-5","messages":[{"role":"user","content":"hello"}]})).unwrap();
  let response = run(client.chat_completions().create(&request)).unwrap();
  assert_eq!(
    serde_json::to_value(response.data).unwrap()["choices"][0]["message"]["content"],
    "hello"
  );
  let sent = sent.borrow();
  assert_eq!(sent[0].url, "https://api.githubcopilot.com/responses");
  assert_eq!(
    sent[0].headers.get("authorization").unwrap().as_str(),
    "Bearer test-key"
  );
  assert!(sent[0]
    .headers
    .get("user-agent")
    .unwrap()
    .as_str()
    .starts_with("copilot/"));
  drop(sent);

  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":{"named":"cline"}},
    "providers":{"upstream":{"driver":"deepseek","base_url":"https://example.com/v1"}}
  }))
  .unwrap();
  let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    plan,
    vec![account_config("primary", "test-key", "active")],
    Injected::new(
      MemoryTransport::new(&sent, [Reply::json(CHAT_JSON)]),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  run(client.chat_completions().create(&chat())).unwrap();
  assert!(sent.borrow()[0]
    .headers
    .get("user-agent")
    .unwrap()
    .as_str()
    .to_ascii_lowercase()
    .contains("cline"));
}

/// Fixed account relays preserve payloads and honor the configured POST replay
/// policy. Managed provider transforms must not mutate an opaque relay body.
pub fn configured_relay_policies() {
  for (retry_kind, request_count) in [("buffered", 2), ("safe_methods", 1), ("never", 1)] {
    let retry = if retry_kind == "never" {
      json!({"kind":"never"})
    } else {
      json!({"kind":retry_kind,"policy":"retry"})
    };
    let raw=serde_json::from_value(json!({
      "schema_version":2,
      "profiles":{"default":{"route":"relay","wire_identity":"none"}},
      "routes":{"relay":{"kind":"relay","destination":{"kind":"fixed_provider","provider":"upstream"},"credentials":{"kind":"account_pool"},"retry":retry}},
      "retry_policies":{"retry":{"max_retries":1,"initial_backoff_ms":1}},
      "providers":{"upstream":{"driver":"deepseek","base_url":"https://example.com/v1"}}
    })).unwrap();
    let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
    let sent = RefCell::new(Vec::new());
    let replies = [
      Reply {
        status: 503,
        content_type: "application/json",
        bytes: br#"{"error":{"message":"retry"}}"#,
      },
      Reply::json(CHAT_JSON),
    ];
    let client = Client::new(
      plan,
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      Injected::new(MemoryTransport::new(&sent, replies), TestRuntime::default()),
    )
    .unwrap();
    let body = json!({"model":"namespace/unchanged-model","messages":[{"role":"user","content":"hello"}],"reasoning":{"effort":"high"},"stream":false,"vendor_field":{"preserve":true}});
    let response = run(client.execute(
      tokn_sdk::Endpoint::ChatCompletions,
      body.clone(),
      RequestOptions::default(),
    ));
    assert_eq!(response.is_ok(), retry_kind == "buffered");
    assert_eq!(sent.borrow().len(), request_count);
    for request in sent.borrow().iter() {
      let payload: Value = serde_json::from_slice(&request.body).unwrap();
      assert_eq!(payload, body, "opaque relay payload must survive provider preparation");
      assert!(request.url.ends_with("/chat/completions"));
    }
  }
}

/// A fixed client-credential relay runs without accounts, preserving caller
/// credentials and payloads. Only explicitly buffered POSTs may retry transport failures.
pub fn configured_client_relay() {
  for (retry_kind, request_count) in [("buffered", 2), ("safe_methods", 1), ("never", 1)] {
    let retry = if retry_kind == "never" {
      json!({"kind":"never"})
    } else {
      json!({"kind":retry_kind,"policy":"retry"})
    };
    let raw=serde_json::from_value(json!({
      "schema_version":2,
      "profiles":{"default":{"route":"relay"}},
      "routes":{"relay":{"kind":"relay","destination":{"kind":"fixed_provider","provider":"upstream"},"credentials":{"kind":"client"},"retry":retry}},
      "retry_policies":{"retry":{"max_retries":1,"initial_backoff_ms":1}},
      "providers":{"upstream":{"driver":"openai","base_url":"https://example.com/v1"}}
    })).unwrap();
    let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
    assert!(plan.account_pools().is_empty());
    let sent = RefCell::new(Vec::new());
    let replies = [
      Reply {
        status: 0,
        content_type: "",
        bytes: &[],
      },
      Reply::json(CHAT_JSON),
    ];
    let client = Client::new(
      plan,
      Vec::new(),
      Injected::new(MemoryTransport::new(&sent, replies), TestRuntime::default()),
    )
    .unwrap();
    let body = json!({"model":"namespace/unchanged","messages":[{"role":"user","content":"hello"}],"vendor_field":{"untouched":true},"stream":false});
    let options = RequestOptions::default()
      .with_header("Authorization", "Bearer caller-token")
      .with_header("x-api-key", "caller-api-key")
      .with_header("x-vendor", "preserve")
      .with_header("Host", "wrong.example")
      .with_header("Content-Length", "1")
      .with_header("x-tokn-router-private", "strip");
    let response = run(client.execute(tokn_sdk::Endpoint::ChatCompletions, body.clone(), options));
    assert_eq!(response.is_ok(), retry_kind == "buffered");
    assert_eq!(sent.borrow().len(), request_count);
    for request in sent.borrow().iter() {
      assert_eq!(
        request.headers.get("authorization").unwrap().as_str(),
        "Bearer caller-token"
      );
      assert_eq!(request.headers.get("x-api-key").unwrap().as_str(), "caller-api-key");
      assert_eq!(request.headers.get("x-vendor").unwrap().as_str(), "preserve");
      for stripped in ["host", "content-length", "x-tokn-router-private"] {
        assert!(!request.headers.contains_key(stripped));
      }
      assert_eq!(serde_json::from_slice::<Value>(&request.body).unwrap(), body);
    }
  }
  // HTTP errors remain responses on a transparent client-credential relay.
  let raw=serde_json::from_value(json!({
    "schema_version":2,
    "profiles":{"default":{"route":"relay"}},
    "routes":{"relay":{"kind":"relay","destination":{"kind":"fixed_provider","provider":"upstream"},"credentials":{"kind":"client"},"retry":{"kind":"buffered","policy":"retry"}}},
    "retry_policies":{"retry":{"max_retries":1,"initial_backoff_ms":1}},
    "providers":{"upstream":{"driver":"openai","base_url":"https://example.com/v1"}}
  })).unwrap();
  let sent = RefCell::new(Vec::new());
  let client = Client::new(
    tokn_sdk::config::v2::compile_execution(&raw).unwrap(),
    Vec::new(),
    Injected::new(
      MemoryTransport::new(
        &sent,
        [
          Reply {
            status: 503,
            content_type: "text/plain",
            bytes: b"opaque upstream failure",
          },
          Reply::sse(CHAT_SSE),
        ],
      ),
      TestRuntime::default(),
    ),
  )
  .unwrap();
  let response = run(client.execute(
    tokn_sdk::Endpoint::ChatCompletions,
    json!({"model":"model","stream":false}),
    RequestOptions::default(),
  ))
  .unwrap();
  assert_eq!(response.status, 503);
  assert_eq!(response.headers.get("content-type").unwrap().as_str(), "text/plain");
  assert_eq!(sent.borrow().len(), 1);
  assert_eq!(response.into_json::<Value>().unwrap_err().stage, "response");

  // Opaque buffered SSE also bypasses normalization: callers receive its
  // original media type and cannot decode its wire events as a JSON response.
  let response = run(client.execute(
    tokn_sdk::Endpoint::ChatCompletions,
    json!({"model":"model","stream":false}),
    RequestOptions::default(),
  ))
  .unwrap();
  assert_eq!(
    response.headers.get("content-type").unwrap().as_str(),
    "text/event-stream; charset=utf-8"
  );
  assert_eq!(response.into_json::<Value>().unwrap_err().stage, "response");
  assert_eq!(sent.borrow().len(), 2);
}

/// Managed status classification matches the standard pipeline: 429 is
/// permanent, all 5xx statuses are recoverable, and redirects reach decoding.
pub fn configured_status_policy() {
  for (status, expected_sends) in [(429, 1), (501, 2), (302, 1)] {
    let sent = RefCell::new(Vec::new());
    let replies = [
      Reply {
        status,
        content_type: "application/json",
        bytes: if status == 302 {
          CHAT_JSON
        } else {
          br#"{"error":{"message":"retry"}}"#
        },
      },
      Reply::json(CHAT_JSON),
      Reply::json(CHAT_JSON),
    ];
    let client = Client::new(
      retry_plan(1, 1),
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      Injected::new(MemoryTransport::new(&sent, replies), TestRuntime::default()),
    )
    .unwrap();
    let first = run(client.chat_completions().create(&chat()));
    assert_eq!(first.is_ok(), status != 429);
    assert_eq!(sent.borrow().len(), expected_sends);
    if status == 429 {
      // A permanent failure does not cool the binding as a recoverable send failure would.
      run(client.chat_completions().create(&chat())).unwrap();
      assert_eq!(
        sent.borrow()[1].headers.get("authorization").unwrap().as_str(),
        "Bearer primary-key"
      );
    }
  }
}

/// Compression negotiation follows the transport's decoder capability rather
/// than its impersonated client headers; returned metadata describes decoded bytes.
pub fn configured_transport_encoding() {
  let sent = RefCell::new(Vec::new());
  let transport = MemoryTransport::new(&sent, [Reply::json(CHAT_JSON)]).with_encoding("gzip");
  let client = Client::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    Injected::new(transport, TestRuntime::default()),
  )
  .unwrap();
  let response = run(client.chat_completions().create(&chat())).unwrap();
  assert!(!response.headers.contains_key("content-encoding"));
  assert!(!response.headers.contains_key("content-length"));
  assert_eq!(
    sent.borrow()[0].headers.get("accept-encoding").unwrap().as_str(),
    "gzip"
  );
}
