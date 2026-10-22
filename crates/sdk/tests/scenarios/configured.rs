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
  Client, Injected, Lifecycle, Limits, RequestOptions, Runtime,
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
}
impl<'a> MemoryTransport<'a> {
  fn with_encoding(mut self, encoding: &'static str) -> Self {
    self.encoding = encoding;
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
    assert_eq!(request.method, "POST");
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
    head.headers.insert("content-type", reply.content_type);
    if self.encoding != "identity" {
      head.headers.insert("content-encoding", self.encoding);
      head.headers.insert("content-length", "1");
    }
    Ok(head)
  }
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    poll_fn(|context| {
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
    true
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
fn plan(driver: &str) -> tokn_sdk::policy::ExecutionPlan {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none"},
    "providers":{"upstream":{"driver":driver,"base_url":"https://example.com/v1"}}
  }))
  .unwrap();
  tokn_sdk::config::v2::compile_execution(&raw).unwrap()
}
fn retry_plan(max_retries: u32, initial_backoff_ms: u64) -> tokn_sdk::policy::ExecutionPlan {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none","retry":{"kind":"recoverable","policy":"retry"}},
    "retry_policies":{"retry":{"max_retries":max_retries,"initial_backoff_ms":initial_backoff_ms}},
    "providers":{"upstream":{"driver":"openai","base_url":"https://example.com/v1"}}
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
  Client::from_platform(
    Injected::new(
      plan(driver),
      vec![account_config("primary", "test-key", "active")],
      MemoryTransport::new(requests, replies),
      TestRuntime::default(),
    )
    .unwrap(),
  )
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
  let client = Client::from_platform(
    Injected::new(
      retry_plan(1, 2),
      vec![
        account_config("primary", "primary-key", "active"),
        account_config("backup", "backup-key", "fallback"),
      ],
      MemoryTransport::new(&sent, replies),
      runtime,
    )
    .unwrap(),
  );
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
    let client = Client::from_platform(
      Injected::new(
        retry_plan(2, initial_backoff_ms),
        vec![
          account_config("primary", "primary-key", "active"),
          account_config("secondary", "secondary-key", "active"),
          account_config("backup", "backup-key", "fallback"),
        ],
        MemoryTransport::new(&sent, replies),
        runtime,
      )
      .unwrap(),
    );
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
  }
}

/// A successful send ends replay eligibility even when response decoding fails.
pub fn response_decode_failure_does_not_retry() {
  let sent = RefCell::new(Vec::new());
  let runtime = TestRuntime::default();
  let trace = runtime.clone();
  let client = Client::from_platform(
    Injected::new(
      retry_plan(2, 2),
      vec![account_config("primary", "primary-key", "active")],
      MemoryTransport::new(&sent, [Reply::json(b"invalid JSON"), Reply::json(CHAT_JSON)]),
      runtime,
    )
    .unwrap(),
  );
  let error = run(client.chat_completions().create(&chat())).unwrap_err();
  assert_eq!(error.stage, "convert_response");
  assert!(!error.recoverable);
  assert_eq!(sent.borrow().len(), 1);
  assert!(trace.0.borrow().sleeps.is_empty());

  // The failed response releases the transport and does not cool the account.
  run(client.chat_completions().create(&chat())).unwrap();
  assert_eq!(sent.borrow().len(), 2);
  assert_eq!(
    sent.borrow()[1].headers.get("authorization").unwrap().as_str(),
    "Bearer primary-key"
  );
}

/// EOF and event limits are errors, and dropping a stream releases the borrowed session.
pub fn stream_errors_and_cancellation() {
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
  let platform = Injected::new(
    plan("openai"),
    vec![account_config("primary", "test-key", "active")],
    MemoryTransport::new(&sent, [Reply::sse(CHAT_SSE)]),
    TestRuntime::default(),
  )
  .unwrap()
  .with_limits(Limits {
    max_event_bytes: 16,
    ..Limits::default()
  });
  let bounded = Client::from_platform(platform);
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
  let client = Client::from_platform(
    Injected::new(
      plan,
      vec![account],
      MemoryTransport::new(&sent, [Reply::json(RESPONSES_JSON)]),
      TestRuntime::default(),
    )
    .unwrap(),
  );
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
  let client = Client::from_platform(
    Injected::new(
      plan,
      vec![account_config("primary", "test-key", "active")],
      MemoryTransport::new(&sent, [Reply::json(CHAT_JSON)]),
      TestRuntime::default(),
    )
    .unwrap(),
  );
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
    let client = Client::from_platform(
      Injected::new(
        plan,
        vec![
          account_config("primary", "primary-key", "active"),
          account_config("backup", "backup-key", "fallback"),
        ],
        MemoryTransport::new(&sent, replies),
        TestRuntime::default(),
      )
      .unwrap(),
    );
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
    let client = Client::from_platform(
      Injected::new(
        plan,
        Vec::new(),
        MemoryTransport::new(&sent, replies),
        TestRuntime::default(),
      )
      .unwrap(),
    );
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
  let client = Client::from_platform(
    Injected::new(
      tokn_sdk::config::v2::compile_execution(&raw).unwrap(),
      Vec::new(),
      MemoryTransport::new(
        &sent,
        [Reply {
          status: 503,
          content_type: "text/plain",
          bytes: b"opaque upstream failure",
        }],
      ),
      TestRuntime::default(),
    )
    .unwrap(),
  );
  let response = run(client.execute(
    tokn_sdk::Endpoint::ChatCompletions,
    json!({"model":"model","stream":false}),
    RequestOptions::default(),
  ))
  .unwrap();
  assert_eq!(response.status, 503);
  assert_eq!(response.headers.get("content-type").unwrap().as_str(), "text/plain");
  assert_eq!(sent.borrow().len(), 1);
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
    let client = Client::from_platform(
      Injected::new(
        retry_plan(1, 1),
        vec![
          account_config("primary", "primary-key", "active"),
          account_config("backup", "backup-key", "fallback"),
        ],
        MemoryTransport::new(&sent, replies),
        TestRuntime::default(),
      )
      .unwrap(),
    );
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
  let client = Client::from_platform(
    Injected::new(
      plan("openai"),
      vec![account_config("primary", "test-key", "active")],
      transport,
      TestRuntime::default(),
    )
    .unwrap(),
  );
  let response = run(client.chat_completions().create(&chat())).unwrap();
  assert!(!response.headers.contains_key("content-encoding"));
  assert!(!response.headers.contains_key("content-length"));
  assert_eq!(
    sent.borrow()[0].headers.get("accept-encoding").unwrap().as_str(),
    "gzip"
  );
}
