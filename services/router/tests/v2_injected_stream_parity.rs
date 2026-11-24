//! Live managed streams share event conversion and completion semantics across
//! the HTTP gateway and the alloc-only injected transport adapter.

use axum::{
  body::Body,
  extract::State,
  http::{Request as HttpRequest, Response as HttpResponse, Uri},
  routing::post,
  Router,
};
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use std::{
  cell::RefCell,
  convert::Infallible,
  path::Path,
  rc::Rc,
  sync::{Arc, Mutex},
  time::Duration,
};
use tokn_access::AccessStore;
use tokn_config::v2::{compile, compile_execution, RawConfig};
use tokn_convert::sse::{Decoder, SseEvent};
use tokn_core::{
  account::AccountConfig,
  event::{Event, EventBus},
  request_event::{RequestEventPayload, StageEvent},
  Endpoint,
};
use tokn_requests::configured::{Lifecycle, RequestOptions, ResponseBody, Runtime};
use tokn_service::transport::{Request, ResponseHead, Transport};
use tower::ServiceExt;

#[path = "../../requests/tests/support/injected.rs"]
mod injected_fixture;
use injected_fixture::BorrowedSession;

const ENDPOINTS: [Endpoint; 3] = [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];
const TEXT: &str = "héllo 🌍";
const CREATED_AT: u64 = 1_700_000_000;
const TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Reply {
  bytes: Vec<u8>,
  hold_open: bool,
  chunk_size: usize,
}

struct Upstream {
  reply: Reply,
  paths: Mutex<Vec<String>>,
}

async fn upstream(State(state): State<Arc<Upstream>>, uri: Uri) -> HttpResponse<Body> {
  state.paths.lock().unwrap().push(uri.path().to_owned());
  let chunks: Vec<_> = state
    .reply
    .bytes
    .chunks(state.reply.chunk_size)
    .map(|chunk| Ok::<_, Infallible>(Bytes::copy_from_slice(chunk)))
    .collect();
  let chunks = stream::iter(chunks);
  let body = if state.reply.hold_open {
    Body::from_stream(chunks.chain(stream::pending()))
  } else {
    Body::from_stream(chunks)
  };
  HttpResponse::builder()
    .header("content-type", "text/event-stream; charset=utf-8")
    .body(body)
    .unwrap()
}

struct MemoryTransport {
  reply: Reply,
  position: usize,
  paths: Rc<RefCell<Vec<String>>>,
}

impl Transport for MemoryTransport {
  type Error = Infallible;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    self
      .paths
      .borrow_mut()
      .push(reqwest::Url::parse(&request.url).unwrap().path().to_owned());
    self.position = 0;
    let mut headers = tokn_core::headers::HeaderMap::new();
    headers.insert("content-type", "text/event-stream; charset=utf-8");
    Ok(ResponseHead { status: 200, headers })
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    if self.position == self.reply.bytes.len() && self.reply.hold_open {
      return std::future::pending().await;
    }
    let count = buffer
      .len()
      .min(self.reply.chunk_size)
      .min(self.reply.bytes.len() - self.position);
    buffer[..count].copy_from_slice(&self.reply.bytes[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }
}

struct TestRuntime(Rc<RefCell<Vec<bool>>>);
impl Runtime for TestRuntime {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    CREATED_AT
  }
  async fn sleep(&mut self, _: Duration) {
    panic!("response processing must never retry")
  }
  fn observe(&mut self, event: Lifecycle<'_>) {
    if event.stage == "completed" {
      self.0.borrow_mut().push(event.success.unwrap());
    }
  }
}

struct Fixture {
  app: Router,
  injected: BorrowedSession<MemoryTransport, TestRuntime>,
  standard: Arc<Upstream>,
  injected_paths: Rc<RefCell<Vec<String>>>,
  injected_completions: Rc<RefCell<Vec<bool>>>,
  events: Arc<EventBus>,
  receiver: tokio::sync::broadcast::Receiver<Arc<Event>>,
  server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
  fn drop(&mut self) {
    self.server.abort();
  }
}

impl Fixture {
  async fn new(bytes: Vec<u8>, hold_open: bool, chunk_size: usize) -> Self {
    let reply = Reply {
      bytes,
      hold_open,
      chunk_size,
    };
    let standard = Arc::new(Upstream {
      reply: reply.clone(),
      paths: Mutex::new(Vec::new()),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upstream = Router::new()
      .route("/{*path}", post(upstream))
      .with_state(standard.clone());
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let raw: RawConfig = serde_json::from_value(json!({
      "schema_version": 2,
      "listeners": {"api": {"kind": "llm_api", "bind": "127.0.0.1:4141", "client_auth": "none"}},
      "providers": {"upstream": {"driver": "opencode-go", "base_url": format!("http://{address}/v1")}},
      "profiles": {"default": {"route": "route", "wire_identity": "none"}},
      "routes": {"route": {
        "kind": "managed", "provider": {"kind": "fixed", "provider": "upstream"},
        "model": {"kind": "capability"}, "operation": "translate_compatible",
        "retry": {"kind": "recoverable", "policy": "retry"}
      }},
      "retry_policies": {"retry": {"max_retries": 1, "initial_backoff_ms": 1}}
    }))
    .unwrap();
    let accounts: Vec<AccountConfig> = serde_json::from_value(json!([
      {"id": "test", "provider": "upstream", "api_key": "test-key"}
    ]))
    .unwrap();
    let events = Arc::new(EventBus::new(256));
    let receiver = events.subscribe();
    let state = tokn_router::v2::build_states(
      compile(&raw, Path::new("stream-parity.toml")).unwrap(),
      &accounts,
      Arc::new(AccessStore::disabled()),
      events.clone(),
    )
    .unwrap()
    .pop()
    .unwrap();
    let injected_paths = Rc::default();
    let injected_completions = Rc::default();
    let injected = BorrowedSession::new(
      compile_execution(&raw).unwrap(),
      accounts,
      MemoryTransport {
        reply,
        position: 0,
        paths: Rc::clone(&injected_paths),
      },
      TestRuntime(Rc::clone(&injected_completions)),
    )
    .unwrap();
    Self {
      app: tokn_router::v2::router(state),
      injected,
      standard,
      injected_paths,
      injected_completions,
      events,
      receiver,
      server,
    }
  }

  async fn standard_body(&self, upstream: Endpoint, inbound: Endpoint) -> Body {
    let response = self
      .app
      .clone()
      .oneshot(
        HttpRequest::post(format!("/v1/{}", operation(inbound)))
          .header("content-type", "application/json")
          .body(Body::from(request(upstream, inbound).to_string()))
          .unwrap(),
      )
      .await
      .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers()["content-type"]
      .to_str()
      .unwrap()
      .starts_with("text/event-stream"));
    response.into_body()
  }

  async fn assert_completions(&mut self, upstream: Endpoint, success: bool) {
    // Shutdown waits for both the usage tap and response accumulator finalizers.
    // Drain through its barrier so a duplicate/delayed completion cannot escape.
    let mut completions = Vec::new();
    let drain = async {
      loop {
        let event = self.receiver.recv().await.unwrap();
        match &*event {
          Event::Requests(event) => {
            if let RequestEventPayload::Stage(StageEvent::Completed { success, .. }) = event.payload {
              completions.push(success);
            }
          }
          Event::Shutdown(sender) => {
            sender.lock().unwrap().take().unwrap().send(()).unwrap();
            break;
          }
          _ => {}
        }
      }
    };
    tokio::time::timeout(TIMEOUT, async { tokio::join!(self.events.shutdown(), drain) })
      .await
      .expect("stream finalizers must stop");
    assert_eq!(completions, [success], "std completes exactly once");
    assert_eq!(
      *self.injected_completions.borrow(),
      [success],
      "injected completes exactly once"
    );
    assert_eq!(self.standard.paths.lock().unwrap().len(), 1, "std must not retry");
    assert_eq!(self.injected_paths.borrow().len(), 1, "injected must not retry");
    let expected = vec![format!("/v1/{}", operation(upstream))];
    assert_eq!(*self.standard.paths.lock().unwrap(), expected);
    assert_eq!(*self.injected_paths.borrow(), expected);
  }

  async fn compare(&mut self, upstream: Endpoint, inbound: Endpoint, success: bool) -> Vec<SseEvent> {
    let mut standard = self.standard_body(upstream, inbound).await.into_data_stream();
    let (bytes, errors) = tokio::time::timeout(TIMEOUT, async {
      let mut bytes = Vec::new();
      let mut errors = 0;
      while let Some(item) = standard.next().await {
        match item {
          Ok(chunk) => bytes.extend(chunk),
          Err(_) => errors += 1,
        }
      }
      (bytes, errors)
    })
    .await
    .expect("a terminal event closes std without waiting for upstream EOF");
    drop(standard);
    assert_eq!(errors, usize::from(!success), "std body error count");
    let standard_events = decode(&bytes);

    let (injected_events, errors) = {
      let response = self
        .injected
        .execute(inbound, request(upstream, inbound), RequestOptions::default())
        .await
        .unwrap();
      let ResponseBody::Stream(mut injected) = response.body else {
        panic!("request was streaming")
      };
      let (injected_events, errors) = tokio::time::timeout(TIMEOUT, async {
        let mut events = Vec::new();
        let mut errors = 0;
        loop {
          match injected.next_event().await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(error) => {
              assert_eq!(error.stage, "stream");
              assert!(!error.recoverable);
              errors += 1;
            }
          }
        }
        (events, errors)
      })
      .await
      .expect("a terminal event closes injected transport without waiting for EOF");
      drop(injected);
      (injected_events, errors)
    };
    assert_eq!(errors, usize::from(!success), "injected event error count");
    assert_eq!(
      comparable(&standard_events, upstream, inbound),
      comparable(&injected_events, upstream, inbound),
      "{upstream:?} -> {inbound:?} event parity"
    );
    self.assert_completions(upstream, success).await;
    standard_events
  }
}

fn operation(endpoint: Endpoint) -> &'static str {
  match endpoint {
    Endpoint::ChatCompletions => "chat/completions",
    Endpoint::Responses => "responses",
    Endpoint::Messages => "messages",
  }
}

fn request(upstream: Endpoint, inbound: Endpoint) -> Value {
  let model = match upstream {
    Endpoint::ChatCompletions => "test-chat",
    Endpoint::Responses => "gpt-test",
    Endpoint::Messages => "qwen-test",
  };
  match inbound {
    Endpoint::Responses => json!({"model": model, "input": "hello", "stream": true}),
    _ => json!({"model": model, "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16, "stream": true}),
  }
}

fn prefix(endpoint: Endpoint) -> Vec<u8> {
  let events = match endpoint {
    Endpoint::ChatCompletions => vec![json!({
      "id": "response-test", "model": "fixture", "choices": [{"index": 0,
        "delta": {"role": "assistant", "content": TEXT}, "finish_reason": null}]
    })],
    Endpoint::Responses => vec![
      json!({"type": "response.created", "response": {"id": "response-test", "model": "fixture", "created_at": CREATED_AT, "status": "in_progress", "output": []}}),
      json!({"type": "response.output_text.delta", "item_id": "message-test", "output_index": 0, "content_index": 0, "delta": TEXT}),
    ],
    Endpoint::Messages => vec![
      json!({"type": "message_start", "message": {"id": "response-test", "model": "fixture", "role": "assistant", "content": [], "usage": {"input_tokens": 7}}}),
      json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
      json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": TEXT}}),
    ],
  };
  encode(&events)
}

fn terminal(endpoint: Endpoint) -> Vec<u8> {
  match endpoint {
    Endpoint::ChatCompletions => {
      let mut bytes = encode(&[json!({
        "id": "response-test", "model": "fixture", "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}
      })]);
      bytes.extend_from_slice(b"data: [DONE]\r\n\r\n");
      bytes
    }
    Endpoint::Responses => encode(&[json!({"type": "response.completed", "response": {
      "id": "response-test", "model": "fixture", "created_at": CREATED_AT, "status": "completed",
      "output": [{"type": "message", "id": "message-test", "role": "assistant", "content": [{"type": "output_text", "text": TEXT, "annotations": []}]}],
      "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
    }})]),
    Endpoint::Messages => encode(&[
      json!({"type": "content_block_stop", "index": 0}),
      json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 3}}),
      json!({"type": "message_stop"}),
    ]),
  }
}

fn encode(events: &[Value]) -> Vec<u8> {
  let mut bytes = String::new();
  for event in events {
    if let Some(kind) = event["type"].as_str() {
      bytes.push_str(&format!("event: {kind}\r\n"));
    }
    bytes.push_str(&format!("data: {event}\r\n\r\n"));
  }
  bytes.into_bytes()
}

fn decode(bytes: &[u8]) -> Vec<SseEvent> {
  let mut decoder = Decoder::new(usize::MAX);
  bytes
    .iter()
    .filter_map(|&byte| decoder.push(byte).unwrap().map(SseEvent::from))
    .collect()
}

fn comparable(events: &[SseEvent], upstream: Endpoint, inbound: Endpoint) -> Vec<(String, Value)> {
  events
    .iter()
    .map(|event| {
      let mut value = event.json.clone().unwrap_or_else(|| Value::String(event.data.clone()));
      if upstream != inbound && inbound == Endpoint::Responses {
        // Only the Responses renderer's synthesized clock values differ: std
        // uses wall time; the injected runtime supplies the fixed fixture time.
        if let Some(response) = value.get_mut("response").and_then(Value::as_object_mut) {
          for key in ["created_at", "completed_at"] {
            if let Some(timestamp) = response.get_mut(key) {
              assert!(timestamp.is_u64());
              *timestamp = json!(CREATED_AT);
            }
          }
        }
      }
      (event.event.as_deref().unwrap_or("message").to_owned(), value)
    })
    .collect()
}

#[tokio::test]
async fn all_native_pairs_complete_once_at_the_upstream_terminal_without_eof() {
  for upstream in ENDPOINTS {
    let mut bytes = prefix(upstream);
    bytes.extend(terminal(upstream));
    for inbound in ENDPOINTS {
      // Three-byte source chunks split UTF-8, JSON, and SSE delimiters. The
      // source remains open forever after the successful terminal record.
      let mut fixture = Fixture::new(bytes.clone(), true, 3).await;
      let events = fixture.compare(upstream, inbound, true).await;
      assert!(events.iter().any(|event| event.data.contains(TEXT)));
      let mut terminal = tokn_convert::sse::TerminalState::new(inbound);
      for event in events {
        terminal.observe(&event).unwrap();
      }
      terminal.finish().unwrap();
    }
  }
}

fn mixed_tool_stream(endpoint: Endpoint) -> Vec<u8> {
  let events = match endpoint {
    Endpoint::ChatCompletions => vec![
      json!({"id": "response-test", "model": "fixture", "choices": [{"index": 0,
        "delta": {"reasoning_content": "think "}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"reasoning_content": "carefully"}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"content": TEXT}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 4, "id": "call-a", "type": "function", "function": {"name": "lookup", "arguments": "{\"q\":"}},
        {"index": 11, "id": "call-b", "type": "function", "function": {"name": "sum", "arguments": "{\"a\":"}}
      ]}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {"tool_calls": [
        {"index": 11, "function": {"arguments": "2}"}},
        {"index": 4, "function": {"arguments": "\"x\"}"}}
      ]}, "finish_reason": null}]}),
      json!({"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls"}]}),
      // Chat can report final usage after the semantic finish reason.
      json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}}),
    ],
    Endpoint::Responses => vec![
      json!({"type": "response.created", "response": {"id": "response-test", "model": "fixture", "status": "in_progress", "output": []}}),
      json!({"type": "response.reasoning_text.delta", "item_id": "reasoning-test", "output_index": 0, "content_index": 0, "delta": "think "}),
      json!({"type": "response.reasoning_text.delta", "item_id": "reasoning-test", "output_index": 0, "content_index": 0, "delta": "carefully"}),
      json!({"type": "response.output_text.delta", "item_id": "message-test", "output_index": 1, "content_index": 0, "delta": TEXT}),
      json!({"type": "response.output_item.added", "output_index": 4,
        "item": {"type": "function_call", "id": "item-a", "call_id": "call-a", "name": "lookup", "arguments": ""}}),
      json!({"type": "response.output_item.added", "output_index": 11,
        "item": {"type": "function_call", "id": "item-b", "call_id": "call-b", "name": "sum", "arguments": ""}}),
      json!({"type": "response.function_call_arguments.delta", "output_index": 4, "item_id": "item-a", "delta": "{\"q\":"}),
      json!({"type": "response.function_call_arguments.delta", "output_index": 11, "item_id": "item-b", "delta": "{\"a\":"}),
      json!({"type": "response.function_call_arguments.delta", "output_index": 11, "item_id": "item-b", "delta": "2}"}),
      json!({"type": "response.function_call_arguments.delta", "output_index": 4, "item_id": "item-a", "delta": "\"x\"}"}),
      json!({"type": "response.completed", "response": {
        "id": "response-test", "model": "fixture", "status": "completed",
        "output": [
          {"type": "reasoning", "id": "reasoning-test", "content": [{"type": "reasoning_text", "text": "think carefully"}]},
          {"type": "message", "id": "message-test", "role": "assistant", "content": [{"type": "output_text", "text": TEXT}]},
          {"type": "function_call", "id": "item-a", "call_id": "call-a", "name": "lookup", "arguments": "{\"q\":\"x\"}"},
          {"type": "function_call", "id": "item-b", "call_id": "call-b", "name": "sum", "arguments": "{\"a\":2}"}
        ],
        "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
      }}),
    ],
    Endpoint::Messages => unreachable!("fixture exercises cross-endpoint conversion into Messages"),
  };
  let mut bytes = encode(&events);
  if endpoint == Endpoint::ChatCompletions {
    bytes.extend_from_slice(b"data: [DONE]\r\n\r\n");
  }
  bytes
}

fn assert_messages_block_lifecycle(events: &[SseEvent]) {
  assert_eq!(
    events.first().and_then(|event| event.event.as_deref()),
    Some("message_start")
  );
  let mut blocks = Vec::<Value>::new();
  let mut arguments = Vec::<String>::new();
  let mut active = None;
  let mut message_delta_seen = false;
  let mut stopped = false;
  let mut final_usage = Value::Null;
  let mut stop_reason = Value::Null;
  for (position, event) in events.iter().enumerate() {
    assert!(!stopped, "no events follow message_stop");
    let value = event.json.as_ref().expect("Messages events contain JSON");
    let kind = value["type"].as_str().unwrap();
    assert_eq!(event.event.as_deref(), Some(kind));
    match kind {
      "message_start" => {
        assert_eq!(position, 0, "message_start occurs exactly once at the beginning");
        assert_eq!(value["message"]["content"], json!([]));
        assert_eq!(value["message"]["id"], "response-test");
        assert_eq!(value["message"]["model"], "fixture");
      }
      "content_block_start" => {
        assert!(!message_delta_seen, "content precedes the final message deltas");
        assert!(active.is_none(), "the previous block closes before the next starts");
        let index = value["index"].as_u64().unwrap() as usize;
        assert_eq!(index, blocks.len(), "target block indexes are dense and never reused");
        blocks.push(value["content_block"].clone());
        arguments.push(String::new());
        active = Some(index);
      }
      "content_block_delta" => {
        assert!(!message_delta_seen);
        let index = value["index"].as_u64().unwrap() as usize;
        assert_eq!(active, Some(index), "every delta belongs to its open block");
        let delta = &value["delta"];
        let field = match (blocks[index]["type"].as_str().unwrap(), delta["type"].as_str().unwrap()) {
          ("text", "text_delta") => "text",
          ("thinking", "thinking_delta") => "thinking",
          ("tool_use", "input_json_delta") => {
            arguments[index].push_str(delta["partial_json"].as_str().unwrap());
            continue;
          }
          pair => panic!("delta type must match the open content block: {pair:?}"),
        };
        let mut text = blocks[index][field].as_str().unwrap().to_owned();
        text.push_str(delta[field].as_str().unwrap());
        blocks[index][field] = Value::String(text);
      }
      "content_block_stop" => {
        let index = value["index"].as_u64().unwrap() as usize;
        assert_eq!(active.take(), Some(index), "each started block closes exactly once");
        if blocks[index]["type"] == "tool_use" {
          assert_eq!(
            blocks[index]["input"],
            json!({}),
            "tool input starts with an empty object"
          );
          blocks[index]["input"] = serde_json::from_str(&arguments[index]).expect("complete fixture arguments");
        }
      }
      "message_delta" => {
        assert!(active.is_none(), "all content blocks close before message_delta");
        message_delta_seen = true;
        if let Some(usage) = value.get("usage") {
          final_usage = usage.clone();
        }
        if let Some(reason) = value.pointer("/delta/stop_reason").filter(|reason| !reason.is_null()) {
          stop_reason = reason.clone();
        }
      }
      "message_stop" => {
        assert!(active.is_none());
        assert!(message_delta_seen);
        stopped = true;
      }
      other => panic!("unexpected converted Messages event: {other}"),
    }
  }
  assert!(stopped, "converted stream ends with message_stop");
  assert_eq!(stop_reason, "tool_use");
  assert_eq!(final_usage["input_tokens"], 7);
  assert_eq!(final_usage["output_tokens"], 3);
  assert_eq!(blocks.len(), 4, "no phantom text or tool blocks");
  assert_eq!(blocks[0]["type"], "thinking");
  assert_eq!(blocks[0]["thinking"], "think carefully");
  assert_eq!(blocks[1], json!({"type": "text", "text": TEXT}));
  assert_eq!(
    blocks[2],
    json!({"type": "tool_use", "id": "call-a", "name": "lookup", "input": {"q": "x"}})
  );
  assert_eq!(
    blocks[3],
    json!({"type": "tool_use", "id": "call-b", "name": "sum", "input": {"a": 2}})
  );
}

#[tokio::test]
async fn messages_conversion_preserves_mixed_content_and_interleaved_tools_across_transports() {
  for upstream in [Endpoint::ChatCompletions, Endpoint::Responses] {
    let mut fixture = Fixture::new(mixed_tool_stream(upstream), true, 3).await;
    let events = fixture.compare(upstream, Endpoint::Messages, true).await;
    assert_messages_block_lifecycle(&events);
  }
}

#[tokio::test]
async fn missing_and_failed_terminal_events_fail_once_without_retrying() {
  for upstream in ENDPOINTS {
    let failure = match upstream {
      Endpoint::Responses => {
        json!({"type": "response.failed", "response": {"status": "failed", "error": {"message": "failure"}}})
      }
      _ => json!({"type": "error", "error": {"message": "failure"}}),
    };
    for suffix in [vec![], encode(&[failure])] {
      let hold_open = !suffix.is_empty();
      let mut bytes = prefix(upstream);
      bytes.extend(suffix);
      for inbound in ENDPOINTS {
        let mut fixture = Fixture::new(bytes.clone(), hold_open, 3).await;
        fixture.compare(upstream, inbound, false).await;
      }
    }
  }
}

#[tokio::test]
async fn malformed_records_fail_once_even_when_followed_by_a_success_terminal() {
  for upstream in ENDPOINTS {
    let inbound = if upstream == Endpoint::ChatCompletions {
      Endpoint::Messages
    } else {
      Endpoint::ChatCompletions
    };
    for malformed in [b"data: not-json\n\n".as_slice(), b"data: \xff\n\n".as_slice()] {
      let mut bytes = prefix(upstream);
      bytes.extend_from_slice(malformed);
      bytes.extend(terminal(upstream));
      let mut fixture = Fixture::new(bytes, true, 3).await;
      fixture.compare(upstream, inbound, false).await;
    }
  }
  let mut bytes = prefix(Endpoint::Responses);
  bytes.extend_from_slice(b"event: response.failed\ndata: {\"type\":\"response.completed\"}\n\n");
  let mut fixture = Fixture::new(bytes, true, 3).await;
  fixture.compare(Endpoint::Responses, Endpoint::Messages, false).await;
}

#[tokio::test]
async fn cancellation_does_not_succeed_from_a_prefetched_upstream_terminal() {
  let upstream = Endpoint::ChatCompletions;
  for inbound in [Endpoint::Responses, Endpoint::Messages] {
    let mut bytes = prefix(upstream);
    bytes.extend(terminal(upstream));
    let mut fixture = Fixture::new(bytes, true, usize::MAX).await;
    let mut standard = fixture.standard_body(upstream, inbound).await.into_data_stream();
    tokio::time::timeout(TIMEOUT, standard.next())
      .await
      .unwrap()
      .unwrap()
      .unwrap();
    // The complete upstream body is in the same transport chunk, but the
    // consumer has only accepted the first converted output event.
    drop(standard);

    {
      let response = fixture
        .injected
        .execute(inbound, request(upstream, inbound), RequestOptions::default())
        .await
        .unwrap();
      let ResponseBody::Stream(mut injected) = response.body else {
        panic!("request was streaming")
      };
      assert!(injected.next_event().await.unwrap().is_some());
      injected.cancel();
      injected.cancel();
      assert!(injected.next_event().await.unwrap().is_none());
      drop(injected);
    }
    fixture.assert_completions(upstream, false).await;
  }
}
