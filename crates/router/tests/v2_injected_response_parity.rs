//! Buffered response behavior through the real gateway and injected execution.
//! OpenCode model rules select each native format without custom stage adapters.

use axum::{
  body::{to_bytes, Body},
  extract::State,
  http::{Request as HttpRequest, Response as HttpResponse, Uri},
  routing::post,
  Router,
};
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
use tokn_core::{account::AccountConfig, event::EventBus, Endpoint};
use tokn_requests::configured::{Injected, RequestOptions, ResponseBody, Runtime};
use tokn_service::transport::{Request, ResponseHead, Transport};
use tower::ServiceExt;

const ENDPOINTS: [Endpoint; 3] = [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];
const TEXT: &str = "héllo 🌍";

#[derive(Clone)]
struct Reply {
  status: u16,
  content_type: Option<&'static str>,
  bytes: Vec<u8>,
  buffered_sse: bool,
}

impl Reply {
  fn new(content_type: Option<&'static str>, bytes: impl Into<Vec<u8>>) -> Self {
    Self {
      status: 200,
      content_type,
      bytes: bytes.into(),
      buffered_sse: false,
    }
  }

  fn with_buffered_sse(mut self) -> Self {
    self.buffered_sse = true;
    self
  }
}

struct Upstream {
  reply: Reply,
  paths: Mutex<Vec<String>>,
}

async fn upstream(State(state): State<Arc<Upstream>>, uri: Uri) -> HttpResponse<Body> {
  state.paths.lock().unwrap().push(uri.path().to_owned());
  let mut response = HttpResponse::builder()
    .status(state.reply.status)
    .header("x-upstream", "preserve");
  if let Some(content_type) = state.reply.content_type {
    response = response.header("content-type", content_type);
  }
  response.body(Body::from(state.reply.bytes.clone())).unwrap()
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
    headers.insert("x-upstream", "preserve");
    if let Some(content_type) = self.reply.content_type {
      headers.insert("content-type", content_type);
    }
    Ok(ResponseHead {
      status: self.reply.status,
      headers,
    })
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    // Split UTF-8, JSON strings, and SSE delimiters across transport reads.
    let count = buffer.len().min(3).min(self.reply.bytes.len() - self.position);
    buffer[..count].copy_from_slice(&self.reply.bytes[self.position..self.position + count]);
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
    panic!("response processing must never retry")
  }
}

#[derive(Clone, Copy)]
enum Mode {
  Managed,
  AccountRelay,
  ClientRelay,
}

struct Fixture {
  mode: Mode,
  app: Router,
  injected: Injected<MemoryTransport, TestRuntime>,
  standard: Arc<Upstream>,
  injected_paths: Rc<RefCell<Vec<String>>>,
  server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
  fn drop(&mut self) {
    self.server.abort();
  }
}

impl Fixture {
  async fn new(mode: Mode, reply: Reply) -> Self {
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
    let route = match mode {
      Mode::Managed => json!({
        "kind": "managed", "provider": {"kind": "fixed", "provider": "upstream"},
        "model": {"kind": "capability"}, "operation": "translate_compatible",
        "retry": {"kind": "recoverable", "policy": "retry"}
      }),
      Mode::AccountRelay | Mode::ClientRelay => json!({
        "kind": "relay", "destination": {"kind": "fixed_provider", "provider": "upstream"},
        "credentials": {"kind": if matches!(mode, Mode::ClientRelay) {"client"} else {"account_pool"}},
        "retry": {"kind": "buffered", "policy": "retry"}
      }),
    };
    let raw: RawConfig = serde_json::from_value(json!({
      "schema_version": 2,
      "listeners": {"api": {"kind": "llm_api", "bind": "127.0.0.1:4141", "client_auth": "none"}},
      "providers": {"upstream": {"driver": "opencode-go", "base_url": format!("http://{address}/v1")}},
      "profiles": {"default": {"route": "route", "wire_identity": "none"}},
      "routes": {"route": route},
      "retry_policies": {"retry": {"max_retries": 1, "initial_backoff_ms": 1}}
    }))
    .unwrap();
    let account: AccountConfig = serde_json::from_value(json!({
      "id": "test", "provider": "upstream", "api_key": "test-key"
    }))
    .unwrap();
    let accounts = if matches!(mode, Mode::ClientRelay) {
      vec![]
    } else {
      vec![account]
    };
    let state = tokn_router::v2::build_states(
      compile(&raw, Path::new("response-parity.toml")).unwrap(),
      &accounts,
      Arc::new(AccessStore::disabled()),
      Arc::new(EventBus::noop()),
    )
    .unwrap()
    .pop()
    .unwrap();
    let injected_paths = Rc::default();
    let injected = Injected::new(
      compile_execution(&raw).unwrap(),
      accounts,
      MemoryTransport {
        reply,
        position: 0,
        paths: Rc::clone(&injected_paths),
      },
      TestRuntime,
    )
    .unwrap();
    Self {
      mode,
      app: tokn_router::v2::router(state),
      injected,
      standard,
      injected_paths,
      server,
    }
  }

  async fn compare(&self, upstream: Endpoint, inbound: Endpoint, fails: bool) -> Vec<u8> {
    let model = match upstream {
      Endpoint::ChatCompletions => "test-chat",
      Endpoint::Responses => "gpt-test",
      Endpoint::Messages => "qwen-test",
    };
    let body = match inbound {
      Endpoint::Responses => json!({"model": model, "input": "hello", "stream": false}),
      _ => {
        json!({"model": model, "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16, "stream": false})
      }
    };
    let standard = self
      .app
      .clone()
      .oneshot(
        HttpRequest::post(format!("/v1/{}", operation(inbound)))
          .header("content-type", "application/json")
          .header("authorization", "Bearer caller-key")
          .body(Body::from(body.to_string()))
          .unwrap(),
      )
      .await
      .unwrap();
    let status = standard.status();
    let headers = standard.headers().clone();
    let standard_body = to_bytes(standard.into_body(), usize::MAX).await.unwrap();
    let injected = self
      .injected
      .execute(
        inbound,
        body,
        RequestOptions::default().with_header("authorization", "Bearer caller-key"),
      )
      .await;
    if fails {
      assert!(
        status.is_server_error(),
        "{status}: {}",
        String::from_utf8_lossy(&standard_body)
      );
      let error = injected.err().expect("injected conversion must fail too");
      assert_eq!(error.stage, "convert_response");
      assert!(!error.recoverable);
    } else {
      assert_eq!(
        status.as_u16(),
        self.standard.reply.status,
        "{}",
        String::from_utf8_lossy(&standard_body)
      );
      let injected = injected.unwrap_or_else(|error| panic!("{upstream:?} -> {inbound:?}: {error}"));
      assert_eq!(injected.status, status.as_u16());
      assert_eq!(injected.headers.get("x-upstream").unwrap().as_str(), "preserve");
      let injected_type = injected.headers.get("content-type").map(|value| value.as_str());
      let standard_type = headers.get("content-type").map(|value| value.to_str().unwrap());
      if matches!(self.mode, Mode::Managed) {
        // Gateway HTTP output applies its own JSON MIME. The platform adapter
        // keeps upstream metadata unless it rendered buffered SSE as JSON.
        assert_eq!(standard_type, Some("application/json"));
        assert_eq!(
          injected_type,
          if self.standard.reply.buffered_sse {
            Some("application/json")
          } else {
            self.standard.reply.content_type
          }
        );
      } else {
        assert_eq!(injected_type, standard_type);
      }
      let opaque_stream = !matches!(self.mode, Mode::Managed)
        && self.standard.reply.status < 400
        && self.standard.reply.content_type == Some("text/event-stream");
      let injected_body = match injected.body {
        ResponseBody::Buffered(bytes) if !opaque_stream => bytes.to_vec(),
        ResponseBody::Stream(mut stream) if opaque_stream => {
          let mut bytes = Vec::new();
          while let Some(chunk) = stream.next_chunk().await.expect("opaque bytes are forwarded") {
            bytes.extend_from_slice(&chunk);
          }
          bytes
        }
        _ => panic!("response body must follow the shared managed/opaque MIME policy"),
      };
      assert_eq!(
        injected_body.as_slice(),
        standard_body.as_ref(),
        "{upstream:?} -> {inbound:?}"
      );
    }
    let expected = vec![format!("/v1/{}", operation(upstream))];
    assert_eq!(*self.standard.paths.lock().unwrap(), expected, "no response retries");
    assert_eq!(*self.injected_paths.borrow(), expected, "no response retries");
    standard_body.to_vec()
  }
}

fn operation(endpoint: Endpoint) -> &'static str {
  match endpoint {
    Endpoint::ChatCompletions => "chat/completions",
    Endpoint::Responses => "responses",
    Endpoint::Messages => "messages",
  }
}

fn response(endpoint: Endpoint) -> Value {
  match endpoint {
    Endpoint::ChatCompletions => json!({
      "id": "response-test", "model": "fixture", "object": "chat.completion",
      "choices": [{"index": 0, "message": {"role": "assistant", "content": TEXT,
        "tool_calls": [{"id": "call-test", "type": "function", "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}]},
        "finish_reason": "tool_calls"}],
      "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}, "vendor_extra": {"preserve": true}
    }),
    Endpoint::Responses => json!({
      "id": "response-test", "model": "fixture", "status": "completed",
      "output": [{"type": "message", "id": "message-test", "role": "assistant", "content": [{"type": "output_text", "text": TEXT, "annotations": []}]},
        {"type": "function_call", "call_id": "call-test", "name": "lookup", "arguments": "{\"q\":\"x\"}"}],
      "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}, "vendor_extra": {"preserve": true}
    }),
    Endpoint::Messages => json!({
      "id": "response-test", "model": "fixture", "type": "message", "role": "assistant",
      "content": [{"type": "text", "text": TEXT}, {"type": "tool_use", "id": "call-test", "name": "lookup", "input": {"q": "x"}}],
      "stop_reason": "tool_use", "usage": {"input_tokens": 7, "output_tokens": 3}, "vendor_extra": {"preserve": true}
    }),
  }
}

fn sse(endpoint: Endpoint) -> Vec<u8> {
  let events = match endpoint {
    Endpoint::ChatCompletions => vec![json!({
      "id": "response-test", "model": "fixture", "choices": [{"index": 0, "delta": {"role": "assistant", "content": TEXT,
        "tool_calls": [{"index": 0, "id": "call-test", "type": "function", "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}]}, "finish_reason": "tool_calls"}],
      "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}
    })],
    Endpoint::Responses => vec![
      json!({"type": "response.output_text.delta", "output_index": 0, "delta": TEXT}),
      json!({"type": "response.output_item.added", "output_index": 1, "item": {"type": "function_call", "id": "item-test", "call_id": "call-test", "name": "lookup", "arguments": ""}}),
      json!({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": "{\"q\":\"x\"}"}),
      json!({"type": "response.completed", "response": response(endpoint)}),
    ],
    Endpoint::Messages => vec![
      json!({"type": "message_start", "message": {"id": "response-test", "model": "fixture", "role": "assistant", "content": [], "usage": {"input_tokens": 7}}}),
      json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
      json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": TEXT}}),
      json!({"type": "content_block_stop", "index": 0}),
      json!({"type": "content_block_start", "index": 1, "content_block": {"type": "tool_use", "id": "call-test", "name": "lookup", "input": {}}}),
      json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"q\":\"x\"}"}}),
      json!({"type": "content_block_stop", "index": 1}),
      json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 3}}),
      json!({"type": "message_stop"}),
    ],
  };
  let mut bytes = String::new();
  for event in events {
    if let Some(kind) = event["type"].as_str() {
      bytes.push_str(&format!("event: {kind}\r\n"));
    }
    bytes.push_str(&format!("data: {event}\r\n\r\n"));
  }
  if endpoint == Endpoint::ChatCompletions {
    bytes.push_str("data: [DONE]\r\n\r\n");
  }
  bytes.into_bytes()
}

fn assert_content(endpoint: Endpoint, bytes: &[u8]) {
  let value: Value = serde_json::from_slice(bytes).unwrap();
  let (text, name, arguments, input, output) = match endpoint {
    Endpoint::ChatCompletions => {
      let message = &value["choices"][0]["message"];
      let call = message["tool_calls"]
        .as_array()
        .unwrap()
        .iter()
        .find(|call| call["id"] == "call-test")
        .unwrap();
      (
        message["content"].clone(),
        call["function"]["name"].clone(),
        serde_json::from_str(call["function"]["arguments"].as_str().unwrap()).unwrap(),
        value["usage"]["prompt_tokens"].clone(),
        value["usage"]["completion_tokens"].clone(),
      )
    }
    Endpoint::Responses => {
      let items = value["output"].as_array().unwrap();
      let message = items.iter().find(|item| item["type"] == "message").unwrap();
      let call = items
        .iter()
        .find(|item| item["type"] == "function_call" && item["call_id"] == "call-test")
        .unwrap();
      (
        message["content"][0]["text"].clone(),
        call["name"].clone(),
        serde_json::from_str(call["arguments"].as_str().unwrap()).unwrap(),
        value["usage"]["input_tokens"].clone(),
        value["usage"]["output_tokens"].clone(),
      )
    }
    Endpoint::Messages => {
      let items = value["content"].as_array().unwrap();
      let text = items.iter().find(|item| item["type"] == "text").unwrap();
      let call = items
        .iter()
        .find(|item| item["type"] == "tool_use" && item["id"] == "call-test")
        .unwrap();
      (
        text["text"].clone(),
        call["name"].clone(),
        call["input"].clone(),
        value["usage"]["input_tokens"].clone(),
        value["usage"]["output_tokens"].clone(),
      )
    }
  };
  assert_eq!(text, TEXT);
  assert_eq!(name, "lookup");
  // Existing renderers can retain accumulated tool arguments as a JSON string.
  // Full body parity above checks the wire representation; here check content.
  let arguments: Value = if let Some(arguments) = arguments.as_str() {
    serde_json::from_str(arguments).unwrap()
  } else {
    arguments
  };
  assert_eq!(arguments, json!({"q": "x"}));
  assert_eq!(input, 7);
  assert_eq!(output, 3);
}

#[tokio::test]
async fn json_responses_preserve_raw_bytes_or_translate_all_native_pairs() {
  for upstream in ENDPOINTS {
    let bytes = format!(" \n{}\n ", serde_json::to_string_pretty(&response(upstream)).unwrap()).into_bytes();
    for inbound in ENDPOINTS {
      let fixture = Fixture::new(
        Mode::Managed,
        Reply::new(Some("application/json; charset=utf-8"), bytes.clone()),
      )
      .await;
      let body = fixture.compare(upstream, inbound, false).await;
      assert_content(inbound, &body);
      if inbound == upstream {
        assert_eq!(body, bytes, "same endpoint preserves whitespace and unknown fields");
      }
    }
  }
}

#[tokio::test]
async fn buffered_sse_renders_directly_into_every_native_response_format() {
  for upstream in ENDPOINTS {
    for inbound in ENDPOINTS {
      let fixture = Fixture::new(
        Mode::Managed,
        Reply::new(Some("Text/Event-Stream; charset=utf-8"), sse(upstream)).with_buffered_sse(),
      )
      .await;
      assert_content(inbound, &fixture.compare(upstream, inbound, false).await);
    }
  }
}

#[tokio::test]
async fn buffered_detection_empty_and_malformed_responses_match_without_retries() {
  for content_type in [None, Some("application/json"), Some("text/plain")] {
    let mut bytes = b" \r\n".to_vec();
    bytes.extend(sse(Endpoint::ChatCompletions));
    let fixture = Fixture::new(Mode::Managed, Reply::new(content_type, bytes).with_buffered_sse()).await;
    assert_content(
      Endpoint::Messages,
      &fixture
        .compare(Endpoint::ChatCompletions, Endpoint::Messages, false)
        .await,
    );
  }
  for content_type in [None, Some("application/json")] {
    let fixture = Fixture::new(Mode::Managed, Reply::new(content_type, vec![])).await;
    assert!(fixture
      .compare(Endpoint::ChatCompletions, Endpoint::ChatCompletions, false)
      .await
      .is_empty());
  }
  for (content_type, bytes) in [
    (Some("text/event-stream"), b"".as_slice()),
    (
      Some("text/event-stream"),
      b"data: not-json\n\ndata: [DONE]\n\n".as_slice(),
    ),
    (None, b"data: {\"choices\":[]}\n\n".as_slice()),
    (Some("application/json"), b"not-json".as_slice()),
  ] {
    let fixture = Fixture::new(Mode::Managed, Reply::new(content_type, bytes)).await;
    fixture
      .compare(Endpoint::ChatCompletions, Endpoint::ChatCompletions, true)
      .await;
  }
}

#[tokio::test]
async fn opaque_relays_preserve_bytes_without_sniffing_or_conversion() {
  for mode in [Mode::AccountRelay, Mode::ClientRelay] {
    for (content_type, bytes) in [
      (Some("application/octet-stream"), b"opaque \0\xff".as_slice()),
      (None, b"data: not-json\n\n".as_slice()),
      (
        Some("text/event-stream"),
        b"data: invalid and incomplete\n\n".as_slice(),
      ),
    ] {
      let fixture = Fixture::new(mode, Reply::new(content_type, bytes)).await;
      assert_eq!(
        fixture
          .compare(Endpoint::ChatCompletions, Endpoint::ChatCompletions, false)
          .await,
        bytes
      );
    }
  }
  let bytes = b"data: opaque upstream failure\n\n";
  let mut reply = Reply::new(Some("application/octet-stream"), bytes.as_slice());
  reply.status = 503;
  let fixture = Fixture::new(Mode::ClientRelay, reply).await;
  assert_eq!(
    fixture
      .compare(Endpoint::ChatCompletions, Endpoint::ChatCompletions, false)
      .await,
    bytes
  );
}
