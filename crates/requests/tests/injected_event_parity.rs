//! Response usage observations share payloads and boundaries across adapters.
#![cfg(feature = "std")]

use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{cell::RefCell, convert::Infallible, rc::Rc, sync::Arc, time::Duration};
use tokn_convert::usage::{observed_usage, observed_usage_json};
use tokn_core::{
  account::AccountConfig,
  event::{Event, EventBus},
  headers::HeaderMap,
  request_event::{RecordEvent, RequestEvent, RequestEventPayload, Stage, StageEvent},
  usage::{Usage, UsageType},
  Endpoint,
};
use tokn_requests::{
  configured::{RequestOptions, ResponseBody, Runtime},
  stage_traits::{ConvertResponseStage, ConvertedBody, SentResponse},
  stages::DefaultConvertResponse,
  PipelineCtx,
};
use tokn_service::transport::{Request, ResponseHead, Transport};

#[path = "support/injected.rs"]
mod injected_fixture;
use injected_fixture::BorrowedSession;

const ENDPOINTS: [Endpoint; 3] = [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];
const REQUEST_ID: &str = "event-parity";

#[derive(Clone)]
struct Reply {
  bytes: Vec<u8>,
  content_type: &'static str,
}

struct MemoryTransport {
  reply: Reply,
  position: usize,
}

impl Transport for MemoryTransport {
  type Error = Infallible;

  async fn send(&mut self, _: Request) -> Result<ResponseHead, Self::Error> {
    self.position = 0;
    Ok(ResponseHead {
      status: 200,
      headers: headers(self.reply.content_type),
    })
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
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
    panic!("response processing never retries")
  }
}

fn headers(content_type: &'static str) -> HeaderMap {
  let mut headers = HeaderMap::new();
  headers.insert("content-type", content_type);
  headers
}

fn request(from: Endpoint, to: Endpoint, stream: bool) -> Value {
  let model = match from {
    Endpoint::ChatCompletions => "test-chat",
    Endpoint::Responses => "gpt-test",
    Endpoint::Messages => "qwen-test",
  };
  match to {
    Endpoint::Responses => json!({"model":model,"input":"hello","stream":stream}),
    _ => json!({"model":model,"messages":[{"role":"user","content":"hello"}],"max_tokens":16,"stream":stream}),
  }
}

#[derive(Clone, Copy)]
enum ReadMode {
  Buffered,
  Stream,
  CancelAfterFirst,
}

impl ReadMode {
  fn streaming(self) -> bool {
    !matches!(self, Self::Buffered)
  }
}

struct Observed {
  events: Vec<RequestEvent>,
  body: Option<Vec<u8>>,
  failed: bool,
}

impl Observed {
  fn usage(&self) -> Vec<Usage> {
    self
      .events
      .iter()
      .filter_map(|event| match &event.payload {
        RequestEventPayload::Record(RecordEvent::Usage(usage)) => Some(usage.clone()),
        _ => None,
      })
      .collect()
  }

  fn assert_terminal(&self, success: bool) {
    let completions: Vec<_> = self
      .events
      .iter()
      .filter_map(|event| match &event.payload {
        RequestEventPayload::Stage(StageEvent::Completed { success, attempts }) => Some((*success, *attempts)),
        _ => None,
      })
      .collect();
    assert_eq!(completions, [(success, 1)]);
    let errors: Vec<_> = self
      .events
      .iter()
      .filter_map(|event| match &event.payload {
        RequestEventPayload::Stage(StageEvent::Error {
          stage,
          recoverable,
          stop,
          ..
        }) => Some((*stage, *recoverable, *stop)),
        _ => None,
      })
      .collect();
    assert_eq!(
      errors,
      if success {
        vec![]
      } else {
        vec![(Stage::ConvertResponse, false, false)]
      }
    );
  }
}

async fn injected(from: Endpoint, to: Endpoint, reply: Reply, mode: ReadMode) -> Observed {
  let raw = serde_json::from_value(json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"},"wire_identity":"none"},
    "providers":{"upstream":{"driver":"opencode-go","base_url":"https://example.com/v1"}}
  }))
  .unwrap();
  let account: AccountConfig = serde_json::from_value(json!({
    "id":"test","provider":"upstream","api_key":"test-key"
  }))
  .unwrap();
  let events = Rc::new(RefCell::new(Vec::new()));
  let sink = events.clone();
  let platform = BorrowedSession::new(
    tokn_config::v2::compile_execution(&raw).unwrap(),
    vec![account],
    MemoryTransport { reply, position: 0 },
    TestRuntime,
  )
  .unwrap()
  .with_event_sink(move |event: RequestEvent| sink.borrow_mut().push(event));
  let mut failed = false;
  let mut body = None;
  match platform
    .execute(
      to,
      request(from, to, mode.streaming()),
      RequestOptions::default().with_request_id(REQUEST_ID),
    )
    .await
  {
    Err(_) => failed = true,
    Ok(response) => match response.body {
      ResponseBody::Buffered(bytes) => body = Some(bytes.to_vec()),
      ResponseBody::Stream(mut stream) => {
        loop {
          match stream.next_event().await {
            Ok(Some(_)) if matches!(mode, ReadMode::CancelAfterFirst) => break,
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
              failed = true;
              break;
            }
          }
        }
        stream.cancel();
        stream.cancel();
      }
    },
  }
  drop(platform);
  let events = Rc::try_unwrap(events).unwrap().into_inner();
  for event in &events {
    assert_eq!(event.request_id.as_str(), REQUEST_ID);
    assert_eq!(event.attempt, 0);
    if let RequestEventPayload::Stage(StageEvent::Resolve(summary)) = &event.payload {
      assert_eq!(summary.resolved_endpoint, Some(to));
      assert_eq!(summary.upstream_endpoint, Some(from));
    }
  }
  Observed { events, body, failed }
}

async fn standard(from: Endpoint, to: Endpoint, reply: Reply, mode: ReadMode) -> Observed {
  let bus = Arc::new(EventBus::new(128));
  let mut receiver = bus.subscribe();
  let ctx = PipelineCtx::new(REQUEST_ID, to.into(), bus.clone());
  let response = http::Response::builder()
    .status(200)
    .header("content-type", reply.content_type)
    .body(reply.bytes)
    .unwrap();
  let sent = SentResponse {
    status: 200,
    headers: headers(reply.content_type),
    stream: mode.streaming(),
    upstream_endpoint: Some(from),
    response: reqwest::Response::from(response),
  };
  let mut failed = false;
  let mut body = None;
  match DefaultConvertResponse::new().convert_response(&ctx, sent).await {
    Err(_) => failed = true,
    Ok(response) => match response.body {
      ConvertedBody::Buffered { body_bytes, .. } => body = Some(body_bytes.to_vec()),
      ConvertedBody::Stream { mut body } => {
        while let Some(event) = body.next().await {
          if event.is_err() {
            failed = true;
            break;
          }
          if matches!(mode, ReadMode::CancelAfterFirst) {
            break;
          }
        }
      }
    },
  }
  // Usage and completion have independent std background consumers. Drain all
  // finalizers before comparing their observations; do not assume task order.
  let (_, events) = tokio::time::timeout(Duration::from_secs(5), async {
    tokio::join!(bus.shutdown(), async {
      let mut events = Vec::new();
      loop {
        match receiver.recv().await.unwrap().as_ref() {
          Event::Requests(event) => events.push(event.clone()),
          Event::Shutdown(sender) => {
            sender.lock().unwrap().take().unwrap().send(()).unwrap();
            break events;
          }
          _ => {}
        }
      }
    })
  })
  .await
  .expect("std event finalizers finish after response consumption or drop");
  for event in &events {
    assert_eq!(event.request_id.as_str(), REQUEST_ID);
    assert_eq!(event.attempt, 0);
  }
  Observed { events, body, failed }
}

fn response(endpoint: Endpoint) -> Value {
  match endpoint {
    Endpoint::ChatCompletions => json!({
      "id":"response-test","model":"fixture","object":"chat.completion",
      "choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],
      "usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10,
        "prompt_tokens_details":{"cached_tokens":2},"completion_tokens_details":{"reasoning_tokens":1}}
    }),
    Endpoint::Responses => json!({
      "id":"response-test","model":"fixture","status":"completed",
      "output":[{"type":"message","id":"message-test","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}],
      "usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10,
        "input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":1}}
    }),
    Endpoint::Messages => json!({
      "id":"response-test","model":"fixture","type":"message","role":"assistant",
      "content":[{"type":"text","text":"hello"}],"stop_reason":"end_turn",
      "usage":{"input_tokens":3,"output_tokens":3,"cache_read_input_tokens":2,"cache_creation_input_tokens":2}
    }),
  }
}

fn frames(endpoint: Endpoint) -> Vec<Value> {
  match endpoint {
    Endpoint::ChatCompletions => vec![
      json!({"id":"response-test","model":"fixture","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},"finish_reason":null}],"usage":{"prompt_tokens":0,"completion_tokens":0}}),
      json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
      json!({"choices":[],"usage":response(endpoint)["usage"]}),
    ],
    Endpoint::Responses => vec![
      json!({"type":"response.created","response":{"id":"response-test","model":"fixture","output":[],"usage":{"input_tokens":0,"output_tokens":0}}}),
      json!({"type":"response.output_text.delta","output_index":0,"delta":"hello"}),
      json!({"type":"response.completed","response":response(endpoint)}),
    ],
    Endpoint::Messages => vec![
      json!({"type":"message_start","message":{"id":"response-test","model":"fixture","role":"assistant","content":[],"usage":{"input_tokens":3,"output_tokens":0,"cache_read_input_tokens":2,"cache_creation_input_tokens":2}}}),
      json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
      json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}),
      json!({"type":"content_block_stop","index":0}),
      json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}),
      json!({"type":"message_stop"}),
    ],
  }
}

fn sse(endpoint: Endpoint) -> Vec<u8> {
  let mut bytes = String::new();
  for frame in frames(endpoint) {
    if let Some(kind) = frame["type"].as_str() {
      bytes.push_str(&format!("event: {kind}\r\n"));
    }
    bytes.push_str(&format!("data: {frame}\r\n\r\n"));
  }
  if endpoint == Endpoint::ChatCompletions {
    bytes.push_str("data: [DONE]\r\n\r\n");
  }
  bytes.into_bytes()
}

#[tokio::test]
async fn buffered_json_observes_upstream_usage_for_every_format_pair() {
  for from in ENDPOINTS {
    for to in ENDPOINTS {
      let reply = Reply {
        bytes: serde_json::to_vec(&response(from)).unwrap(),
        content_type: "application/json",
      };
      let standard = standard(from, to, reply.clone(), ReadMode::Buffered).await;
      let injected = injected(from, to, reply, ReadMode::Buffered).await;
      assert!(!standard.failed && !injected.failed, "{from:?} -> {to:?}");
      assert_eq!(standard.usage(), [observed_usage(&response(from)).unwrap()]);
      assert_eq!(injected.usage(), standard.usage(), "{from:?} -> {to:?}");
      injected.assert_terminal(true);
    }
  }
}

#[tokio::test]
async fn buffered_sse_observes_rendered_usage_for_every_format_pair() {
  for from in ENDPOINTS {
    for to in ENDPOINTS {
      let reply = Reply {
        bytes: sse(from),
        content_type: "text/event-stream",
      };
      let standard = standard(from, to, reply.clone(), ReadMode::Buffered).await;
      let injected = injected(from, to, reply, ReadMode::Buffered).await;
      assert!(!standard.failed && !injected.failed, "{from:?} -> {to:?}");
      let rendered_usage = observed_usage_json(standard.body.as_ref().unwrap()).unwrap();
      assert_eq!(standard.usage(), [rendered_usage]);
      assert_eq!(injected.usage(), standard.usage(), "{from:?} -> {to:?}");
      injected.assert_terminal(true);
    }
  }
}

#[tokio::test]
async fn live_sse_observes_upstream_usage_once_before_translation_for_every_format_pair() {
  for from in ENDPOINTS {
    let expected: Vec<_> = frames(from).iter().filter_map(observed_usage).collect();
    assert_eq!(expected.len(), 2, "initial and terminal usage observations");
    if from == Endpoint::Messages {
      assert_eq!(expected[0].usage_type, Some(UsageType::Messages));
      assert_eq!(expected[0].input_tokens, Some(7));
      assert_eq!(expected[1].usage_type, Some(UsageType::Responses));
      assert_eq!(expected[1].input_tokens, None);
    }
    for to in ENDPOINTS {
      let reply = Reply {
        bytes: sse(from),
        content_type: "text/event-stream",
      };
      let standard = standard(from, to, reply.clone(), ReadMode::Stream).await;
      let injected = injected(from, to, reply, ReadMode::Stream).await;
      assert!(!standard.failed && !injected.failed, "{from:?} -> {to:?}");
      assert_eq!(standard.usage(), expected, "{from:?} -> {to:?}");
      assert_eq!(injected.usage(), standard.usage(), "{from:?} -> {to:?}");
      standard.assert_terminal(true);
      injected.assert_terminal(true);
    }
  }
}

#[tokio::test]
async fn buffered_usage_survives_conversion_failure_without_false_observations() {
  for bytes in [
    br#"{"choices":[],"usage":{"prompt_tokens":7}}"#.to_vec(),
    b"not json".to_vec(),
    Vec::new(),
  ] {
    let expected: Vec<_> = observed_usage_json(&bytes).into_iter().collect();
    let reply = Reply {
      bytes,
      content_type: "application/json",
    };
    let standard = standard(
      Endpoint::ChatCompletions,
      Endpoint::Messages,
      reply.clone(),
      ReadMode::Buffered,
    )
    .await;
    let injected = injected(Endpoint::ChatCompletions, Endpoint::Messages, reply, ReadMode::Buffered).await;
    assert_eq!(standard.failed, injected.failed);
    assert_eq!(standard.usage(), expected);
    assert_eq!(injected.usage(), expected);
    injected.assert_terminal(!injected.failed);
    if !expected.is_empty() {
      let usage = injected
        .events
        .iter()
        .position(|event| matches!(&event.payload, RequestEventPayload::Record(RecordEvent::Usage(_))))
        .unwrap();
      let error = injected
        .events
        .iter()
        .position(|event| matches!(&event.payload, RequestEventPayload::Stage(StageEvent::Error { .. })))
        .unwrap();
      assert!(usage < error, "usage remains observable when conversion later fails");
    }
  }
}

#[tokio::test]
async fn live_truncation_and_cancellation_report_one_failed_terminal() {
  for mode in [ReadMode::Stream, ReadMode::CancelAfterFirst] {
    let reply = Reply {
      bytes: b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}],\"usage\":{\"prompt_tokens\":7}}\n\n"
        .to_vec(),
      content_type: "text/event-stream",
    };
    let standard = standard(
      Endpoint::ChatCompletions,
      Endpoint::ChatCompletions,
      reply.clone(),
      mode,
    )
    .await;
    let injected = injected(Endpoint::ChatCompletions, Endpoint::ChatCompletions, reply, mode).await;
    assert_eq!(standard.failed, matches!(mode, ReadMode::Stream));
    assert_eq!(standard.failed, injected.failed);
    assert_eq!(standard.usage().len(), 1);
    assert_eq!(injected.usage(), standard.usage());
    standard.assert_terminal(false);
    injected.assert_terminal(false);
  }
}
