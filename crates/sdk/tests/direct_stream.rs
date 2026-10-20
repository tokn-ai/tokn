use core::{
  future::{poll_fn, Future},
  pin::pin,
  task::{Context, Poll, Waker},
};
use std::{
  cell::RefCell,
  fmt,
  rc::Rc,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  task::Wake,
};

use serde_json::{json, Value};
use tokn_sdk::{
  chat_completions::{ChatEvent, ChatRequest},
  direct::{Header, OpenAi, Request, StreamBody, StreamError, StreamOptions, StreamingResponse, StreamingTransport},
  responses::{ResponsesEvent, ResponsesRequest},
  DirectClient,
};

#[derive(Debug, PartialEq)]
struct TransportError(&'static str);

impl fmt::Display for TransportError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.0)
  }
}

impl std::error::Error for TransportError {}

#[derive(Default)]
struct Stats {
  requests: Vec<Request>,
  reads: usize,
  bytes: usize,
  pending: usize,
  dropped: usize,
}

// Rc and the borrowing body deliberately rule out Send and 'static requirements.
struct MockTransport {
  stats: Rc<RefCell<Stats>>,
  bytes: Vec<u8>,
  position: usize,
  chunk_size: usize,
  yielded: bool,
  status: u16,
  headers: Vec<Header>,
  open_error: bool,
  read_error: bool,
  invalid_read_size: bool,
}

impl MockTransport {
  fn new(bytes: impl AsRef<[u8]>, chunk_size: usize) -> Self {
    Self {
      stats: Rc::new(RefCell::new(Stats::default())),
      bytes: bytes.as_ref().to_vec(),
      position: 0,
      chunk_size,
      yielded: false,
      status: 200,
      headers: vec![
        Header::new("CoNtEnT-TyPe", "Text/Event-Stream; charset=utf-8"),
        Header::new("x-request-id", "request-stream"),
      ],
      open_error: false,
      read_error: false,
      invalid_read_size: false,
    }
  }
}

struct BorrowedBody<'a>(&'a mut MockTransport);

impl Drop for BorrowedBody<'_> {
  fn drop(&mut self) {
    self.0.stats.borrow_mut().dropped += 1;
  }
}

impl StreamBody for BorrowedBody<'_> {
  type Error = TransportError;

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    poll_fn(|cx| {
      if !self.0.yielded {
        self.0.yielded = true;
        self.0.stats.borrow_mut().pending += 1;
        cx.waker().wake_by_ref();
        return Poll::Pending;
      }
      self.0.yielded = false;
      self.0.stats.borrow_mut().reads += 1;
      if self.0.read_error {
        return Poll::Ready(Err(TransportError("body read failed")));
      }
      if self.0.invalid_read_size {
        return Poll::Ready(Ok(buffer.len() + 1));
      }
      let length = buffer
        .len()
        .min(self.0.chunk_size)
        .min(self.0.bytes.len() - self.0.position);
      buffer[..length].copy_from_slice(&self.0.bytes[self.0.position..self.0.position + length]);
      self.0.position += length;
      self.0.stats.borrow_mut().bytes += length;
      Poll::Ready(Ok(length))
    })
    .await
  }
}

impl StreamingTransport for MockTransport {
  type Error = TransportError;
  type Body<'a> = BorrowedBody<'a>;

  async fn send_stream(&mut self, request: Request) -> Result<StreamingResponse<Self::Body<'_>>, Self::Error> {
    let mut yielded = false;
    poll_fn(|cx| {
      if core::mem::replace(&mut yielded, true) {
        Poll::Ready(())
      } else {
        cx.waker().wake_by_ref();
        Poll::Pending
      }
    })
    .await;
    self.stats.borrow_mut().requests.push(request);
    if self.open_error {
      return Err(TransportError("connection failed"));
    }
    Ok(StreamingResponse {
      status: self.status,
      headers: self.headers.clone(),
      body: BorrowedBody(self),
    })
  }
}

struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
  fn wake(self: Arc<Self>) {
    self.0.store(true, Ordering::SeqCst);
  }
}

fn run<F: Future>(future: F) -> F::Output {
  let flag = Arc::new(WakeFlag(AtomicBool::new(false)));
  let waker = Waker::from(flag.clone());
  let mut context = Context::from_waker(&waker);
  let mut future = pin!(future);
  for _ in 0..100_000 {
    flag.0.store(false, Ordering::SeqCst);
    match future.as_mut().poll(&mut context) {
      Poll::Ready(value) => return value,
      Poll::Pending => assert!(flag.0.load(Ordering::SeqCst), "mock must wake before returning Pending"),
    }
  }
  panic!("stream did not make progress");
}

fn client(transport: MockTransport) -> DirectClient<MockTransport> {
  DirectClient::new(transport, OpenAi::new("test-stream-key").unwrap())
}

fn responses_request() -> ResponsesRequest {
  serde_json::from_value(json!({
    "model": "test-model",
    "input": "hello",
    "max_output_tokens": 32
  }))
  .unwrap()
}

fn chat_request() -> ChatRequest {
  serde_json::from_value(json!({
    "model": "test-model",
    "messages": [{"role": "user", "content": "hello"}]
  }))
  .unwrap()
}

#[test]
fn responses_stream_handles_fragmented_utf8_multiline_data_and_terminal_event() {
  let wire = concat!(
    "\u{feff}: heartbeat\r\n\r\n",
    "id: ignored\r\n",
    "event: response.output_text.delta\r\n",
    "data: {\"type\":\"response.output_text.delta\",\r\n",
    "data: \"delta\":\"héllo 🦀\",\"sequence_number\":1}\r\n\r\n",
    "event: response.completed\r\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-stream\",\"status\":\"completed\"}}\r\n\r\n",
    "data: malformed trailing bytes must not be decoded\n\n"
  );
  for chunk_size in [1, 2, 7, wire.len()] {
    let transport = MockTransport::new(wire, chunk_size);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut request = responses_request();
    request.stream = Some(false);
    request.extras.insert("stream".into(), Value::Bool(false));
    let mut stream = run(client.responses_stream(&request, StreamOptions::default())).unwrap();
    assert_eq!(stream.status, 200);
    assert_eq!(stream.headers[1].value, "request-stream");
    let Some(ResponsesEvent::OutputTextDelta { delta, common, .. }) = run(stream.next_event()).unwrap() else {
      panic!("expected text delta");
    };
    assert_eq!(delta, "héllo 🦀");
    assert_eq!(common.sequence_number, Some(1));
    assert!(matches!(
      run(stream.next_event()).unwrap(),
      Some(ResponsesEvent::Completed { .. })
    ));
    assert_eq!(
      stats.borrow().dropped,
      1,
      "terminal event releases the body immediately"
    );
    let reads = stats.borrow().reads;
    assert!(run(stream.next_event()).unwrap().is_none());
    assert_eq!(stats.borrow().reads, reads);
    assert!(stats.borrow().pending > 0);
    let stats = stats.borrow();
    let sent = &stats.requests[0];
    assert_eq!(sent.method, "POST");
    assert_eq!(sent.url, "https://api.openai.com/v1/responses");
    assert!(sent
      .headers
      .iter()
      .any(|h| h.name == "accept" && h.value == "text/event-stream"));
    assert!(sent
      .headers
      .iter()
      .any(|h| h.name == "authorization" && h.value == "Bearer test-stream-key"));
    let body: Value = serde_json::from_slice(&sent.body).unwrap();
    assert_eq!(body["stream"], true);
    assert_eq!(body["max_output_tokens"], 32);
  }
}

#[test]
fn chat_stream_preserves_usage_after_finish_reason_and_yields_done() {
  let wire = concat!(
    "data: {\"id\":\"chat-stream\",\"error\":null,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}]}\n\n",
    "data: {\"id\":\"chat-stream\",\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\n",
    "data: [DONE]\n\n"
  );
  let transport = MockTransport::new(wire, 3);
  let stats = transport.stats.clone();
  let mut client = client(transport);
  let mut request = chat_request();
  request.stream = Some(false);
  request.extras.insert("stream".into(), Value::Bool(false));
  let mut stream = run(client.chat_completions_stream(&request, StreamOptions::default())).unwrap();
  let Some(ChatEvent::Chunk(chunk)) = run(stream.next_event()).unwrap() else {
    panic!("expected text chunk");
  };
  assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("hello"));
  assert!(chunk.choices[0].finish_reason.is_some());
  assert_eq!(chunk.extras.get("error"), Some(&Value::Null));
  assert_eq!(stats.borrow().dropped, 0);
  let Some(ChatEvent::Chunk(chunk)) = run(stream.next_event()).unwrap() else {
    panic!("expected usage chunk");
  };
  assert_eq!(chunk.usage.unwrap().total_tokens, Some(3));
  assert!(matches!(run(stream.next_event()).unwrap(), Some(ChatEvent::Done)));
  assert_eq!(stats.borrow().dropped, 1);
  assert!(run(stream.next_event()).unwrap().is_none());
  let stats = stats.borrow();
  assert_eq!(stats.requests[0].url, "https://api.openai.com/v1/chat/completions");
  let body: Value = serde_json::from_slice(&stats.requests[0].body).unwrap();
  assert_eq!(body["stream"], true);
}

#[test]
fn responses_terminal_failure_events_remain_visible_to_the_caller() {
  for kind in ["response.failed", "response.incomplete", "response.cancelled", "error"] {
    let wire = format!("event: {kind}\ndata: {{\"type\":\"{kind}\",\"error\":{{\"message\":\"failed\"}}}}\n\n");
    let transport = MockTransport::new(wire, 17);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
    assert_eq!(run(stream.next_event()).unwrap().unwrap().kind(), kind);
    assert_eq!(stats.borrow().dropped, 1);
    assert!(run(stream.next_event()).unwrap().is_none());
  }
}

#[test]
fn responses_preserves_unknown_events_without_treating_them_as_terminal() {
  let transport = MockTransport::new(
    concat!(
      "event: response.future_feature\ndata: {\"type\":\"response.future_feature\",\"extra\":42}\n\n",
      "data: {\"type\":\"response.completed\"}\n\n"
    ),
    5,
  );
  let mut client = client(transport);
  let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
  let Some(ResponsesEvent::Other(data)) = run(stream.next_event()).unwrap() else {
    panic!("expected unknown event");
  };
  assert_eq!(data["extra"], 42);
  assert!(matches!(
    run(stream.next_event()).unwrap(),
    Some(ResponsesEvent::Completed { .. })
  ));
}

#[test]
fn premature_eof_is_an_error_even_after_a_chat_finish_reason() {
  for wire in [
    "",
    ": heartbeat\n\n",
    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n",
  ] {
    let transport = MockTransport::new(wire, 2);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut stream = run(client.chat_completions_stream(&chat_request(), StreamOptions::default())).unwrap();
    let error = loop {
      match run(stream.next_event()) {
        Ok(Some(_)) => {}
        Err(error) => break error,
        Ok(None) => panic!("EOF without a terminal event must fail"),
      }
    };
    assert!(matches!(error, StreamError::UnexpectedEof));
    assert_eq!(stats.borrow().dropped, 1);
    assert!(run(stream.next_event()).unwrap().is_none());
  }
}

#[test]
fn invalid_json_utf8_and_event_envelopes_fail_and_release_the_body() {
  let fixtures: &[&[u8]] = &[
    b"data: {not json}\n\n",
    b"data: \xff\n\n",
    b"data: []\n\n",
    b"data: {\"delta\":\"missing type\"}\n\n",
    b"event: response.failed\ndata: {\"type\":\"response.completed\"}\n\n",
  ];
  for wire in fixtures {
    let transport = MockTransport::new(wire, 1);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
    assert!(matches!(
      run(stream.next_event()),
      Err(StreamError::Deserialize(_) | StreamError::Sse(_) | StreamError::InvalidEvent(_))
    ));
    assert_eq!(stats.borrow().dropped, 1);
    assert!(run(stream.next_event()).unwrap().is_none());
  }
}

#[test]
fn malformed_known_responses_events_do_not_fall_back_to_other_or_complete() {
  for payload in [
    json!({"type": "response.output_text.delta", "delta": 17}),
    json!({"type": "response.completed", "response": "invalid"}),
    json!({"type": "response.failed", "sequence_number": "invalid"}),
  ] {
    // The shared schema remains tolerant for existing callers. Direct streaming
    // detects this fallback so malformed known events cannot end successfully.
    assert!(matches!(
      serde_json::from_value::<ResponsesEvent>(payload.clone()).unwrap(),
      ResponsesEvent::Other(_)
    ));
    let wire = format!("data: {payload}\n\n");
    let transport = MockTransport::new(wire, 3);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
    assert!(matches!(run(stream.next_event()), Err(StreamError::InvalidEvent(_))));
    assert_eq!(stats.borrow().dropped, 1);
    assert!(run(stream.next_event()).unwrap().is_none());
  }
}

#[test]
fn event_limit_applies_to_data_and_ignored_fields_before_unbounded_reads() {
  for prefix in ["data: ", ": ", "id: "] {
    let wire = format!("{prefix}{}\n\n", "x".repeat(256));
    let transport = MockTransport::new(wire, 1);
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let options = StreamOptions {
      max_event_bytes: 32,
      ..StreamOptions::default()
    };
    let mut stream = run(client.responses_stream(&responses_request(), options)).unwrap();
    assert!(matches!(run(stream.next_event()), Err(StreamError::Sse(_))));
    assert!(
      stats.borrow().bytes <= 33,
      "parser must stop when the event exceeds the limit"
    );
    assert_eq!(stats.borrow().dropped, 1);
  }
}

#[test]
fn http_errors_keep_metadata_and_a_bounded_body_prefix() {
  for (body, limit, truncated) in [
    ("too many requests", 8, true),
    ("exact", 5, false),
    ("small", 8, false),
    ("not empty", 0, true),
    ("", 0, false),
  ] {
    let mut transport = MockTransport::new(body, 2);
    transport.status = 429;
    transport.headers = vec![
      Header::new("content-type", "application/json"),
      Header::new("retry-after", "7"),
    ];
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let options = StreamOptions {
      max_error_body_bytes: limit,
      ..StreamOptions::default()
    };
    let result = run(client.responses_stream(&responses_request(), options));
    let Err(StreamError::HttpStatus {
      response,
      truncated: actual,
    }) = result
    else {
      panic!("expected retained HTTP failure");
    };
    assert_eq!(response.status, 429);
    assert_eq!(response.headers[1].value, "7");
    assert_eq!(response.body, body.as_bytes()[..limit.min(body.len())]);
    assert_eq!(actual, truncated);
    assert_eq!(stats.borrow().dropped, 1);
    assert!(
      stats.borrow().bytes <= limit + 2,
      "HTTP errors should not drain arbitrarily large bodies"
    );
  }
}

#[test]
fn successful_non_sse_content_type_is_rejected_without_reading_the_body() {
  for headers in [vec![], vec![Header::new("content-type", "application/json")]] {
    let mut transport = MockTransport::new("never read", 1);
    transport.headers = headers;
    let stats = transport.stats.clone();
    let mut client = client(transport);
    assert!(matches!(
      run(client.responses_stream(&responses_request(), StreamOptions::default())),
      Err(StreamError::UnexpectedContentType { status: 200, .. })
    ));
    assert_eq!(stats.borrow().reads, 0);
    assert_eq!(stats.borrow().dropped, 1);
  }
}

#[test]
fn transport_failures_and_invalid_read_sizes_end_the_stream() {
  let mut transport = MockTransport::new("", 1);
  transport.open_error = true;
  let mut open_client = client(transport);
  assert!(matches!(
    run(open_client.responses_stream(&responses_request(), StreamOptions::default())),
    Err(StreamError::Transport(TransportError("connection failed")))
  ));

  for invalid_size in [false, true] {
    let mut transport = MockTransport::new("", 1);
    transport.read_error = !invalid_size;
    transport.invalid_read_size = invalid_size;
    let stats = transport.stats.clone();
    let mut client = client(transport);
    let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
    let error = run(stream.next_event()).unwrap_err();
    if invalid_size {
      assert!(matches!(error, StreamError::InvalidReadSize));
    } else {
      assert!(matches!(
        error,
        StreamError::Transport(TransportError("body read failed"))
      ));
    }
    assert_eq!(stats.borrow().dropped, 1);
    assert!(run(stream.next_event()).unwrap().is_none());
  }
}

#[test]
fn chat_provider_errors_do_not_decode_as_empty_success_chunks() {
  let transport = MockTransport::new(
    "data: {\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"private provider message\"}}\n\n",
    8,
  );
  let stats = transport.stats.clone();
  let mut client = client(transport);
  let mut stream = run(client.chat_completions_stream(&chat_request(), StreamOptions::default())).unwrap();
  let error = run(stream.next_event()).unwrap_err();
  assert!(!format!("{error:?}").contains("private provider message"));
  let StreamError::Provider { data } = error else {
    panic!("expected provider error");
  };
  assert_eq!(data["error"]["code"], "rate_limit_exceeded");
  assert_eq!(stats.borrow().dropped, 1);
  assert!(run(stream.next_event()).unwrap().is_none());
}

#[test]
fn chat_error_event_cannot_end_successfully_with_a_done_sentinel() {
  let transport = MockTransport::new("event: error\ndata: [DONE]\n\n", 1);
  let stats = transport.stats.clone();
  let mut client = client(transport);
  let mut stream = run(client.chat_completions_stream(&chat_request(), StreamOptions::default())).unwrap();
  assert!(matches!(run(stream.next_event()), Err(StreamError::InvalidEvent(_))));
  assert_eq!(stats.borrow().dropped, 1);
  assert!(run(stream.next_event()).unwrap().is_none());
}

#[test]
fn cancelling_next_event_future_preserves_partial_event_and_explicit_cancel_releases_body() {
  let wire = concat!(
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
    "data: {\"type\":\"response.completed\"}\n\n"
  );
  let transport = MockTransport::new(wire, 1);
  let stats = transport.stats.clone();
  let mut client = client(transport);
  let mut stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
  {
    let next = stream.next_event();
    let mut next = pin!(next);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..8 {
      assert!(next.as_mut().poll(&mut context).is_pending());
    }
  }
  assert!(stats.borrow().bytes > 0, "cancellation should occur in a partial event");
  let Some(ResponsesEvent::OutputTextDelta { delta, .. }) = run(stream.next_event()).unwrap() else {
    panic!("cancelled next_event lost parser progress");
  };
  assert_eq!(delta, "hello");
  stream.cancel();
  assert_eq!(stats.borrow().dropped, 1);
  let reads = stats.borrow().reads;
  assert!(run(stream.next_event()).unwrap().is_none());
  assert_eq!(stats.borrow().reads, reads);
  drop(stream);
  assert_eq!(stats.borrow().dropped, 1);
  assert_eq!(client.transport().stats.borrow().dropped, 1);
}

#[test]
fn dropping_a_stream_cancels_without_waiting_for_more_events() {
  let transport = MockTransport::new("infinite stream fixture", 1);
  let stats = transport.stats.clone();
  let mut client = client(transport);
  let stream = run(client.responses_stream(&responses_request(), StreamOptions::default())).unwrap();
  drop(stream);
  assert_eq!(stats.borrow().dropped, 1);
  assert_eq!(stats.borrow().reads, 0);
}
