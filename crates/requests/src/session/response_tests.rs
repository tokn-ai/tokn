use super::*;
use core::convert::Infallible;
use serde_json::json;
use std::{cell::RefCell, rc::Rc};

struct Source {
  bytes: Vec<u8>,
  position: usize,
  reads: Rc<RefCell<usize>>,
}

impl Transport for Source {
  type Error = Infallible;

  async fn send(&mut self, _: Request) -> Result<ResponseHead, Self::Error> {
    unreachable!("fixture response is already open")
  }

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    *self.reads.borrow_mut() += 1;
    assert!(
      self.position < self.bytes.len(),
      "successful native terminal must close before upstream EOF"
    );
    let count = buffer.len().min(self.bytes.len() - self.position);
    buffer[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }
}

struct Time;
impl Clock for Time {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    0
  }
  async fn sleep(&mut self, _: Duration) {
    unreachable!("native response reads must not retry")
  }
}

struct Recorded(Rc<RefCell<Vec<bool>>>);
impl Events for Recorded {
  fn enabled(&self) -> bool {
    false
  }
  fn emit(&self, _: tokn_core::request_event::RequestEvent) {
    unreachable!("fixture disables payload recording")
  }
  fn observe(&self, event: Lifecycle<'_>) {
    if event.stage == "completed" {
      self.0.borrow_mut().push(event.success.unwrap());
    }
  }
}

type LiveStream = EventStream<Source, Time, Recorded>;

struct Fixture {
  stream: LiveStream,
  reads: Rc<RefCell<usize>>,
  completions: Rc<RefCell<Vec<bool>>>,
}

fn stream(frames: &[Value]) -> Fixture {
  let mut bytes = Vec::new();
  for frame in frames {
    bytes.extend_from_slice(b"data: ");
    bytes.extend(serde_json::to_vec(frame).unwrap());
    bytes.extend_from_slice(b"\n\n");
  }
  bytes.extend_from_slice(b"data: [DONE]\n\n");
  let reads = Rc::new(RefCell::new(0));
  let completions = Rc::new(RefCell::new(Vec::new()));
  let request = json!({"model":"model", "messages":[{"role":"user","content":"question"}], "stream":true});
  let max_response_bytes = Limits::default().max_response_bytes;
  let generation = NativeGeneration::new(
    ProviderOrigin::new(
      "provider",
      "driver",
      "account",
      "https://backend.example/v1",
      Endpoint::ChatCompletions,
      "model",
    ),
    Endpoint::Responses,
    None,
    request,
    ContinuationOutcome::Started,
    Limits::default(),
  )
  .unwrap();
  let source = Source {
    bytes,
    position: 0,
    reads: reads.clone(),
  };
  let stream = EventStream::new(
    Observer {
      clock: Time,
      events: Recorded(completions.clone()),
    },
    Limits {
      max_response_bytes,
      ..Limits::default()
    },
    Exchange(source),
    Endpoint::ChatCompletions,
    Endpoint::Responses,
    SmolStr::new("model"),
    "request-fixture".into(),
    0,
    ResponseHead {
      status: 200,
      headers: HeaderMap::new(),
    },
    false,
    true,
    Some(generation),
  );
  Fixture {
    stream,
    reads,
    completions,
  }
}

fn complete_frames() -> Vec<Value> {
  alloc::vec![
    json!({"id":"response-fixture", "model":"model", "choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}),
    json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
  ]
}

#[tokio::test]
async fn completed_generation_is_lazy_terminal_gated_and_taken_once() {
  let Fixture {
    mut stream,
    reads,
    completions,
  } = stream(&complete_frames());
  assert_eq!(*reads.borrow(), 0);
  assert!(stream.take_completed_generation().is_none());
  while stream.next_event().await.unwrap().is_some() {
    if stream.processor.outcome() != Some(StreamOutcome::Complete) {
      assert!(stream.take_completed_generation().is_none());
    }
  }
  let (canonical, continuation) = stream.take_completed_generation().unwrap();
  assert_eq!(canonical["output_text"], "hello");
  assert_eq!(continuation.outcome, ContinuationOutcome::Started);
  assert!(stream.take_completed_generation().is_none());
  assert_eq!(*completions.borrow(), [true]);
}

#[tokio::test]
async fn cancellation_discards_native_completion_until_terminal_output_is_delivered() {
  let Fixture {
    mut stream,
    completions,
    ..
  } = stream(&complete_frames());
  while stream.completed_generation.is_none() {
    assert!(stream.next_event().await.unwrap().is_some());
  }
  assert_eq!(
    stream.processor.outcome(),
    None,
    "translated terminal output is still queued"
  );
  assert!(stream.take_completed_generation().is_none());
  stream.cancel();
  assert!(stream.take_completed_generation().is_none());
  assert!(stream.next_event().await.unwrap().is_none());
  assert_eq!(*completions.borrow(), [false]);
}
