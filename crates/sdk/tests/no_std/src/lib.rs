//! Compile and exercise the public direct API with an entirely alloc-only graph.
//!
//! The in-memory transport and its borrowing body are deliberately not Send.
//! Firmware supplies its own allocator, executor and HTTP/TLS implementation.

#![no_std]

extern crate alloc;

use alloc::rc::Rc;
use core::{
  convert::Infallible,
  future::{poll_fn, Future},
  pin::pin,
  task::{Context, Poll, Waker},
};
use tokn_sdk::{
  chat_completions::{ChatEvent, ChatRequest},
  direct::{
    Header, OpenAi, Request, Response, StreamBody, StreamOptions, StreamingResponse, StreamingTransport, Transport,
  },
  responses::{ResponsesEvent, ResponsesRequest},
  DirectClient,
};

struct MemoryTransport(Rc<()>);

struct MemoryBody<'a> {
  transport: &'a mut MemoryTransport,
  bytes: &'static [u8],
  position: usize,
  yield_next: bool,
}

impl StreamBody for MemoryBody<'_> {
  type Error = Infallible;

  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    poll_fn(|context| {
      if self.yield_next {
        self.yield_next = false;
        context.waker().wake_by_ref();
        return Poll::Pending;
      }
      assert_eq!(Rc::strong_count(&self.transport.0), 1);
      // One-byte reads deliberately split UTF-8 code points and CRLF pairs.
      let length = buffer.len().min(1).min(self.bytes.len() - self.position);
      buffer[..length].copy_from_slice(&self.bytes[self.position..self.position + length]);
      self.position += length;
      self.yield_next = true;
      Poll::Ready(Ok(length))
    })
    .await
  }
}

impl StreamingTransport for MemoryTransport {
  type Error = Infallible;
  type Body<'a> = MemoryBody<'a>;

  async fn send_stream(&mut self, request: Request) -> Result<StreamingResponse<Self::Body<'_>>, Self::Error> {
    assert_eq!(request.method, "POST");
    assert!(request.url.starts_with("https://api.openai.com/v1/"));
    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["stream"], true);
    let bytes = if request.url.ends_with("/responses") {
      concat!(
        "event: response.output_text.delta\r\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"héllo 🌍\"}\r\n\r\n",
        "event: response.completed\r\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"status\":\"completed\"}}\r\n\r\n",
      )
      .as_bytes()
    } else {
      concat!(
        "data: {\"id\":\"chat_test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"héllo 🌍\"}}]}\n\n",
        "data: [DONE]\n\n",
      )
      .as_bytes()
    };
    Ok(StreamingResponse {
      status: 200,
      headers: alloc::vec![Header::new("content-type", "text/event-stream")],
      body: MemoryBody {
        transport: self,
        bytes,
        position: 0,
        yield_next: true,
      },
    })
  }
}

impl Transport for MemoryTransport {
  type Error = Infallible;

  async fn send(&mut self, request: Request) -> Result<Response, Self::Error> {
    assert_eq!(Rc::strong_count(&self.0), 1);
    assert_eq!(request.method, "POST");
    assert!(request.url.starts_with("https://api.openai.com/v1/"));
    let body = if request.url.ends_with("/responses") {
      br#"{"id":"resp_test","status":"completed","output_text":"hello"}"#.as_slice()
    } else {
      br#"{"id":"chat_test","choices":[{"message":{"role":"assistant","content":"hello"}}]}"#.as_slice()
    };
    Ok(Response {
      status: 200,
      headers: alloc::vec![],
      body: body.to_vec(),
    })
  }
}

fn run_ready<F: Future>(future: F) -> F::Output {
  let mut future = pin!(future);
  let mut context = Context::from_waker(Waker::noop());
  match future.as_mut().poll(&mut context) {
    Poll::Ready(result) => result,
    Poll::Pending => panic!("in-memory transport must be immediately ready"),
  }
}

fn run_yielding<F: Future>(future: F) -> F::Output {
  let mut future = pin!(future);
  let mut context = Context::from_waker(Waker::noop());
  for pending_polls in 0..1024 {
    if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
      assert!(pending_polls > 0, "stream reads must exercise a pending future");
      return result;
    }
  }
  panic!("in-memory stream did not finish within the poll budget");
}

/// Instantiates both endpoints, serialization, transport futures, and decoding.
/// Builds on bare metal and runs in a host test without enabling SDK std.
pub fn buffered_round_trip() {
  let mut client = DirectClient::new(
    MemoryTransport(Rc::new(())),
    OpenAi::new("test-key").expect("valid API key"),
  );
  let request: ResponsesRequest =
    serde_json::from_str(r#"{"model":"test-model","input":"hello","max_output_tokens":32}"#).unwrap();
  let response = run_ready(client.responses(&request)).unwrap();
  assert_eq!(response.data.id.as_deref(), Some("resp_test"));
  assert_eq!(response.data.output_text.as_deref(), Some("hello"));

  let request: ChatRequest =
    serde_json::from_str(r#"{"model":"test-model","messages":[{"role":"user","content":"hello"}]}"#).unwrap();
  let response = run_ready(client.chat_completions(&request)).unwrap();
  assert_eq!(response.data.id.as_deref(), Some("chat_test"));
  assert_eq!(response.data.choices.len(), 1);
}

/// Instantiates streaming with a borrowing, single-threaded body and yields at
/// every byte. Builds on bare metal; host tests exercise suspended parser reads.
pub fn streaming_round_trip() {
  let mut client = DirectClient::new(
    MemoryTransport(Rc::new(())),
    OpenAi::new("test-key").expect("valid API key"),
  );
  let request: ResponsesRequest =
    serde_json::from_str(r#"{"model":"test-model","input":"hello","max_output_tokens":32}"#).unwrap();
  let options = StreamOptions {
    max_event_bytes: 256,
    max_error_body_bytes: 64,
  };
  {
    let mut stream = run_ready(client.responses_stream(&request, options)).unwrap();
    assert_eq!(stream.status, 200);
    let Some(ResponsesEvent::OutputTextDelta { delta, .. }) = run_yielding(stream.next_event()).unwrap() else {
      panic!("expected a Responses text delta");
    };
    assert_eq!(delta, "héllo 🌍");
    assert!(matches!(
      run_yielding(stream.next_event()).unwrap(),
      Some(ResponsesEvent::Completed { .. })
    ));
    assert!(run_ready(stream.next_event()).unwrap().is_none());
  }
  let request: ChatRequest =
    serde_json::from_str(r#"{"model":"test-model","messages":[{"role":"user","content":"hello"}]}"#).unwrap();
  let mut stream = run_ready(client.chat_completions_stream(&request, options)).unwrap();
  let Some(ChatEvent::Chunk(chunk)) = run_yielding(stream.next_event()).unwrap() else {
    panic!("expected a Chat Completions chunk");
  };
  assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("héllo 🌍"));
  assert!(matches!(
    run_yielding(stream.next_event()).unwrap(),
    Some(ChatEvent::Done)
  ));
  assert!(run_ready(stream.next_event()).unwrap().is_none());
}

#[cfg(test)]
mod tests {
  #[test]
  fn portable_client_round_trip() {
    super::buffered_round_trip();
  }

  #[test]
  fn portable_streaming_round_trip() {
    super::streaming_round_trip();
  }
}
