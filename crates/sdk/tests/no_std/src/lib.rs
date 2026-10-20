//! Compile and exercise the public direct API with an entirely alloc-only graph.
//!
//! The in-memory transport is deliberately not Send. Firmware supplies its own
//! allocator, executor and HTTP/TLS implementation instead.

#![no_std]

extern crate alloc;

use alloc::rc::Rc;
use core::{
  convert::Infallible,
  future::Future,
  pin::pin,
  task::{Context, Poll, Waker},
};
use tokn_sdk::{
  chat_completions::ChatRequest,
  direct::{OpenAi, Request, Response, Transport},
  responses::ResponsesRequest,
  DirectClient,
};

struct MemoryTransport(Rc<()>);

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

#[cfg(test)]
mod tests {
  #[test]
  fn portable_client_round_trip() {
    super::buffered_round_trip();
  }
}
