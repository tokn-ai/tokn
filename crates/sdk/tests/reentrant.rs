//! Injected exchanges must release driver resources on cancellation and reject
//! synchronous observer reentry without panicking or closing a newer response.

use std::{
  boxed::Box,
  cell::{Cell, RefCell},
  collections::VecDeque,
  convert::Infallible,
  future::{poll_fn, Future},
  pin::pin,
  rc::{Rc, Weak},
  string::{String, ToString},
  task::{Context, Poll, Waker},
  time::Duration,
  vec,
  vec::Vec,
};

use serde_json::json;
use tokn_sdk::{
  account::AccountConfig,
  chat_completions::ChatRequest,
  transport::{Request, ResponseHead, Transport},
  Client, EventRuntime, Injected, Lifecycle, RequestEvent, RequestEventPayload, RequestEventSink, RequestOptions,
  Runtime, StageEvent,
};

const JSON: &[u8] = br#"{"id":"chat-local","object":"chat.completion","model":"model","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#;
const RETRY_ERROR: &[u8] = br#"{"error":{"message":"retry"}}"#;
const SSE: &[u8] =
  b"data: {\"id\":\"chat-local\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n";

type LocalClient = Client<Injected<LocalTransport, LocalRuntime>>;
type TypedClient = Client<Injected<LocalTransport, EventRuntime<LocalRuntime, TypedSink>>>;

#[derive(Default)]
struct Shared {
  sent: Cell<usize>,
  closed: Cell<usize>,
  released: Cell<usize>,
  pause_send: Cell<bool>,
  pause_read: Cell<bool>,
  pause_sleep: Cell<bool>,
  sleeping: Cell<bool>,
  resource: RefCell<Weak<Resource>>,
  client: RefCell<Weak<LocalClient>>,
  typed_client: RefCell<Weak<TypedClient>>,
  typed_events: RefCell<Vec<RequestEvent>>,
  reenter: Cell<bool>,
  typed_reenter: Cell<bool>,
  reentry_errors: RefCell<Vec<String>>,
}

struct Resource(Rc<Shared>);
impl Drop for Resource {
  fn drop(&mut self) {
    self.0.released.set(self.0.released.get() + 1);
  }
}

struct Active {
  _resource: Rc<Resource>,
  bytes: &'static [u8],
  position: usize,
}
struct LocalTransport {
  shared: Rc<Shared>,
  replies: VecDeque<&'static [u8]>,
  active: Option<Active>,
}
impl Transport for LocalTransport {
  type Error = Infallible;

  async fn send(&mut self, _: Request) -> Result<ResponseHead, Infallible> {
    self.shared.sent.set(self.shared.sent.get() + 1);
    let resource = Rc::new(Resource(self.shared.clone()));
    *self.shared.resource.borrow_mut() = Rc::downgrade(&resource);
    let bytes = self.replies.pop_front().expect("fixture reply for every request");
    self.active = Some(Active {
      _resource: resource,
      bytes,
      position: 0,
    });
    poll_fn(|_| {
      if self.shared.pause_send.get() {
        Poll::Pending
      } else {
        Poll::Ready(())
      }
    })
    .await;
    let mut head = ResponseHead {
      status: if bytes == RETRY_ERROR { 503 } else { 200 },
      headers: Default::default(),
    };
    head.headers.insert(
      "content-type",
      if bytes == SSE {
        "text/event-stream"
      } else {
        "application/json"
      },
    );
    Ok(head)
  }

  async fn read(&mut self, output: &mut [u8]) -> Result<usize, Infallible> {
    poll_fn(|_| {
      if self.shared.pause_read.get() {
        return Poll::Pending;
      }
      let active = self.active.as_mut().expect("response remains open while reading");
      let count = output.len().min(active.bytes.len() - active.position);
      output[..count].copy_from_slice(&active.bytes[active.position..active.position + count]);
      active.position += count;
      Poll::Ready(Ok(count))
    })
    .await
  }

  fn close_response(&mut self) {
    self.shared.closed.set(self.shared.closed.get() + 1);
    self.active = None;
  }
}

struct LocalRuntime(Rc<Shared>);
impl Runtime for LocalRuntime {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    0
  }
  async fn sleep(&mut self, _: Duration) {
    self.0.sleeping.set(true);
    poll_fn(|_| {
      if self.0.pause_sleep.get() {
        Poll::Pending
      } else {
        Poll::Ready(())
      }
    })
    .await;
  }
  fn observe(&mut self, event: Lifecycle<'_>) {
    if event.stage == "completed" && event.success == Some(true) && self.0.reenter.replace(false) {
      let client = self.0.client.borrow().upgrade().expect("client remains alive");
      let result = ready(
        client
          .chat_completions()
          .create_with(&request(), RequestOptions::default().with_request_id("from-observer")),
      );
      match result {
        Err(error) => self.0.reentry_errors.borrow_mut().push(error.to_string()),
        Ok(_) => panic!("an observer cannot reenter its borrowed runtime"),
      }
    }
  }
}

struct TypedSink(Rc<Shared>);
impl RequestEventSink for TypedSink {
  fn emit_request(&mut self, event: RequestEvent) {
    let completed = matches!(
      event.payload,
      RequestEventPayload::Stage(StageEvent::Completed { success: true, .. })
    );
    self.0.typed_events.borrow_mut().push(event);
    if completed && self.0.typed_reenter.replace(false) {
      let client = self.0.typed_client.borrow().upgrade().expect("client remains alive");
      let error = ready(
        client
          .chat_completions()
          .create_with(&request(), RequestOptions::default().with_request_id("from-typed-sink")),
      )
      .expect_err("a typed sink cannot reenter its borrowed runtime");
      self.0.reentry_errors.borrow_mut().push(error.to_string());
    }
  }
}

fn client(shared: &Rc<Shared>, replies: impl IntoIterator<Item = &'static [u8]>) -> Rc<LocalClient> {
  client_with_retry(shared, replies, false)
}

fn client_with_retry(
  shared: &Rc<Shared>,
  replies: impl IntoIterator<Item = &'static [u8]>,
  retry: bool,
) -> Rc<LocalClient> {
  let client = Rc::new(Client::from_platform(platform(shared, replies, retry)));
  *shared.client.borrow_mut() = Rc::downgrade(&client);
  client
}

fn platform(
  shared: &Rc<Shared>,
  replies: impl IntoIterator<Item = &'static [u8]>,
  retry: bool,
) -> Injected<LocalTransport, LocalRuntime> {
  let mut raw = json!({
    "schema_version": 2,
    "defaults": {"provider": {"kind":"fixed", "provider":"upstream"}, "wire_identity":"none"},
    "providers": {"upstream": {"driver":"openai", "base_url":"https://example.com/v1"}}
  });
  if retry {
    raw["defaults"]["retry"] = json!({"kind":"recoverable", "policy":"retry"});
    raw["retry_policies"] = json!({"retry":{"max_retries":1,"initial_backoff_ms":1}});
  }
  let raw = serde_json::from_value(raw).unwrap();
  let plan = tokn_sdk::config::v2::compile_execution(&raw).unwrap();
  let account: AccountConfig =
    serde_json::from_value(json!({"id":"local", "provider":"upstream", "api_key":"test-key"})).unwrap();
  let mut accounts = vec![account];
  if retry {
    accounts.push(
      serde_json::from_value(json!({
        "id":"fallback", "provider":"upstream", "api_key":"backup-key", "tier":"fallback"
      }))
      .unwrap(),
    );
  }
  Injected::new(
    plan,
    accounts,
    LocalTransport {
      shared: shared.clone(),
      replies: replies.into_iter().collect(),
      active: None,
    },
    LocalRuntime(shared.clone()),
  )
  .unwrap()
}

fn typed_client(shared: &Rc<Shared>, replies: impl IntoIterator<Item = &'static [u8]>) -> Rc<TypedClient> {
  let client = Rc::new(Client::from_platform(
    platform(shared, replies, false).with_event_sink(TypedSink(shared.clone())),
  ));
  *shared.typed_client.borrow_mut() = Rc::downgrade(&client);
  client
}

fn request() -> ChatRequest {
  serde_json::from_value(json!({"model":"model", "messages":[{"role":"user", "content":"hello"}]})).unwrap()
}
fn ready<F: Future>(future: F) -> F::Output {
  let mut future = pin!(future);
  match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
    Poll::Ready(value) => value,
    Poll::Pending => panic!("fixture should be immediately ready"),
  }
}

#[test]
fn terminal_observer_reentry_returns_busy_without_closing_another_exchange() {
  let state = Rc::new(Shared::default());
  let client = client(&state, [SSE, JSON]);
  state.reenter.set(true);
  let endpoint = client.chat_completions();
  let request = request();
  let mut events = ready(endpoint.stream(&request)).unwrap();
  assert!(ready(events.next_event()).unwrap().is_some());
  assert!(ready(events.next_event()).unwrap().unwrap().is_done());
  assert_eq!(state.reentry_errors.borrow().len(), 1);
  assert!(state.reentry_errors.borrow()[0].contains("runtime"));
  assert_eq!(state.sent.get(), 1);
  assert_eq!(state.closed.get(), 1);
  assert_eq!(state.released.get(), 1);
  assert!(state.resource.borrow().upgrade().is_none());
  drop(events);
  assert_eq!(state.closed.get(), 1);
  ready(client.chat_completions().create(&request)).unwrap();
  assert_eq!(state.sent.get(), 2);
  assert_eq!(state.closed.get(), 2);
  assert_eq!(state.released.get(), 2);
}

#[test]
fn terminal_typed_sink_reentry_returns_busy_and_keeps_events_once() {
  let state = Rc::new(Shared::default());
  let client = typed_client(&state, [SSE, JSON]);
  state.typed_reenter.set(true);
  let endpoint = client.chat_completions();
  let request = request();
  let mut stream = ready(endpoint.stream(&request)).unwrap();
  assert!(ready(stream.next_event()).unwrap().is_some());
  assert!(ready(stream.next_event()).unwrap().unwrap().is_done());
  assert_eq!(state.reentry_errors.borrow().len(), 1);
  assert!(state.reentry_errors.borrow()[0].contains("runtime"));
  assert_eq!(state.sent.get(), 1);
  assert_eq!(state.closed.get(), 1);
  assert_eq!(state.released.get(), 1);
  drop(stream);
  let completed = state
    .typed_events
    .borrow()
    .iter()
    .filter(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Completed { .. })))
    .count();
  assert_eq!(completed, 1);
  ready(endpoint.create(&request)).unwrap();
  assert_eq!(state.sent.get(), 2);
  assert_eq!(state.closed.get(), 2);
}

#[test]
fn cancelled_send_and_read_emit_one_typed_failure() {
  for pause_send in [true, false] {
    let state = Rc::new(Shared::default());
    state.pause_send.set(pause_send);
    state.pause_read.set(!pause_send);
    let client = typed_client(&state, [JSON, JSON]);
    let endpoint = client.chat_completions();
    let request = request();
    let mut operation = Box::pin(endpoint.create(&request));
    assert!(operation
      .as_mut()
      .poll(&mut Context::from_waker(Waker::noop()))
      .is_pending());
    drop(operation);
    let events = state.typed_events.borrow();
    let outcomes: Vec<_> = events
      .iter()
      .filter_map(|event| match event.payload {
        RequestEventPayload::Stage(StageEvent::Completed { success, .. }) => Some(success),
        _ => None,
      })
      .collect();
    assert_eq!(outcomes, [false]);
    assert_eq!(
      events
        .iter()
        .filter(|event| matches!(event.payload, RequestEventPayload::Stage(StageEvent::Error { .. })))
        .count(),
      1
    );
    drop(events);
    assert_eq!(state.closed.get(), 1);
    assert_eq!(state.released.get(), 1);
    state.pause_send.set(false);
    state.pause_read.set(false);
    ready(endpoint.create(&request)).unwrap();
  }
}

#[test]
fn cancelled_send_and_buffered_read_release_transport_owned_resources() {
  for pause_during_send in [true, false] {
    let state = Rc::new(Shared::default());
    state.pause_send.set(pause_during_send);
    state.pause_read.set(!pause_during_send);
    let client = client(&state, [JSON, JSON]);
    let endpoint = client.chat_completions();
    let request = request();
    let mut operation = Box::pin(endpoint.create(&request));
    assert!(operation
      .as_mut()
      .poll(&mut Context::from_waker(Waker::noop()))
      .is_pending());
    assert!(state.resource.borrow().upgrade().is_some());
    assert_eq!(state.closed.get(), 0);
    drop(operation);
    assert!(state.resource.borrow().upgrade().is_none());
    assert_eq!(state.closed.get(), 1);
    assert_eq!(state.released.get(), 1);
    state.pause_send.set(false);
    state.pause_read.set(false);
    ready(endpoint.create(&request)).unwrap();
    assert_eq!(state.closed.get(), 2);
    assert_eq!(state.released.get(), 2);
  }
}

#[test]
fn explicit_stream_cancellation_releases_the_driver_once() {
  let state = Rc::new(Shared::default());
  let client = client(&state, [SSE, JSON]);
  let endpoint = client.chat_completions();
  let request = request();
  let mut stream = ready(endpoint.stream(&request)).unwrap();
  assert!(ready(stream.next_event()).unwrap().is_some());
  stream.cancel();
  stream.cancel();
  assert!(ready(stream.next_event()).unwrap().is_none());
  assert_eq!(state.closed.get(), 1);
  assert_eq!(state.released.get(), 1);
  assert!(state.resource.borrow().upgrade().is_none());
  ready(endpoint.create(&request)).unwrap();
  drop(stream);
  assert_eq!(state.closed.get(), 2);
  assert_eq!(state.released.get(), 2);
}

#[test]
fn cancelling_retry_sleep_releases_runtime_and_transport_borrows() {
  let state = Rc::new(Shared::default());
  state.pause_sleep.set(true);
  let client = client_with_retry(&state, [RETRY_ERROR, JSON], true);
  let endpoint = client.chat_completions();
  let request = request();
  let mut operation = Box::pin(endpoint.create(&request));
  assert!(operation
    .as_mut()
    .poll(&mut Context::from_waker(Waker::noop()))
    .is_pending());
  assert!(state.sleeping.get());
  drop(operation);
  assert!(state.resource.borrow().upgrade().is_none());
  assert_eq!(state.closed.get(), 1);
  assert_eq!(state.released.get(), 1);
  state.pause_sleep.set(false);
  ready(endpoint.create(&request)).unwrap();
  assert_eq!(state.closed.get(), 2);
  assert_eq!(state.released.get(), 2);
}
