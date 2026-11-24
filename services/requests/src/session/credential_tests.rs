use super::*;
use core::{
  convert::Infallible,
  future::Future,
  task::{Context, Poll},
};
use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
  },
  task::{Wake, Waker},
};
use tokn_core::request_event::RequestEvent;

const TOKEN: &[u8] = br#"{"token":"fresh-api-token","expires_at":3600}"#;
const REPLY: &[u8] = br#"{"id":"chat-test","object":"chat.completion","model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}"#;

#[derive(Default)]
struct Calls {
  refresh: AtomicUsize,
  operation: AtomicUsize,
  aborted_refresh: AtomicUsize,
}

struct Http {
  calls: Arc<Calls>,
  pause_refresh: bool,
  refreshing: bool,
  body: &'static [u8],
  position: usize,
}

impl Http {
  fn new(calls: Arc<Calls>, pause_refresh: bool) -> Self {
    Self {
      calls,
      pause_refresh,
      refreshing: false,
      body: &[],
      position: 0,
    }
  }
}

impl Transport for Http {
  type Error = Infallible;

  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    self.position = 0;
    if request.method == "GET" {
      assert_eq!(request.url, "https://example.com/token");
      assert_eq!(
        request.headers.get("authorization").unwrap().as_str(),
        "token long-lived-token"
      );
      self.calls.refresh.fetch_add(1, Ordering::SeqCst);
      self.refreshing = true;
      if self.pause_refresh {
        core::future::pending::<()>().await;
      }
      self.refreshing = false;
      self.body = TOKEN;
    } else {
      assert_eq!(request.method, "POST");
      assert_eq!(
        request.headers.get("authorization").unwrap().as_str(),
        "Bearer fresh-api-token"
      );
      self.calls.operation.fetch_add(1, Ordering::SeqCst);
      self.body = REPLY;
    }
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    Ok(ResponseHead { status: 200, headers })
  }

  async fn read(&mut self, output: &mut [u8]) -> Result<usize, Self::Error> {
    let count = output.len().min(self.body.len() - self.position);
    output[..count].copy_from_slice(&self.body[self.position..self.position + count]);
    self.position += count;
    Ok(count)
  }

  fn close_response(&mut self) {
    if core::mem::take(&mut self.refreshing) {
      self.calls.aborted_refresh.fetch_add(1, Ordering::SeqCst);
    }
    self.body = &[];
    self.position = 0;
  }
}

struct Time;
impl Clock for Time {
  fn now(&self) -> Duration {
    Duration::ZERO
  }
  fn unix_seconds(&self) -> u64 {
    1_000
  }
  async fn sleep(&mut self, _: Duration) {
    panic!("credential cancellation does not retry or sleep")
  }
}

#[derive(Clone, Default)]
struct Recorded(Arc<Mutex<Vec<RequestEvent>>>);
impl Events for Recorded {
  fn emit(&self, event: RequestEvent) {
    self.0.lock().unwrap().push(event);
  }
}

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
  fn wake(self: Arc<Self>) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
  fn wake_by_ref(self: &Arc<Self>) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
}

fn input(id: &str) -> RequestInput {
  RequestInput::new(
    Endpoint::ChatCompletions,
    serde_json::json!({"model":"gpt-test", "messages":[{"role":"user", "content":"hello"}]}),
    HeaderMap::new(),
    id.into(),
  )
}

#[test]
fn cancelling_refresh_owner_wakes_waiter_and_releases_credential_lease() {
  let raw = serde_json::from_value(serde_json::json!({
    "schema_version":2,
    "defaults":{"provider":{"kind":"fixed","provider":"upstream"}},
    "providers":{"upstream":{"driver":"github-copilot", "base_url":"https://example.com"}}
  }))
  .unwrap();
  let plan = tokn_config::v2::compile_execution(&raw).unwrap();
  let account = serde_json::from_value(serde_json::json!({
    "id":"account", "provider":"upstream", "refresh_token":"long-lived-token",
    "refresh_url":"https://example.com/token"
  }))
  .unwrap();
  let state = Arc::new(ExecutionState::new(plan, vec![account]).unwrap());
  let calls = Arc::new(Calls::default());
  let events = Recorded::default();
  let mut owner = Box::pin(
    Session::new(
      state.clone(),
      input("cancelled"),
      Http::new(calls.clone(), true),
      Time,
      events.clone(),
    )
    .run(),
  );
  let wakes = Arc::new(Wakes::default());
  let waker = Waker::from(wakes.clone());
  let mut context = Context::from_waker(&waker);
  assert!(owner.as_mut().poll(&mut context).is_pending());
  assert_eq!(calls.refresh.load(Ordering::SeqCst), 1);

  let mut waiter = Box::pin(
    Session::new(
      state.clone(),
      input("surviving"),
      Http::new(calls.clone(), false),
      Time,
      events.clone(),
    )
    .run(),
  );
  assert!(waiter.as_mut().poll(&mut context).is_pending());
  assert_eq!(
    calls.refresh.load(Ordering::SeqCst),
    1,
    "waiter must share the active credential exchange"
  );
  assert_eq!(calls.operation.load(Ordering::SeqCst), 0);

  drop(owner);
  assert_eq!(calls.aborted_refresh.load(Ordering::SeqCst), 1);
  assert_eq!(
    wakes.0.load(Ordering::SeqCst),
    1,
    "cancelled refresh must wake its waiter"
  );
  let Poll::Ready(response) = waiter.as_mut().poll(&mut context) else {
    panic!("waiter stayed blocked after cancellation");
  };
  assert_eq!(response.unwrap().status, 200);
  assert_eq!(calls.refresh.load(Ordering::SeqCst), 2);
  assert_eq!(calls.operation.load(Ordering::SeqCst), 1);
  state.bindings[0].credentials.with(|credentials| {
    assert!(!credentials.refreshing);
    assert!(credentials.waiters.is_empty());
    assert_eq!(
      credentials.account.access_token.as_ref().unwrap().expose(),
      "fresh-api-token"
    );
  });
  let events = events.0.lock().unwrap();
  let completed: Vec<_> = events
    .iter()
    .filter_map(|event| match event.payload {
      RequestEventPayload::Stage(StageEvent::Completed { success, .. }) => Some((event.request_id.as_str(), success)),
      _ => None,
    })
    .collect();
  assert_eq!(completed, [("cancelled", false), ("surviving", true)]);
}
