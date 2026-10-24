//! In-memory configuration and borrowed adapters for the shared request session.
//!
//! The application owns transport and runtime services. All interpretation,
//! routing, provider policy, retries, and response lifecycle run in `session`.

use crate::session::{self, ExecutionState, ModelCapabilities, ModelInventory, RequestInput};
use alloc::{
  collections::{BTreeMap, BTreeSet},
  format,
  string::{String, ToString},
  vec::Vec,
};
use core::{
  cell::{Cell, RefCell, RefMut},
  time::Duration,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokn_core::{
  account::AccountConfig,
  headers::{HeaderMap, HeaderName, HeaderValue},
  request_event::{RequestEvent, RequestEventSink},
  types::{Bytes, Shared, SmolStr},
  Endpoint,
};
use tokn_policy::{ExecutionPlan, ProviderId, RelayDestination, RoutePlan};
use tokn_service::transport::{EncodeError, Request, ResponseHead, Transport};

mod events;
#[cfg(all(test, feature = "std"))]
mod interpretation_tests;
pub use crate::session::{Error, Limits};
pub use events::EventRuntime;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestOptions {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub profile: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub request_id: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub session_id: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub project_id: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub initiator: Option<String>,
  #[serde(skip_serializing_if = "Vec::is_empty")]
  pub headers: Vec<(String, String)>,
}

impl RequestOptions {
  /// Build SDK inbound headers using standard precedence: defaults, option
  /// fields, then explicit headers. Header aliases are interpreted afterwards.
  pub fn inbound_headers(&self) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(&tokn_headers::keys::CONTENT_TYPE, "application/json");
    headers.insert(&tokn_headers::keys::ACCEPT, "application/json");
    for (name, value) in [
      ("x-request-id", self.request_id.as_deref()),
      ("x-session-id", self.session_id.as_deref()),
      ("x-project-cwd", self.project_id.as_deref()),
      ("x-initiator", self.initiator.as_deref()),
    ] {
      if let Some(value) = value {
        headers.insert(name, HeaderValue::from_string(value.to_string()));
      }
    }
    for (name, value) in &self.headers {
      headers.insert(HeaderName::new(name.as_str()), HeaderValue::from_string(value.clone()));
    }
    headers
  }

  pub fn is_empty(&self) -> bool {
    self == &Self::default()
  }
  pub fn with_profile(mut self, value: impl Into<String>) -> Self {
    self.profile = Some(value.into());
    self
  }
  pub fn with_request_id(mut self, value: impl Into<String>) -> Self {
    self.request_id = Some(value.into());
    self
  }
  pub fn with_session_id(mut self, value: impl Into<String>) -> Self {
    self.session_id = Some(value.into());
    self
  }
  pub fn with_project_id(mut self, value: impl Into<String>) -> Self {
    self.project_id = Some(value.into());
    self
  }
  pub fn with_initiator(mut self, value: impl Into<String>) -> Self {
    self.initiator = Some(value.into());
    self
  }
  pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
    self.headers.push((name.into(), value.into()));
    self
  }
}

/// Application-supplied monotonic time, wall time, timer, and lifecycle observer.
#[allow(async_fn_in_trait)]
pub trait Runtime {
  fn now(&self) -> Duration;
  fn unix_seconds(&self) -> u64;
  fn unix_millis(&self) -> i64 {
    self.unix_seconds().saturating_mul(1_000).min(i64::MAX as u64) as i64
  }
  async fn sleep(&mut self, duration: Duration);
  /// Optional typed request-event destination. Delivery is synchronous and local.
  fn event_sink(&mut self) -> Option<&mut dyn RequestEventSink> {
    None
  }
  fn observe(&mut self, _event: Lifecycle<'_>) {}
}

pub use crate::session::Lifecycle;

/// An in-memory client with one exclusive, application-owned HTTP exchange.
///
/// A streaming response retains the transport lease. State transactions are
/// shared with standard execution and never retain a lock across I/O.
pub struct Injected<T, R> {
  state: Shared<ExecutionState>,
  transport: RefCell<T>,
  runtime: RefCell<R>,
  limits: Limits,
  request_sequence: Cell<u64>,
}

impl<T: Transport, R: Runtime> Injected<T, R> {
  pub fn new(plan: ExecutionPlan, accounts: Vec<AccountConfig>, transport: T, runtime: R) -> Result<Self, Error> {
    Self::with_models(plan, accounts, BTreeMap::new(), transport, runtime)
  }

  pub fn with_models(
    plan: ExecutionPlan,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, BTreeSet<SmolStr>>,
    transport: T,
    runtime: R,
  ) -> Result<Self, Error> {
    let models = models
      .into_iter()
      .map(|(provider, models)| {
        (
          provider,
          models
            .into_iter()
            .map(|model| (model, ModelCapabilities::default()))
            .collect(),
        )
      })
      .collect();
    Self::with_model_metadata(plan, accounts, models, transport, runtime)
  }

  /// Supply the same model evidence used by standard catalogue/discovery clients.
  pub fn with_model_metadata(
    plan: ExecutionPlan,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, ModelInventory>,
    transport: T,
    runtime: R,
  ) -> Result<Self, Error> {
    if plan.profiles().is_empty() {
      return Err(Error::new("config", "no execution profiles configured"));
    }
    for (id, profile) in plan.profiles() {
      if matches!(plan.route(profile.route()), Some(RoutePlan::Relay(relay))
        if !matches!(relay.destination(), RelayDestination::FixedProvider(_)))
      {
        return Err(Error::new(
          "config",
          format!("profile '{id}' requires original-destination proxy context"),
        ));
      }
    }
    Ok(Self {
      state: Shared::new(ExecutionState::with_models(plan, accounts, models)?),
      transport: RefCell::new(transport),
      runtime: RefCell::new(runtime),
      limits: Limits::default(),
      request_sequence: Cell::new(0),
    })
  }

  pub fn with_limits(mut self, limits: Limits) -> Self {
    self.limits = limits;
    self
  }

  /// Attach event delivery without changing transport or timing services.
  pub fn with_event_sink<S: RequestEventSink>(self, sink: S) -> Injected<T, EventRuntime<R, S>> {
    Injected {
      state: self.state,
      transport: self.transport,
      runtime: RefCell::new(EventRuntime::new(self.runtime.into_inner(), sink)),
      limits: self.limits,
      request_sequence: self.request_sequence,
    }
  }

  pub async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<Response<'_, T, R>, Error> {
    let transport = self
      .transport
      .try_borrow_mut()
      .map_err(|_| Error::new("send", "previous response still owns the transport"))?;
    // Callbacks may reenter after a stream releases its transport. Report busy
    // while a callback owns the runtime instead of panicking on its RefCell.
    drop(
      self
        .runtime
        .try_borrow_mut()
        .map_err(|_| Error::new("runtime", "runtime services are busy delivering a callback"))?,
    );
    let request_id = options.request_id.clone().unwrap_or_else(|| {
      let sequence = self.request_sequence.get();
      self.request_sequence.set(sequence.wrapping_add(1));
      format!("req-{}-{sequence}", self.runtime.borrow().now().as_nanos())
    });
    let mut input = RequestInput::new(endpoint, body, options.inbound_headers(), request_id);
    input.profile = options.profile;
    session::Session::new(
      self.state.clone(),
      input,
      BorrowedTransport(transport),
      LocalClock(&self.runtime),
      LocalEvents(&self.runtime),
    )
    .with_limits(self.limits)
    .run()
    .await
  }
}

/// Standard and injected callers receive the same response and stream objects.
pub type Response<'a, T, R> = session::Response<BorrowedTransport<'a, T>, LocalClock<'a, R>, LocalEvents<'a, R>>;
pub type ResponseBody<'a, T, R> =
  session::ResponseBody<BorrowedTransport<'a, T>, LocalClock<'a, R>, LocalEvents<'a, R>>;
pub type EventStream<'a, T, R> = session::EventStream<BorrowedTransport<'a, T>, LocalClock<'a, R>, LocalEvents<'a, R>>;

/// Exclusive lease on an application's transport, retained by streamed responses.
pub struct BorrowedTransport<'a, T>(RefMut<'a, T>);
impl<T: Transport> Transport for BorrowedTransport<'_, T> {
  type Error = T::Error;
  async fn send(&mut self, request: Request) -> Result<ResponseHead, Self::Error> {
    self.0.send(request).await
  }
  async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
    self.0.read(buffer).await
  }
  fn configure_body_mode(&mut self, preserve_encoding: bool) {
    self.0.configure_body_mode(preserve_encoding);
  }
  fn response_is_decoded(&self) -> bool {
    self.0.response_is_decoded()
  }
  fn encode_body(&self, body: &[u8], encoding: Option<&str>) -> Result<Bytes, EncodeError> {
    self.0.encode_body(body, encoding)
  }
  fn accept_encoding(&self) -> &str {
    self.0.accept_encoding()
  }
  fn close_response(&mut self) {
    self.0.close_response();
  }
  fn recoverable(&self, error: &Self::Error) -> bool {
    self.0.recoverable(error)
  }
}

/// Borrowed timing adapter for application runtime services.
pub struct LocalClock<'a, R>(&'a RefCell<R>);
impl<R: Runtime> session::Clock for LocalClock<'_, R> {
  fn now(&self) -> Duration {
    self.0.borrow().now()
  }
  fn unix_seconds(&self) -> u64 {
    self.0.borrow().unix_seconds()
  }
  fn unix_millis(&self) -> i64 {
    self.0.borrow().unix_millis()
  }
  #[allow(
    clippy::await_holding_refcell_ref,
    reason = "the application's timer is exclusive; the transport lease rejects concurrent execution and cancellation releases the borrow"
  )]
  #[allow(
    clippy::await_holding_refcell_ref,
    reason = "the exclusive injected runtime lease is retained across sleep and released on cancellation"
  )]
  async fn sleep(&mut self, duration: Duration) {
    self.0.borrow_mut().sleep(duration).await;
  }
}

/// Borrowed event adapter. The runtime stays borrowed for each callback.
pub struct LocalEvents<'a, R>(&'a RefCell<R>);
impl<R: Runtime> session::Events for LocalEvents<'_, R> {
  fn enabled(&self) -> bool {
    self.0.borrow_mut().event_sink().is_some()
  }
  fn emit(&self, event: RequestEvent) {
    if let Some(sink) = self.0.borrow_mut().event_sink() {
      sink.emit_request(event);
    }
  }
  fn observe(&self, event: Lifecycle<'_>) {
    self.0.borrow_mut().observe(event);
  }
}
