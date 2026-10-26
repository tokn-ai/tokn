//! One request implementation for standard and injected platform services.
//!
//! Domain state and policy are shared. Only HTTP exchange I/O, clocks/timers,
//! and event delivery are supplied by the execution host.
use crate::interpretation::RequestInterpretation;
use crate::retry::{RetryFailure, RetryPolicy};
use crate::send_policy::{AccountFeedback, AccountFeedbackPolicy, ResponsePolicy, SendOutcome};
use alloc::{
  collections::{BTreeMap, BTreeSet},
  format,
  string::{String, ToString},
  vec::Vec,
};
use core::{fmt, time::Duration};
use serde_json::Value;
use tokn_accounts::{
  link::portable::{link_pool_metadata, link_provider_metadata, DestinationMetadata},
  native,
  pool_state::{AffinityPolicy, PoolState, Selection},
  selection::{select_route, CandidateBinding, RouteSelection, SelectionRequest},
};
use tokn_core::generation::{GenerationOptions, ReasoningEffort};
use tokn_core::request_event::RecordEvent;
use tokn_core::{
  account::AccountConfig,
  headers::{HeaderMap, HeaderName, HeaderValue},
  provider::PreparedRequest,
  request_event::{RequestEventPayload, SentSummary, Stage, StageEvent},
  types::{Bytes, Shared, SmolStr},
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};
use tokn_policy::{
  AccountPoolId, DriverId, ExecutionRules, ProviderId, RelayCredentials, RelayDestination, RoutePlan, WireIdentity,
};
use tokn_service::transport::{Request, ResponseHead, Transport};

mod events;
mod response;

use events::Observer;
pub use events::{Events, Lifecycle, StreamProgress};
use response::{read_all, read_error_body};
pub use response::{EventStream, Response, ResponseBody};

#[allow(async_fn_in_trait)]
pub trait Clock {
  fn now(&self) -> Duration;
  fn unix_seconds(&self) -> u64;
  fn unix_millis(&self) -> i64 {
    self.unix_seconds().saturating_mul(1_000).min(i64::MAX as u64) as i64
  }
  async fn sleep(&mut self, delay: Duration);
}

#[derive(Debug)]
pub enum ErrorKind {
  Other,
  UnknownProfile { profile: SmolStr },
  InvalidRoute,
  NoAccount { model: SmolStr },
  ProviderAccessDenied,
  UpstreamStatus { status: u16, body: SmolStr },
  Stopped,
  Preparation(crate::preparation::PreparationError),
}
#[derive(Debug)]
pub struct Error {
  pub stage: &'static str,
  pub message: SmolStr,
  pub recoverable: bool,
  pub stop: bool,
  pub kind: ErrorKind,
}
impl Error {
  pub fn new(stage: &'static str, message: impl fmt::Display) -> Self {
    Self {
      stage,
      message: SmolStr::new(message.to_string()),
      recoverable: false,
      stop: false,
      kind: ErrorKind::Other,
    }
  }
  fn with_kind(mut self, kind: ErrorKind) -> Self {
    self.kind = kind;
    self
  }
}
impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "[{}] {}", self.stage, self.message)
  }
}
impl core::error::Error for Error {}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
  pub max_response_bytes: usize,
  pub max_event_bytes: usize,
  pub max_error_body_bytes: usize,
}
impl Default for Limits {
  fn default() -> Self {
    Self {
      max_response_bytes: 4 * 1024 * 1024,
      max_event_bytes: 64 * 1024,
      max_error_body_bytes: 8 * 1024,
    }
  }
}

/// Host-independent model evidence, including explicit empty capability lists.
#[derive(Clone, Debug, Default)]
pub struct ModelCapabilities {
  pub endpoints: Option<Vec<Endpoint>>,
  pub reasoning_efforts: Option<Vec<ReasoningEffort>>,
  pub reasoning_supported: Option<bool>,
}
pub type ModelInventory = BTreeMap<SmolStr, ModelCapabilities>;

/// Synchronous state transactions. Guards never escape into request I/O.
struct StateCell<T> {
  #[cfg(feature = "std")]
  inner: std::sync::Mutex<T>,
  #[cfg(not(feature = "std"))]
  inner: core::cell::RefCell<T>,
}
impl<T> StateCell<T> {
  fn new(value: T) -> Self {
    Self { inner: value.into() }
  }
  fn with<U>(&self, operation: impl FnOnce(&mut T) -> U) -> U {
    #[cfg(feature = "std")]
    {
      operation(&mut self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }
    #[cfg(not(feature = "std"))]
    {
      operation(&mut self.inner.borrow_mut())
    }
  }
}

struct CredentialState {
  account: AccountConfig,
  refreshing: bool,
  waiters: Vec<core::task::Waker>,
}
impl CredentialState {
  fn new(account: AccountConfig) -> Self {
    Self {
      account,
      refreshing: false,
      waiters: Vec::new(),
    }
  }
}
struct Pool {
  members: Vec<usize>,
  state: StateCell<PoolState<usize>>,
  #[cfg(feature = "std")]
  standard: StateCell<Option<Shared<tokn_accounts::link::AccountPoolRuntime>>>,
}

struct Binding {
  account: AccountConfig,
  provider: ProviderId,
  driver: DriverId,
  driver_identity: SmolStr,
  base_url: String,
  models: StateCell<ModelInventory>,
  credentials: StateCell<CredentialState>,
  cleartext: CleartextHttpPolicy,
}
impl CandidateBinding for Binding {
  fn provider_id(&self) -> &ProviderId {
    &self.provider
  }
  fn driver_id(&self) -> &DriverId {
    &self.driver
  }
  fn supports(&self, model: &str, endpoint: Endpoint) -> bool {
    (model.is_empty() || self.models.with(|models| models.contains_key(model))) && self.has_endpoint(model, endpoint)
  }
  fn has_endpoint(&self, model: &str, endpoint: Endpoint) -> bool {
    self
      .models
      .with(|models| {
        models
          .get(model)
          .and_then(|m| m.endpoints.as_ref())
          .map(|endpoints| endpoints.contains(&endpoint))
      })
      .unwrap_or_else(|| native::supports_model_endpoint(self.driver.as_str(), model, endpoint, &self.base_url))
  }
}

/// Linked v2 resources and transactionally shared mutable account state.
pub struct ExecutionState {
  plan: ExecutionRules,
  destinations: BTreeMap<ProviderId, DestinationMetadata>,
  bindings: Vec<Binding>,
  pools: BTreeMap<AccountPoolId, Pool>,
}
impl ExecutionState {
  pub fn rules(&self) -> &ExecutionRules {
    &self.plan
  }

  pub fn new(plan: ExecutionRules, accounts: Vec<AccountConfig>) -> Result<Self, Error> {
    Self::with_models(plan, accounts, BTreeMap::new())
  }
  pub fn with_models(
    plan: ExecutionRules,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, ModelInventory>,
  ) -> Result<Self, Error> {
    let metadata = link_provider_metadata(&plan, &accounts, native::provider_default_base_url)
      .map_err(|e| Error::new("config", e))?;
    let destinations = metadata
      .destinations()
      .map(|(id, destination)| (id.clone(), destination.clone()))
      .collect::<BTreeMap<_, _>>();
    for (id, profile) in plan.profiles() {
      let route = plan
        .route(profile.route())
        .ok_or_else(|| Error::new("config", format!("profile '{id}' references an unknown route")))?;
      if route.header_patches().is_some() {
        return Err(Error::new(
          "config",
          format!("profile '{id}' uses unsupported header patches"),
        ));
      }
      RetryPolicy::from_route(&plan, route)
        .map_err(|error| Error::new("config", format!("profile '{id}': {error}")))?;
      if let RoutePlan::Relay(relay) = route {
        if let RelayDestination::FixedProvider(provider) = relay.destination() {
          if !destinations.contains_key(provider) {
            return Err(Error::new(
              "config",
              format!("profile '{id}' references an unknown provider"),
            ));
          }
        }
        if matches!(relay.credentials(), RelayCredentials::Client) {
          continue;
        }
      }
      let pool = profile
        .account_pool()
        .ok_or_else(|| Error::new("config", format!("profile '{id}' has no account pool")))?;
      if plan.account_pool(pool).is_none() {
        return Err(Error::new(
          "config",
          format!("profile '{id}' references an unknown account pool"),
        ));
      }
    }
    let bindings = metadata
      .bindings()
      .iter()
      .map(|binding| {
        let provider = binding.provider_id();
        let destination = &destinations[provider];
        Binding {
          account: accounts[binding.account_order()].clone(),
          provider: provider.clone(),
          driver: binding.driver_id().clone(),
          driver_identity: SmolStr::new(binding.driver_identity()),
          base_url: destination.base_url().to_string(),
          models: StateCell::new(models.get(provider).cloned().unwrap_or_default()),
          credentials: StateCell::new(CredentialState::new(accounts[binding.account_order()].clone())),
          cleartext: destination.cleartext_policy(),
        }
      })
      .collect();
    let mut pools = BTreeMap::new();
    for (id, policy) in link_pool_metadata(&plan, &metadata).map_err(|e| Error::new("config", e))? {
      let affinity = policy
        .session_affinity()
        .map(|p| AffinityPolicy::new(p.ttl(), p.expired_retention()));
      let state = PoolState::new(
        policy.active().to_vec(),
        policy.fallback().to_vec(),
        policy.failure_cooldown(),
        affinity,
      )
      .map_err(|e| Error::new("config", e))?;
      pools.insert(
        id,
        Pool {
          members: policy.active().iter().chain(policy.fallback()).copied().collect(),
          state: StateCell::new(state),
          #[cfg(feature = "std")]
          standard: StateCell::new(None),
        },
      );
    }
    Ok(Self {
      plan,
      destinations,
      bindings,
      pools,
    })
  }

  pub fn set_binding_models(&self, provider: &ProviderId, account_id: &str, models: ModelInventory) {
    for binding in &self.bindings {
      if &binding.provider == provider && binding.account.id == account_id {
        binding.models.with(|current| *current = models.clone());
      }
    }
  }
  #[cfg(feature = "std")]
  pub fn use_standard_pools(&self, pools: &tokn_accounts::link::AccountPoolRuntimes) {
    for (id, pool) in &self.pools {
      pool.standard.with(|current| *current = pools.runtime(id).cloned());
    }
  }
  fn select(&self, pool: &Pool, now: Duration, session: Option<&str>, ranks: &BTreeMap<usize, i32>) -> Option<usize> {
    #[cfg(feature = "std")]
    if let Some(standard) = pool.standard.with(|state| state.clone()) {
      use tokn_accounts::link::{PoolAcquire, ProviderBindingKey};
      let keyed = ranks
        .iter()
        .map(|(&index, &rank)| {
          let binding = &self.bindings[index];
          (
            ProviderBindingKey::new(binding.provider.clone(), binding.account.id.as_str()),
            rank,
          )
        })
        .collect();
      return match standard.acquire_ranks(session, &keyed) {
        PoolAcquire::Selected(selected) => pool.members.iter().copied().find(|&index| {
          let binding = &self.bindings[index];
          &ProviderBindingKey::new(binding.provider.clone(), binding.account.id.as_str()) == selected.key()
        }),
        PoolAcquire::CoolingDown { .. } | PoolAcquire::NoEligible => None,
      };
    }
    pool.state.with(|state| match state.select(now, session, ranks) {
      Selection::Selected(index) => Some(index),
      Selection::CoolingDown { .. } | Selection::NoEligible => None,
    })
  }

  fn record_feedback(
    &self,
    pool: &AccountPoolId,
    index: usize,
    session: Option<&str>,
    now: Duration,
    feedback: AccountFeedback,
  ) -> Result<(), Error> {
    let pool = &self.pools[pool];
    #[cfg(feature = "std")]
    if let Some(standard) = pool.standard.with(|state| state.clone()) {
      let binding = &self.bindings[index];
      let key = tokn_accounts::link::ProviderBindingKey::new(binding.provider.clone(), binding.account.id.as_str());
      return match feedback {
        AccountFeedback::Success => standard.record_success(session, &key),
        AccountFeedback::Failure => standard.record_failure(&key).map(|_| ()),
        AccountFeedback::None => Ok(()),
      }
      .map_err(|error| Error::new("send", error));
    }
    pool
      .state
      .with(|state| match feedback {
        AccountFeedback::Success => state.record_success(now, session, &index),
        AccountFeedback::Failure => state.record_failure(now, &index).map(|_| ()),
        AccountFeedback::None => Ok(()),
      })
      .map_err(|error| Error::new("send", error))
  }
}

pub struct RequestInput {
  pub endpoint: Endpoint,
  pub body: Value,
  pub headers: HeaderMap,
  pub raw_body: Bytes,
  pub decoded_body: Bytes,
  pub request_id: String,
  pub profile: Option<String>,
  pub generation_options: Option<GenerationOptions>,
  pub allowed_providers: Option<BTreeSet<String>>,
  pub content_encoding: Option<String>,
  pub method: &'static str,
  pub dry_run: bool,
}
impl RequestInput {
  pub fn new(endpoint: Endpoint, body: Value, headers: HeaderMap, request_id: String) -> Self {
    let raw_body = Bytes::from(serde_json::to_vec(&body).expect("JSON serializes"));
    let content_encoding = headers.get("content-encoding").map(|value| value.as_str().to_string());
    Self {
      endpoint,
      body,
      headers,
      decoded_body: raw_body.clone(),
      raw_body,
      request_id,
      profile: None,
      generation_options: None,
      allowed_providers: None,
      content_encoding,
      method: "POST",
      dry_run: false,
    }
  }
}

/// An exchange is closed on every exit, including cancellation and unwinding.
struct Exchange<T: Transport>(T);
impl<T: Transport> core::ops::Deref for Exchange<T> {
  type Target = T;
  fn deref(&self) -> &T {
    &self.0
  }
}
impl<T: Transport> core::ops::DerefMut for Exchange<T> {
  fn deref_mut(&mut self) -> &mut T {
    &mut self.0
  }
}
impl<T: Transport> Drop for Exchange<T> {
  fn drop(&mut self) {
    self.0.close_response();
  }
}

#[derive(Clone)]
enum Target {
  Account { binding: usize, pool: AccountPoolId },
  Client,
}
#[derive(Clone)]
pub struct Resolved {
  target: Target,
  provider: ProviderId,
  endpoint: Endpoint,
  model: SmolStr,
  wire_identity: WireIdentity,
  opaque: bool,
}
pub struct Converted {
  prepared: PreparedRequest,
  shaped_body: Value,
  upstream_stream: bool,
}

pub struct Session<T: Transport, C: Clock, E: Events> {
  state: Shared<ExecutionState>,
  transport: Option<Exchange<T>>,
  observer: Option<Observer<C, E>>,
  input: RequestInput,
  limits: Limits,
  attempt: u32,
  retry: RetryPolicy,
  terminal: bool,
  stage: Stage,
}
impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  pub fn new(state: Shared<ExecutionState>, input: RequestInput, transport: T, clock: C, events: E) -> Self {
    Self {
      state,
      input,
      transport: Some(Exchange(transport)),
      observer: Some(Observer { clock, events }),
      limits: Limits::default(),
      attempt: 0,
      retry: RetryPolicy::default(),
      terminal: false,
      stage: Stage::Extract,
    }
  }
  pub fn with_limits(mut self, limits: Limits) -> Self {
    self.limits = limits;
    self
  }
  pub async fn run(self) -> Result<Response<T, C, E>, Error> {
    crate::driver::run(self).await
  }
  fn observer(&self) -> &Observer<C, E> {
    self.observer.as_ref().expect("session owns observer")
  }
}
impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  fn record_send_outcome(&self, target: &Target, session_id: Option<&str>, outcome: SendOutcome) -> Result<(), Error> {
    let Target::Account { binding, pool } = target else {
      return Ok(());
    };
    let feedback = AccountFeedbackPolicy::Api.feedback(outcome);
    if feedback == AccountFeedback::None {
      return Ok(());
    }
    let now = self.observer().clock.now();
    self.state.record_feedback(pool, *binding, session_id, now, feedback)
  }

  fn template_vars(&self) -> tokn_core::headers::TemplateVars {
    let mut vars = tokn_headers::inbound::build_template_vars(&self.input.headers);
    vars
      .request_id
      .get_or_insert_with(|| SmolStr::new(&self.input.request_id));
    vars
  }

  fn header_identity(&self, resolved: &Resolved) -> tokn_core::AgentId {
    self
      .wire_identity(resolved)
      .or_else(|| {
        tokn_core::AgentId::provider_default(self.state.destinations[&resolved.provider].driver_id().as_str())
      })
      .unwrap_or_default()
  }

  fn wire_identity(&self, resolved: &Resolved) -> Option<tokn_core::AgentId> {
    crate::preparation::wire_identity(
      self.state.destinations[&resolved.provider].driver_id().as_str(),
      &resolved.wire_identity,
    )
  }
}

// Cancellation releases refresh ownership and wakes waiting request futures.
// Wakers run after the state transaction, so reentrant executors cannot deadlock.
struct CredentialLease<'a>(&'a StateCell<CredentialState>);
impl Drop for CredentialLease<'_> {
  fn drop(&mut self) {
    let waiters = self.0.with(|state| {
      state.refreshing = false;
      core::mem::take(&mut state.waiters)
    });
    for waiter in waiters {
      waiter.wake();
    }
  }
}

impl<T: Transport, C: Clock, E: Events> Session<T, C, E> {
  async fn ensure_credentials(&mut self, index: usize) -> Result<AccountConfig, Error> {
    use core::task::Poll;
    let shared = self.state.clone();
    let binding = &shared.bindings[index];
    let decision = core::future::poll_fn(|context| {
      binding.credentials.with(|state| {
        if state.refreshing {
          if !state.waiters.iter().any(|waiter| waiter.will_wake(context.waker())) {
            state.waiters.push(context.waker().clone());
          }
          return Poll::Pending;
        }
        let now = self.observer().clock.unix_seconds().min(i64::MAX as u64) as i64;
        let request =
          native::credential_refresh_request(binding.driver.as_str(), &state.account, now, binding.cleartext)
            .map_err(|error| Error::new("send", error));
        if matches!(&request, Ok(Some(_))) {
          state.refreshing = true;
        }
        Poll::Ready(request.map(|request| (state.account.clone(), request)))
      })
    })
    .await?;
    let (account, Some(request)) = decision else {
      return Ok(decision.0);
    };
    let lease = CredentialLease(&binding.credentials);
    self.stage = Stage::Send;
    let transport = self.transport.as_mut().expect("session owns transport");
    transport.configure_body_mode(false);
    let head = transport
      .send(Request {
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: request.body,
      })
      .await
      .map_err(|source| {
        let mut error = Error::new("send", &source);
        error.recoverable = transport.recoverable(&source);
        error
      })?;
    if !(200..300).contains(&head.status) {
      let detail = read_error_body(&mut **transport, self.limits.max_error_body_bytes).await;
      transport.close_response();
      let mut error = Error::new("send", format!("credential exchange {}: {detail}", head.status));
      error.recoverable =
        crate::driver::classify_status(head.status) == crate::driver::StatusDisposition::RecoverableFailure;
      return Err(error);
    }
    let bytes = read_all(&mut **transport, self.limits.max_response_bytes)
      .await
      .map_err(|error| Error::new("send", error))?;
    transport.close_response();
    let now = self.observer().clock.unix_seconds().min(i64::MAX as u64) as i64;
    let update = native::decode_credential_refresh(binding.driver.as_str(), &bytes, now)
      .map_err(|error| Error::new("send", error))?;
    let mut account = account;
    account.access_token = Some(update.access_token);
    account.access_token_expires_at = Some(update.expires_at);
    binding.credentials.with(|state| state.account = account.clone());
    drop(lease);
    Ok(account)
  }
}

impl<T: Transport, C: Clock, E: Events> crate::driver::Session for Session<T, C, E> {
  type Extracted = RequestInterpretation;
  type Resolved = Resolved;
  type Headers = HeaderMap;
  type Converted = Converted;
  type Sent = (ResponseHead, Resolved, bool, bool);
  type Output = Response<T, C, E>;
  type Error = Error;

  fn start(&mut self, attempt: u32) {
    self.attempt = attempt;
    self.terminal = false;
    self.stage = Stage::Extract;
    self
      .observer()
      .emit_event(&self.input.request_id, self.attempt, Some(("started", None)), || {
        Some(RequestEventPayload::Stage(StageEvent::Started {
          request_endpoint: self.input.endpoint.into(),
        }))
      });
  }
  async fn extract(&mut self) -> Result<RequestInterpretation, Error> {
    self.stage = Stage::Extract;
    let relay = self
      .state
      .plan
      .profiles()
      .get(self.input.profile.as_deref().unwrap_or("default"))
      .and_then(|profile| self.state.plan.route(profile.route()))
      .is_some_and(|route| matches!(route, RoutePlan::Relay(_)));
    let extracted = if relay {
      let (interpreted, body) = RequestInterpretation::inspect_wire(
        &self.input.endpoint.into(),
        &self.input.headers,
        &self.input.decoded_body,
      );
      self.input.body = body;
      interpreted
    } else {
      RequestInterpretation::from_json(&self.input.endpoint.into(), &self.input.headers, &self.input.body)
    };
    self.emit_extracted(&extracted);
    Ok(extracted)
  }
  async fn resolve(&mut self, extracted: &RequestInterpretation) -> Result<Resolved, Error> {
    self.stage = Stage::Resolve;
    let profile = match self.input.profile.as_deref() {
      Some(id) => self.state.plan.profiles().get(id),
      None => self.state.plan.profiles().get("default"),
    }
    .ok_or_else(|| {
      Error::new(
        "resolve",
        format!(
          "unknown profile '{}'",
          self.input.profile.as_deref().unwrap_or("default")
        ),
      )
      .with_kind(ErrorKind::InvalidRoute)
    })?;
    let route = self
      .state
      .plan
      .route(profile.route())
      .ok_or_else(|| Error::new("resolve", "unknown route"))?;
    if profile.api_binding().is_some_and(|binding| {
      !binding
        .endpoints()
        .iter()
        .any(|operation| operation.as_str() == self.input.endpoint.as_str())
    }) {
      return Err(Error::new(
        "resolve",
        format!("profile does not expose operation '{}'", self.input.endpoint.as_str()),
      ));
    }
    self.retry = RetryPolicy::from_route(&self.state.plan, route).map_err(|error| Error::new("resolve", error))?;
    if let RoutePlan::Relay(relay) = route {
      let RelayDestination::FixedProvider(provider) = relay.destination() else {
        return Err(Error::new("resolve", "original destination requires proxy context"));
      };
      if matches!(relay.credentials(), RelayCredentials::Client) {
        if !tokn_accounts::selection::provider_allowed(provider.as_str(), self.input.allowed_providers.as_ref()) {
          return Err(Error::new("resolve", "provider access denied").with_kind(ErrorKind::ProviderAccessDenied));
        }
        if !route.allows_provider(provider) {
          return Err(Error::new("resolve", "relay provider is excluded by its policy"));
        }
        let resolved = Resolved {
          target: Target::Client,
          provider: provider.clone(),
          endpoint: self.input.endpoint,
          model: SmolStr::new(&extracted.model),
          wire_identity: WireIdentity::None,
          opaque: true,
        };
        self.emit_resolved(extracted, &resolved);
        return Ok(resolved);
      }
    }
    let pool_id = profile
      .account_pool()
      .ok_or_else(|| Error::new("resolve", "profile has no account pool"))?;
    let pool = self
      .state
      .pools
      .get(pool_id)
      .ok_or_else(|| Error::new("resolve", "missing account pool"))?;
    let now = self.observer().clock.now();
    let selection = select_route(
      route,
      SelectionRequest {
        model: &extracted.model,
        endpoint: self.input.endpoint,
        allowed_providers: self.input.allowed_providers.as_ref(),
      },
      pool.members.iter().map(|&index| (index, &self.state.bindings[index])),
      |ranks| self.state.select(pool, now, extracted.session_id.as_deref(), ranks),
    )
    .map_err(|e| Error::new("resolve", e).with_kind(ErrorKind::InvalidRoute))?;
    match selection {
      RouteSelection::Selected {
        binding,
        model,
        endpoint,
      } => {
        let resolved = Resolved {
          target: Target::Account {
            binding,
            pool: pool_id.clone(),
          },
          provider: self.state.bindings[binding].provider.clone(),
          endpoint,
          model,
          wire_identity: profile.wire_identity().clone(),
          opaque: matches!(route, RoutePlan::Relay(_)),
        };
        self.emit_resolved(extracted, &resolved);
        Ok(resolved)
      }
      RouteSelection::NoAccount => Err(Error::new("resolve", "no eligible account for this request").with_kind(
        ErrorKind::NoAccount {
          model: extracted.model.clone(),
        },
      )),
      RouteSelection::ProviderAccessDenied => {
        Err(Error::new("resolve", "provider access denied").with_kind(ErrorKind::ProviderAccessDenied))
      }
    }
  }
  async fn build_headers(&mut self, _: &RequestInterpretation, resolved: &Resolved) -> Result<HeaderMap, Error> {
    self.stage = Stage::BuildHeaders;
    if matches!(resolved.target, Target::Client) {
      let mut headers = crate::header_policy::passthrough_headers(&self.input.headers, false, true);
      headers.remove("content-length");
      headers.insert("x-request-id", self.input.request_id.clone());
      self.emit_headers(&headers, resolved);
      return Ok(headers);
    }
    let Target::Account { binding, .. } = resolved.target else {
      unreachable!("client relay returned above")
    };
    let binding = &self.state.bindings[binding];
    let inbound = &self.input.headers;
    let mut vars = self.template_vars();
    let identity = self.wire_identity(resolved);
    let mut headers = if resolved.opaque {
      let mut headers = crate::header_policy::passthrough_headers(inbound, false, false);
      headers.remove("content-length");
      headers
    } else {
      crate::preparation::identity_headers(
        binding.driver.as_str(),
        identity.as_ref(),
        &mut vars,
        inbound,
        &self.input.request_id,
      )
    };
    for (name, value) in &binding.account.headers {
      headers.insert(HeaderName::new(name.as_str()), HeaderValue::from_string(value.clone()));
    }
    headers.insert("x-request-id", self.input.request_id.clone());
    self.emit_headers(&headers, resolved);
    Ok(headers)
  }
  async fn convert_request(
    &mut self,
    extracted: &RequestInterpretation,
    resolved: &Resolved,
    headers: &HeaderMap,
  ) -> Result<Converted, Error> {
    self.stage = Stage::ConvertRequest;
    let destination = &self.state.destinations[&resolved.provider];
    let driver = destination.driver_id().as_str().to_string();
    let base_url = destination.base_url().as_str().to_string();
    let cleartext = destination.cleartext_policy();
    let mut prepared;
    let mut shaped;
    if resolved.opaque {
      crate::preparation::validate_relay_options(&driver, resolved.endpoint, self.input.generation_options.as_ref())
        .map_err(|error| Error::new("convert_request", error))?;
    }
    if resolved.opaque {
      shaped = self.input.body.clone();
      let segments = native::operation_segments(&driver, resolved.endpoint, &base_url).ok_or_else(|| {
        Error::new(
          "convert_request",
          "provider has no native path for this relay operation",
        )
      })?;
      prepared = tokn_core::provider::prepare_request_with_policy(
        &base_url,
        segments,
        resolved.endpoint,
        &shaped,
        headers.clone(),
        cleartext,
      )
      .map_err(|error| Error::new("convert_request", error))?;
    } else {
      let Target::Account { binding, .. } = resolved.target else {
        unreachable!("managed request requires an account")
      };
      let metadata = self.state.bindings[binding]
        .models
        .with(|models| models.get(&resolved.model).cloned())
        .unwrap_or_default();
      shaped = crate::preparation::prepare_managed_body(
        self.input.endpoint,
        &self.input.body,
        crate::preparation::GenerationTarget {
          endpoint: resolved.endpoint,
          provider_id: &driver,
          model: &resolved.model,
          reasoning_efforts: metadata.reasoning_efforts.as_deref(),
          reasoning_supported: metadata.reasoning_supported,
        },
        self.input.generation_options.as_ref(),
      )
      .map_err(|error| Error::new("convert_request", &error).with_kind(ErrorKind::Preparation(error)))?;
      shaped = native::transform_body(&driver, resolved.endpoint, shaped, metadata.reasoning_supported)
        .map_err(|error| Error::new("convert_request", error))?;
      let segments = native::operation_segments(&driver, resolved.endpoint, &base_url)
        .ok_or_else(|| Error::new("convert_request", "provider has no native path for this operation"))?;
      prepared = tokn_core::provider::prepare_request_with_policy(
        &base_url,
        segments,
        resolved.endpoint,
        &shaped,
        headers.clone(),
        cleartext,
      )
      .map_err(|error| Error::new("convert_request", error))?;
    }
    crate::preparation::apply_compaction_priority(&driver, extracted.request_classification, &mut shaped);
    let mut decoded = Bytes::from(serde_json::to_vec(&shaped).map_err(|error| Error::new("convert_request", error))?);
    prepared.body = if shaped == self.input.body {
      self.input.raw_body.clone()
    } else {
      match self
        .transport
        .as_ref()
        .expect("session owns transport")
        .encode_body(&decoded, self.input.content_encoding.as_deref())
      {
        Ok(body) => body,
        // Relay inspection is best-effort. An unsupported inbound coding must
        // not turn a transparent request into a failed compaction rewrite.
        Err(_) if resolved.opaque => {
          shaped = self.input.body.clone();
          decoded = self.input.decoded_body.clone();
          self.input.raw_body.clone()
        }
        Err(error) => return Err(Error::new("convert_request", error)),
      }
    };
    if prepared.body != self.input.raw_body {
      crate::header_policy::remove_body_digests(&mut prepared.headers);
    }
    crate::preparation::apply_compaction_priority_routing_hint(
      &driver,
      extracted.request_classification,
      &decoded,
      &mut prepared.headers,
    );
    let upstream_stream = shaped.get("stream").and_then(Value::as_bool).unwrap_or(false);
    self.emit_converted_request(&prepared, &shaped, &decoded);
    Ok(Converted {
      prepared,
      shaped_body: shaped,
      upstream_stream,
    })
  }

  async fn send(
    &mut self,
    extracted: &RequestInterpretation,
    resolved: &Resolved,
    _headers: &HeaderMap,
    converted: &Converted,
  ) -> Result<Self::Sent, Error> {
    self.stage = Stage::Send;
    if self.input.dry_run {
      let mut error = Error::new("send", "request stopped before sending").with_kind(ErrorKind::Stopped);
      error.stop = true;
      return Err(error);
    }
    let mut prepared = converted.prepared.clone();
    if let Target::Account { binding, .. } = resolved.target {
      let account = match self.ensure_credentials(binding).await {
        Ok(account) => account,
        Err(error) => {
          self.record_send_outcome(
            &resolved.target,
            extracted.session_id.as_deref(),
            SendOutcome::Failure {
              recoverable: error.recoverable,
            },
          )?;
          return Err(error);
        }
      };
      self.stage = Stage::Send;
      let driver = self.state.bindings[binding].driver.as_str();
      let vars = self.template_vars();
      let agent_id = self.header_identity(resolved);
      let context = tokn_core::provider::HeaderPatchCtx {
        request_kind: tokn_core::provider::ProviderRequestKind::Operation(resolved.endpoint),
        body: &converted.shaped_body,
        bearer_token: native::credential(driver, &account),
        content_encoding: self.input.content_encoding.as_deref(),
        stream: extracted.stream,
        initiator: extracted.initiator.as_deref().unwrap_or("user"),
        inbound_headers: &self.input.headers,
        vars: &vars,
        agent_id: &agent_id,
      };
      native::patch_headers(driver, &mut prepared.headers, &context, &account)
        .map_err(|error| Error::new("send", error))?;
    }
    let transport = self.transport.as_mut().expect("session owns transport");
    transport.configure_body_mode(resolved.opaque);
    prepared.headers.remove("host");
    prepared.headers.remove("content-length");
    if transport.response_is_decoded() {
      prepared.headers.insert(
        "accept-encoding",
        HeaderValue::from_string(transport.accept_encoding().to_string()),
      );
    }
    let request = Request {
      method: self.input.method,
      url: prepared.url,
      headers: prepared.headers,
      body: prepared.body,
    };
    self.observer.as_ref().expect("session owns observer").emit_event(
      &self.input.request_id,
      self.attempt,
      None,
      || {
        Some(RequestEventPayload::Record(RecordEvent::UpstreamReq {
          method: SmolStr::new(request.method),
          url: SmolStr::new(&request.url),
          headers: request.headers.clone(),
          body: request.body.clone(),
        }))
      },
    );
    let head = match transport.send(request).await {
      Ok(head) => head,
      Err(source) => {
        let mut error = Error::new("send", &source);
        error.recoverable = transport.recoverable(&source);
        self.record_send_outcome(
          &resolved.target,
          extracted.session_id.as_deref(),
          SendOutcome::Failure {
            recoverable: error.recoverable,
          },
        )?;
        return Err(error);
      }
    };
    self.observer.as_ref().expect("session owns observer").emit_event(
      &self.input.request_id,
      self.attempt,
      None,
      || {
        Some(RequestEventPayload::Record(RecordEvent::UpstreamResp {
          status: head.status,
          headers: head.headers.clone(),
        }))
      },
    );
    // Account-credential API relays have the same failure policy as managed
    // API calls. Only client-credential relays forward every received status.
    let policy = match resolved.target {
      Target::Account { .. } => ResponsePolicy::Api,
      Target::Client => ResponsePolicy::ForwardAll,
    };
    let outcome = SendOutcome::Response { status: head.status };
    let disposition = policy.classify_status(head.status);
    if disposition != crate::driver::StatusDisposition::Success {
      let detail = read_error_body(&mut **transport, self.limits.max_error_body_bytes).await;
      self.record_send_outcome(&resolved.target, extracted.session_id.as_deref(), outcome)?;
      self
        .observer()
        .emit_event(&self.input.request_id, self.attempt, None, || {
          Some(RequestEventPayload::Record(RecordEvent::UpstreamBody {
            body: Bytes::copy_from_slice(detail.as_bytes()),
            error: None,
          }))
        });
      let mut error =
        Error::new("send", format!("upstream {}: {detail}", head.status)).with_kind(ErrorKind::UpstreamStatus {
          status: head.status,
          body: SmolStr::new(&detail),
        });
      error.recoverable = disposition == crate::driver::StatusDisposition::RecoverableFailure;
      return Err(error);
    }
    self.record_send_outcome(&resolved.target, extracted.session_id.as_deref(), outcome)?;
    self.emit_stage(Stage::Send, || {
      StageEvent::Send(SentSummary {
        status: head.status,
        headers: head.headers.clone(),
        upstream_endpoint: Some(resolved.endpoint),
        stream: extracted.stream,
      })
    });
    Ok((head, resolved.clone(), extracted.stream, converted.upstream_stream))
  }
  async fn convert_response(mut self, sent: Self::Sent) -> Result<Self::Output, Error> {
    self.stage = Stage::ConvertResponse;
    let result = self.finish_response(sent).await;
    if let Err(error) = &result {
      self.report_failure(error, false);
    }
    result
  }
  fn failed(&mut self, attempt: u32, error: &Error) -> Option<Duration> {
    self.report_failure(error, true);
    self.retry.next_delay(
      attempt,
      RetryFailure {
        send_stage: error.stage == "send",
        recoverable: error.recoverable,
        stop: error.stop,
      },
      Some(self.input.method),
    )
  }
  async fn sleep(&mut self, delay: Duration) {
    self
      .observer
      .as_mut()
      .expect("session owns observer")
      .clock
      .sleep(delay)
      .await;
  }
}
impl<T: Transport, C: Clock, E: Events> Drop for Session<T, C, E> {
  fn drop(&mut self) {
    if !self.terminal {
      self.report_failure(
        &Error::new(self.stage.as_str(), "pipeline attempt cancelled before completion"),
        false,
      );
    }
  }
}

#[cfg(all(test, feature = "std"))]
mod credential_tests;
