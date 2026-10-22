//! Configured execution with injected platform services.
//!
//! This is an adapter to the shared six-stage driver. Routing predicates,
//! mutable pool selection, provider preparation, and protocol conversion come
//! from their existing domain crates.

use crate::interpretation::RequestInterpretation;
use alloc::{
  collections::{BTreeMap, BTreeSet, VecDeque},
  format,
  string::{String, ToString},
  vec::Vec,
};
use core::{
  cell::{Cell, RefCell, RefMut},
  fmt,
  time::Duration,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokn_accounts::{
  native,
  pool_state::{AffinityPolicy, PoolState, Selection},
  selection::{
    managed_binding_matches, model_candidates, model_provider_score, operation_candidates, CandidateBinding,
  },
};
use tokn_convert::sse::{Decoder, EndpointTranslator, EventTransformer, SseEvent, TerminalState};
use tokn_core::{
  account::{AccountConfig, AccountTier},
  headers::{HeaderMap, HeaderName, HeaderValue},
  provider::PreparedRequest,
  types::{Bytes, SmolStr},
  upstream_url::CleartextHttpPolicy,
  Endpoint,
};
use tokn_policy::{
  AccountPoolId, DriverId, ExecutionPlan, ManagedRetry, ProviderId, RelayCredentials, RelayDestination, RelayRetry,
  RetryPolicyPlan, RoutePlan, WireIdentity,
};
use tokn_service::transport::{Request, ResponseHead, Transport};

#[cfg(all(test, feature = "std"))]
mod interpretation_tests;

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
  async fn sleep(&mut self, duration: Duration);
  fn observe(&mut self, _event: Lifecycle<'_>) {}
}

#[derive(Clone, Copy, Debug)]
pub struct Lifecycle<'a> {
  pub request_id: &'a str,
  pub attempt: u32,
  pub stage: &'static str,
  pub success: Option<bool>,
}

#[derive(Debug)]
pub struct Error {
  pub stage: &'static str,
  pub message: SmolStr,
  pub recoverable: bool,
}
impl Error {
  pub fn new(stage: &'static str, message: impl fmt::Display) -> Self {
    Self {
      stage,
      message: SmolStr::new(message.to_string()),
      recoverable: false,
    }
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

struct Destination {
  driver: DriverId,
  base_url: String,
  cleartext: CleartextHttpPolicy,
}

struct Binding {
  account: AccountConfig,
  provider: ProviderId,
  driver: DriverId,
  base_url: String,
  models: BTreeSet<SmolStr>,
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
    self.models.contains(model) && self.has_endpoint(model, endpoint)
  }
  fn has_endpoint(&self, model: &str, endpoint: Endpoint) -> bool {
    native::supports_model_endpoint(self.driver.as_str(), model, endpoint, &self.base_url)
  }
}

/// An in-memory configured platform with one injected HTTP session.
///
/// Borrowing the session fails immediately while an earlier stream retains it.
/// Pool borrows are short state transactions and are never held during I/O.
/// The standard runtime uses independent concurrent sessions instead.
pub struct Injected<T, R> {
  plan: ExecutionPlan,
  destinations: BTreeMap<ProviderId, Destination>,
  bindings: Vec<Binding>,
  pools: RefCell<BTreeMap<AccountPoolId, PoolState<usize>>>,
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
    if plan.profiles().is_empty() {
      return Err(Error::new("config", "no execution profiles configured"));
    }
    // Resolve provider destinations before accounts, just like the standard
    // linker. A v2 provider owns its URL; legacy per-account URLs do not override it.
    let mut destinations = BTreeMap::new();
    for (provider, destination) in plan.providers() {
      let driver = destination.driver();
      let preset = tokn_core::provider::official_provider_preset(provider.as_str())
        .filter(|preset| preset.driver == driver.as_str());
      let default_identity = preset.map_or(driver.as_str(), |preset| preset.id);
      let base_url = destination
        .base_url()
        .or_else(|| native::default_base_url(default_identity))
        .ok_or_else(|| Error::new("config", format!("unknown or unsupported driver '{driver}'")))?;
      if ![Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages]
        .into_iter()
        .any(|endpoint| native::supports_endpoint(driver.as_str(), endpoint, base_url))
      {
        return Err(Error::new(
          "config",
          format!("unknown or unsupported driver '{driver}'"),
        ));
      }
      let cleartext = if destination.allow_insecure_http() {
        CleartextHttpPolicy::Allow
      } else {
        CleartextHttpPolicy::LoopbackOnly
      };
      let base_url = tokn_core::upstream_url::CanonicalUpstreamUrl::parse(base_url, cleartext)
        .map_err(|e| Error::new("config", e))?
        .to_string();
      destinations.insert(
        provider.clone(),
        Destination {
          driver: driver.clone(),
          base_url,
          cleartext,
        },
      );
    }
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
      let retry = match route {
        RoutePlan::Managed(managed) => match managed.retry() {
          ManagedRetry::Never => None,
          ManagedRetry::Recoverable(retry) => Some(retry),
        },
        RoutePlan::Relay(relay) => {
          if !matches!(relay.destination(), RelayDestination::FixedProvider(_)) {
            return Err(Error::new(
              "config",
              format!("profile '{id}' requires original-destination proxy context"),
            ));
          }
          match relay.retry() {
            RelayRetry::Never => None,
            RelayRetry::SafeMethods(retry) | RelayRetry::Buffered(retry) => Some(retry),
          }
        }
      };
      if retry.is_some_and(|retry| plan.retry_policy(retry).is_none()) {
        return Err(Error::new(
          "config",
          format!("profile '{id}' references an unknown retry policy"),
        ));
      }
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
    let mut ids = BTreeSet::new();
    let mut bindings = Vec::new();
    for account in accounts {
      if !ids.insert(account.id.clone()) {
        return Err(Error::new("config", "duplicate account id"));
      }
      let provider = ProviderId::new(&account.provider).map_err(|e| Error::new("config", e))?;
      let Some(destination) = destinations.get(&provider) else {
        // An omitted official provider was deliberately disabled by config.
        // Keep its stored account as a valid pool reference without binding it.
        if tokn_core::provider::official_provider_preset(provider.as_str()).is_some() {
          continue;
        }
        return Err(Error::new("config", format!("unknown provider '{provider}'")));
      };
      if !account.enabled {
        continue;
      }
      let (driver, base_url, cleartext) = (
        destination.driver.clone(),
        destination.base_url.clone(),
        destination.cleartext,
      );
      bindings.push(Binding {
        models: models.get(&provider).cloned().unwrap_or_default(),
        account,
        provider,
        driver,
        base_url,
        cleartext,
      });
    }
    let mut pools = BTreeMap::new();
    for (id, policy) in plan.account_pools() {
      if policy
        .selector()
        .accounts()
        .is_some_and(|selected| selected.iter().any(|account| !ids.contains(account.as_str())))
      {
        return Err(Error::new(
          "config",
          format!("pool '{id}' references an unknown account"),
        ));
      }
      let mut active = Vec::new();
      let mut fallback = Vec::new();
      for (index, binding) in bindings.iter().enumerate() {
        if policy
          .selector()
          .providers()
          .is_none_or(|ids| ids.contains(&binding.provider))
          && policy
            .selector()
            .accounts()
            .is_none_or(|ids| ids.contains(binding.account.id.as_str()))
        {
          match binding.account.tier {
            AccountTier::Active => active.push(index),
            AccountTier::Fallback => fallback.push(index),
          }
        }
      }
      let affinity = policy
        .session_affinity()
        .map(|p| AffinityPolicy::new(p.ttl(), p.expired_retention()));
      pools.insert(
        id.clone(),
        PoolState::new(active, fallback, policy.failure_cooldown(), affinity).map_err(|e| Error::new("config", e))?,
      );
    }
    Ok(Self {
      plan,
      destinations,
      bindings,
      pools: RefCell::new(pools),
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
    // Observers may try to start a request after a stream releases its HTTP
    // session. Return a normal busy error while that observer owns the runtime.
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
    crate::driver::run(LocalSession {
      platform: self,
      transport: Some(Exchange(transport)),
      endpoint,
      body,
      headers: options.inbound_headers(),
      options,
      request_id,
      attempt: 0,
      retry: None,
      terminal: false,
    })
    .await
  }
}

pub struct Response<'a, T: Transport, R: Runtime> {
  pub status: u16,
  pub headers: HeaderMap,
  pub body: ResponseBody<'a, T, R>,
}
pub enum ResponseBody<'a, T: Transport, R: Runtime> {
  Buffered(Bytes),
  Stream(alloc::boxed::Box<EventStream<'a, T, R>>),
}
impl<T: Transport, R: Runtime> Response<'_, T, R> {
  pub fn into_json<D: serde::de::DeserializeOwned>(self) -> Result<(u16, HeaderMap, D), Error> {
    let ResponseBody::Buffered(bytes) = self.body else {
      return Err(Error::new("response", "expected a buffered response"));
    };
    let data = serde_json::from_slice(&bytes).map_err(|e| Error::new("response", e))?;
    Ok((self.status, self.headers, data))
  }
}

/// Ensures every exit releases transport-owned response resources, not only
/// the runtime borrow of the injected driver.
struct Exchange<'a, T: Transport>(RefMut<'a, T>);
impl<T: Transport> core::ops::Deref for Exchange<'_, T> {
  type Target = T;
  fn deref(&self) -> &T {
    &self.0
  }
}
impl<T: Transport> core::ops::DerefMut for Exchange<'_, T> {
  fn deref_mut(&mut self) -> &mut T {
    &mut self.0
  }
}
impl<T: Transport> Drop for Exchange<'_, T> {
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
struct Resolved {
  target: Target,
  provider: ProviderId,
  endpoint: Endpoint,
  model: SmolStr,
  wire_identity: WireIdentity,
  opaque: bool,
}
struct Converted {
  prepared: PreparedRequest,
  upstream_stream: bool,
}
struct LocalSession<'a, T: Transport, R: Runtime> {
  platform: &'a Injected<T, R>,
  transport: Option<Exchange<'a, T>>,
  endpoint: Endpoint,
  body: Value,
  headers: HeaderMap,
  options: RequestOptions,
  request_id: String,
  attempt: u32,
  retry: Option<RetryPolicyPlan>,
  terminal: bool,
}
impl<T: Transport, R: Runtime> LocalSession<'_, T, R> {
  fn emit(&self, stage: &'static str, success: Option<bool>) {
    self.platform.runtime.borrow_mut().observe(Lifecycle {
      request_id: &self.request_id,
      attempt: self.attempt,
      stage,
      success,
    });
  }

  fn template_vars(&self) -> tokn_core::headers::TemplateVars {
    let mut vars = tokn_headers::inbound::build_template_vars(&self.headers);
    vars.request_id.get_or_insert_with(|| SmolStr::new(&self.request_id));
    vars
  }

  fn wire_identity(&self, resolved: &Resolved) -> Option<tokn_core::AgentId> {
    match &resolved.wire_identity {
      WireIdentity::None => None,
      WireIdentity::ProviderDefault => Some(
        tokn_core::AgentId::provider_default(self.platform.destinations[&resolved.provider].driver.as_str())
          .unwrap_or_default(),
      ),
      WireIdentity::Named(id) => Some(tokn_core::AgentId::from(id.as_str())),
    }
  }
}

impl<'a, T: Transport, R: Runtime> crate::driver::Session for LocalSession<'a, T, R> {
  type Extracted = RequestInterpretation;
  type Resolved = Resolved;
  type Headers = HeaderMap;
  type Converted = Converted;
  type Sent = (ResponseHead, Resolved, bool, bool);
  type Output = Response<'a, T, R>;
  type Error = Error;

  fn start(&mut self, attempt: u32) {
    self.attempt = attempt;
    self.terminal = false;
    self.emit("started", None);
  }
  async fn extract(&mut self) -> Result<RequestInterpretation, Error> {
    let extracted = RequestInterpretation::from_json(&self.endpoint.into(), &self.headers, &self.body);
    self.emit("extract", None);
    Ok(extracted)
  }
  async fn resolve(&mut self, extracted: &RequestInterpretation) -> Result<Resolved, Error> {
    let profile = match self.options.profile.as_deref() {
      Some(id) => self.platform.plan.profiles().get(id),
      None => self
        .platform
        .plan
        .profiles()
        .get("default")
        .or_else(|| self.platform.plan.profiles().values().next()),
    }
    .ok_or_else(|| Error::new("resolve", "unknown profile"))?;
    let route = self
      .platform
      .plan
      .route(profile.route())
      .ok_or_else(|| Error::new("resolve", "unknown route"))?;
    if profile.api_binding().is_some_and(|binding| {
      !binding
        .endpoints()
        .iter()
        .any(|operation| operation.as_str() == self.endpoint.as_str())
    }) {
      return Err(Error::new("resolve", "profile does not expose this operation"));
    }
    if let RoutePlan::Relay(relay) = route {
      let RelayDestination::FixedProvider(provider) = relay.destination() else {
        return Err(Error::new("resolve", "original destination requires proxy context"));
      };
      // SDK requests are POSTs. Safe-method retry policies therefore do not
      // permit replay; explicitly buffered relays do.
      self.retry = match relay.retry() {
        RelayRetry::Buffered(id) => self.platform.plan.retry_policy(id).copied(),
        RelayRetry::Never | RelayRetry::SafeMethods(_) => None,
      };
      if matches!(relay.credentials(), RelayCredentials::Client) {
        if !route.allows_provider(provider) {
          return Err(Error::new("resolve", "relay provider is excluded by its policy"));
        }
        self.emit("resolve", None);
        return Ok(Resolved {
          target: Target::Client,
          provider: provider.clone(),
          endpoint: self.endpoint,
          model: SmolStr::new(&extracted.model),
          wire_identity: WireIdentity::None,
          opaque: true,
        });
      }
      let pool = profile
        .account_pool()
        .ok_or_else(|| Error::new("resolve", "profile has no account pool"))?;
      let now = self.platform.runtime.borrow().now();
      let ranks = self
        .platform
        .bindings
        .iter()
        .enumerate()
        .filter(|(_, binding)| binding.provider_id() == provider && route.allows_provider(binding.provider_id()))
        .map(|(index, _)| (index, 0))
        .collect();
      let selected = self
        .platform
        .pools
        .borrow_mut()
        .get_mut(pool)
        .ok_or_else(|| Error::new("resolve", "missing account pool"))?
        .select(now, extracted.session_id.as_deref(), &ranks);
      return match selected {
        Selection::Selected(binding) => {
          self.emit("resolve", None);
          Ok(Resolved {
            target: Target::Account {
              binding,
              pool: pool.clone(),
            },
            provider: self.platform.bindings[binding].provider.clone(),
            endpoint: self.endpoint,
            model: SmolStr::new(&extracted.model),
            wire_identity: profile.wire_identity().clone(),
            opaque: true,
          })
        }
        Selection::CoolingDown { .. } | Selection::NoEligible => {
          Err(Error::new("resolve", "no eligible account for this relay"))
        }
      };
    }
    let RoutePlan::Managed(managed) = route else {
      unreachable!("relay returned above")
    };
    self.retry = match managed.retry() {
      ManagedRetry::Never => None,
      ManagedRetry::Recoverable(id) => self.platform.plan.retry_policy(id).copied(),
    };
    let pool = profile
      .account_pool()
      .ok_or_else(|| Error::new("resolve", "profile has no account pool"))?;
    let now = self.platform.runtime.borrow().now();
    let candidates = model_candidates(managed, &extracted.model).map_err(|e| Error::new("resolve", e))?;
    for candidate in candidates {
      for evidence in candidate.evidence.order().iter().copied() {
        for endpoint in operation_candidates(managed.operation(), self.endpoint) {
          let ranks = self
            .platform
            .bindings
            .iter()
            .enumerate()
            .filter(|(_, binding)| {
              route.allows_provider(binding.provider_id())
                && managed_binding_matches(managed, &candidate, evidence, endpoint, *binding)
            })
            .map(|(index, binding)| (index, model_provider_score(managed, &candidate, binding)))
            .collect();
          let selected = self
            .platform
            .pools
            .borrow_mut()
            .get_mut(pool)
            .ok_or_else(|| Error::new("resolve", "missing account pool"))?
            .select(now, extracted.session_id.as_deref(), &ranks);
          if let Selection::Selected(binding) = selected {
            self.emit("resolve", None);
            return Ok(Resolved {
              target: Target::Account {
                binding,
                pool: pool.clone(),
              },
              provider: self.platform.bindings[binding].provider.clone(),
              endpoint,
              model: candidate.model.clone(),
              wire_identity: profile.wire_identity().clone(),
              opaque: false,
            });
          }
        }
      }
    }
    Err(Error::new("resolve", "no eligible account for this request"))
  }
  async fn build_headers(&mut self, _: &RequestInterpretation, resolved: &Resolved) -> Result<HeaderMap, Error> {
    if matches!(resolved.target, Target::Client) {
      let mut headers = crate::header_policy::passthrough_headers(&self.headers, false, true);
      headers.remove("content-length");
      if !headers.contains_key("content-type") {
        headers.insert("content-type", "application/json");
      }
      if !headers.contains_key("accept") {
        headers.insert("accept", "application/json");
      }
      headers.insert("x-request-id", self.request_id.clone());
      self.emit("build_headers", None);
      return Ok(headers);
    }
    let Target::Account { binding, .. } = resolved.target else {
      unreachable!("client relay returned above")
    };
    let binding = &self.platform.bindings[binding];
    let inbound = &self.headers;
    let vars = self.template_vars();
    let identity = self.wire_identity(resolved);
    let mut headers = if resolved.opaque {
      let mut headers = crate::header_policy::passthrough_headers(inbound, false, false);
      headers.remove("content-length");
      headers
    } else {
      identity.map_or_else(HeaderMap::new, |identity| {
        tokn_headers::registry::build_wire_identity_headers(binding.driver.as_str(), identity.as_str(), &vars, inbound)
      })
    };
    for (name, value) in &binding.account.headers {
      headers.insert(HeaderName::new(name.as_str()), HeaderValue::from_string(value.clone()));
    }
    headers.insert("x-request-id", self.request_id.clone());
    self.emit("build_headers", None);
    Ok(headers)
  }
  async fn convert_request(
    &mut self,
    extracted: &RequestInterpretation,
    resolved: &Resolved,
    headers: &HeaderMap,
  ) -> Result<Converted, Error> {
    if matches!(resolved.target, Target::Client) {
      let destination = &self.platform.destinations[&resolved.provider];
      let segments = native::operation_segments(destination.driver.as_str(), resolved.endpoint, &destination.base_url)
        .ok_or_else(|| {
          Error::new(
            "convert_request",
            "provider has no native path for this relay operation",
          )
        })?;
      let prepared = tokn_core::provider::prepare_request_with_policy(
        &destination.base_url,
        segments,
        resolved.endpoint,
        &self.body,
        headers.clone(),
        destination.cleartext,
      )
      .map_err(|e| Error::new("convert_request", e))?;
      let upstream_stream = self.body.get("stream").and_then(Value::as_bool).unwrap_or(false);
      self.emit("convert_request", None);
      return Ok(Converted {
        prepared,
        upstream_stream,
      });
    }
    let Target::Account { binding, .. } = resolved.target else {
      unreachable!("client relay returned above")
    };
    let binding = &self.platform.bindings[binding];
    let mut body = if resolved.opaque {
      self.body.clone()
    } else {
      tokn_convert::convert_request(self.endpoint, resolved.endpoint, &self.body)
        .map_err(|e| Error::new("convert_request", e))?
    };
    if !resolved.opaque {
      if let Some(object) = body.as_object_mut() {
        object.insert("model".into(), Value::String(resolved.model.to_string()));
      }
    }
    // Apply the selected driver to the headers already built by the profile.
    let credential = if matches!(binding.driver.as_str(), "codex" | "github-copilot") {
      binding
        .account
        .access_token
        .as_ref()
        .or(binding.account.api_key.as_ref())
    } else {
      binding
        .account
        .api_key
        .as_ref()
        .or(binding.account.access_token.as_ref())
    }
    .map(|secret| secret.expose().as_str())
    .unwrap_or("");
    let mut prepared = native::prepare_with_policy(
      binding.driver.as_str(),
      resolved.endpoint,
      &binding.base_url,
      credential,
      body,
      headers.clone(),
      binding.cleartext,
    )
    .map_err(|e| Error::new("convert_request", e))?;
    if resolved.opaque {
      prepared.body = Bytes::from(serde_json::to_vec(&self.body).map_err(|e| Error::new("convert_request", e))?);
      prepared.headers.insert(
        "accept",
        if self.body["stream"].as_bool().unwrap_or(false) {
          "text/event-stream"
        } else {
          "application/json"
        },
      );
    }
    let upstream_stream = serde_json::from_slice::<Value>(&prepared.body)
      .ok()
      .and_then(|body| body.get("stream").and_then(Value::as_bool))
      .unwrap_or(false);
    let shaped_body: Value =
      serde_json::from_slice(&prepared.body).map_err(|error| Error::new("convert_request", error))?;
    let vars = self.template_vars();
    let agent_id = self.wire_identity(resolved).unwrap_or_default();
    let context = tokn_core::provider::HeaderPatchCtx {
      request_kind: tokn_core::provider::ProviderRequestKind::Operation(resolved.endpoint),
      body: &shaped_body,
      bearer_token: Some(credential),
      content_encoding: None,
      stream: extracted.stream,
      initiator: extracted.initiator.as_deref().unwrap_or("user"),
      inbound_headers: &self.headers,
      vars: &vars,
      agent_id: &agent_id,
    };
    native::normalize_headers(
      binding.driver.as_str(),
      &mut prepared.headers,
      &context,
      &binding.account,
    )
    .map_err(|error| Error::new("convert_request", error))?;
    self.emit("convert_request", None);
    Ok(Converted {
      prepared,
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
    let mut prepared = converted.prepared.clone();
    let transport = self.transport.as_mut().expect("session owns transport");
    prepared.headers.insert(
      "accept-encoding",
      HeaderValue::from_string(transport.accept_encoding().to_string()),
    );
    let request = Request {
      method: "POST",
      url: prepared.url,
      headers: prepared.headers,
      body: prepared.body,
    };
    let head = match transport.send(request).await {
      Ok(head) => head,
      Err(source) => {
        let mut error = Error::new("send", &source);
        error.recoverable = transport.recoverable(&source);
        if error.recoverable {
          if let Target::Account { binding, ref pool } = resolved.target {
            self
              .platform
              .pools
              .borrow_mut()
              .get_mut(pool)
              .expect("linked pool")
              .record_failure(self.platform.runtime.borrow().now(), &binding)
              .map_err(|e| Error::new("send", e))?;
          }
        }
        return Err(error);
      }
    };
    let disposition = crate::driver::classify_status(head.status);
    if disposition != crate::driver::StatusDisposition::Success && !matches!(resolved.target, Target::Client) {
      let detail = read_error_body(&mut **transport, self.platform.limits.max_error_body_bytes).await;
      if disposition == crate::driver::StatusDisposition::RecoverableFailure {
        if let Target::Account { binding, ref pool } = resolved.target {
          self
            .platform
            .pools
            .borrow_mut()
            .get_mut(pool)
            .expect("linked pool")
            .record_failure(self.platform.runtime.borrow().now(), &binding)
            .map_err(|e| Error::new("send", e))?;
        }
      }
      let mut error = Error::new("send", format!("upstream {}: {detail}", head.status));
      error.recoverable = disposition == crate::driver::StatusDisposition::RecoverableFailure;
      return Err(error);
    }
    if let Target::Account { binding, ref pool } = resolved.target {
      self
        .platform
        .pools
        .borrow_mut()
        .get_mut(pool)
        .expect("linked pool")
        .record_success(
          self.platform.runtime.borrow().now(),
          extracted.session_id.as_deref(),
          &binding,
        )
        .map_err(|e| Error::new("send", e))?;
    }
    self.emit("send", None);
    Ok((head, resolved.clone(), extracted.stream, converted.upstream_stream))
  }
  async fn convert_response(
    mut self,
    (mut head, resolved, stream, upstream_stream): Self::Sent,
  ) -> Result<Self::Output, Error> {
    let mut body = self.transport.take().expect("session owns transport");
    // The transport yields decoded entities, so wire compression/framing no
    // longer describes any returned body, including transparent relays.
    head.headers.remove("content-encoding");
    head.headers.remove("content-length");
    head.headers.remove("transfer-encoding");
    if resolved.opaque && !stream {
      let bytes = read_all(&mut *body, self.platform.limits.max_response_bytes).await?;
      self.terminal = true;
      self.emit("convert_response", None);
      self.emit("completed", Some(true));
      return Ok(Response {
        status: head.status,
        headers: head.headers,
        body: ResponseBody::Buffered(Bytes::from(bytes)),
      });
    }
    let event_stream = head.headers.get("content-type").is_some_and(|value| {
      value
        .as_str()
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))
    });
    if upstream_stream && !event_stream {
      return Err(Error::new(
        "convert_response",
        "streaming upstream did not return text/event-stream",
      ));
    }
    if stream {
      if !event_stream {
        return Err(Error::new("convert_response", "expected text/event-stream"));
      }
      head.headers.remove("content-length");
      head.headers.remove("transfer-encoding");
      head.headers.insert("content-type", "text/event-stream");
      let events = EventStream::new(
        self.platform,
        body,
        resolved.endpoint,
        self.endpoint,
        self.request_id.clone(),
        self.attempt,
        head.clone(),
      );
      self.terminal = true;
      self.emit("convert_response", None);
      return Ok(Response {
        status: head.status,
        headers: head.headers,
        body: ResponseBody::Stream(alloc::boxed::Box::new(events)),
      });
    }
    let value = if event_stream {
      // Forced upstream streaming uses the same parser, terminal validation,
      // and accumulator as the standard buffered response stage.
      let bytes = read_all(&mut *body, self.platform.limits.max_response_bytes).await?;
      let response =
        tokn_convert::sse::accumulate_slice(resolved.endpoint, &bytes, self.platform.limits.max_event_bytes)
          .map_err(|error| Error::new("convert_response", error))?;
      match resolved.endpoint {
        Endpoint::ChatCompletions => tokn_convert::value::chat::response_to_value(&response),
        Endpoint::Responses => tokn_convert::value::responses::response_to_value(&response),
        Endpoint::Messages => tokn_convert::value::messages::response_to_value(&response),
      }
      .map_err(|e| Error::new("convert_response", e))?
    } else {
      let bytes = read_all(&mut *body, self.platform.limits.max_response_bytes).await?;
      serde_json::from_slice(&bytes).map_err(|e| Error::new("convert_response", e))?
    };
    let value = tokn_convert::convert_response(resolved.endpoint, self.endpoint, &value)
      .map_err(|e| Error::new("convert_response", e))?;
    let bytes = serde_json::to_vec(&value).map_err(|e| Error::new("convert_response", e))?;
    if bytes.len() > self.platform.limits.max_response_bytes {
      return Err(Error::new("convert_response", "converted response exceeds byte limit"));
    }
    head.headers.remove("content-length");
    head.headers.remove("transfer-encoding");
    head.headers.insert("content-type", "application/json");
    self.terminal = true;
    self.emit("convert_response", None);
    self.emit("completed", Some(true));
    Ok(Response {
      status: head.status,
      headers: head.headers,
      body: ResponseBody::Buffered(Bytes::from(bytes)),
    })
  }
  fn failed(&mut self, attempt: u32, error: &Error) -> Option<Duration> {
    self.terminal = true;
    self.emit(error.stage, Some(false));
    self.emit("completed", Some(false));
    self
      .retry
      .filter(|policy| error.stage == "send" && error.recoverable && attempt < policy.max_retries())
      .map(|policy| crate::driver::backoff(policy.initial_backoff(), attempt))
  }
  #[allow(
    clippy::await_holding_refcell_ref,
    reason = "the injected timer is local and serialized; cancellation drops the borrow and concurrent execution is rejected by the transport lease"
  )]
  async fn sleep(&mut self, delay: Duration) {
    self.platform.runtime.borrow_mut().sleep(delay).await;
  }
}
impl<T: Transport, R: Runtime> Drop for LocalSession<'_, T, R> {
  fn drop(&mut self) {
    if !self.terminal {
      self.emit("completed", Some(false));
    }
  }
}

async fn read_all<T: Transport>(transport: &mut T, limit: usize) -> Result<Vec<u8>, Error> {
  let mut bytes = Vec::new();
  let mut buffer = [0u8; 1024];
  loop {
    let count = transport
      .read(&mut buffer)
      .await
      .map_err(|e| Error::new("convert_response", e))?;
    if count > buffer.len() {
      return Err(Error::new(
        "convert_response",
        "transport returned an invalid byte count",
      ));
    }
    if count == 0 {
      return Ok(bytes);
    }
    if count > limit.saturating_sub(bytes.len()) {
      return Err(Error::new("convert_response", "response exceeds byte limit"));
    }
    bytes.extend_from_slice(&buffer[..count]);
  }
}

async fn read_error_body<T: Transport>(transport: &mut T, limit: usize) -> String {
  let mut bytes = Vec::new();
  let mut buffer = [0u8; 1024];
  loop {
    let capacity = buffer.len().min(limit.saturating_sub(bytes.len()).saturating_add(1));
    let count = match transport.read(&mut buffer[..capacity]).await {
      Ok(count) if count <= capacity => count,
      Ok(_) => return "transport returned an invalid byte count".into(),
      Err(error) => return format!("failed to read error body: {error}"),
    };
    if count == 0 {
      return String::from_utf8_lossy(&bytes).into_owned();
    }
    let remaining = limit.saturating_sub(bytes.len());
    bytes.extend_from_slice(&buffer[..count.min(remaining)]);
    if count > remaining {
      return format!("{} [truncated]", String::from_utf8_lossy(&bytes));
    }
  }
}

/// Pull-based SSE response retaining only its borrowed HTTP session and parser state.
pub struct EventStream<'a, T: Transport, R: Runtime> {
  pub status: u16,
  pub headers: HeaderMap,
  platform: &'a Injected<T, R>,
  body: Option<Exchange<'a, T>>,
  decoder: Decoder,
  terminal: TerminalState,
  translator: Option<EndpointTranslator>,
  pending: VecDeque<SseEvent>,
  buffer: [u8; 1024],
  position: usize,
  length: usize,
  request_id: String,
  attempt: u32,
  report_terminal: bool,
}
impl<'a, T: Transport, R: Runtime> EventStream<'a, T, R> {
  fn new(
    platform: &'a Injected<T, R>,
    body: Exchange<'a, T>,
    from: Endpoint,
    to: Endpoint,
    request_id: String,
    attempt: u32,
    head: ResponseHead,
  ) -> Self {
    Self {
      status: head.status,
      headers: head.headers,
      platform,
      body: Some(body),
      decoder: Decoder::new(platform.limits.max_event_bytes),
      terminal: TerminalState::new(from),
      translator: (from != to)
        .then(|| EndpointTranslator::with_created_at(from, to, platform.runtime.borrow().unix_seconds())),
      pending: VecDeque::new(),
      buffer: [0; 1024],
      position: 0,
      length: 0,
      request_id,
      attempt,
      report_terminal: true,
    }
  }
  fn close(&mut self, success: bool) {
    if self.body.take().is_some() && self.report_terminal {
      self.platform.runtime.borrow_mut().observe(Lifecycle {
        request_id: &self.request_id,
        attempt: self.attempt,
        stage: "completed",
        success: Some(success),
      });
    }
  }
  pub fn cancel(&mut self) {
    self.close(false);
    self.pending.clear();
  }
  pub async fn next_event(&mut self) -> Result<Option<SseEvent>, Error> {
    let result = self.read_event().await;
    if result.is_err() {
      self.close(false);
      self.pending.clear();
    }
    result
  }
  async fn read_event(&mut self) -> Result<Option<SseEvent>, Error> {
    loop {
      if let Some(event) = self.pending.pop_front() {
        return Ok(Some(event));
      }
      let Some(body) = self.body.as_mut() else {
        return Ok(None);
      };
      if self.position == self.length {
        let count = body.read(&mut self.buffer).await.map_err(|e| Error::new("stream", e))?;
        if count > self.buffer.len() {
          return Err(Error::new("stream", "transport returned an invalid byte count"));
        }
        if count == 0 {
          self.terminal.finish().map_err(|e| Error::new("stream", e))?;
          self.close(true);
          return Ok(None);
        }
        self.position = 0;
        self.length = count;
      }
      while self.position < self.length {
        let byte = self.buffer[self.position];
        self.position += 1;
        let Some(record) = self.decoder.push(byte).map_err(|e| Error::new("stream", e))? else {
          continue;
        };
        if record.data.trim().is_empty() {
          continue;
        }
        let event = SseEvent::from(record);
        let complete = self.terminal.observe(&event).map_err(|e| Error::new("stream", e))?;
        if let Some(translator) = self.translator.as_mut() {
          self
            .pending
            .extend(translator.transform(event).map_err(|e| Error::new("stream", e))?);
          if complete {
            self
              .pending
              .extend(translator.finish().map_err(|e| Error::new("stream", e))?);
          }
        } else {
          self.pending.push_back(event);
        }
        if complete {
          self.close(true);
        }
        break;
      }
    }
  }
}
impl<T: Transport, R: Runtime> Drop for EventStream<'_, T, R> {
  fn drop(&mut self) {
    self.close(false);
  }
}
