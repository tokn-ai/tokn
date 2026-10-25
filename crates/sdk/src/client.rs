use alloc::{
  collections::{BTreeMap, BTreeSet},
  format,
  string::{String, ToString},
  vec::Vec,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokn_core::{
  account::AccountConfig,
  generation::GenerationOptions,
  headers::HeaderMap,
  types::{Shared, SmolStr},
  Endpoint,
};
use tokn_policy::{ExecutionRules, ProfileId, ProviderId, RelayDestination, RoutePlan};
use tokn_requests::session::{self, Clock, ExecutionState, Limits, ModelCapabilities, ModelInventory, RequestInput};
use tokn_service::transport::Transport;

pub use tokn_requests::configured::RequestOptions;

/// The response operations common to standard and borrowed exchanges.
pub trait PlatformResponse: Sized {
  type Error;
  type Stream;
  fn into_json<T: DeserializeOwned>(self) -> Result<BufferedResponse<T>, Self::Error>;
  fn into_stream(self) -> Result<Self::Stream, Self::Error>;
}

#[derive(Debug)]
pub struct BufferedResponse<T> {
  pub status: u16,
  pub headers: HeaderMap,
  pub data: T,
}

/// External services acquired for one session. An exchange may own pooled
/// connection handles or borrow an application's exclusive transport.
pub struct Services<T, C, E> {
  pub transport: T,
  pub clock: C,
  pub events: E,
}

/// The exchange, clock, and events produced by one service factory acquisition.
pub type PlatformServices<'a, P> =
  Services<<P as Platform>::Exchange<'a>, <P as Platform>::Clock<'a>, <P as Platform>::Events<'a>>;

/// A factory for transport, time, and event services. It neither owns execution
/// state nor interprets requests. Pool handles are snapshotted with client state;
/// an acquired exchange must not borrow those handles from the snapshot.
pub trait Platform {
  type Pool;
  type Exchange<'a>: Transport
  where
    Self: 'a;
  type Clock<'a>: session::Clock
  where
    Self: 'a;
  type Events<'a>: session::Events
  where
    Self: 'a;

  fn pool(&self) -> Self::Pool;
  fn acquire<'a>(&'a self, pool: &Self::Pool) -> Result<PlatformServices<'a, Self>, session::Error>;
}

/// Synchronous presentation of the shared session's result. This compatibility
/// boundary preserves standard owned byte streams and local borrowed SSE streams
/// without requiring local adapters or futures to be `Send`.
pub trait ResponseAdapter: Platform {
  type Error;
  type Stream<'a>
  where
    Self: 'a;
  type Response<'a>: PlatformResponse<Error = Self::Error, Stream = Self::Stream<'a>>
  where
    Self: 'a;

  fn encode_error(source: serde_json::Error) -> Self::Error;
  fn execution_error(source: session::Error, endpoint: Endpoint) -> Self::Error;
  fn finish_response<'a>(
    response: session::Response<Self::Exchange<'a>, Self::Clock<'a>, Self::Events<'a>>,
  ) -> Self::Response<'a>
  where
    Self: 'a;
}

/// Everything replaced atomically when configuration is reloaded. Mutable
/// credentials, account feedback, and model evidence belong to `execution`.
pub(crate) struct Snapshot<Pool> {
  pub execution: Shared<ExecutionState>,
  pub pool: Pool,
  pub default_profile: String,
  pub profile_aliases: BTreeMap<String, String>,
  #[cfg(feature = "std")]
  pub metadata: Option<tokn_requests::v2::StandardMetadata>,
  #[cfg(feature = "std")]
  pub files: Option<crate::std_client::FileMetadata>,
}

pub(crate) struct SnapshotCell<Pool> {
  #[cfg(feature = "std")]
  inner: arc_swap::ArcSwap<Snapshot<Pool>>,
  #[cfg(not(feature = "std"))]
  inner: Shared<Snapshot<Pool>>,
}

impl<Pool> SnapshotCell<Pool> {
  fn new(snapshot: Snapshot<Pool>) -> Self {
    Self {
      #[cfg(feature = "std")]
      inner: arc_swap::ArcSwap::from_pointee(snapshot),
      #[cfg(not(feature = "std"))]
      inner: Shared::new(snapshot),
    }
  }
  pub(crate) fn load(&self) -> Shared<Snapshot<Pool>> {
    #[cfg(feature = "std")]
    {
      self.inner.load_full()
    }
    #[cfg(not(feature = "std"))]
    {
      self.inner.clone()
    }
  }
  #[cfg(feature = "std")]
  pub(crate) fn store(&self, snapshot: Snapshot<Pool>) {
    self.inner.store(Shared::new(snapshot));
  }
}

struct Sequence {
  #[cfg(feature = "std")]
  inner: core::sync::atomic::AtomicU64,
  #[cfg(not(feature = "std"))]
  inner: core::cell::Cell<u64>,
}
impl Sequence {
  fn new() -> Self {
    Self {
      #[cfg(feature = "std")]
      inner: core::sync::atomic::AtomicU64::new(0),
      #[cfg(not(feature = "std"))]
      inner: core::cell::Cell::new(0),
    }
  }
  fn next(&self) -> u64 {
    #[cfg(feature = "std")]
    {
      self.inner.fetch_add(1, core::sync::atomic::Ordering::Relaxed)
    }
    #[cfg(not(feature = "std"))]
    {
      let value = self.inner.get();
      self.inner.set(value.wrapping_add(1));
      value
    }
  }
}

/// Configured execution state and one shared request orchestration path.
#[cfg(feature = "std")]
pub struct Client<P: Platform = crate::StdPlatform> {
  pub(crate) platform: P,
  pub(crate) snapshot: SnapshotCell<P::Pool>,
  limits: Limits,
  request_sequence: Sequence,
  pub(crate) source: Option<crate::std_client::Source>,
}
#[cfg(not(feature = "std"))]
pub struct Client<P: Platform> {
  pub(crate) platform: P,
  pub(crate) snapshot: SnapshotCell<P::Pool>,
  limits: Limits,
  request_sequence: Sequence,
}

impl<P: Platform> Client<P> {
  pub(crate) fn from_snapshot(platform: P, snapshot: Snapshot<P::Pool>) -> Self {
    Self {
      platform,
      snapshot: SnapshotCell::new(snapshot),
      limits: Limits::default(),
      request_sequence: Sequence::new(),
      #[cfg(feature = "std")]
      source: None,
    }
  }

  pub fn with_limits(mut self, limits: Limits) -> Self {
    self.limits = limits;
    self
  }

  /// Extract external services, dropping this client's configuration and state.
  pub fn into_platform(self) -> P {
    self.platform
  }
}

impl<P: ResponseAdapter> Client<P> {
  /// Construct a client entirely in memory, independently of service selection.
  pub fn new(rules: ExecutionRules, accounts: Vec<AccountConfig>, platform: P) -> Result<Self, P::Error> {
    Self::with_model_metadata(rules, accounts, BTreeMap::new(), platform)
  }

  pub fn with_models(
    rules: ExecutionRules,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, BTreeSet<SmolStr>>,
    platform: P,
  ) -> Result<Self, P::Error> {
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
    Self::with_model_metadata(rules, accounts, models, platform)
  }

  /// Model evidence is configuration, shared by standard and custom adapters.
  pub fn with_model_metadata(
    rules: ExecutionRules,
    accounts: Vec<AccountConfig>,
    models: BTreeMap<ProviderId, ModelInventory>,
    platform: P,
  ) -> Result<Self, P::Error> {
    let build = || {
      if rules.profiles().is_empty() {
        return Err(session::Error::new("config", "no execution profiles configured"));
      }
      for (id, profile) in rules.profiles() {
        if matches!(rules.route(profile.route()), Some(RoutePlan::Relay(relay))
          if !matches!(relay.destination(), RelayDestination::FixedProvider(_)))
        {
          return Err(session::Error::new(
            "config",
            format!("profile '{id}' requires original-destination proxy context"),
          ));
        }
      }
      ExecutionState::with_models(rules, accounts, models)
    };
    let execution = build().map_err(|error| P::execution_error(error, Endpoint::Responses))?;
    let pool = platform.pool();
    Ok(Self::from_snapshot(
      platform,
      Snapshot {
        execution: Shared::new(execution),
        pool,
        default_profile: "default".into(),
        profile_aliases: BTreeMap::new(),
        #[cfg(feature = "std")]
        metadata: None,
        #[cfg(feature = "std")]
        files: None,
      },
    ))
  }

  pub fn responses(&self) -> crate::Responses<'_, P> {
    crate::Responses::new(self)
  }
  pub fn chat_completions(&self) -> crate::ChatCompletions<'_, P> {
    crate::ChatCompletions::new(self)
  }
  pub fn messages(&self) -> crate::Messages<'_, P> {
    crate::Messages::new(self)
  }

  pub async fn execute(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
  ) -> Result<P::Response<'_>, P::Error> {
    self
      .execute_with_generation_options(endpoint, body, options, None)
      .await
  }

  pub(crate) async fn execute_with_generation_options(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
    mut generation_options: Option<GenerationOptions>,
  ) -> Result<P::Response<'_>, P::Error> {
    let snapshot = self.snapshot.load();
    // Acquire external services before reading clocks. A local callback may
    // reenter execution; acquisition reports busy without borrowing its runtime.
    let Services {
      transport,
      clock,
      events,
    } = self
      .platform
      .acquire(&snapshot.pool)
      .map_err(|error| P::execution_error(error, endpoint))?;
    let profile = Self::select_profile(&snapshot, options.profile.as_deref())
      .map_err(|error| P::execution_error(error, endpoint))?;
    Self::prepare_generation(&snapshot, &profile, endpoint, &body, &mut generation_options)
      .map_err(|error| P::execution_error(error, endpoint))?;
    #[cfg(feature = "std")]
    if let Some(metadata) = &snapshot.metadata {
      metadata.refresh(&snapshot.execution);
    }
    let request_id = options
      .request_id
      .clone()
      .unwrap_or_else(|| format!("req-{}-{}", clock.now().as_nanos(), self.request_sequence.next()));
    let mut input = RequestInput::new(endpoint, body, options.inbound_headers(), request_id);
    input.profile = Some(profile);
    input.generation_options = generation_options;
    let response = session::Session::new(snapshot.execution.clone(), input, transport, clock, events)
      .with_limits(self.limits)
      .run()
      .await
      .map_err(|error| P::execution_error(error, endpoint))?;
    Ok(P::finish_response(response))
  }

  pub(crate) fn select_profile(
    snapshot: &Snapshot<P::Pool>,
    requested: Option<&str>,
  ) -> Result<String, session::Error> {
    let name = requested.unwrap_or(&snapshot.default_profile);
    let canonical = if requested.is_some() {
      snapshot.profile_aliases.get(name).map(String::as_str).unwrap_or(name)
    } else {
      name
    };
    let rules = snapshot.execution.rules();
    let profile = ProfileId::new(canonical)
      .ok()
      .and_then(|id| rules.profile(&id))
      .ok_or_else(|| {
        let mut error = session::Error::new("resolve", format!("unknown profile '{name}'"));
        error.kind = session::ErrorKind::UnknownProfile {
          profile: SmolStr::new(name),
        };
        error
      })?;
    if matches!(rules.route(profile.route()), Some(RoutePlan::Relay(relay)) if matches!(relay.destination(), RelayDestination::Original))
    {
      return Err(session::Error::new("config", format!("SDK profile '{name}' requires an original proxy destination; select a managed or fixed-provider relay profile")));
    }
    Ok(canonical.to_string())
  }

  fn prepare_generation(
    snapshot: &Snapshot<P::Pool>,
    profile: &str,
    endpoint: Endpoint,
    body: &Value,
    options: &mut Option<GenerationOptions>,
  ) -> Result<(), session::Error> {
    let rules = snapshot.execution.rules();
    let is_relay = rules
      .profiles()
      .get(profile)
      .and_then(|profile| rules.route(profile.route()))
      .is_some_and(|route| matches!(route, RoutePlan::Relay(_)));
    if let (true, Some(generation)) = (is_relay, options.as_ref()) {
      if generation.top_k.is_some() || generation.reasoning.is_some() {
        return Err(session::Error::new("generation", "typed top_k and reasoning controls require a managed route; a relay profile forwards the generated Responses payload verbatim"));
      }
      if let Some(limit) = generation.max_output_tokens {
        if endpoint != Endpoint::Responses || body.get("max_output_tokens").and_then(Value::as_u64) != Some(limit) {
          return Err(session::Error::new("generation", "typed max_output_tokens cannot be lowered in a relay profile; the verbatim request must already contain the matching Responses field"));
        }
      }
      *options = None;
    }
    Ok(())
  }

  pub(crate) async fn execute_typed<T: Serialize>(
    &self,
    endpoint: Endpoint,
    request: &T,
    stream: bool,
    options: RequestOptions,
  ) -> Result<P::Response<'_>, P::Error> {
    let mut body = serde_json::to_value(request).map_err(P::encode_error)?;
    body["stream"] = Value::Bool(stream);
    self.execute(endpoint, body, options).await
  }
}

impl<T: Transport, R: tokn_requests::configured::Runtime> Platform for tokn_requests::configured::Injected<T, R> {
  type Pool = ();
  type Exchange<'a>
    = tokn_requests::configured::BorrowedTransport<'a, T>
  where
    Self: 'a;
  type Clock<'a>
    = tokn_requests::configured::LocalClock<'a, R>
  where
    Self: 'a;
  type Events<'a>
    = tokn_requests::configured::LocalEvents<'a, R>
  where
    Self: 'a;
  fn pool(&self) {}
  fn acquire<'a>(
    &'a self,
    _: &(),
  ) -> Result<Services<Self::Exchange<'a>, Self::Clock<'a>, Self::Events<'a>>, session::Error> {
    let (transport, clock, events) = self.acquire()?;
    Ok(Services {
      transport,
      clock,
      events,
    })
  }
}

impl<T: Transport, R: tokn_requests::configured::Runtime> ResponseAdapter
  for tokn_requests::configured::Injected<T, R>
{
  type Error = session::Error;
  type Response<'a>
    = tokn_requests::configured::Response<'a, T, R>
  where
    Self: 'a;
  type Stream<'a>
    = tokn_requests::configured::EventStream<'a, T, R>
  where
    Self: 'a;
  fn encode_error(source: serde_json::Error) -> Self::Error {
    session::Error::new("request", source)
  }
  fn execution_error(source: session::Error, _: Endpoint) -> Self::Error {
    source
  }
  fn finish_response<'a>(
    response: session::Response<Self::Exchange<'a>, Self::Clock<'a>, Self::Events<'a>>,
  ) -> Self::Response<'a>
  where
    Self: 'a,
  {
    response
  }
}

impl<'a, T: Transport, R: tokn_requests::configured::Runtime> PlatformResponse
  for tokn_requests::configured::Response<'a, T, R>
{
  type Error = session::Error;
  type Stream = tokn_requests::configured::EventStream<'a, T, R>;
  fn into_json<D: DeserializeOwned>(self) -> Result<BufferedResponse<D>, Self::Error> {
    let (status, headers, data) = self.into_json()?;
    Ok(BufferedResponse { status, headers, data })
  }
  fn into_stream(self) -> Result<Self::Stream, Self::Error> {
    match self.body {
      session::ResponseBody::Stream(stream) => Ok(*stream),
      _ => Err(session::Error::new("response", "expected a streaming response")),
    }
  }
}
