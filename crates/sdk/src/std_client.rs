use arc_swap::ArcSwap;
use serde_json::Value;
use smol_str::SmolStr;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokn_auth::{default_auth_path, AuthStore};
use tokn_config::{v2, ExecutionConfigInput};
use tokn_core::event::{Event, EventBus};
use tokn_core::generation::GenerationOptions;
use tokn_core::provider::Endpoint;
use tokn_core::request_event::RequestEndpoint;
use tokn_policy::{ExecutionRules, ProfileId, RouteKind};
use tokn_requests::v2::{ApiProfileRuntime, ConfiguredRuntime};
use tokn_requests::{ExecutionRequest, RawInbound};

use crate::response::RawResponse;
use crate::{Client, ConfigWarning, RequestOptions};
use crate::{Error, Result};

const EVENT_BUS_CAPACITY: usize = 256;
const DEFAULT_PROFILE: &str = "default";

#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
  config_path: Option<PathBuf>,
  auth_path: Option<PathBuf>,
  profile: Option<String>,
}

impl ClientBuilder {
  /// Load native v2 or legacy configuration. Legacy input is projected in memory
  /// before using the same v2 compiler and runtime; listeners are not started.
  pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.config_path = Some(path.into());
    self
  }

  pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.auth_path = Some(path.into());
    self
  }

  /// Select the default request profile (otherwise `default`).
  /// Individual requests can override this with `RequestOptions::profile`.
  pub fn profile(mut self, profile: impl Into<String>) -> Self {
    self.profile = Some(profile.into());
    self
  }

  pub fn build(self) -> Result<Client> {
    StdPlatform::build(self).map(Client::from_platform)
  }
}

struct Source {
  config_path: Option<PathBuf>,
  auth_path: Option<PathBuf>,
  profile: Option<String>,
}

struct Snapshot {
  state: ConfiguredRuntime,
  plan: ExecutionRules,
  default_profile: String,
  profile_aliases: BTreeMap<String, String>,
  config_warnings: Vec<ConfigWarning>,
  config_path: PathBuf,
  auth_path: PathBuf,
}

pub struct StdPlatform {
  source: Source,
  events: Arc<EventBus>,
  snapshot: ArcSwap<Snapshot>,
}

impl StdPlatform {
  fn build(builder: ClientBuilder) -> Result<Self> {
    let source = Source {
      config_path: builder.config_path,
      auth_path: builder.auth_path,
      profile: builder.profile,
    };
    let events = Arc::new(EventBus::new(EVENT_BUS_CAPACITY));
    let snapshot = load_snapshot(&source, events.clone())?;
    Ok(Self {
      source,
      events,
      snapshot: ArcSwap::from_pointee(snapshot),
    })
  }

  pub fn reload(&self) -> Result<()> {
    self
      .snapshot
      .store(Arc::new(load_snapshot(&self.source, self.events.clone())?));
    Ok(())
  }

  /// Subscribe to the same lifecycle stream used by the configured request
  /// pipeline. The subscription remains valid across successful reloads.
  pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Arc<Event>> {
    self.events.subscribe()
  }

  pub fn config_path(&self) -> PathBuf {
    self.snapshot.load().config_path.clone()
  }

  pub fn auth_path(&self) -> PathBuf {
    self.snapshot.load().auth_path.clone()
  }

  /// Conversion diagnostics belonging to the active configuration snapshot.
  pub fn config_warnings(&self) -> Vec<ConfigWarning> {
    self.snapshot.load().config_warnings.clone()
  }

  pub(crate) async fn execute_generation(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
    generation_options: GenerationOptions,
  ) -> Result<RawResponse> {
    let generation_options = (!generation_options.is_empty()).then_some(generation_options);
    self
      .execute_with_generation_options(endpoint, body, options, generation_options)
      .await
  }

  async fn execute_with_generation_options(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
    mut generation_options: Option<GenerationOptions>,
  ) -> Result<RawResponse> {
    let snapshot = self.snapshot.load_full();
    let profile = select_profile(&snapshot, options.profile.as_deref())?;
    if let (RouteKind::Relay, Some(generation)) = (profile.route_kind(), generation_options.as_ref()) {
      if generation.top_k.is_some() || generation.reasoning.is_some() {
        return Err(Error::InvalidGenerateRequest {
          message: "typed top_k and reasoning controls require a managed route; a relay profile forwards the generated Responses payload verbatim".into(),
        });
      }
      if let Some(max_output_tokens) = generation.max_output_tokens {
        let wire_limit = body.get("max_output_tokens").and_then(Value::as_u64);
        if endpoint != Endpoint::Responses || wire_limit != Some(max_output_tokens) {
          return Err(Error::InvalidGenerateRequest {
            message: "typed max_output_tokens cannot be lowered in a relay profile; the verbatim request must already contain the matching Responses field".into(),
          });
        }
      }
      // The friendly generator already rendered a Responses-native output
      // limit into `body`. Do not carry the same intent into a verbatim stage,
      // where out-of-band controls are deliberately rejected.
      generation_options = None;
    }
    let raw = raw_request(endpoint, body, &options)?;
    let mut config = profile
      .run_config(endpoint)
      .map_err(|source| Error::BuildEngine { source })?;
    if let Some(generation_options) = generation_options {
      config = config.with_generation_options(generation_options);
    }
    let config = config.build();
    let response = profile
      .request_service()
      .execute(ExecutionRequest::new(raw).with_config(config))
      .await
      .map_err(|source| Error::Request { source })?;
    Ok(RawResponse::from(response))
  }
}

fn load_snapshot(source: &Source, events: Arc<EventBus>) -> Result<Snapshot> {
  let config_path = match &source.config_path {
    Some(path) => path.clone(),
    None => tokn_config::paths::config_path().map_err(|source| Error::LoadConfig { source })?,
  };
  let input = tokn_config::load_execution_input(&config_path).map_err(|source| Error::LoadConfig { source })?;
  let auth_path = match &source.auth_path {
    Some(path) => path.clone(),
    None => default_auth_path().map_err(|source| Error::LoadCredentials {
      path: PathBuf::from("<default>"),
      source,
    })?,
  };
  let credentials = AuthStore::load(Some(&auth_path), None).map_err(|source| Error::LoadCredentials {
    path: auth_path.clone(),
    source,
  })?;
  let (raw, accounts, config_warnings) = match input {
    ExecutionConfigInput::V2(raw) => (raw, credentials.accounts, Vec::new()),
    ExecutionConfigInput::Legacy {
      loaded,
      inline_accounts,
    } => {
      // Current auth sources own duplicate ids. Legacy-only accounts remain
      // usable in memory without migrating or rewriting either credential file.
      let mut accounts = credentials.accounts;
      let auth_ids: BTreeSet<_> = accounts.iter().map(|account| account.id.clone()).collect();
      accounts.extend(
        inline_accounts
          .into_iter()
          .filter(|account| !auth_ids.contains(&account.id)),
      );
      tokn_router_legacy_config::v2::project_v2_execution_config(&loaded.config, &accounts)
        .map_err(|source| Error::ProjectConfig {
          path: config_path.clone(),
          source: Box::new(source),
        })?
        .into_parts()
    }
  };
  let (plan, service) = v2::compile_execution_config(&raw, &config_path)
    .map_err(|source| Error::LoadConfig {
      source: tokn_config::Error::V2 {
        source: Box::new(source),
      },
    })?
    .into_parts();
  let profile_aliases: BTreeMap<String, String> = config_warnings
    .iter()
    .filter_map(|warning| match warning {
      ConfigWarning::ProfileResourceRenamed { profile, resource_id } => Some((profile.clone(), resource_id.clone())),
      _ => None,
    })
    .collect();
  let state = ConfiguredRuntime::build(&plan, &accounts, &service.outbound().to_http_client_options(), events)
    .map_err(|source| Error::BuildEngine { source })?;
  let default_profile = source
    .profile
    .as_deref()
    .map(|name| profile_aliases.get(name).map(String::as_str).unwrap_or(name).to_owned())
    .unwrap_or_else(|| DEFAULT_PROFILE.to_owned());
  let snapshot = Snapshot {
    state,
    plan,
    default_profile,
    config_path,
    auth_path,
    profile_aliases,
    config_warnings,
  };
  select_profile(&snapshot, None)?;
  Ok(snapshot)
}

fn select_profile<'a>(snapshot: &'a Snapshot, request_profile: Option<&str>) -> Result<&'a ApiProfileRuntime> {
  let name = request_profile.unwrap_or(&snapshot.default_profile);
  // An explicit legacy name selects its original policy even if its
  // spelling collides with another profile's generated resource id. The
  // implicit default is already canonicalized at snapshot construction.
  let canonical = if request_profile.is_some() {
    snapshot.profile_aliases.get(name).map(String::as_str).unwrap_or(name)
  } else {
    name
  };
  let id = ProfileId::new(canonical)
    .ok()
    .filter(|id| snapshot.plan.profile(id).is_some())
    .ok_or_else(|| Error::UnknownProfile {
      profile: name.to_owned(),
    })?;
  snapshot.state.profile(&id).ok_or_else(|| Error::BuildEngine {
    source: anyhow::anyhow!(
      "SDK profile '{name}' requires an original proxy destination; select a managed or fixed-provider relay profile"
    ),
  })
}

fn raw_request(endpoint: Endpoint, body: Value, options: &RequestOptions) -> Result<RawInbound> {
  let bytes = serde_json::to_vec(&body).map_err(|source| Error::SerializeRequest { source })?;
  let headers = options.inbound_headers();
  let bytes = bytes::Bytes::from(bytes);
  Ok(RawInbound {
    request_endpoint: RequestEndpoint::from(endpoint),
    headers,
    raw_body: bytes.clone(),
    decoded_body: bytes,
    body_json: body,
    request_id: options.request_id.as_deref().map(SmolStr::new),
  })
}

impl crate::Platform for StdPlatform {
  type Error = crate::Error;
  type Response<'a> = crate::RawResponse;
  type Stream<'a> = crate::StreamResponse;

  fn encode_error(source: serde_json::Error) -> Self::Error {
    crate::Error::SerializeRequest { source }
  }

  async fn execute(&self, endpoint: Endpoint, body: Value, options: RequestOptions) -> Result<RawResponse> {
    self
      .execute_with_generation_options(endpoint, body, options, None)
      .await
  }
}

impl Client<StdPlatform> {
  pub fn builder() -> ClientBuilder {
    ClientBuilder::default()
  }
  pub fn from_default_config() -> Result<Self> {
    Self::builder().build()
  }
  pub fn reload(&self) -> Result<()> {
    self.platform.reload()
  }
  pub fn config_path(&self) -> PathBuf {
    self.platform.config_path()
  }
  pub fn auth_path(&self) -> PathBuf {
    self.platform.auth_path()
  }
  /// Warnings from normalizing the active legacy configuration into v2.
  /// Failed reloads preserve the previous snapshot and its diagnostics.
  pub fn config_warnings(&self) -> Vec<ConfigWarning> {
    self.platform.config_warnings()
  }
  pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Arc<Event>> {
    self.platform.subscribe_events()
  }
  pub(crate) async fn execute_generation(
    &self,
    endpoint: Endpoint,
    body: Value,
    options: RequestOptions,
    generation_options: GenerationOptions,
  ) -> Result<RawResponse> {
    self
      .platform
      .execute_generation(endpoint, body, options, generation_options)
      .await
  }
}
