use arc_swap::ArcSwap;
use serde_json::Value;
use smol_str::SmolStr;
use std::path::PathBuf;
use std::sync::Arc;
use tokn_auth::{default_auth_path, AuthStore};
use tokn_config::Config;
use tokn_core::event::{Event, EventBus};
use tokn_core::generation::GenerationOptions;
use tokn_core::provider::Endpoint;
use tokn_core::request_event::RequestEndpoint;
use tokn_headers::keys::{ACCEPT, CONTENT_TYPE};
use tokn_headers::{HeaderMap, HeaderName, HeaderValue};
use tokn_requests::runtime::{ConfiguredRuntime, RequestPolicyRuntime};
use tokn_requests::{ExecutionRequest, RawInbound, RunConfig};

use crate::response::RawResponse;
use crate::{Client, RequestOptions};
use crate::{Error, Result};

const EVENT_BUS_CAPACITY: usize = 256;

#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
  config_path: Option<PathBuf>,
  auth_path: Option<PathBuf>,
  profile: Option<String>,
}

impl ClientBuilder {
  pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.config_path = Some(path.into());
    self
  }

  pub fn auth_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.auth_path = Some(path.into());
    self
  }

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
  default_profile: Option<String>,
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
    let policy = select_policy(&snapshot, options.profile.as_deref())?;
    let verbatim_mode = match policy.mode {
      tokn_config::RouteMode::Passthrough => Some("passthrough"),
      tokn_config::RouteMode::Switch => Some("switch"),
      _ => None,
    };
    if let (Some(mode), Some(generation)) = (verbatim_mode, generation_options.as_ref()) {
      if generation.top_k.is_some() || generation.reasoning.is_some() {
        return Err(Error::InvalidGenerateRequest {
          message: format!(
            "typed top_k and reasoning controls require a managed route; profile mode '{mode}' forwards the generated Responses payload verbatim"
          ),
        });
      }
      if let Some(max_output_tokens) = generation.max_output_tokens {
        let wire_limit = body.get("max_output_tokens").and_then(Value::as_u64);
        if endpoint != Endpoint::Responses || wire_limit != Some(max_output_tokens) {
          return Err(Error::InvalidGenerateRequest {
            message: format!(
              "typed max_output_tokens cannot be lowered in profile mode '{mode}'; the verbatim request must already contain the matching Responses field"
            ),
          });
        }
      }
      // The friendly generator already rendered a Responses-native output
      // limit into `body`. Do not carry the same intent into a verbatim stage,
      // where out-of-band controls are deliberately rejected.
      generation_options = None;
    }
    let raw = raw_request(endpoint, body, &options)?;
    let mut config = RunConfig::builder().with_agent_id_opt(policy.agent_id.clone());
    if let Some(generation_options) = generation_options {
      config = config.with_generation_options(generation_options);
    }
    let config = config.build();
    let service = match policy.mode {
      tokn_config::RouteMode::Passthrough => policy.passthrough_runtime.request_service(),
      tokn_config::RouteMode::Switch => policy.switch_runtime.request_service(),
      _ => policy.request_runtime.request_service(),
    };
    let response = service
      .execute(ExecutionRequest::new(raw).with_config(config))
      .await
      .map_err(|source| Error::Request { source })?;
    Ok(RawResponse::from(response))
  }
}

fn load_snapshot(source: &Source, events: Arc<EventBus>) -> Result<Snapshot> {
  let (config, config_path) =
    Config::load(source.config_path.as_deref()).map_err(|source| Error::LoadConfig { source })?;
  let auth_path = match &source.auth_path {
    Some(path) => path.clone(),
    None => default_auth_path().map_err(|source| Error::LoadCredentials {
      path: PathBuf::from("<default>"),
      source,
    })?,
  };
  let credentials = AuthStore::load(Some(&auth_path), Some(&config_path)).map_err(|source| Error::LoadCredentials {
    path: auth_path.clone(),
    source,
  })?;
  let state = tokn_requests::runtime::build_configured_runtime(&config, &credentials.accounts, events)
    .map_err(|source| Error::BuildEngine { source })?;
  if let Some(profile) = &source.profile {
    ensure_profile(&state, profile)?;
  }
  Ok(Snapshot {
    state,
    default_profile: source.profile.clone(),
    config_path,
    auth_path,
  })
}

fn ensure_profile(state: &ConfiguredRuntime, profile: &str) -> Result<()> {
  if state.profiles.contains_key(profile) {
    Ok(())
  } else {
    Err(Error::UnknownProfile {
      profile: profile.to_string(),
    })
  }
}

fn select_policy(snapshot: &Snapshot, request_profile: Option<&str>) -> Result<Arc<RequestPolicyRuntime>> {
  let profile = request_profile.or(snapshot.default_profile.as_deref());
  match profile {
    Some(profile) => snapshot
      .state
      .profiles
      .get(profile)
      .cloned()
      .ok_or_else(|| Error::UnknownProfile {
        profile: profile.to_string(),
      }),
    None => Ok(snapshot.state.default_policy.clone()),
  }
}

fn raw_request(endpoint: Endpoint, body: Value, options: &RequestOptions) -> Result<RawInbound> {
  let bytes = serde_json::to_vec(&body).map_err(|source| Error::SerializeRequest { source })?;
  let mut headers = HeaderMap::new();
  headers.insert(CONTENT_TYPE.clone(), HeaderValue::from_static("application/json"));
  headers.insert(ACCEPT.clone(), HeaderValue::from_static("application/json"));
  insert_option(&mut headers, "x-request-id", options.request_id.as_deref());
  insert_option(&mut headers, "x-session-id", options.session_id.as_deref());
  insert_option(&mut headers, "x-project-cwd", options.project_id.as_deref());
  insert_option(&mut headers, "x-initiator", options.initiator.as_deref());
  for (name, value) in &options.headers {
    headers.insert(HeaderName::new(name.clone()), HeaderValue::from_string(value.clone()));
  }
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

fn insert_option(headers: &mut HeaderMap, name: &'static str, value: Option<&str>) {
  if let Some(value) = value {
    headers.insert(HeaderName::new(name), HeaderValue::from_string(value.to_string()));
  }
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
