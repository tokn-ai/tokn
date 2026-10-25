use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokn_auth::{default_auth_path, AuthStore};
use tokn_config::{v2, ExecutionConfigInput};
use tokn_core::event::{Event, EventBus};
use tokn_core::generation::GenerationOptions;
use tokn_core::provider::Endpoint;
use tokn_core::util::http::HttpClientOptions;
use tokn_requests::session::{Error as SessionError, ErrorKind, Limits};
use tokn_requests::v2::adapters::{
  into_standard_response, pipeline_error, ReqwestExchange, StandardEvents, TokioRuntime,
};
use tokn_requests::v2::{prepare_execution, HttpClients};

use crate::client::Snapshot;
use crate::response::RawResponse;
use crate::{Client, ConfigWarning, Platform, RequestOptions, ResponseAdapter, Services};
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
  /// before constructing the shared client runtime; listeners are not started.
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
    let source = Source {
      config_path: self.config_path,
      auth_path: self.auth_path,
      profile: self.profile,
    };
    let snapshot = load_snapshot(&source)?;
    let platform = StdPlatform::with_http_clients(snapshot.pool.clone());
    let mut client = Client::from_snapshot(platform, snapshot).with_limits(Limits {
      max_response_bytes: usize::MAX,
      max_event_bytes: usize::MAX,
      max_error_body_bytes: 8 * 1024,
    });
    client.source = Some(source);
    Ok(client)
  }
}

pub(crate) struct Source {
  config_path: Option<PathBuf>,
  auth_path: Option<PathBuf>,
  profile: Option<String>,
}

pub(crate) struct FileMetadata {
  pub config_path: PathBuf,
  pub auth_path: PathBuf,
  pub config_warnings: Vec<ConfigWarning>,
}

/// Standard transport pools, timing, and event delivery.
///
/// Rules, credentials, selection state, and configuration files belong to
/// [`Client`]. Each acquired exchange independently owns its response lifecycle.
pub struct StdPlatform {
  http: HttpClients,
  clock: TokioRuntime,
  events: Arc<EventBus>,
}

impl StdPlatform {
  pub fn new() -> Result<Self> {
    Self::with_outbound(&HttpClientOptions::default())
  }

  /// Configure the external HTTP pools without choosing any routing rules.
  pub fn with_outbound(options: &HttpClientOptions) -> Result<Self> {
    let http = HttpClients::build(options).map_err(|source| Error::BuildEngine { source })?;
    Ok(Self::with_http_clients(http))
  }

  pub fn with_http_clients(http: HttpClients) -> Self {
    Self {
      http,
      clock: TokioRuntime::new(),
      events: Arc::new(EventBus::new(EVENT_BUS_CAPACITY)),
    }
  }

  pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Arc<Event>> {
    self.events.subscribe()
  }
}

impl Platform for StdPlatform {
  type Pool = HttpClients;
  type Exchange<'a> = ReqwestExchange;
  type Clock<'a> = TokioRuntime;
  type Events<'a> = StandardEvents;

  fn pool(&self) -> Self::Pool {
    self.http.clone()
  }

  fn acquire<'a>(
    &'a self,
    pool: &Self::Pool,
  ) -> std::result::Result<Services<Self::Exchange<'a>, Self::Clock<'a>, Self::Events<'a>>, SessionError> {
    Ok(Services {
      transport: ReqwestExchange::with_clients(pool.managed.clone(), pool.opaque.clone()),
      clock: self.clock.clone(),
      events: StandardEvents::new(self.events.clone()),
    })
  }
}

impl ResponseAdapter for StdPlatform {
  type Error = Error;
  type Response<'a> = RawResponse;
  type Stream<'a> = crate::StreamResponse;

  fn encode_error(source: serde_json::Error) -> Self::Error {
    Error::SerializeRequest { source }
  }

  fn execution_error(error: SessionError, endpoint: Endpoint) -> Self::Error {
    if let ErrorKind::UnknownProfile { profile } = &error.kind {
      return Error::UnknownProfile {
        profile: profile.to_string(),
      };
    }
    if error.stage == "generation" {
      return Error::InvalidGenerateRequest {
        message: error.message.to_string(),
      };
    }
    if error.stage == "config" {
      return Error::BuildEngine {
        source: anyhow::Error::new(error),
      };
    }
    Error::Request {
      source: pipeline_error(error, endpoint).into(),
    }
  }

  fn finish_response<'a>(
    response: tokn_requests::session::Response<Self::Exchange<'a>, Self::Clock<'a>, Self::Events<'a>>,
  ) -> Self::Response<'a>
  where
    Self: 'a,
  {
    RawResponse::from(into_standard_response(response))
  }
}

fn load_snapshot(source: &Source) -> Result<Snapshot<HttpClients>> {
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
  let (rules, service) = v2::compile_execution_config(&raw, &config_path)
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
  let (execution, metadata) = prepare_execution(&rules, &accounts)
    .map_err(|source| Error::BuildEngine { source })?
    .into_parts();
  let pool =
    HttpClients::build(&service.outbound().to_http_client_options()).map_err(|source| Error::BuildEngine { source })?;
  let default_profile = source
    .profile
    .as_deref()
    .map(|name| profile_aliases.get(name).map(String::as_str).unwrap_or(name).to_owned())
    .unwrap_or_else(|| DEFAULT_PROFILE.to_owned());
  let snapshot = Snapshot {
    execution,
    pool,
    default_profile,
    profile_aliases,
    metadata: Some(metadata),
    files: Some(FileMetadata {
      config_path,
      auth_path,
      config_warnings,
    }),
  };
  Client::<StdPlatform>::select_profile(&snapshot, None)
    .map_err(|error| StdPlatform::execution_error(error, Endpoint::Responses))?;
  Ok(snapshot)
}

impl Client<StdPlatform> {
  pub fn builder() -> ClientBuilder {
    ClientBuilder::default()
  }

  pub fn from_default_config() -> Result<Self> {
    Self::builder().build()
  }

  /// Atomically replace configuration, credentials, model metadata, and HTTP
  /// pools. Active requests retain their original execution snapshot.
  pub fn reload(&self) -> Result<()> {
    let source = self.source.as_ref().ok_or(Error::ReloadUnavailable)?;
    self.snapshot.store(load_snapshot(source)?);
    Ok(())
  }

  /// The configuration file, when this client was built from files.
  pub fn config_path(&self) -> Option<PathBuf> {
    self
      .snapshot
      .load()
      .files
      .as_ref()
      .map(|files| files.config_path.clone())
  }

  /// The credential file, when this client was built from files.
  pub fn auth_path(&self) -> Option<PathBuf> {
    self.snapshot.load().files.as_ref().map(|files| files.auth_path.clone())
  }

  /// Warnings from normalizing the active legacy configuration into v2.
  /// Failed reloads preserve the previous snapshot and its diagnostics.
  pub fn config_warnings(&self) -> Vec<ConfigWarning> {
    self
      .snapshot
      .load()
      .files
      .as_ref()
      .map(|files| files.config_warnings.clone())
      .unwrap_or_default()
  }

  /// Subscribe to lifecycle events. Subscriptions remain valid across reloads.
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
      .execute_with_generation_options(
        endpoint,
        body,
        options,
        (!generation_options.is_empty()).then_some(generation_options),
      )
      .await
  }
}
