//! Standard platform assembly for the shared request domain.
//!
//! SDK and gateway callers construct the same policies and request services;
//! HTTP admission and administrative state remain with the gateway.

use anyhow::Result;
use std::{
  collections::{BTreeMap, BTreeSet},
  sync::Arc,
  time::Duration,
};
use tokn_accounts::{registry::Registry as ProviderRegistry, routing::RouteResolver};
use tokn_accounts::{AccountInventory, AccountPool, AccountPoolRuleset};
use tokn_config::{AgentId, Config, ModelFamily, ProfileConfig, RouteMode};
use tokn_core::{account::AccountConfig, event::EventBus};

const PIPELINE_RETRY_POLICY: crate::RetryPolicy = crate::RetryPolicy::new(2, Duration::from_millis(100));

#[derive(Clone)]
pub struct RequestPolicyRuntime {
  pub mode: RouteMode,
  pub agent_id: Option<AgentId>,
  pub default_provider_id: Option<String>,
  pub ruleset: AccountPoolRuleset,
  pub pool: Arc<AccountPool>,
  pub route: Arc<RouteResolver>,
  pub model_families: Vec<ModelFamily>,
  pub request_runtime: RequestRuntime,
  pub passthrough_runtime: RequestRuntime,
  pub switch_runtime: RequestRuntime,
}

/// One compiled request-domain service and its HTTP-facing adapter.
#[derive(Clone)]
pub struct RequestRuntime {
  requests: crate::RequestService,
  http: tokn_service::HttpService,
}

impl RequestRuntime {
  fn new(requests: crate::RequestService) -> Self {
    let http = requests.http_service();
    Self { requests, http }
  }

  /// Request-domain entry point used by in-process SDK clients.
  pub fn request_service(&self) -> &crate::RequestService {
    &self.requests
  }

  /// Native HTTP entry point used by router endpoints.
  pub fn http_service(&self) -> &tokn_service::HttpService {
    &self.http
  }
}

#[derive(Clone)]
struct PolicyBuildDeps {
  cfg: Config,
  inventory: Arc<AccountInventory>,
  http: reqwest::Client,
  events: Arc<EventBus>,
}

struct PolicySpec {
  mode: RouteMode,
  agent_id: Option<AgentId>,
  default_provider_id: Option<String>,
  providers: Option<Vec<String>>,
  accounts: Option<Vec<String>>,
  families: Vec<ModelFamily>,
}

/// A compiled standard runtime, independent of any HTTP server or SDK facade.
#[derive(Clone)]
pub struct ConfiguredRuntime {
  pub inventory: Arc<AccountInventory>,
  pub pool: Arc<AccountPool>,
  pub provider_registry: Arc<ProviderRegistry>,
  pub route: Arc<RouteResolver>,
  pub default_policy: Arc<RequestPolicyRuntime>,
  pub profiles: Arc<BTreeMap<String, Arc<RequestPolicyRuntime>>>,
  pub http: reqwest::Client,
  pub events: Arc<EventBus>,
}

#[derive(Clone, Copy)]
pub enum RuntimeKind {
  Api,
  Proxy,
}

/// Build configured in-process API execution with the same policies as a gateway.
pub fn build_configured_runtime(
  cfg: &Config,
  accounts: &[AccountConfig],
  events: Arc<EventBus>,
) -> Result<ConfiguredRuntime> {
  build_runtime(cfg, accounts, events, RuntimeKind::Api)
}

pub fn build_runtime(
  cfg: &Config,
  accounts: &[AccountConfig],
  events: Arc<EventBus>,
  kind: RuntimeKind,
) -> Result<ConfiguredRuntime> {
  cfg.validate()?;
  let provider_registry = Arc::new(ProviderRegistry::builtin());
  let _ = validate_proxy_provider_modes(cfg, provider_registry.as_ref());
  validate_policy_providers(cfg, provider_registry.as_ref())?;
  validate_policy_accounts(cfg, accounts)?;
  let default_mode = effective_default_mode(cfg);
  let inventory = if accounts.is_empty() && matches!(default_mode, RouteMode::Passthrough) {
    AccountInventory::empty()
  } else {
    let registry = provider_registry.clone();
    AccountInventory::from_accounts_with(accounts, move |account| registry.build(account))?
  };
  let pool = AccountPool::from_inventory(&inventory, cfg, &AccountPoolRuleset::all())?;
  let default_families = effective_default_families(cfg);
  if matches!(kind, RuntimeKind::Api) {
    validate_api_default_provider_modes(cfg, default_mode)?;
  }
  let route = Arc::new(RouteResolver::with_default_provider(
    default_mode,
    cfg.defaults.default_provider_id.clone(),
    &default_families,
  ));
  let http_options = cfg.proxy.to_http_options();
  let http = tokn_core::util::http::build_client(&http_options)?;
  let default_policy = build_policy_runtime(
    "default",
    PolicySpec {
      mode: default_mode,
      agent_id: cfg.defaults.agent_id.clone(),
      default_provider_id: cfg.defaults.default_provider_id.clone(),
      providers: cfg.defaults.providers.clone(),
      accounts: cfg.defaults.accounts.clone(),
      families: default_families,
    },
    PolicyBuildDeps {
      cfg: cfg.clone(),
      inventory: inventory.clone(),
      http: http.clone(),
      events: events.clone(),
    },
  );
  let mut profiles = BTreeMap::new();
  for (name, profile) in &cfg.profiles {
    let runtime = build_profile_runtime(
      name,
      profile,
      default_policy.as_ref(),
      PolicyBuildDeps {
        cfg: cfg.clone(),
        inventory: inventory.clone(),
        http: http.clone(),
        events: events.clone(),
      },
    );
    profiles.insert(name.clone(), runtime);
  }
  Ok(ConfiguredRuntime {
    inventory,
    pool,
    provider_registry,
    route,
    default_policy,
    profiles: Arc::new(profiles),
    http,
    events,
  })
}

fn effective_default_mode(cfg: &Config) -> RouteMode {
  if cfg.defaults.mode == RouteMode::Route && cfg.server.route_mode != RouteMode::Route {
    cfg.server.route_mode
  } else {
    cfg.defaults.mode
  }
}

fn effective_default_families(cfg: &Config) -> Vec<ModelFamily> {
  if cfg.defaults.model_families.is_empty() {
    cfg.model_families.clone()
  } else {
    cfg.defaults.model_families.clone()
  }
}

fn build_profile_runtime(
  name: &str,
  profile: &ProfileConfig,
  default_policy: &RequestPolicyRuntime,
  deps: PolicyBuildDeps,
) -> Arc<RequestPolicyRuntime> {
  let mode = profile.mode.unwrap_or(default_policy.mode);
  let agent_id = profile.agent_id.clone().or_else(|| default_policy.agent_id.clone());
  let default_provider_id = profile
    .default_provider_id
    .clone()
    .or_else(|| default_policy.default_provider_id.clone());
  let providers = profile.providers.clone().or_else(|| {
    default_policy
      .ruleset
      .providers
      .as_ref()
      .map(|providers| providers.iter().cloned().collect())
  });
  let accounts = profile.accounts.clone().or_else(|| {
    default_policy
      .ruleset
      .accounts
      .as_ref()
      .map(|accounts| accounts.iter().cloned().collect())
  });
  let families = profile
    .model_families
    .clone()
    .unwrap_or_else(|| default_policy.model_families.clone());
  build_policy_runtime(
    name,
    PolicySpec {
      mode,
      agent_id,
      default_provider_id,
      providers,
      accounts,
      families,
    },
    deps,
  )
}

fn build_policy_runtime(name: &str, spec: PolicySpec, deps: PolicyBuildDeps) -> Arc<RequestPolicyRuntime> {
  let route = Arc::new(RouteResolver::with_default_provider(
    spec.mode,
    spec.default_provider_id.clone(),
    &spec.families,
  ));
  let ruleset = AccountPoolRuleset::from_filters(spec.providers, spec.accounts);
  let pool = AccountPool::from_inventory(&deps.inventory, &deps.cfg, &ruleset)
    .expect("building account pool from validated inventory must not fail");
  let request_service = build_request_service(
    format!("router-{name}"),
    pool.clone(),
    route.clone(),
    deps.http.clone(),
    deps.events.clone(),
  );
  let passthrough_service = build_passthrough_service(
    format!("router-{name}-passthrough"),
    pool.clone(),
    route.clone(),
    deps.http.clone(),
    deps.events.clone(),
    PassthroughAuthMode::PreserveClient,
  );
  let switch_service = build_passthrough_service(
    format!("router-{name}-switch"),
    pool.clone(),
    route.clone(),
    deps.http,
    deps.events,
    PassthroughAuthMode::Router,
  );
  Arc::new(RequestPolicyRuntime {
    mode: spec.mode,
    agent_id: spec.agent_id,
    default_provider_id: spec.default_provider_id,
    ruleset,
    pool: pool.clone(),
    route: route.clone(),
    model_families: spec.families,
    request_runtime: RequestRuntime::new(request_service),
    passthrough_runtime: RequestRuntime::new(passthrough_service),
    switch_runtime: RequestRuntime::new(switch_service),
  })
}

fn validate_api_default_provider_modes(cfg: &Config, default_mode: RouteMode) -> Result<()> {
  let mut missing = Vec::new();
  if default_mode.is_verbatim() && cfg.defaults.default_provider_id.is_none() {
    missing.push("defaults.default_provider_id".to_string());
  }
  for (profile_name, profile) in &cfg.profiles {
    let mode = profile.mode.unwrap_or(default_mode);
    let default_provider_id = profile
      .default_provider_id
      .as_ref()
      .or(cfg.defaults.default_provider_id.as_ref());
    if mode.is_verbatim() && default_provider_id.is_none() {
      missing.push(format!("profiles.{profile_name}.default_provider_id"));
    }
  }
  if missing.is_empty() {
    return Ok(());
  }
  anyhow::bail!(
    "API passthrough/switch policies require default_provider_id: {}",
    missing.join(", ")
  )
}

fn validate_proxy_provider_modes(cfg: &Config, provider_registry: &ProviderRegistry) -> Result<()> {
  let mut unresolved = Vec::new();
  for provider_id in cfg.proxy_mode.provider_modes.keys() {
    if provider_registry.resolve(provider_id).is_none() {
      tracing::warn!(
        provider_id = %provider_id,
        "ignoring unresolved [proxy_mode].provider_modes entry"
      );
      unresolved.push(provider_id.clone());
    }
  }
  if unresolved.is_empty() {
    return Ok(());
  }
  anyhow::bail!(
    "[proxy_mode].provider_modes contains unresolved provider ids: {}",
    unresolved.join(", ")
  );
}

fn validate_policy_providers(cfg: &Config, provider_registry: &ProviderRegistry) -> Result<()> {
  let mut unresolved = Vec::new();
  if let Some(provider_id) = cfg.defaults.default_provider_id.as_deref() {
    if provider_registry.resolve(provider_id).is_none() {
      unresolved.push(format!("defaults.default_provider_id:{provider_id}"));
    }
  }
  for provider_id in cfg
    .defaults
    .providers
    .iter()
    .flat_map(|providers| providers.iter().map(String::as_str))
  {
    if provider_registry.resolve(provider_id).is_none() {
      unresolved.push(format!("defaults.providers:{provider_id}"));
    }
  }
  for (profile_name, profile) in &cfg.profiles {
    if let Some(provider_id) = profile.default_provider_id.as_deref() {
      if provider_registry.resolve(provider_id).is_none() {
        unresolved.push(format!("profiles.{profile_name}.default_provider_id:{provider_id}"));
      }
    }
    for provider_id in profile
      .providers
      .iter()
      .flat_map(|providers| providers.iter().map(String::as_str))
    {
      if provider_registry.resolve(provider_id).is_none() {
        unresolved.push(format!("profiles.{profile_name}.providers:{provider_id}"));
      }
    }
  }
  if unresolved.is_empty() {
    return Ok(());
  }
  anyhow::bail!(
    "profile/default provider filters contain unknown provider ids: {}",
    unresolved.join(", ")
  )
}

fn validate_policy_accounts(cfg: &Config, accounts: &[AccountConfig]) -> Result<()> {
  let known = accounts
    .iter()
    .map(|account| account.id.as_str())
    .collect::<BTreeSet<_>>();
  let mut unresolved = Vec::new();
  for account_id in cfg
    .defaults
    .accounts
    .iter()
    .flat_map(|accounts| accounts.iter().map(String::as_str))
  {
    if !known.contains(account_id) {
      unresolved.push(format!("defaults.accounts:{account_id}"));
    }
  }
  for (profile_name, profile) in &cfg.profiles {
    for account_id in profile
      .accounts
      .iter()
      .flat_map(|accounts| accounts.iter().map(String::as_str))
    {
      if !known.contains(account_id) {
        unresolved.push(format!("profiles.{profile_name}.accounts:{account_id}"));
      }
    }
  }
  if unresolved.is_empty() {
    return Ok(());
  }
  anyhow::bail!(
    "profile/default account filters contain unknown account ids: {}",
    unresolved.join(", ")
  )
}

/// Construct the default `tokn-requests` pipeline for router-owned JSON
/// endpoints. The pipeline shares `AppState.events` so persistence
/// (`RequestEventHandler`) receives `StageEvent::*` and `RecordEvent::*`
/// automatically.
fn build_request_service(
  name: impl Into<smol_str::SmolStr>,
  pool: Arc<AccountPool>,
  route: Arc<RouteResolver>,
  http: reqwest::Client,
  events: Arc<EventBus>,
) -> crate::RequestService {
  use crate::stages::{
    DefaultBuildHeaders, DefaultConvertRequest, DefaultConvertResponse, DefaultExtract, DefaultSend,
    PoolAccountSelector, PoolResolve,
  };
  let selector = Arc::new(PoolAccountSelector::new(pool, route));
  let profile = crate::Profile::full(
    name,
    Arc::new(DefaultExtract),
    Arc::new(PoolResolve::new(selector)),
    Arc::new(DefaultBuildHeaders::with_provider_defaults()),
    Arc::new(DefaultConvertRequest),
    Arc::new(DefaultSend::new(http)),
    Arc::new(DefaultConvertResponse::new()),
  );
  crate::RequestService::from_pipeline(Arc::new(crate::Pipeline::new_with_retry(
    Arc::new(profile),
    events,
    PIPELINE_RETRY_POLICY,
  )))
}

/// Construct the passthrough `tokn-requests` pipeline. Forwards the
/// inbound request body bytes verbatim with no JSON parsing or
/// cross-endpoint translation. Auth is still injected by the provider
/// during Send (via the upstream account handle), and observability
/// events still flow through `events` so persistence works.
///
/// Mirrors the behaviour of the legacy `crates/router/src/relay/passthrough.rs`
/// helpers but reuses the standard pipeline plumbing.
fn build_passthrough_service(
  name: impl Into<smol_str::SmolStr>,
  pool: Arc<AccountPool>,
  route: Arc<RouteResolver>,
  http: reqwest::Client,
  events: Arc<EventBus>,
  auth_mode: PassthroughAuthMode,
) -> crate::RequestService {
  use crate::stages::{
    DefaultSend, PassthroughBuildHeaders, PassthroughConvertRequest, PassthroughConvertResponse, PassthroughExtract,
    PoolAccountSelector, PoolResolve,
  };
  let selector = Arc::new(PoolAccountSelector::new(pool, route));
  let profile = crate::Profile::full(
    name,
    Arc::new(PassthroughExtract),
    Arc::new(PoolResolve::new(selector)),
    Arc::new(match auth_mode {
      PassthroughAuthMode::PreserveClient => PassthroughBuildHeaders::new(),
      PassthroughAuthMode::Router => PassthroughBuildHeaders::router_auth(),
    }),
    Arc::new(PassthroughConvertRequest),
    Arc::new(DefaultSend::new(http)),
    Arc::new(PassthroughConvertResponse::new()),
  );
  crate::RequestService::from_pipeline(Arc::new(crate::Pipeline::new_with_retry(
    Arc::new(profile),
    events,
    PIPELINE_RETRY_POLICY,
  )))
}

#[derive(Clone, Copy)]
enum PassthroughAuthMode {
  PreserveClient,
  Router,
}

/// Construct the proxy-passthrough `tokn-requests` pipeline used by the
/// MITM proxy when the resolved route mode is
/// [`RouteMode::Passthrough`]. Unlike [`build_passthrough_service`],
/// this variant performs **no account resolution** — the intercepted
/// TLS host is the upstream, the client's `Authorization` reaches it
/// untouched, and there is no provider-side auth injection.
///
/// The proxy transport layer supplies per-request hints
/// (`proxy.host`, `proxy.path`, `proxy.method`, …) through a
/// [`crate::RunConfig`] carried by an execution request.
/// [`crate::stages::ProxyResolve`] and [`crate::stages::ProxySend`] read those keys; the remaining
/// stages are the same as the standard passthrough variant.
pub fn build_proxy_passthrough_service(http: reqwest::Client, events: Arc<EventBus>) -> tokn_service::HttpService {
  use crate::stages::{
    PassthroughBuildHeaders, PassthroughConvertRequest, PassthroughConvertResponse, PassthroughExtract, ProxyResolve,
    ProxySend,
  };
  let profile = crate::Profile::full(
    "router-proxy-passthrough",
    Arc::new(PassthroughExtract),
    Arc::new(ProxyResolve),
    Arc::new(PassthroughBuildHeaders::preserve_host()),
    Arc::new(PassthroughConvertRequest),
    Arc::new(ProxySend::new(http)),
    Arc::new(PassthroughConvertResponse::new()),
  );
  crate::RequestService::http_from_pipeline(Arc::new(crate::Pipeline::new_with_retry(
    Arc::new(profile),
    events,
    PIPELINE_RETRY_POLICY,
  )))
}

pub fn build_proxy_switch_service(
  pool: Arc<AccountPool>,
  http: reqwest::Client,
  events: Arc<EventBus>,
) -> tokn_service::HttpService {
  use crate::stages::{
    PassthroughBuildHeaders, PassthroughConvertRequest, PassthroughConvertResponse, PassthroughExtract,
    ProxyProviderResolve, ProxySend,
  };
  let profile = crate::Profile::full(
    "router-proxy-switch",
    Arc::new(PassthroughExtract),
    Arc::new(ProxyProviderResolve::new(pool)),
    Arc::new(PassthroughBuildHeaders::preserve_host_with_router_auth()),
    Arc::new(PassthroughConvertRequest),
    Arc::new(ProxySend::new(http)),
    Arc::new(PassthroughConvertResponse::new()),
  );
  crate::RequestService::http_from_pipeline(Arc::new(crate::Pipeline::new(Arc::new(profile), events)))
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  fn validate_proxy_provider_modes_returns_error_for_unknown_provider() {
    let mut cfg = Config::default();
    cfg
      .proxy_mode
      .provider_modes
      .insert("made-up-provider".into(), tokn_config::ProxyProviderMode::Switch);
    let registry = ProviderRegistry::builtin();

    let res = validate_proxy_provider_modes(&cfg, &registry);
    let err = res.expect_err("helper should still return an error for outside callers");
    assert!(err.to_string().contains("unresolved provider ids"));
    assert!(err.to_string().contains("made-up-provider"));
  }
}
