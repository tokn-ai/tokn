//! Standard execution of compiled v2 profiles, independent of gateway listeners.

mod adapters;
pub mod selection;

use anyhow::Context;
use http::Method;
use std::{collections::BTreeMap, sync::Arc};
use tokn_accounts::{
  link::{
    build_account_pool_runtimes, link_execution_account_pools, link_execution_provider_graph, ProviderDestination,
    ProviderGraph,
  },
  registry::Registry,
};
use tokn_core::{account::AccountConfig, event::EventBus, util::http::HttpClientOptions, AgentId, Endpoint};
use tokn_policy::{
  CredentialPolicy, ExecutionRules, ProfileId, RelayCredentials, RelayDestination, RouteKind, RoutePlan, WireIdentity,
};

use crate::{session::ExecutionState, RequestService, RetryPolicy, RunConfig, RunConfigBuilder};

/// Whether profile assembly includes the upstream send and response stages.
#[derive(Clone, Copy)]
pub enum BuildMode {
  Full,
  DryRun,
}

/// The HTTP clients used by standard managed and opaque execution.
pub struct HttpClients {
  pub managed: reqwest::Client,
  pub opaque: reqwest::Client,
}

impl HttpClients {
  pub fn build(options: &HttpClientOptions) -> anyhow::Result<Self> {
    Ok(Self {
      managed: tokn_core::util::http::build_managed_client(options)?,
      opaque: tokn_core::util::http::build_opaque_client(options)?,
    })
  }
}

/// One configured standard runtime. Profiles share provider instances and pools,
/// while each request owns an independent pipeline session.
pub struct ConfiguredRuntime {
  profiles: BTreeMap<ProfileId, ApiProfileRuntime>,
}

impl ConfiguredRuntime {
  pub fn build(
    plan: &ExecutionRules,
    accounts: &[AccountConfig],
    outbound: &HttpClientOptions,
    events: Arc<EventBus>,
  ) -> anyhow::Result<Self> {
    let providers = link_execution_provider_graph(plan, accounts, &Registry::builtin())?;
    let linked_pools = link_execution_account_pools(plan, &providers)?;
    let pools = build_account_pool_runtimes(&linked_pools);
    let http = HttpClients::build(outbound)?;
    let execution = ExecutionState::new(plan.clone(), accounts.to_vec())?;
    execution.use_standard_pools(&pools);
    let execution = Arc::new(execution);
    let mut profiles = BTreeMap::new();
    for id in plan.profiles().keys() {
      if let Some(profile) = build_api_profile(
        plan,
        id,
        &providers,
        &http,
        execution.clone(),
        events.clone(),
        BuildMode::Full,
      )? {
        profiles.insert(id.clone(), profile);
      }
    }
    Ok(Self { profiles })
  }

  pub fn profile(&self, id: &ProfileId) -> Option<&ApiProfileRuntime> {
    self.profiles.get(id)
  }
}

/// A listener-independent API profile and the context needed to execute it.
#[derive(Clone)]
pub struct ApiProfileRuntime {
  service: RequestService,
  http: tokn_service::HttpService,
  kind: RouteKind,
  credentials: CredentialPolicy,
  agent_id: Option<AgentId>,
  destination: Option<ProviderDestination>,
  operations: Option<Vec<String>>,
}

impl ApiProfileRuntime {
  pub fn request_service(&self) -> &RequestService {
    &self.service
  }

  pub fn http_service(&self) -> &tokn_service::HttpService {
    &self.http
  }

  pub fn route_kind(&self) -> RouteKind {
    self.kind
  }

  pub fn credential_policy(&self) -> CredentialPolicy {
    self.credentials
  }

  pub fn agent_id(&self) -> Option<&AgentId> {
    self.agent_id.as_ref()
  }

  /// Construct the semantic execution context used by in-process SDK calls.
  pub fn run_config(&self, endpoint: Endpoint) -> anyhow::Result<RunConfigBuilder> {
    self.run_config_for_method(endpoint, &Method::POST)
  }

  /// Construct execution context after gateway admission, retaining its method.
  pub fn run_config_for_method(&self, endpoint: Endpoint, method: &Method) -> anyhow::Result<RunConfigBuilder> {
    if self
      .operations
      .as_ref()
      .is_some_and(|operations| !operations.iter().any(|operation| operation == endpoint.as_str()))
    {
      anyhow::bail!("profile does not expose operation '{}'", endpoint.as_str());
    }
    let mut config = RunConfig::builder().with_agent_id_opt(self.agent_id.clone());
    if let Some(destination) = &self.destination {
      let url = destination
        .operation_url(endpoint)
        .with_context(|| format!("resolve operation URL for provider '{}'", destination.provider_id()))?;
      let mut path = url.path().to_string();
      if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
      }
      use crate::stages::{resolve::proxy::keys, send::proxy::send_keys};
      config = config
        .with_str(keys::HOST, url.authority())
        .with_str(keys::PROVIDER_ID, destination.provider_id().to_string())
        .with_str(keys::PROVIDER_DRIVER_ID, destination.driver_id().to_string())
        .with_str(keys::PATH, path.clone())
        .with_str(send_keys::PATH, path)
        .with_str(send_keys::METHOD, method.as_str())
        .with_str(send_keys::SCHEME, url.scheme());
    }
    Ok(config)
  }
}

/// Assemble the same generic API execution for SDK and gateway callers. Original-origin
/// relay profiles require gateway proxy context and therefore return `None`.
#[allow(clippy::too_many_arguments)]
pub fn build_api_profile(
  plan: &ExecutionRules,
  id: &ProfileId,
  providers: &ProviderGraph,
  http: &HttpClients,
  execution: Arc<ExecutionState>,
  events: Arc<EventBus>,
  mode: BuildMode,
) -> anyhow::Result<Option<ApiProfileRuntime>> {
  let profile = plan
    .profile(id)
    .ok_or_else(|| anyhow::anyhow!("missing profile '{id}'"))?;
  let route = plan
    .route(profile.route())
    .ok_or_else(|| anyhow::anyhow!("profile '{id}' references missing route '{}'", profile.route()))?;
  if route.header_patches().is_some() {
    anyhow::bail!("profile '{id}' uses unsupported header patches");
  }
  if matches!(route, RoutePlan::Relay(relay) if matches!(relay.destination(), RelayDestination::Original)) {
    return Ok(None);
  }
  RetryPolicy::from_route(plan, route)?;
  let destination = match route {
    RoutePlan::Relay(relay) if *relay.credentials() == RelayCredentials::Client => {
      let RelayDestination::FixedProvider(provider) = relay.destination() else {
        unreachable!("original-origin relay returned above")
      };
      Some(
        providers
          .destination(provider)
          .cloned()
          .ok_or_else(|| anyhow::anyhow!("profile '{id}' references missing provider '{provider}'"))?,
      )
    }
    _ => None,
  };
  let service = adapters::request_service(execution, id.clone(), providers, http, events, mode);
  let http = service.http_service();
  Ok(Some(ApiProfileRuntime {
    service,
    http,
    kind: route.kind(),
    credentials: route.credential_policy(),
    agent_id: wire_agent(profile.wire_identity()),
    destination,
    operations: profile
      .api_binding()
      .map(|binding| binding.endpoints().iter().map(ToString::to_string).collect()),
  }))
}

pub fn wire_agent(identity: &WireIdentity) -> Option<AgentId> {
  match identity {
    WireIdentity::None | WireIdentity::ProviderDefault => None,
    WireIdentity::Named(identity) => Some(AgentId::from(identity.as_str())),
  }
}
