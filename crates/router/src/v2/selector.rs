use async_trait::async_trait;
use smol_str::SmolStr;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokn_accounts::link::{AccountPoolRuntimes, PoolAcquire};
use tokn_accounts::selection::provider_allowed;
use tokn_core::provider::ProviderRequestKind;
use tokn_policy::{AccountPoolId, ProviderId, RelayDestination, RelayRoute};
use tokn_requests::event::Stage;
use tokn_requests::pipeline::ctx::PipelineCtx;
use tokn_requests::pipeline::error::{PipelineError, RequestsError};
use tokn_requests::pipeline::stages::{
  BuiltHeaders, ConvertedRequest, Extracted, ResolveStage, Resolved, ResolvedRoute, SendStage, SentResponse,
};
use tokn_requests::send_policy::AccountFeedbackPolicy;
use tokn_requests::stages::{resolve::proxy::keys as proxy_keys, ProxyResolve, ProxySend};
use tokn_requests::v2::selection::{allowed_provider_ids, invalid_route_request, ClientResolve, SelectionState};

pub(super) const V2_PROXY_ORIGIN_KEY: &str = "v2.proxy.origin";

pub(super) struct V2ClientResolve {
  fixed_provider: Option<ProviderId>,
  allowed_origins: Option<BTreeMap<String, ProviderId>>,
}

impl V2ClientResolve {
  pub(super) fn new(fixed_provider: Option<ProviderId>) -> Self {
    Self {
      fixed_provider,
      allowed_origins: None,
    }
  }

  pub(super) fn with_allowed_origins(mut self, origins: Option<BTreeMap<String, ProviderId>>) -> Self {
    self.allowed_origins = origins;
    self
  }
}

#[async_trait]
impl ResolveStage for V2ClientResolve {
  async fn resolve(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<Resolved, PipelineError> {
    if let Some(origins) = &self.allowed_origins {
      let allowed = allowed_provider_ids(ctx)?;
      let provider = ctx
        .config
        .get_str(V2_PROXY_ORIGIN_KEY)
        .and_then(|origin| origins.get(origin));
      if !provider.is_some_and(|id| provider_allowed(id.as_str(), allowed.as_ref())) {
        return Err(PipelineError::permanent(
          Stage::Resolve,
          RequestsError::ProviderAccessDenied,
        ));
      }
    }
    match &self.fixed_provider {
      Some(provider) => ClientResolve::new(provider.clone()).resolve(ctx, extracted).await,
      None => ProxyResolve.resolve(ctx, extracted).await,
    }
  }
}

pub(super) struct V2ProxyResolve {
  target: ProxyRelayTarget,
  state: Arc<SelectionState>,
}

enum ProxyRelayTarget {
  Fixed(ProviderId),
  FromOrigin(BTreeMap<String, ProviderId>),
}

impl V2ProxyResolve {
  pub(super) fn new(
    route: &RelayRoute,
    pool_id: &AccountPoolId,
    pools: &AccountPoolRuntimes,
    origins: BTreeMap<String, ProviderId>,
  ) -> anyhow::Result<(Self, Arc<SelectionState>)> {
    let pool = pools
      .runtime(pool_id)
      .cloned()
      .ok_or_else(|| anyhow::anyhow!("relay route references missing account pool '{pool_id}'"))?;
    let state = Arc::new(SelectionState::new(pool));
    let target = match route.destination() {
      RelayDestination::FixedProvider(provider) => ProxyRelayTarget::Fixed(provider.clone()),
      RelayDestination::Original => ProxyRelayTarget::FromOrigin(origins),
    };
    Ok((
      Self {
        target,
        state: state.clone(),
      },
      state,
    ))
  }

  fn provider<'a>(&'a self, ctx: &PipelineCtx) -> Result<&'a ProviderId, PipelineError> {
    match &self.target {
      ProxyRelayTarget::Fixed(provider) => Ok(provider),
      ProxyRelayTarget::FromOrigin(origins) => {
        let origin = ctx
          .config
          .get_str(V2_PROXY_ORIGIN_KEY)
          .ok_or_else(|| invalid_route_request("origin relay requires an admitted proxy origin"))?;
        origins
          .get(origin)
          .ok_or_else(|| invalid_route_request(format!("no configured provider owns proxy origin '{origin}'")))
      }
    }
  }
}

#[async_trait]
impl ResolveStage for V2ProxyResolve {
  async fn resolve(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<Resolved, PipelineError> {
    let provider = self.provider(ctx)?;
    let allowed = allowed_provider_ids(ctx)?;
    if !provider_allowed(provider.as_str(), allowed.as_ref()) {
      return Err(PipelineError::permanent(
        Stage::Resolve,
        RequestsError::ProviderAccessDenied,
      ));
    }
    let binding = match self.state.pool().acquire(extracted.session_id.as_deref(), |binding| {
      binding.provider_id() == provider
    }) {
      PoolAcquire::Selected(binding) => binding,
      PoolAcquire::CoolingDown { .. } | PoolAcquire::NoEligible => {
        return Err(PipelineError::permanent(
          Stage::Resolve,
          RequestsError::NoProviderAccount {
            provider_id: SmolStr::new(provider.as_str()),
          },
        ));
      }
    };
    let request_kind = ctx
      .request_endpoint
      .resolved()
      .map(ProviderRequestKind::Operation)
      .or_else(|| {
        ctx
          .config
          .get_str(proxy_keys::PATH)
          .map(ProviderRequestKind::from_provider_path)
      })
      .unwrap_or(ProviderRequestKind::Opaque);
    let route = match ctx.request_endpoint.resolved() {
      Some(endpoint) => ResolvedRoute::operation(endpoint, endpoint),
      None => ResolvedRoute::provider_traffic(request_kind),
    };
    Ok(Resolved {
      agent_id: extracted.agent_id.clone(),
      model: extracted.model.clone(),
      upstream_model: extracted.model.clone(),
      route,
      account_id: SmolStr::new(binding.account_id()),
      provider_id: SmolStr::new(binding.driver().info().id.as_str()),
      account_handle: binding.handle().clone(),
    })
  }
}

pub(super) struct ProxyPoolAwareSend {
  inner: ProxySend,
  state: Arc<SelectionState>,
}

impl ProxyPoolAwareSend {
  pub(super) fn new(http: reqwest::Client, state: Arc<SelectionState>) -> Self {
    Self {
      inner: ProxySend::forward_all_statuses(http),
      state,
    }
  }
}

#[async_trait]
impl SendStage for ProxyPoolAwareSend {
  async fn send(
    &self,
    ctx: &PipelineCtx,
    extracted: &Extracted,
    resolved: &Resolved,
    headers: &BuiltHeaders,
    body: &ConvertedRequest,
  ) -> Result<SentResponse, PipelineError> {
    let binding = self
      .state
      .binding_for_handle(&resolved.account_handle)
      .ok_or_else(|| invalid_route_request("selected account is not a member of the relay route's v2 account pool"))?;
    let result = self.inner.send(ctx, extracted, resolved, headers, body).await;
    self.state.record_send_result(
      AccountFeedbackPolicy::ForwardProxy,
      binding,
      extracted.session_id.as_deref(),
      &result,
    );
    result
  }
}
