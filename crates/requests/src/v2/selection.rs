//! Standard adapters for v2 account selection and send feedback.

use crate::event::Stage;
use crate::pipeline::ctx::PipelineCtx;
use crate::pipeline::error::{PipelineError, RequestsError};
use crate::pipeline::stages::{
  BuiltHeaders, ConvertedRequest, Extracted, ResolveStage, Resolved, SendStage, SentResponse,
};
use crate::send_policy::{AccountFeedback, AccountFeedbackPolicy, SendOutcome};
use crate::stages::{AccountSelector, DefaultSend, ProxyResolve, SelectorOutcome, ACCESS_ALLOWED_PROVIDERS_KEY};
use async_trait::async_trait;
use smol_str::SmolStr;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokn_accounts::link::{AccountPoolRuntime, AccountPoolRuntimes, PoolAcquire, ProviderBinding};
#[cfg(test)]
use tokn_accounts::selection::{
  model_candidates, operation_candidates, qualification_name, DiscoveryEvidence, ProviderConstraint,
};
use tokn_accounts::selection::{provider_allowed, select_route, RouteSelection, SelectionRequest};
use tokn_accounts::AccountHandle;
use tokn_core::provider::Endpoint;
use tokn_policy::{ExecutionPlan, ProfileId, ProviderId, RelayDestination, RoutePlan};
#[cfg(test)]
use tokn_policy::{GatewayPlan, ManagedRoute, OperationPolicy, QualificationNamespace, RouteId};

pub struct SelectionState {
  pool: Arc<AccountPoolRuntime>,
  bindings: Box<[Arc<ProviderBinding>]>,
}

impl SelectionState {
  pub fn pool(&self) -> &AccountPoolRuntime {
    &self.pool
  }
  pub fn new(pool: Arc<AccountPoolRuntime>) -> Self {
    let bindings = pool
      .pool()
      .active()
      .iter()
      .chain(pool.pool().fallback())
      .map(|account| account.binding().clone())
      .collect::<Vec<_>>()
      .into_boxed_slice();
    Self { pool, bindings }
  }

  pub fn binding_for_handle(&self, handle: &Arc<AccountHandle>) -> Option<&Arc<ProviderBinding>> {
    self
      .bindings
      .iter()
      .find(|binding| Arc::ptr_eq(binding.handle(), handle))
  }

  pub fn record_send_result(
    &self,
    policy: AccountFeedbackPolicy,
    binding: &ProviderBinding,
    session_id: Option<&str>,
    result: &Result<SentResponse, PipelineError>,
  ) {
    let outcome = match result {
      Ok(response) => SendOutcome::Response {
        status: response.status,
      },
      Err(error) => SendOutcome::Failure {
        recoverable: error.recoverable,
      },
    };
    // Feedback belongs to the send result, before response conversion and
    // independently of whether the request's retry budget permits replay.
    match policy.feedback(outcome) {
      AccountFeedback::Success => {
        if let Err(error) = self.pool.record_success(session_id, binding.key()) {
          tracing::warn!(%error, account = %binding.account_id(), "could not record v2 account-pool success");
        }
      }
      AccountFeedback::Failure => {
        if let Err(error) = self.pool.record_failure(binding.key()) {
          tracing::warn!(%error, account = %binding.account_id(), "could not record v2 account-pool failure");
        }
      }
      AccountFeedback::None => {}
    }
  }
}

pub struct V2AccountSelector {
  route: RoutePlan,
  state: Arc<SelectionState>,
}

impl V2AccountSelector {
  pub fn new(
    plan: &ExecutionPlan,
    profile_id: &ProfileId,
    pools: &AccountPoolRuntimes,
  ) -> anyhow::Result<(Self, Arc<SelectionState>)> {
    let profile = plan
      .profile(profile_id)
      .ok_or_else(|| anyhow::anyhow!("missing profile '{profile_id}'"))?;
    let route = plan
      .route(profile.route())
      .cloned()
      .ok_or_else(|| anyhow::anyhow!("missing route for profile '{profile_id}'"))?;
    let pool_id = profile
      .account_pool()
      .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' does not use account-pool credentials"))?;
    let pool = pools
      .runtime(pool_id)
      .cloned()
      .ok_or_else(|| anyhow::anyhow!("profile '{profile_id}' references missing account pool '{pool_id}'"))?;
    let state = Arc::new(SelectionState::new(pool));
    Ok((
      Self {
        route,
        state: state.clone(),
      },
      state,
    ))
  }

  fn route(&self) -> &RoutePlan {
    &self.route
  }

  fn select_account(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<SelectorOutcome, PipelineError> {
    let endpoint = resolved_endpoint(ctx)?;
    if matches!(self.route(), RoutePlan::Relay(route) if !matches!(route.destination(), RelayDestination::FixedProvider(_)))
    {
      return Err(invalid_route_request(
        "origin-based relay cannot run on an LLM API listener",
      ));
    }
    let allowed = allowed_provider_ids(ctx)?;
    let request = SelectionRequest {
      model: extracted.model.as_str(),
      endpoint,
      allowed_providers: allowed.as_ref(),
    };
    let result = select_route(
      self.route(),
      request,
      self
        .state
        .bindings
        .iter()
        .map(|binding| (binding.key().clone(), binding.as_ref())),
      |ranks| match self.state.pool.acquire_ranks(extracted.session_id.as_deref(), ranks) {
        PoolAcquire::Selected(binding) => Some(binding),
        PoolAcquire::CoolingDown { .. } | PoolAcquire::NoEligible => None,
      },
    )
    .map_err(|error| invalid_route_request(error.to_string()))?;
    Ok(match result {
      RouteSelection::Selected {
        binding,
        model,
        endpoint,
      } => selected(binding, endpoint, model),
      RouteSelection::NoAccount => SelectorOutcome::NoAccount,
      RouteSelection::ProviderAccessDenied => SelectorOutcome::ProviderAccessDenied,
    })
  }
}

#[async_trait]
impl AccountSelector for V2AccountSelector {
  async fn select(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<SelectorOutcome, PipelineError> {
    self.select_account(ctx, extracted)
  }
}

/// Resolve a fixed destination while preserving caller-supplied credentials.
pub struct ClientResolve {
  provider: ProviderId,
}

impl ClientResolve {
  pub fn new(provider: ProviderId) -> Self {
    Self { provider }
  }
}

#[async_trait]
impl ResolveStage for ClientResolve {
  async fn resolve(&self, ctx: &PipelineCtx, extracted: &Extracted) -> Result<Resolved, PipelineError> {
    let allowed = allowed_provider_ids(ctx)?;
    if !provider_allowed(self.provider.as_str(), allowed.as_ref()) {
      return Err(PipelineError::permanent(
        Stage::Resolve,
        RequestsError::ProviderAccessDenied,
      ));
    }
    let resolved = ProxyResolve.resolve(ctx, extracted).await?;
    if resolved.provider_id.as_str() != self.provider.as_str() {
      return Err(invalid_route_request(format!(
        "fixed relay expected provider '{}', got '{}'",
        self.provider, resolved.provider_id
      )));
    }
    Ok(resolved)
  }
}

pub struct PoolAwareSend {
  inner: DefaultSend,
  state: Arc<SelectionState>,
}

impl PoolAwareSend {
  pub fn new(http: reqwest::Client, state: Arc<SelectionState>) -> Self {
    Self {
      inner: DefaultSend::new(http),
      state,
    }
  }
}

#[async_trait]
impl SendStage for PoolAwareSend {
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
      .ok_or_else(|| invalid_route_request("selected account is not a member of the route's v2 account pool"))?;
    let result = self.inner.send(ctx, extracted, resolved, headers, body).await;
    self.state.record_send_result(
      AccountFeedbackPolicy::Api,
      binding,
      extracted.session_id.as_deref(),
      &result,
    );
    result
  }
}

fn selected(binding: Arc<ProviderBinding>, operation: Endpoint, model: SmolStr) -> SelectorOutcome {
  SelectorOutcome::Selected {
    account_id: SmolStr::new(binding.account_id()),
    // The six-stage pipeline consumes the provider dialect exposed by the
    // reusable driver instance. Shared drivers may retain a more specific
    // named-provider identity such as `zhipuai` here.
    provider_id: SmolStr::new(binding.driver().info().id.as_str()),
    upstream_endpoint: Some(operation),
    upstream_model: model,
    account_handle: binding.handle().clone(),
  }
}

pub fn allowed_provider_ids(ctx: &PipelineCtx) -> Result<Option<BTreeSet<String>>, PipelineError> {
  let Some(value) = ctx.config.get(ACCESS_ALLOWED_PROVIDERS_KEY) else {
    return Ok(None);
  };
  let Some(values) = value.as_array() else {
    return Err(PipelineError::permanent(
      Stage::Resolve,
      RequestsError::InvalidAccessPolicy,
    ));
  };
  values
    .iter()
    .map(|value| value.as_str().map(str::to_string))
    .collect::<Option<BTreeSet<_>>>()
    .map(Some)
    .ok_or_else(|| PipelineError::permanent(Stage::Resolve, RequestsError::InvalidAccessPolicy))
}

fn resolved_endpoint(ctx: &PipelineCtx) -> Result<Endpoint, PipelineError> {
  ctx.request_endpoint.resolved().ok_or_else(|| {
    PipelineError::permanent(
      Stage::Resolve,
      RequestsError::MissingResolvedEndpoint {
        request_endpoint: SmolStr::new(ctx.request_endpoint.as_str()),
      },
    )
  })
}

pub fn invalid_route_request(message: impl Into<String>) -> PipelineError {
  PipelineError::permanent(
    Stage::Resolve,
    RequestsError::InvalidRouteRequest {
      message: SmolStr::new(message.into()),
    },
  )
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::path::Path;

  fn managed_plan(model: &str) -> GatewayPlan {
    managed_plan_with_pool(model, r#"["*"]"#)
  }

  fn managed_plan_with_pool(model: &str, pool_accounts: &str) -> GatewayPlan {
    let config = format!(
      r#"
schema_version = 2

[listeners.api]
kind = "llm_api"
bind = "127.0.0.1:4141"
client_auth = "none"

[profiles.default]
route = "default"

[profiles.default.account_pool]
accounts = {pool_accounts}

[routes.default]
kind = "managed"
providers = ["*"]
provider = {{ kind = "any" }}
model = {model}
operation = "translate_compatible"

[providers.local]
driver = "openai"

[providers.outside]
driver = "openai"
"#
    );
    tokn_config::v2::parse(&config, Path::new("selector-test.toml")).unwrap()
  }

  fn managed_route(plan: &GatewayPlan) -> &ManagedRoute {
    match plan.route(&RouteId::new("default").unwrap()).unwrap() {
      RoutePlan::Managed(route) => route,
      _ => panic!("expected managed route"),
    }
  }

  #[test]
  fn builds_driver_and_provider_qualified_model_candidates() {
    for (namespace, requested, expected_qualifier) in [
      ("driver", "openai/gpt-5", "openai"),
      ("provider", "local/gpt-5", "local"),
    ] {
      let plan = managed_plan(&format!(r#"{{ kind = "qualified", namespace = "{namespace}" }}"#));
      let candidates = model_candidates(managed_route(&plan), requested).unwrap();
      assert_eq!(candidates.len(), 1);
      assert_eq!(candidates[0].model, "gpt-5");
      assert_eq!(candidates[0].evidence, DiscoveryEvidence::Ignored);
      match &candidates[0].constraint {
        ProviderConstraint::Driver(id) => assert_eq!(id.as_str(), expected_qualifier),
        ProviderConstraint::Provider(id) => assert_eq!(id.as_str(), expected_qualifier),
        ProviderConstraint::Any => panic!("expected qualified constraint"),
      }
      assert!(model_candidates(managed_route(&plan), "gpt-5").is_err());
      assert!(model_candidates(managed_route(&plan), &format!("{expected_qualifier}/ ")).is_err());
    }
  }

  #[test]
  fn expands_only_named_families_and_preserves_member_order() {
    let plan = managed_plan(r#"{ kind = "family", families = { coding = ["gpt-5", "gpt-4o"] } }"#);
    let candidates = model_candidates(managed_route(&plan), "coding").unwrap();
    assert_eq!(
      candidates
        .iter()
        .map(|candidate| candidate.model.as_str())
        .collect::<Vec<_>>(),
      ["gpt-5", "gpt-4o"]
    );
    assert!(matches!(candidates[0].constraint, ProviderConstraint::Any));
    assert!(matches!(candidates[1].constraint, ProviderConstraint::Any));
    assert_eq!(candidates[0].evidence, DiscoveryEvidence::Required);
    assert_eq!(candidates[1].evidence, DiscoveryEvidence::Required);

    let concrete = model_candidates(managed_route(&plan), "gpt-4o").unwrap();
    assert_eq!(concrete.len(), 1);
    assert_eq!(concrete[0].model, "gpt-4o");

    let unknown = model_candidates(managed_route(&plan), "unknown").unwrap();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].model, "unknown");
    assert_eq!(unknown[0].evidence, DiscoveryEvidence::Preferred);
  }

  #[test]
  fn operation_and_access_candidates_preserve_policy_order() {
    assert_eq!(
      operation_candidates(OperationPolicy::Preserve, Endpoint::Responses),
      [Endpoint::Responses]
    );
    assert_eq!(
      operation_candidates(OperationPolicy::TranslateCompatible, Endpoint::Responses),
      [Endpoint::Responses, Endpoint::ChatCompletions, Endpoint::Messages]
    );

    let allowed = BTreeSet::from(["local".to_string()]);
    assert!(provider_allowed("local", Some(&allowed)));
    assert!(!provider_allowed("openai", Some(&allowed)));
    assert!(provider_allowed("anything", None));
    assert_eq!(qualification_name(QualificationNamespace::Driver), "driver");
    assert_eq!(qualification_name(QualificationNamespace::Provider), "provider");
  }

  fn selector(plan: GatewayPlan) -> (V2AccountSelector, Arc<SelectionState>) {
    use tokn_accounts::link::{build_account_pool_runtimes, link_account_pools, link_provider_graph};
    use tokn_accounts::registry::Registry;
    let accounts = [
      serde_json::from_value(serde_json::json!({ "id": "inside", "provider": "local", "api_key": "fixture" })).unwrap(),
      serde_json::from_value(serde_json::json!({ "id": "outside", "provider": "outside", "api_key": "fixture" }))
        .unwrap(),
    ];
    let providers = link_provider_graph(&plan, &accounts, &Registry::builtin()).unwrap();
    let pools = link_account_pools(&plan, &providers).unwrap();
    let runtimes = build_account_pool_runtimes(&pools);
    V2AccountSelector::new(plan.execution(), &ProfileId::new("default").unwrap(), &runtimes).unwrap()
  }

  fn context(allowed: Option<&[&str]>) -> PipelineCtx {
    use crate::{event::EventBus, pipeline::config::RunConfig};
    let config = match allowed {
      Some(allowed) => RunConfig::builder()
        .with(ACCESS_ALLOWED_PROVIDERS_KEY, serde_json::json!(allowed))
        .build(),
      None => RunConfig::default(),
    };
    PipelineCtx::new_with_config(
      "route-test",
      Endpoint::Responses.into(),
      Arc::new(EventBus::noop()),
      Arc::new(config),
    )
  }

  async fn extracted(ctx: &PipelineCtx, model: &str) -> Extracted {
    use crate::pipeline::stages::{ExtractStage, RawInbound};
    use crate::stages::DefaultExtract;
    let body_json = serde_json::json!({ "model": model });
    let body = tokn_core::types::Bytes::from(serde_json::to_vec(&body_json).unwrap());
    DefaultExtract
      .extract(
        ctx,
        RawInbound {
          request_endpoint: ctx.request_endpoint.clone(),
          headers: Default::default(),
          raw_body: body.clone(),
          decoded_body: body,
          body_json,
          request_id: None,
        },
      )
      .await
      .unwrap()
  }

  #[tokio::test]
  async fn shared_selection_keeps_driver_identity_access_errors_and_cooldown_behavior() {
    let (selector, state) = selector(managed_plan(r#"{ kind = "qualified", namespace = "provider" }"#));
    let allowed_ctx = context(Some(&["local"]));
    let extracted = extracted(&allowed_ctx, "local/unlisted-model").await;
    let selected = selector.select(&allowed_ctx, &extracted).await.unwrap();
    assert!(matches!(selected, SelectorOutcome::Selected {
      account_id,
      provider_id,
      upstream_endpoint: Some(Endpoint::Responses),
      upstream_model,
      ..
    } if account_id == "inside" && provider_id == "openai" && upstream_model == "unlisted-model"));
    let binding = state
      .bindings
      .iter()
      .find(|binding| binding.account_id() == "inside")
      .unwrap();
    state.pool.record_failure(binding.key()).unwrap();
    assert!(matches!(
      selector.select(&allowed_ctx, &extracted).await.unwrap(),
      SelectorOutcome::NoAccount
    ));
    assert!(matches!(
      selector.select(&context(Some(&[])), &extracted).await.unwrap(),
      SelectorOutcome::ProviderAccessDenied
    ));
  }

  #[tokio::test]
  async fn shared_selection_ignores_matching_bindings_in_other_pools() {
    let (selector, _) = selector(managed_plan_with_pool(
      r#"{ kind = "qualified", namespace = "provider" }"#,
      r#"["inside"]"#,
    ));
    let ctx = context(Some(&[]));
    let extracted = extracted(&ctx, "outside/unlisted-model").await;
    assert!(matches!(
      selector.select(&ctx, &extracted).await.unwrap(),
      SelectorOutcome::NoAccount
    ));
  }
}
