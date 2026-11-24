use crate::{AccountPoolId, HeaderPatchSetId, OperationId, ProviderId, RetryPolicyId, RouteId, WireIdentityId};
use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::string::String;
#[cfg(test)]
use alloc::string::ToString;
use alloc::vec::Vec;
use core::time::Duration;
use tokn_core::types::SmolStr;

/// The request-handling families supported by the gateway.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteKind {
  /// Decode a supported LLM request, select a managed account, and allow
  /// request/response translation.
  Managed,
  /// Preserve payload bytes while choosing destination and credential
  /// behavior independently.
  Relay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadTransform {
  Structured,
  Opaque,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialPolicy {
  Account,
  Client,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestinationPolicy {
  SelectedProvider,
  Original,
}

/// Whether a managed route preserves the inbound operation or may translate
/// between compatible API operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPolicy {
  Preserve,
  TranslateCompatible,
}

/// Base header behavior derived from the route family and destination.
/// Optional patch sets may add safe, non-structural changes after this step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeaderStrategy {
  ProviderOwned,
  CrossOriginReplaceCredentials,
  CrossOriginForward,
  SameOriginReplaceCredentials,
  SameOriginForward,
}

/// Namespace accepted before the slash in a qualified model name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualificationNamespace {
  /// Match the reusable driver implementation.
  Driver,
  /// Match one named provider destination exactly.
  Provider,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelFamily {
  name: SmolStr,
  members: Box<[SmolStr]>,
}

impl ModelFamily {
  pub fn new<I, S>(name: impl AsRef<str>, members: I) -> Self
  where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
  {
    Self {
      name: SmolStr::new(name.as_ref()),
      members: members
        .into_iter()
        .map(|member| SmolStr::new(member.as_ref()))
        .collect::<Vec<_>>()
        .into_boxed_slice(),
    }
  }

  pub fn name(&self) -> &str {
    self.name.as_str()
  }

  /// Concrete upstream models in fallback order.
  pub fn members(&self) -> &[SmolStr] {
    &self.members
  }
}

/// How a managed route interprets the requested model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelSelector {
  /// Use discovered model membership to select among providers. With a fixed
  /// provider, forward the concrete model ID even when discovery omits it.
  Capability,
  /// Parse a qualified model and constrain selection to its namespace.
  /// The concrete model ID may be absent from discovery.
  Qualified { namespace: QualificationNamespace },
  /// Expand a named family into discovered upstream models in fallback order.
  /// Requests for names not present in this route remain concrete model requests,
  /// including unlisted IDs when the provider is fixed.
  Family(Box<[ModelFamily]>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSelector {
  /// Select from named providers represented by the profile's account pool.
  Any,
  Fixed(ProviderId),
}

/// Managed selection keeps provider and model policy together so a fixed provider does
/// not accidentally discard model-selection behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedTarget {
  provider: ProviderSelector,
  model: ModelSelector,
}

impl ManagedTarget {
  pub fn new(provider: ProviderSelector, model: ModelSelector) -> Self {
    Self { provider, model }
  }

  pub fn provider(&self) -> &ProviderSelector {
    &self.provider
  }

  pub fn model(&self) -> &ModelSelector {
    &self.model
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayDestination {
  /// Preserve the inbound destination.
  Original,
  /// Send to a configured provider instead of the inbound destination.
  FixedProvider(ProviderId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayCredentials {
  /// Preserve credentials supplied by the client.
  Client,
  /// Replace client credentials with an account from the selected profile.
  AccountPool,
}

/// Bounded exponential-backoff policy referenced by one or more routes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicyPlan {
  max_retries: u32,
  initial_backoff: Duration,
}

impl RetryPolicyPlan {
  pub fn new(max_retries: u32, initial_backoff: Duration) -> Self {
    Self {
      max_retries,
      initial_backoff,
    }
  }

  pub fn max_retries(self) -> u32 {
    self.max_retries
  }

  pub fn initial_backoff(self) -> Duration {
    self.initial_backoff
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedRetry {
  Never,
  Recoverable(RetryPolicyId),
}

/// Retrying an opaque request requires an explicit replay-safety choice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayRetry {
  Never,
  SafeMethods(RetryPolicyId),
  Buffered(RetryPolicyId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireIdentity {
  None,
  ProviderDefault,
  Named(WireIdentityId),
}

/// A managed route always uses structured request handling and account-owned
/// credentials. The selected profile supplies its independently owned pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRoute {
  target: ManagedTarget,
  operation: OperationPolicy,
  header_patches: Option<HeaderPatchSetId>,
  retry: ManagedRetry,
  providers: Option<BTreeSet<ProviderId>>,
  model_scores: BTreeMap<String, BTreeMap<ProviderId, i32>>,
  model_score_prefixes: Vec<(SmolStr, BTreeMap<ProviderId, i32>)>,
}

impl ManagedRoute {
  pub fn new(
    target: ManagedTarget,
    operation: OperationPolicy,
    header_patches: Option<HeaderPatchSetId>,
    retry: ManagedRetry,
  ) -> Self {
    Self {
      target,
      operation,
      header_patches,
      retry,
      providers: None,
      model_scores: BTreeMap::new(),
      model_score_prefixes: Vec::new(),
    }
  }

  pub fn target(&self) -> &ManagedTarget {
    &self.target
  }

  pub fn operation(&self) -> OperationPolicy {
    self.operation
  }

  pub fn header_patches(&self) -> Option<&HeaderPatchSetId> {
    self.header_patches.as_ref()
  }

  pub fn retry(&self) -> &ManagedRetry {
    &self.retry
  }

  pub fn with_model_scores(mut self, scores: BTreeMap<String, BTreeMap<ProviderId, i32>>) -> Self {
    self.model_score_prefixes = scores
      .iter()
      .filter_map(|(pattern, provider_scores)| {
        pattern
          .strip_suffix('*')
          .map(|prefix| (SmolStr::new(prefix), provider_scores.clone()))
      })
      .collect();
    self
      .model_score_prefixes
      .sort_by(|(left, _), (right, _)| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    self.model_scores = scores;
    self
  }

  /// Return the configured score for one provider/model pairing.
  ///
  /// Exact model rules take precedence. Otherwise, the longest trailing-`*`
  /// prefix rule that configures this provider wins. Unconfigured pairings
  /// remain eligible at the neutral score of zero.
  pub fn provider_score(&self, model: &str, provider: &ProviderId) -> i32 {
    if let Some(score) = self
      .model_scores
      .get(model)
      .filter(|_| !model.ends_with('*'))
      .and_then(|scores| scores.get(provider))
    {
      return *score;
    }

    self
      .model_score_prefixes
      .iter()
      .find_map(|(prefix, scores)| {
        if model.starts_with(prefix.as_str()) {
          scores.get(provider).copied()
        } else {
          None
        }
      })
      .unwrap_or_default()
  }

  pub fn model_scores(&self) -> &BTreeMap<String, BTreeMap<ProviderId, i32>> {
    &self.model_scores
  }
}

/// A relay route keeps the payload opaque while choosing its destination and
/// credential source independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayRoute {
  destination: RelayDestination,
  credentials: RelayCredentials,
  header_patches: Option<HeaderPatchSetId>,
  retry: RelayRetry,
  providers: Option<BTreeSet<ProviderId>>,
}

impl RelayRoute {
  pub fn new(
    destination: RelayDestination,
    credentials: RelayCredentials,
    header_patches: Option<HeaderPatchSetId>,
    retry: RelayRetry,
  ) -> Self {
    Self {
      destination,
      credentials,
      header_patches,
      retry,
      providers: None,
    }
  }

  pub fn destination(&self) -> &RelayDestination {
    &self.destination
  }

  pub fn credentials(&self) -> &RelayCredentials {
    &self.credentials
  }

  pub fn header_patches(&self) -> Option<&HeaderPatchSetId> {
    self.header_patches.as_ref()
  }

  pub fn retry(&self) -> &RelayRetry {
    &self.retry
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutePlan {
  Managed(ManagedRoute),
  Relay(RelayRoute),
}

impl RoutePlan {
  pub fn with_providers(mut self, providers: Option<BTreeSet<ProviderId>>) -> Self {
    match &mut self {
      Self::Managed(route) => route.providers = providers,
      Self::Relay(route) => route.providers = providers,
    }
    self
  }

  pub fn providers(&self) -> Option<&BTreeSet<ProviderId>> {
    match self {
      Self::Managed(route) => route.providers.as_ref(),
      Self::Relay(route) => route.providers.as_ref(),
    }
  }

  pub fn allows_provider(&self, provider: &ProviderId) -> bool {
    self.providers().is_none_or(|allowed| allowed.contains(provider))
  }

  pub fn kind(&self) -> RouteKind {
    match self {
      Self::Managed(_) => RouteKind::Managed,
      Self::Relay(_) => RouteKind::Relay,
    }
  }

  pub fn request_transform(&self) -> PayloadTransform {
    match self {
      Self::Managed(_) => PayloadTransform::Structured,
      Self::Relay(_) => PayloadTransform::Opaque,
    }
  }

  pub fn response_transform(&self) -> PayloadTransform {
    self.request_transform()
  }

  pub fn credential_policy(&self) -> CredentialPolicy {
    match self {
      Self::Managed(_) => CredentialPolicy::Account,
      Self::Relay(route) => match route.credentials() {
        RelayCredentials::Client => CredentialPolicy::Client,
        RelayCredentials::AccountPool => CredentialPolicy::Account,
      },
    }
  }

  pub fn destination_policy(&self) -> DestinationPolicy {
    match self {
      Self::Managed(_) => DestinationPolicy::SelectedProvider,
      Self::Relay(route) => match route.destination() {
        RelayDestination::FixedProvider(_) => DestinationPolicy::SelectedProvider,
        RelayDestination::Original => DestinationPolicy::Original,
      },
    }
  }

  pub fn operation_policy(&self) -> OperationPolicy {
    match self {
      Self::Managed(route) => route.operation(),
      Self::Relay(_) => OperationPolicy::Preserve,
    }
  }

  pub fn header_strategy(&self) -> HeaderStrategy {
    match self {
      Self::Managed(_) => HeaderStrategy::ProviderOwned,
      Self::Relay(route) => match (route.destination(), route.credentials()) {
        (RelayDestination::FixedProvider(_), RelayCredentials::AccountPool) => {
          HeaderStrategy::CrossOriginReplaceCredentials
        }
        (RelayDestination::FixedProvider(_), RelayCredentials::Client) => HeaderStrategy::CrossOriginForward,
        (RelayDestination::Original, RelayCredentials::AccountPool) => HeaderStrategy::SameOriginReplaceCredentials,
        (RelayDestination::Original, RelayCredentials::Client) => HeaderStrategy::SameOriginForward,
      },
    }
  }

  pub fn header_patches(&self) -> Option<&HeaderPatchSetId> {
    match self {
      Self::Managed(route) => route.header_patches(),
      Self::Relay(route) => route.header_patches(),
    }
  }
}

/// A named execution context. Routes are reusable policies; account selection
/// state and API exposure belong to profiles.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfilePlan {
  route: RouteId,
  wire_identity: WireIdentity,
  account_pool: Option<AccountPoolId>,
  api_binding: Option<ApiBindingPlan>,
}

impl ProfilePlan {
  pub fn new(route: RouteId, wire_identity: WireIdentity) -> Self {
    Self {
      route,
      wire_identity,
      account_pool: None,
      api_binding: None,
    }
  }

  pub fn with_account_pool(mut self, pool: AccountPoolId) -> Self {
    self.account_pool = Some(pool);
    self
  }

  pub fn account_pool(&self) -> Option<&AccountPoolId> {
    self.account_pool.as_ref()
  }

  pub fn with_api_binding(mut self, binding: ApiBindingPlan) -> Self {
    self.api_binding = Some(binding);
    self
  }

  pub fn api_binding(&self) -> Option<&ApiBindingPlan> {
    self.api_binding.as_ref()
  }

  pub fn route(&self) -> &RouteId {
    &self.route
  }

  pub fn wire_identity(&self) -> &WireIdentity {
    &self.wire_identity
  }
}

/// A canonical API base path and incoming generation-operation allowlist.
/// All API listeners expose this mount. Discovery is not filtered by the list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiBindingPlan {
  path: SmolStr,
  endpoints: BTreeSet<OperationId>,
}

impl ApiBindingPlan {
  pub fn new(path: impl Into<SmolStr>, endpoints: BTreeSet<OperationId>) -> Self {
    Self {
      path: path.into(),
      endpoints,
    }
  }

  pub fn path(&self) -> &str {
    &self.path
  }

  pub fn endpoints(&self) -> &BTreeSet<OperationId> {
    &self.endpoints
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn id<T>(value: &str) -> T
  where
    T: TryFrom<String>,
    T::Error: core::fmt::Debug,
  {
    T::try_from(value.to_string()).unwrap()
  }

  #[test]
  fn managed_route_exposes_structured_account_owned_axes() {
    let route = RoutePlan::Managed(ManagedRoute::new(
      ManagedTarget::new(ProviderSelector::Any, ModelSelector::Capability),
      OperationPolicy::TranslateCompatible,
      None,
      ManagedRetry::Recoverable(id("standard")),
    ));

    assert_eq!(route.kind(), RouteKind::Managed);
    assert_eq!(route.request_transform(), PayloadTransform::Structured);
    assert_eq!(route.response_transform(), PayloadTransform::Structured);
    assert_eq!(route.credential_policy(), CredentialPolicy::Account);
    assert_eq!(route.destination_policy(), DestinationPolicy::SelectedProvider);
    assert_eq!(route.operation_policy(), OperationPolicy::TranslateCompatible);
    assert_eq!(route.header_strategy(), HeaderStrategy::ProviderOwned);
  }

  #[test]
  fn model_scores_use_the_most_specific_rule_per_provider() {
    let openai: ProviderId = id("openai");
    let codex: ProviderId = id("codex");
    let opencode_go: ProviderId = id("opencode-go");
    let route = ManagedRoute::new(
      ManagedTarget::new(ProviderSelector::Any, ModelSelector::Capability),
      OperationPolicy::TranslateCompatible,
      None,
      ManagedRetry::Never,
    )
    .with_model_scores(BTreeMap::from([
      ("*".into(), BTreeMap::from([(openai.clone(), 5)])),
      (
        "gpt-*".into(),
        BTreeMap::from([(openai.clone(), 10), (codex.clone(), 20)]),
      ),
      ("gpt-5.6-*".into(), BTreeMap::from([(opencode_go.clone(), 50)])),
      ("gpt-5.6-luna".into(), BTreeMap::from([(codex.clone(), 100)])),
    ]));

    assert_eq!(route.provider_score("gpt-5.6-luna", &codex), 100);
    assert_eq!(route.provider_score("gpt-5.6-luna", &opencode_go), 50);
    assert_eq!(route.provider_score("gpt-5.6-luna", &openai), 10);
    assert_eq!(route.provider_score("deepseek-v3", &openai), 5);
    assert_eq!(route.provider_score("deepseek-v3", &codex), 0);
  }

  #[test]
  fn fixed_relay_exposes_cross_origin_account_owned_axes() {
    let route = RoutePlan::Relay(RelayRoute::new(
      RelayDestination::FixedProvider(id("openai-public")),
      RelayCredentials::AccountPool,
      None,
      RelayRetry::Never,
    ));

    assert_eq!(route.kind(), RouteKind::Relay);
    assert_eq!(route.request_transform(), PayloadTransform::Opaque);
    assert_eq!(route.credential_policy(), CredentialPolicy::Account);
    assert_eq!(route.destination_policy(), DestinationPolicy::SelectedProvider);
    assert_eq!(route.operation_policy(), OperationPolicy::Preserve);
    assert_eq!(route.header_strategy(), HeaderStrategy::CrossOriginReplaceCredentials);
  }

  #[test]
  fn origin_relay_preserves_destination_and_replaces_credentials() {
    let route = RoutePlan::Relay(RelayRoute::new(
      RelayDestination::Original,
      RelayCredentials::AccountPool,
      None,
      RelayRetry::SafeMethods(id("conservative")),
    ));

    assert_eq!(route.destination_policy(), DestinationPolicy::Original);
    assert_eq!(route.header_strategy(), HeaderStrategy::SameOriginReplaceCredentials);
  }

  #[test]
  fn original_client_relay_replaces_transparent_route() {
    let route = RoutePlan::Relay(RelayRoute::new(
      RelayDestination::Original,
      RelayCredentials::Client,
      None,
      RelayRetry::Never,
    ));

    assert_eq!(route.kind(), RouteKind::Relay);
    assert_eq!(route.request_transform(), PayloadTransform::Opaque);
    assert_eq!(route.credential_policy(), CredentialPolicy::Client);
    assert_eq!(route.destination_policy(), DestinationPolicy::Original);
    assert_eq!(route.operation_policy(), OperationPolicy::Preserve);
    assert_eq!(route.header_strategy(), HeaderStrategy::SameOriginForward);
  }

  #[test]
  fn fixed_client_relay_forwards_credentials_cross_origin() {
    let route = RoutePlan::Relay(RelayRoute::new(
      RelayDestination::FixedProvider(id("openai-public")),
      RelayCredentials::Client,
      None,
      RelayRetry::Never,
    ));

    assert_eq!(route.credential_policy(), CredentialPolicy::Client);
    assert_eq!(route.destination_policy(), DestinationPolicy::SelectedProvider);
    assert_eq!(route.header_strategy(), HeaderStrategy::CrossOriginForward);
  }

  #[test]
  fn model_families_keep_route_local_fallback_order() {
    let selector = ModelSelector::Family(
      vec![ModelFamily::new(
        "smart",
        vec![SmolStr::new("gpt-5"), SmolStr::new("claude-sonnet-4-6")].into_boxed_slice(),
      )]
      .into_boxed_slice(),
    );

    let ModelSelector::Family(families) = selector else {
      panic!("expected family selector");
    };
    assert_eq!(families[0].name(), "smart");
    assert_eq!(families[0].members(), ["gpt-5", "claude-sonnet-4-6"]);
  }

  #[test]
  fn profile_contains_only_route_and_wire_identity() {
    let profile = ProfilePlan::new(id("coding"), WireIdentity::Named(id("codex-cli")));
    assert_eq!(profile.route().as_str(), "coding");
    assert_eq!(profile.wire_identity(), &WireIdentity::Named(id("codex-cli")));
  }
}
