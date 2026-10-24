//! Platform-independent provider bindings and account-pool membership.
//!
//! A platform supplies installed driver defaults. This module owns destination
//! validation, account identity, and pool selectors; it never creates providers,
//! credentials, caches, or synchronization primitives.

use alloc::{
  collections::BTreeMap,
  string::{String, ToString},
  vec::Vec,
};
use core::{fmt, time::Duration};
use tokn_core::{
  account::{AccountConfig, AccountTier},
  provider::official_provider_preset,
  types::SmolStr,
  upstream_url::{CanonicalUpstreamUrl, CleartextHttpPolicy, InvalidUpstreamUrl},
};
use tokn_policy::{AccountPoolId, AccountSelectionStrategy, DriverId, ExecutionPlan, ProviderId, SessionAffinityPlan};

/// The source of a provider URL selected during linking.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderUrlSource {
  Configured,
  ProviderDefault,
  DriverDefault,
}

impl fmt::Display for ProviderUrlSource {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(match self {
      Self::Configured => "configured",
      Self::ProviderDefault => "provider default",
      Self::DriverDefault => "driver default",
    })
  }
}

/// Stable identity of one account binding under one configured provider.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderBindingKey {
  provider_id: ProviderId,
  account_id: SmolStr,
}

impl ProviderBindingKey {
  pub fn new(provider_id: ProviderId, account_id: impl AsRef<str>) -> Self {
    Self {
      provider_id,
      account_id: SmolStr::new(account_id.as_ref()),
    }
  }
  pub fn provider_id(&self) -> &ProviderId {
    &self.provider_id
  }
  pub fn account_id(&self) -> &str {
    self.account_id.as_str()
  }
}

/// A configured provider's validated destination, independent of its runtime.
#[derive(Clone, Debug)]
pub struct DestinationMetadata {
  driver_id: DriverId,
  base_url: CanonicalUpstreamUrl,
  cleartext_policy: CleartextHttpPolicy,
}

impl DestinationMetadata {
  pub fn driver_id(&self) -> &DriverId {
    &self.driver_id
  }
  pub fn base_url(&self) -> &CanonicalUpstreamUrl {
    &self.base_url
  }
  pub fn cleartext_policy(&self) -> CleartextHttpPolicy {
    self.cleartext_policy
  }
}

/// An enabled account's provider identity and original input position.
#[derive(Clone, Debug)]
pub struct BindingMetadata {
  key: ProviderBindingKey,
  driver_id: DriverId,
  driver_identity: SmolStr,
  account_order: usize,
}

impl BindingMetadata {
  pub fn key(&self) -> &ProviderBindingKey {
    &self.key
  }
  pub fn provider_id(&self) -> &ProviderId {
    self.key.provider_id()
  }
  pub fn driver_id(&self) -> &DriverId {
    &self.driver_id
  }
  /// Identity passed to a driver: official aliases retain their dialect;
  /// custom destinations use the selected driver identity.
  pub fn driver_identity(&self) -> &str {
    self.driver_identity.as_str()
  }
  pub fn account_order(&self) -> usize {
    self.account_order
  }
}

/// Structural metadata for every supplied account, including unbound accounts.
/// Credentials remain exclusively in the platform's account storage.
#[derive(Clone, Debug)]
pub struct AccountMetadata {
  account_id: SmolStr,
  input_order: usize,
  enabled: bool,
  tier: AccountTier,
  binding_index: Option<usize>,
}

impl AccountMetadata {
  pub fn account_id(&self) -> &str {
    self.account_id.as_str()
  }
  pub fn input_order(&self) -> usize {
    self.input_order
  }
  pub fn enabled(&self) -> bool {
    self.enabled
  }
  pub fn tier(&self) -> AccountTier {
    self.tier
  }
  pub fn binding_index(&self) -> Option<usize> {
    self.binding_index
  }
}

/// Shared immutable graph. Binding indices follow enabled account input order.
#[derive(Debug)]
pub struct ProviderMetadataGraph {
  destinations: BTreeMap<ProviderId, DestinationMetadata>,
  bindings: Vec<BindingMetadata>,
  accounts: Vec<AccountMetadata>,
  account_indices: BTreeMap<SmolStr, usize>,
}

impl ProviderMetadataGraph {
  pub fn destinations(&self) -> impl ExactSizeIterator<Item = (&ProviderId, &DestinationMetadata)> {
    self.destinations.iter()
  }
  pub fn destination(&self, id: &ProviderId) -> Option<&DestinationMetadata> {
    self.destinations.get(id)
  }
  pub fn bindings(&self) -> &[BindingMetadata] {
    &self.bindings
  }
  pub fn accounts(&self) -> &[AccountMetadata] {
    &self.accounts
  }
  pub fn account(&self, id: &str) -> Option<&AccountMetadata> {
    self.account_indices.get(id).map(|index| &self.accounts[*index])
  }
}

#[derive(Debug)]
pub enum MetadataLinkError {
  DuplicateAccountId {
    account_id: SmolStr,
    first_index: usize,
    duplicate_index: usize,
  },
  UnknownDriver {
    provider: ProviderId,
    driver: DriverId,
  },
  UnknownAccountProvider {
    account_id: SmolStr,
    provider: SmolStr,
  },
  InvalidProviderUrl {
    provider: ProviderId,
    driver: DriverId,
    url_source: ProviderUrlSource,
    base_url: String,
    source: InvalidUpstreamUrl,
  },
}

impl fmt::Display for MetadataLinkError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::DuplicateAccountId {
        account_id,
        first_index,
        duplicate_index,
      } => write!(
        f,
        "duplicate account id '{account_id}' at input positions {first_index} and {duplicate_index}"
      ),
      Self::UnknownDriver { provider, driver } => {
        write!(f, "provider '{provider}' references unknown driver '{driver}'")
      }
      Self::UnknownAccountProvider { account_id, provider } => write!(
        f,
        "account '{account_id}' references unknown configured provider '{provider}'"
      ),
      Self::InvalidProviderUrl {
        provider,
        driver,
        url_source,
        base_url,
        source,
      } => write!(
        f,
        "provider '{provider}' has invalid {url_source} URL '{base_url}' for driver '{driver}': {source}"
      ),
    }
  }
}

impl core::error::Error for MetadataLinkError {
  fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
    match self {
      Self::InvalidProviderUrl { source, .. } => Some(source),
      _ => None,
    }
  }
}

/// Link provider/account identities using a platform's installed driver lookup.
///
/// The resolver returns the default URL for an available provider/driver pair,
/// including named official destination defaults. It must also be consulted
/// when a URL is explicitly configured, so unknown drivers cannot pass linking.
/// Legacy account URLs are intentionally ignored: the provider owns its URL.
pub fn link_provider_metadata(
  plan: &ExecutionPlan,
  accounts: &[AccountConfig],
  resolve_default: impl Fn(&ProviderId, &DriverId) -> Option<&'static str>,
) -> Result<ProviderMetadataGraph, MetadataLinkError> {
  let mut account_indices = BTreeMap::new();
  let mut account_metadata = Vec::with_capacity(accounts.len());
  // Account identity is checked before consulting platform descriptors or
  // constructing any provider, preserving deterministic failure precedence.
  for (input_order, account) in accounts.iter().enumerate() {
    let account_id = SmolStr::new(&account.id);
    if let Some(first_index) = account_indices.insert(account_id.clone(), input_order) {
      return Err(MetadataLinkError::DuplicateAccountId {
        account_id,
        first_index,
        duplicate_index: input_order,
      });
    }
    account_metadata.push(AccountMetadata {
      account_id,
      input_order,
      enabled: account.enabled,
      tier: account.tier,
      binding_index: None,
    });
  }

  let mut destinations = BTreeMap::new();
  for (provider_id, provider) in plan.providers() {
    let driver_id = provider.driver();
    let default_url = resolve_default(provider_id, driver_id).ok_or_else(|| MetadataLinkError::UnknownDriver {
      provider: provider_id.clone(),
      driver: driver_id.clone(),
    })?;
    let (base_url, url_source) = match provider.base_url() {
      Some(base_url) => (base_url, ProviderUrlSource::Configured),
      None => {
        let official =
          official_provider_preset(provider_id.as_str()).is_some_and(|preset| preset.driver == driver_id.as_str());
        (
          default_url,
          if official {
            ProviderUrlSource::ProviderDefault
          } else {
            ProviderUrlSource::DriverDefault
          },
        )
      }
    };
    let cleartext_policy = if provider.allow_insecure_http() {
      CleartextHttpPolicy::Allow
    } else {
      CleartextHttpPolicy::LoopbackOnly
    };
    let base_url = CanonicalUpstreamUrl::parse(base_url, cleartext_policy).map_err(|source| {
      MetadataLinkError::InvalidProviderUrl {
        provider: provider_id.clone(),
        driver: driver_id.clone(),
        url_source,
        base_url: base_url.to_string(),
        source,
      }
    })?;
    destinations.insert(
      provider_id.clone(),
      DestinationMetadata {
        driver_id: driver_id.clone(),
        base_url,
        cleartext_policy,
      },
    );
  }

  let mut bindings = Vec::new();
  for (index, account) in accounts.iter().enumerate() {
    let unknown = || MetadataLinkError::UnknownAccountProvider {
      account_id: SmolStr::new(&account.id),
      provider: SmolStr::new(&account.provider),
    };
    let provider_id = ProviderId::new(&account.provider).map_err(|_| unknown())?;
    let Some(provider) = plan.provider(&provider_id) else {
      // Official providers omitted from a compiled plan were explicitly
      // disabled. Their stored accounts remain valid pool/account references.
      if official_provider_preset(provider_id.as_str()).is_some() {
        continue;
      }
      return Err(unknown());
    };
    // Unknown providers are rejected even for disabled accounts.
    if !account.enabled {
      continue;
    }
    let driver_id = provider.driver();
    let official =
      official_provider_preset(provider_id.as_str()).is_some_and(|preset| preset.driver == driver_id.as_str());
    let driver_identity = SmolStr::new(if official {
      provider_id.as_str()
    } else {
      driver_id.as_str()
    });
    account_metadata[index].binding_index = Some(bindings.len());
    bindings.push(BindingMetadata {
      key: ProviderBindingKey::new(provider_id, &account.id),
      driver_id: driver_id.clone(),
      driver_identity,
      account_order: index,
    });
  }
  Ok(ProviderMetadataGraph {
    destinations,
    bindings,
    accounts: account_metadata,
    account_indices,
  })
}

/// One pool's enabled membership, using indices into the linked binding slice.
#[derive(Debug)]
pub struct PoolMetadata {
  strategy: AccountSelectionStrategy,
  failure_cooldown: Duration,
  session_affinity: Option<SessionAffinityPlan>,
  active: Vec<usize>,
  fallback: Vec<usize>,
}

impl PoolMetadata {
  pub fn strategy(&self) -> AccountSelectionStrategy {
    self.strategy
  }
  pub fn failure_cooldown(&self) -> Duration {
    self.failure_cooldown
  }
  pub fn session_affinity(&self) -> Option<SessionAffinityPlan> {
    self.session_affinity
  }
  pub fn active(&self) -> &[usize] {
    &self.active
  }
  pub fn fallback(&self) -> &[usize] {
    &self.fallback
  }
}

#[derive(Debug)]
pub enum MetadataPoolError {
  UnknownAccount { pool: AccountPoolId, account_id: SmolStr },
}

impl fmt::Display for MetadataPoolError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::UnknownAccount { pool, account_id } => {
        write!(f, "account pool '{pool}' references unknown account '{account_id}'")
      }
    }
  }
}
impl core::error::Error for MetadataPoolError {}

/// Intersect pool selectors over linked accounts. Explicitly named disabled or
/// otherwise unbound accounts are valid references, but never become members.
pub fn link_pool_metadata(
  plan: &ExecutionPlan,
  providers: &ProviderMetadataGraph,
) -> Result<BTreeMap<AccountPoolId, PoolMetadata>, MetadataPoolError> {
  let mut pools = BTreeMap::new();
  for (id, policy) in plan.account_pools() {
    let selector = policy.selector();
    if let Some(selected) = selector.accounts() {
      for account_id in selected {
        if providers.account(account_id.as_str()).is_none() {
          return Err(MetadataPoolError::UnknownAccount {
            pool: id.clone(),
            account_id: account_id.clone(),
          });
        }
      }
    }
    let mut active = Vec::new();
    let mut fallback = Vec::new();
    for account in providers.accounts() {
      let Some(index) = account.binding_index() else {
        continue;
      };
      let binding = &providers.bindings()[index];
      if !selector
        .providers()
        .is_none_or(|ids| ids.contains(binding.provider_id()))
        || !selector.accounts().is_none_or(|ids| ids.contains(account.account_id()))
      {
        continue;
      }
      match account.tier() {
        AccountTier::Active => active.push(index),
        AccountTier::Fallback => fallback.push(index),
      }
    }
    pools.insert(
      id.clone(),
      PoolMetadata {
        strategy: policy.strategy(),
        failure_cooldown: policy.failure_cooldown(),
        session_affinity: policy.session_affinity(),
        active,
        fallback,
      },
    );
  }
  Ok(pools)
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::native::provider_default_base_url;
  use alloc::boxed::Box;
  use tokn_policy::{AccountPoolPlan, AccountSelector, ProviderPlan};

  fn provider_id(id: &str) -> ProviderId {
    ProviderId::new(id).unwrap()
  }

  fn account(id: &str, provider: &str, enabled: bool, tier: AccountTier) -> AccountConfig {
    let mut config: AccountConfig = serde_json::from_value(serde_json::json!({
      "id": id, "provider": provider, "enabled": enabled,
    }))
    .unwrap();
    config.tier = tier;
    config
  }

  fn provider(driver: &str, base_url: Option<&str>) -> ProviderPlan {
    ProviderPlan::new(
      DriverId::new(driver).unwrap(),
      base_url.map(Into::into),
      Box::default(),
      false,
    )
  }

  fn plan(providers: BTreeMap<ProviderId, ProviderPlan>, selector: AccountSelector) -> ExecutionPlan {
    ExecutionPlan::new(
      BTreeMap::new(),
      BTreeMap::new(),
      BTreeMap::new(),
      BTreeMap::from([(
        AccountPoolId::new("pool").unwrap(),
        AccountPoolPlan::new(
          selector,
          AccountSelectionStrategy::RoundRobin,
          Duration::from_secs(17),
          Some(SessionAffinityPlan::new(
            Duration::from_secs(23),
            Duration::from_secs(29),
          )),
        ),
      )]),
      providers,
    )
  }

  #[test]
  fn duplicate_account_preflight_does_not_consult_the_platform() {
    let plan = plan(
      BTreeMap::from([(provider_id("local"), provider("llama-cpp", None))]),
      AccountSelector::all(),
    );
    let accounts = [
      account("duplicate", "local", true, AccountTier::Active),
      account("duplicate", "local", false, AccountTier::Fallback),
    ];
    let result = link_provider_metadata(&plan, &accounts, |_, _| {
      panic!("duplicate account preflight must run first")
    });
    assert!(matches!(
      result,
      Err(MetadataLinkError::DuplicateAccountId {
        first_index: 0,
        duplicate_index: 1,
        ..
      })
    ));
  }

  #[test]
  fn account_slots_preserve_unbound_references_and_pool_order() {
    let plan = plan(
      BTreeMap::from([
        (provider_id("primary"), provider("llama-cpp", None)),
        (provider_id("other"), provider("llama-cpp", None)),
      ]),
      AccountSelector::new(
        Some([provider_id("primary")].into()),
        Some(
          ["fallback", "disabled", "omitted-provider", "active", "other-provider"]
            .into_iter()
            .map(SmolStr::new)
            .collect(),
        ),
      ),
    );
    let accounts = [
      account("fallback", "primary", true, AccountTier::Fallback),
      account("disabled", "primary", false, AccountTier::Active),
      account("omitted-provider", "openai", true, AccountTier::Active),
      account("active", "primary", true, AccountTier::Active),
      account("other-provider", "other", true, AccountTier::Active),
      account("omitted-account", "primary", true, AccountTier::Active),
    ];
    let graph = link_provider_metadata(&plan, &accounts, provider_default_base_url).unwrap();
    assert_eq!(graph.accounts().len(), 6);
    assert_eq!(
      graph
        .bindings()
        .iter()
        .map(BindingMetadata::account_order)
        .collect::<Vec<_>>(),
      [0, 3, 4, 5]
    );
    assert_eq!(graph.account("disabled").unwrap().binding_index(), None);
    assert_eq!(graph.account("omitted-provider").unwrap().binding_index(), None);
    let pools = link_pool_metadata(&plan, &graph).unwrap();
    let pool = pools.get(&AccountPoolId::new("pool").unwrap()).unwrap();
    assert_eq!(pool.active(), [1]);
    assert_eq!(pool.fallback(), [0]);
    assert_eq!(pool.failure_cooldown(), Duration::from_secs(17));
    assert_eq!(
      pool.session_affinity().unwrap().expired_retention(),
      Duration::from_secs(29)
    );
  }

  #[test]
  fn explicit_account_selectors_reject_unknown_names() {
    let plan = plan(
      BTreeMap::new(),
      AccountSelector::new(None, Some([SmolStr::new("missing")].into())),
    );
    let graph = link_provider_metadata(&plan, &[], provider_default_base_url).unwrap();
    assert!(
      matches!(link_pool_metadata(&plan, &graph), Err(MetadataPoolError::UnknownAccount { account_id, .. }) if account_id == "missing")
    );
  }

  #[test]
  fn disabled_accounts_still_require_a_known_provider() {
    let plan = plan(BTreeMap::new(), AccountSelector::all());
    let accounts = [account("disabled", "unconfigured", false, AccountTier::Active)];
    assert!(matches!(
      link_provider_metadata(&plan, &accounts, provider_default_base_url),
      Err(MetadataLinkError::UnknownAccountProvider { .. })
    ));
  }

  #[test]
  fn configured_urls_cannot_bypass_driver_lookup_or_cleartext_policy() {
    let unknown = plan(
      BTreeMap::from([(
        provider_id("custom"),
        provider("unknown", Some("https://upstream.example/v1")),
      )]),
      AccountSelector::all(),
    );
    assert!(matches!(
      link_provider_metadata(&unknown, &[], provider_default_base_url),
      Err(MetadataLinkError::UnknownDriver { .. })
    ));
    let unsafe_url = plan(
      BTreeMap::from([(
        provider_id("custom"),
        provider("openai", Some("http://upstream.example/v1")),
      )]),
      AccountSelector::all(),
    );
    assert!(matches!(
      link_provider_metadata(&unsafe_url, &[], provider_default_base_url),
      Err(MetadataLinkError::InvalidProviderUrl {
        url_source: ProviderUrlSource::Configured,
        source: InvalidUpstreamUrl::InsecureHttp,
        ..
      })
    ));
  }

  #[cfg(feature = "std")]
  #[test]
  fn native_defaults_match_standard_destinations_for_every_official_provider() {
    use crate::{link::link_execution_provider_graph, registry::Registry};
    let plan = plan(
      tokn_core::provider::OFFICIAL_PROVIDER_PRESETS
        .iter()
        .map(|preset| (provider_id(preset.id), provider(preset.driver, None)))
        .collect(),
      AccountSelector::all(),
    );
    let native = link_provider_metadata(&plan, &[], provider_default_base_url).unwrap();
    let standard = link_execution_provider_graph(&plan, &[], &Registry::builtin()).unwrap();
    for (id, destination) in native.destinations() {
      let standard_destination = standard.destination(id).unwrap();
      assert_eq!(destination.base_url(), standard_destination.target().base_url(), "{id}");
      assert_eq!(destination.driver_id(), standard_destination.driver_id(), "{id}");
    }
  }

  #[cfg(feature = "std")]
  #[test]
  fn standard_materialization_matches_portable_binding_and_pool_metadata() {
    use crate::{
      link::{link_execution_account_pools, link_execution_provider_graph},
      registry::Registry,
    };
    let plan = plan(
      BTreeMap::from([
        (
          provider_id("z-primary"),
          provider("llama-cpp", Some("https://same.example/prefix")),
        ),
        (
          provider_id("a-secondary"),
          provider("llama-cpp", Some("https://same.example/prefix")),
        ),
      ]),
      AccountSelector::new(
        None,
        Some(
          ["fallback", "disabled", "omitted-provider", "active", "second"]
            .into_iter()
            .map(SmolStr::new)
            .collect(),
        ),
      ),
    );
    let mut accounts = [
      account("fallback", "z-primary", true, AccountTier::Fallback),
      account("disabled", "z-primary", false, AccountTier::Active),
      account("omitted-provider", "openai", true, AccountTier::Active),
      account("active", "a-secondary", true, AccountTier::Active),
      account("second", "z-primary", true, AccountTier::Active),
    ];
    accounts[0].base_url = Some("https://ignored.example/v1".into());
    let native = link_provider_metadata(&plan, &accounts, provider_default_base_url).unwrap();
    let native_pools = link_pool_metadata(&plan, &native).unwrap();
    let standard = link_execution_provider_graph(&plan, &accounts, &Registry::builtin()).unwrap();
    let standard_pools = link_execution_account_pools(&plan, &standard).unwrap();
    for binding in native.bindings() {
      let runtime = standard
        .binding(binding.provider_id(), binding.key().account_id())
        .unwrap();
      assert_eq!(runtime.key(), binding.key());
      assert_eq!(runtime.account_order(), binding.account_order());
      assert_eq!(runtime.handle().config.load().provider, binding.driver_identity());
      assert_eq!(
        runtime.driver().info().upstream_url,
        native.destination(binding.provider_id()).unwrap().base_url().as_str()
      );
    }
    for (id, pool) in native_pools {
      let standard_pool = standard_pools.pool(&id).unwrap();
      let keys = |indices: &[usize]| {
        indices
          .iter()
          .map(|index| native.bindings()[*index].key())
          .collect::<Vec<_>>()
      };
      assert_eq!(
        keys(pool.active()),
        standard_pool
          .active()
          .iter()
          .map(|account| account.binding().key())
          .collect::<Vec<_>>()
      );
      assert_eq!(
        keys(pool.fallback()),
        standard_pool
          .fallback()
          .iter()
          .map(|account| account.binding().key())
          .collect::<Vec<_>>()
      );
      assert_eq!(pool.strategy(), standard_pool.strategy());
      assert_eq!(pool.session_affinity(), standard_pool.session_affinity());
      assert_eq!(pool.failure_cooldown(), standard_pool.failure_cooldown());
    }
  }

  #[cfg(feature = "std")]
  #[test]
  fn standard_and_portable_link_errors_preserve_context() {
    use crate::{link::link_execution_provider_graph, registry::Registry};
    let registry = Registry::builtin();
    let valid = BTreeMap::from([(provider_id("local"), provider("llama-cpp", None))]);
    let cases = [
      (
        valid.clone(),
        vec![
          account("duplicate", "local", true, AccountTier::Active),
          account("duplicate", "local", false, AccountTier::Active),
        ],
      ),
      (valid, vec![account("unknown", "missing", false, AccountTier::Active)]),
      (
        BTreeMap::from([(
          provider_id("broken"),
          provider("missing", Some("https://upstream.example")),
        )]),
        vec![],
      ),
      (
        BTreeMap::from([(
          provider_id("unsafe"),
          provider("openai", Some("http://upstream.example")),
        )]),
        vec![],
      ),
    ];
    for (providers, accounts) in cases {
      let plan = plan(providers, AccountSelector::all());
      let native = link_provider_metadata(&plan, &accounts, provider_default_base_url).unwrap_err();
      let standard = link_execution_provider_graph(&plan, &accounts, &registry).unwrap_err();
      assert_eq!(native.to_string(), standard.to_string());
    }
  }
}
