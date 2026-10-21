//! Pool-local runtime selection for linked v2 account pools.
//!
//! Immutable provider bindings can be shared by many pools. Selection state
//! cannot: round-robin position, cooldown history, and session affinity all
//! have pool-local policy semantics. [`AccountPoolRuntimes`] therefore owns
//! exactly one shared runtime per linked pool for routes to reuse.

use super::{LinkedAccountPool, LinkedAccountPools, LinkedPoolAccount, ProviderBinding, ProviderBindingKey};
use parking_lot::Mutex;
use smol_str::SmolStr;
use snafu::Snafu;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokn_engine::pool::{AffinityPolicy, PoolState, Selection};
use tokn_policy::{AccountPoolId, ProviderId};

/// Selection result for one request.
#[derive(Clone, Debug)]
pub enum PoolAcquire {
  Selected(Arc<ProviderBinding>),
  CoolingDown { retry_at: Instant },
  NoEligible,
}

/// Failures from mutating pool-local selection state.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PoolRuntimeError {
  #[snafu(display(
    "account pool '{pool}' does not contain binding for provider '{provider}' and account '{account_id}'"
  ))]
  UnknownBinding {
    pool: AccountPoolId,
    provider: ProviderId,
    account_id: SmolStr,
  },
}

pub type PoolRuntimeResult<T> = std::result::Result<T, PoolRuntimeError>;

/// Shared runtime ownership for every linked account pool.
pub struct AccountPoolRuntimes {
  runtimes: BTreeMap<AccountPoolId, Arc<AccountPoolRuntime>>,
}

impl AccountPoolRuntimes {
  pub fn runtime(&self, pool_id: &AccountPoolId) -> Option<&Arc<AccountPoolRuntime>> {
    self.runtimes.get(pool_id)
  }

  pub fn runtimes(&self) -> impl ExactSizeIterator<Item = (&AccountPoolId, &Arc<AccountPoolRuntime>)> {
    self.runtimes.iter()
  }

  pub fn len(&self) -> usize {
    self.runtimes.len()
  }

  pub fn is_empty(&self) -> bool {
    self.runtimes.is_empty()
  }
}

impl std::fmt::Debug for AccountPoolRuntimes {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_map().entries(&self.runtimes).finish()
  }
}

/// Create one shared runtime for every linked pool.
pub fn build_account_pool_runtimes(pools: &LinkedAccountPools) -> AccountPoolRuntimes {
  let runtimes = pools
    .pools()
    .map(|(pool_id, pool)| (pool_id.clone(), Arc::new(AccountPoolRuntime::new(pool.clone()))))
    .collect();
  AccountPoolRuntimes { runtimes }
}

/// Mutable selection state scoped to one linked account pool.
pub struct AccountPoolRuntime {
  pool: Arc<LinkedAccountPool>,
  bindings: BTreeMap<ProviderBindingKey, Arc<ProviderBinding>>,
  state: Mutex<PoolState<ProviderBindingKey, Instant>>,
}

impl AccountPoolRuntime {
  fn new(pool: Arc<LinkedAccountPool>) -> Self {
    let mut bindings = BTreeMap::new();
    for binding in pool
      .active()
      .iter()
      .chain(pool.fallback())
      .map(LinkedPoolAccount::binding)
    {
      let previous = bindings.insert(binding.key().clone(), binding.clone());
      debug_assert!(previous.is_none(), "linked pool contains a duplicate provider binding");
    }
    let active = pool
      .active()
      .iter()
      .map(|account| account.binding().key().clone())
      .collect();
    let fallback = pool
      .fallback()
      .iter()
      .map(|account| account.binding().key().clone())
      .collect();
    let affinity = pool
      .session_affinity()
      .map(|policy| AffinityPolicy::new(policy.ttl(), policy.expired_retention()));
    let state = PoolState::new(active, fallback, pool.failure_cooldown(), affinity)
      .expect("linked pool contains unique provider bindings");

    Self {
      pool,
      bindings,
      state: Mutex::new(state),
    }
  }

  pub fn pool(&self) -> &Arc<LinkedAccountPool> {
    &self.pool
  }

  /// Select a binding while leaving affinity and cooldown state untouched.
  ///
  /// The predicate owns route-, operation-, and model-specific eligibility.
  /// It is evaluated synchronously and outside all selection-state locks.
  pub fn acquire<F>(&self, session_id: Option<&str>, eligible: F) -> PoolAcquire
  where
    F: Fn(&ProviderBinding) -> bool,
  {
    self.acquire_ranked(session_id, eligible, |_| 0)
  }

  /// Select the highest-ranked healthy binding within each account tier.
  /// Equal ranks retain the pool's round-robin behavior, and established
  /// healthy session affinity remains sticky regardless of later score changes.
  /// Eligibility and ranks are snapshotted for all eligible bindings in both
  /// tiers before selection. Callbacks must provide decisions independently of
  /// invocation count; they can also be called for cooled bindings or affinity hits.
  pub fn acquire_ranked<F, R>(&self, session_id: Option<&str>, eligible: F, rank: R) -> PoolAcquire
  where
    F: Fn(&ProviderBinding) -> bool,
    R: Fn(&ProviderBinding) -> i32,
  {
    self.acquire_ranked_with_clock(session_id, eligible, rank, Instant::now)
  }

  fn acquire_ranked_with_clock<F, R>(
    &self,
    session_id: Option<&str>,
    eligible: F,
    rank: R,
    now: impl FnOnce() -> Instant,
  ) -> PoolAcquire
  where
    F: Fn(&ProviderBinding) -> bool,
    R: Fn(&ProviderBinding) -> i32,
  {
    // User callbacks may inspect account state or re-enter this runtime. Gather
    // their results before locking; only the shared selection algorithm runs
    // inside the short state transaction.
    let eligible_ranks = self
      .pool
      .active()
      .iter()
      .chain(self.pool.fallback())
      .map(LinkedPoolAccount::binding)
      .filter(|binding| eligible(binding))
      .map(|binding| (binding.key().clone(), rank(binding)))
      .collect();
    let selected = self.state.lock().select(now(), session_id, &eligible_ranks);
    match selected {
      Selection::Selected(key) => PoolAcquire::Selected(
        self
          .bindings
          .get(&key)
          .expect("shared selection returns a linked binding")
          .clone(),
      ),
      Selection::CoolingDown { retry_at } => PoolAcquire::CoolingDown { retry_at },
      Selection::NoEligible => PoolAcquire::NoEligible,
    }
  }

  /// Commit a successful binding use.
  ///
  /// Success clears this pool's cooldown history for the exact tuple and, if
  /// configured, creates or refreshes affinity only after the caller confirms
  /// the provider operation succeeded.
  pub fn record_success(&self, session_id: Option<&str>, key: &ProviderBindingKey) -> PoolRuntimeResult<()> {
    self.record_success_with_clock(session_id, key, Instant::now)
  }

  fn record_success_with_clock(
    &self,
    session_id: Option<&str>,
    key: &ProviderBindingKey,
    now: impl FnOnce() -> Instant,
  ) -> PoolRuntimeResult<()> {
    self
      .state
      .lock()
      .record_success(now(), session_id, key)
      .map_err(|_| self.unknown_binding(key))
  }

  /// Record one failure and cool only this pool's exact account/provider
  /// tuple. Returns the new retry deadline for logging or scheduling.
  pub fn record_failure(&self, key: &ProviderBindingKey) -> PoolRuntimeResult<Instant> {
    self.record_failure_with_clock(key, Instant::now)
  }

  fn record_failure_with_clock(
    &self,
    key: &ProviderBindingKey,
    now: impl FnOnce() -> Instant,
  ) -> PoolRuntimeResult<Instant> {
    self
      .state
      .lock()
      .record_failure(now(), key)
      .map_err(|_| self.unknown_binding(key))
  }

  fn unknown_binding(&self, key: &ProviderBindingKey) -> PoolRuntimeError {
    PoolRuntimeError::UnknownBinding {
      pool: self.pool.id().clone(),
      provider: key.provider_id().clone(),
      account_id: SmolStr::new(key.account_id()),
    }
  }
}

impl std::fmt::Debug for AccountPoolRuntime {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("AccountPoolRuntime")
      .field("pool", &self.pool.id())
      .field("binding_count", &self.bindings.len())
      .finish_non_exhaustive()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::link::{link_account_pools, link_provider_graph};
  use crate::registry::Registry;
  use smol_str::SmolStr;
  use std::collections::BTreeMap;
  use std::time::Duration;
  use tokn_core::account::{AccountConfig, AccountTier};
  use tokn_core::provider::ID_LLAMA_CPP;
  use tokn_policy::{
    AccountPoolPlan, AccountSelectionStrategy, AccountSelector, DriverId, GatewayPlan, ProviderPlan,
    SessionAffinityPlan,
  };

  fn pool_id(value: &str) -> AccountPoolId {
    AccountPoolId::new(value).unwrap()
  }

  fn provider_id(value: &str) -> ProviderId {
    ProviderId::new(value).unwrap()
  }

  fn driver_id(value: &str) -> DriverId {
    DriverId::new(value).unwrap()
  }

  fn account(id: &str, tier: AccountTier) -> AccountConfig {
    account_at(id, "local", tier)
  }

  fn account_at(id: &str, provider: &str, tier: AccountTier) -> AccountConfig {
    let mut account: AccountConfig = toml::from_str(
      r#"
        id = "fixture"
        provider = "llama-cpp"
      "#,
    )
    .unwrap();
    account.id = id.to_string();
    account.provider = provider.to_string();
    account.tier = tier;
    account
  }

  fn provider() -> ProviderPlan {
    ProviderPlan::new(
      driver_id(ID_LLAMA_CPP),
      Some("https://gateway.example/v1/".into()),
      Box::default(),
      false,
    )
  }

  fn pool(accounts: Option<&[&str]>, affinity: Option<SessionAffinityPlan>) -> AccountPoolPlan {
    AccountPoolPlan::new(
      AccountSelector::new(
        None,
        accounts.map(|account_ids| account_ids.iter().map(SmolStr::new).collect()),
      ),
      AccountSelectionStrategy::RoundRobin,
      Duration::from_secs(60),
      affinity,
    )
  }

  fn runtimes(
    pools: BTreeMap<AccountPoolId, AccountPoolPlan>,
    providers: BTreeMap<ProviderId, ProviderPlan>,
    accounts: &[AccountConfig],
  ) -> AccountPoolRuntimes {
    let plan = GatewayPlan::new(
      BTreeMap::new(),
      BTreeMap::new(),
      BTreeMap::new(),
      BTreeMap::new(),
      pools,
      providers,
    );
    let registry = Registry::builtin();
    let providers = link_provider_graph(&plan, accounts, &registry).unwrap();
    let pools = link_account_pools(&plan, &providers).unwrap();
    build_account_pool_runtimes(&pools)
  }

  fn selected(result: PoolAcquire) -> Arc<ProviderBinding> {
    let PoolAcquire::Selected(binding) = result else {
      panic!("expected selected binding, got {result:?}");
    };
    binding
  }

  #[test]
  fn round_robin_weights_logical_accounts() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    let selected = (0..4)
      .map(|_| selected(runtime.acquire(None, |_| true)))
      .collect::<Vec<_>>();

    assert_eq!(
      selected.iter().map(|binding| binding.account_id()).collect::<Vec<_>>(),
      ["first", "second", "first", "second"]
    );
    assert_eq!(selected[0].provider_id().as_str(), "local");
    assert_eq!(selected[2].provider_id().as_str(), "local");
  }

  #[test]
  fn round_robin_is_fair_across_matching_accounts_with_filtered_gaps() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("ineligible-one", AccountTier::Active),
        account("ineligible-two", AccountTier::Active),
        account("fourth", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    let account_ids = (0..6)
      .map(|_| selected(runtime.acquire(None, |binding| matches!(binding.account_id(), "first" | "fourth"))))
      .map(|binding| binding.account_id().to_string())
      .collect::<Vec<_>>();

    assert_eq!(account_ids, ["first", "fourth", "first", "fourth", "first", "fourth"]);
  }

  #[test]
  fn ranked_selection_prefers_high_scores_and_round_robins_ties() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([
        (provider_id("preferred"), provider()),
        (provider_id("peer"), provider()),
        (provider_id("neutral"), provider()),
      ]),
      &[
        account_at("preferred-account", "preferred", AccountTier::Active),
        account_at("peer-account", "peer", AccountTier::Active),
        account_at("neutral-account", "neutral", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let score = |binding: &ProviderBinding| match binding.provider_id().as_str() {
      "preferred" | "peer" => 100,
      _ => 0,
    };

    let selected_ids = (0..4)
      .map(|_| selected(runtime.acquire_ranked(None, |_| true, score)))
      .map(|binding| binding.account_id().to_string())
      .collect::<Vec<_>>();
    assert_eq!(
      selected_ids,
      ["preferred-account", "peer-account", "preferred-account", "peer-account"]
    );

    runtime
      .record_failure(&ProviderBindingKey::new(provider_id("preferred"), "preferred-account"))
      .unwrap();
    runtime
      .record_failure(&ProviderBindingKey::new(provider_id("peer"), "peer-account"))
      .unwrap();
    assert_eq!(
      selected(runtime.acquire_ranked(None, |_| true, score)).account_id(),
      "neutral-account"
    );
  }

  #[test]
  fn account_tier_and_session_affinity_take_precedence_over_scores() {
    let affinity = SessionAffinityPlan::new(Duration::from_secs(300), Duration::from_secs(60));
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, Some(affinity)))]),
      BTreeMap::from([
        (provider_id("active"), provider()),
        (provider_id("fallback"), provider()),
      ]),
      &[
        account_at("active-account", "active", AccountTier::Active),
        account_at("fallback-account", "fallback", AccountTier::Fallback),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let prefer_fallback = |binding: &ProviderBinding| i32::from(binding.provider_id().as_str() == "fallback") * 100;

    let active = selected(runtime.acquire_ranked(Some("session"), |_| true, prefer_fallback));
    assert_eq!(active.account_id(), "active-account");
    runtime.record_success(Some("session"), active.key()).unwrap();

    let affinity_hit = selected(runtime.acquire_ranked(Some("session"), |_| true, |_| i32::MAX));
    assert_eq!(affinity_hit.account_id(), "active-account");
  }

  #[test]
  fn active_tier_precedes_fallback_until_active_binding_is_cooled() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("active", AccountTier::Active),
        account("fallback", AccountTier::Fallback),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    let active = selected(runtime.acquire(None, |_| true));
    assert_eq!(active.account_id(), "active");
    runtime.record_failure(active.key()).unwrap();

    let fallback = selected(runtime.acquire(None, |_| true));
    assert_eq!(fallback.account_id(), "fallback");
  }

  #[test]
  fn affinity_is_written_only_on_success_and_pins_the_exact_binding() {
    let affinity = SessionAffinityPlan::new(Duration::from_secs(300), Duration::from_secs(60));
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, Some(affinity)))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    let uncommitted = selected(runtime.acquire(Some("session"), |_| true));
    assert_eq!(uncommitted.account_id(), "first");
    let next = selected(runtime.acquire(Some("session"), |_| true));
    assert_eq!(next.account_id(), "second");

    let exact_key = ProviderBindingKey::new(provider_id("local"), "second");
    runtime.record_success(Some("session"), &exact_key).unwrap();
    let affinity_hit = selected(runtime.acquire(Some("session"), |_| true));
    assert_eq!(affinity_hit.key(), &exact_key);

    let fallthrough = selected(runtime.acquire(Some("session"), |binding| binding.key() != &exact_key));
    assert_ne!(fallthrough.key(), &exact_key);

    runtime.record_failure(&exact_key).unwrap();
    let cooled_fallthrough = selected(runtime.acquire(Some("session"), |_| true));
    assert_ne!(cooled_fallthrough.key(), &exact_key);

    runtime.record_success(Some("session"), &exact_key).unwrap();
    let recovered_affinity = selected(runtime.acquire(Some("session"), |_| true));
    assert_eq!(recovered_affinity.key(), &exact_key);
  }

  #[test]
  fn expired_affinity_falls_through_without_sleeping() {
    let immediately_expired = SessionAffinityPlan::new(Duration::ZERO, Duration::from_secs(60));
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, Some(immediately_expired)))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    let first = selected(runtime.acquire(Some("session"), |_| true));
    runtime.record_success(Some("session"), first.key()).unwrap();
    let rebound = selected(runtime.acquire(Some("session"), |_| true));

    assert_eq!(first.account_id(), "first");
    assert_eq!(rebound.account_id(), "second");
  }

  #[test]
  fn cooldown_is_exact_to_binding_and_isolated_between_pools() {
    let runtimes = runtimes(
      BTreeMap::from([
        (pool_id("first"), pool(None, None)),
        (pool_id("second"), pool(None, None)),
      ]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[account("shared", AccountTier::Active)],
    );
    let first_runtime = runtimes.runtime(&pool_id("first")).unwrap();
    let second_runtime = runtimes.runtime(&pool_id("second")).unwrap();
    let first_key = ProviderBindingKey::new(provider_id("local"), "shared");

    first_runtime.record_failure(&first_key).unwrap();
    let other_pool = selected(second_runtime.acquire(None, |_| true));

    assert!(matches!(
      first_runtime.acquire(None, |_| true),
      PoolAcquire::CoolingDown { .. }
    ));
    assert_eq!(other_pool.key(), &first_key);
  }

  #[test]
  fn all_cooled_returns_the_globally_earliest_retry() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("first"), provider()), (provider_id("second"), provider())]),
      &[
        account_at("first-account", "first", AccountTier::Active),
        account_at("second-account", "second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let first_key = ProviderBindingKey::new(provider_id("first"), "first-account");
    let second_key = ProviderBindingKey::new(provider_id("second"), "second-account");
    runtime.record_failure(&first_key).unwrap();
    let later_retry = runtime.record_failure(&first_key).unwrap();
    let earliest_retry = runtime.record_failure(&second_key).unwrap();

    assert!(earliest_retry < later_retry);
    assert!(matches!(
      runtime.acquire(None, |_| true),
      PoolAcquire::CoolingDown { retry_at } if retry_at == earliest_retry
    ));
  }

  #[test]
  fn success_clears_only_the_exact_binding_cooldown() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("first"), provider()), (provider_id("second"), provider())]),
      &[
        account_at("first-account", "first", AccountTier::Active),
        account_at("second-account", "second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let first_key = ProviderBindingKey::new(provider_id("first"), "first-account");
    let second_key = ProviderBindingKey::new(provider_id("second"), "second-account");
    runtime.record_failure(&first_key).unwrap();
    let second_retry = runtime.record_failure(&second_key).unwrap();

    runtime.record_success(None, &first_key).unwrap();

    assert_eq!(
      selected(runtime.acquire(None, |binding| binding.key() == &first_key)).key(),
      &first_key
    );
    assert!(matches!(
      runtime.acquire(None, |binding| binding.key() == &second_key),
      PoolAcquire::CoolingDown { retry_at } if retry_at == second_retry
    ));
  }

  #[test]
  fn no_matching_binding_returns_no_eligible() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[account("only", AccountTier::Active)],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();

    assert!(matches!(runtime.acquire(None, |_| false), PoolAcquire::NoEligible));
  }

  #[test]
  fn unknown_keys_never_mutate_runtime_state() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("known"), provider())]),
      &[account_at("known", "known", AccountTier::Active)],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let known = ProviderBindingKey::new(provider_id("known"), "known");
    let unknown = ProviderBindingKey::new(provider_id("missing"), "missing");
    let known_retry = runtime.record_failure(&known).unwrap();

    assert!(matches!(
      runtime.record_failure(&unknown),
      Err(PoolRuntimeError::UnknownBinding { .. })
    ));
    assert!(matches!(
      runtime.record_success(Some("session"), &unknown),
      Err(PoolRuntimeError::UnknownBinding { .. })
    ));
    assert!(matches!(
      runtime.acquire(None, |_| true),
      PoolAcquire::CoolingDown { retry_at } if retry_at == known_retry
    ));
    assert_eq!(runtime.state.lock().binding_count(), 1);
  }

  #[test]
  fn concurrent_requests_share_round_robin_progress() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("second", AccountTier::Active),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let totals = std::thread::scope(|scope| {
      let workers = (0..4)
        .map(|_| {
          let runtime = runtime.clone();
          let barrier = barrier.clone();
          scope.spawn(move || {
            barrier.wait();
            let mut counts = [0; 2];
            for _ in 0..100 {
              match selected(runtime.acquire(None, |_| true)).account_id() {
                "first" => counts[0] += 1,
                "second" => counts[1] += 1,
                other => panic!("unexpected account {other}"),
              }
            }
            counts
          })
        })
        .collect::<Vec<_>>();
      workers.into_iter().fold([0; 2], |mut total, worker| {
        let counts = worker.join().expect("selection worker completed");
        total[0] += counts[0];
        total[1] += counts[1];
        total
      })
    });

    assert_eq!(totals, [200, 200]);
  }

  #[test]
  fn eligibility_and_ranking_callbacks_can_reenter_state_operations() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[account("only", AccountTier::Active)],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let chosen = selected(runtime.acquire_ranked(
      None,
      |binding| {
        assert!(
          runtime.state.try_lock().is_some(),
          "eligibility runs outside state lock"
        );
        runtime.record_success(None, binding.key()).unwrap();
        true
      },
      |binding| {
        assert!(runtime.state.try_lock().is_some(), "ranking runs outside state lock");
        runtime.record_success(None, binding.key()).unwrap();
        10
      },
    ));

    assert_eq!(chosen.account_id(), "only");
  }

  #[test]
  fn standard_adapter_matches_shared_engine_decision_transcript() {
    fn compare(
      runtime: &AccountPoolRuntime,
      shared: &mut PoolState<ProviderBindingKey, Instant>,
      now: Instant,
      session: Option<&str>,
      ranks: &BTreeMap<ProviderBindingKey, i32>,
    ) {
      let expected = shared.select(now, session, ranks);
      let actual = runtime.acquire_ranked_with_clock(
        session,
        |binding| ranks.contains_key(binding.key()),
        |binding| ranks[binding.key()],
        || now,
      );
      let actual = match actual {
        PoolAcquire::Selected(binding) => Selection::Selected(binding.key().clone()),
        PoolAcquire::CoolingDown { retry_at } => Selection::CoolingDown { retry_at },
        PoolAcquire::NoEligible => Selection::NoEligible,
      };
      assert_eq!(actual, expected);
    }

    let affinity = SessionAffinityPlan::new(Duration::from_secs(5), Duration::from_secs(7));
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, Some(affinity)))]),
      BTreeMap::from([(provider_id("local"), provider())]),
      &[
        account("first", AccountTier::Active),
        account("second", AccountTier::Active),
        account("fallback", AccountTier::Fallback),
      ],
    );
    let runtime = runtimes.runtime(&pool_id("default")).unwrap();
    let key = |id| ProviderBindingKey::new(provider_id("local"), id);
    let first = key("first");
    let second = key("second");
    let fallback = key("fallback");
    let mut shared = PoolState::new(
      vec![first.clone(), second.clone()],
      vec![fallback.clone()],
      Duration::from_secs(60),
      Some(AffinityPolicy::new(affinity.ttl(), affinity.expired_retention())),
    )
    .unwrap();
    let started = Instant::now();
    let at = |seconds| started + Duration::from_secs(seconds);
    let mut ranks = BTreeMap::from([(first.clone(), 10), (second.clone(), 10), (fallback.clone(), 100)]);

    compare(runtime, &mut shared, at(0), Some("session"), &ranks);
    compare(runtime, &mut shared, at(0), Some("session"), &ranks);
    runtime
      .record_success_with_clock(Some("session"), &second, || at(0))
      .unwrap();
    shared.record_success(at(0), Some("session"), &second).unwrap();
    ranks.insert(first.clone(), 100);
    ranks.insert(second.clone(), 0);
    compare(runtime, &mut shared, at(1), Some("session"), &ranks);

    for (key, seconds) in [(&second, 2), (&first, 3), (&fallback, 5), (&second, 7)] {
      let expected_retry = shared.record_failure(at(seconds), key).unwrap();
      assert_eq!(
        runtime.record_failure_with_clock(key, || at(seconds)).unwrap(),
        expected_retry
      );
      compare(runtime, &mut shared, at(seconds), Some("session"), &ranks);
      if seconds == 3 {
        runtime
          .record_success_with_clock(Some("session"), &fallback, || at(4))
          .unwrap();
        shared.record_success(at(4), Some("session"), &fallback).unwrap();
        compare(runtime, &mut shared, at(4), Some("session"), &ranks);
      }
    }

    compare(runtime, &mut shared, at(8), Some("session"), &ranks);
    runtime.record_success_with_clock(None, &first, || at(9)).unwrap();
    shared.record_success(at(9), None, &first).unwrap();
    compare(runtime, &mut shared, at(9), Some("session"), &ranks);
    compare(runtime, &mut shared, at(63), None, &ranks);
    ranks.clear();
    compare(runtime, &mut shared, at(64), None, &ranks);
  }

  #[test]
  fn runtime_set_shares_one_runtime_arc_per_pool_lookup() {
    let runtimes = runtimes(
      BTreeMap::from([(pool_id("default"), pool(None, None))]),
      BTreeMap::new(),
      &[],
    );

    let first = runtimes.runtime(&pool_id("default")).unwrap();
    let second = runtimes.runtime(&pool_id("default")).unwrap();
    assert!(Arc::ptr_eq(first, second));
  }
}
