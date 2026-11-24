//! Pool-local account selection shared by standard and embedded runtimes.
//!
//! The caller supplies eligible binding keys and their ranks before borrowing
//! this state mutably. Selection does not run eligibility/ranking callbacks,
//! perform I/O, or wait. A standard adapter may protect these transactions with a lock;
//! an embedded caller can own the same state directly. Keep one state per pool.

use alloc::{collections::BTreeMap, vec::Vec};
use core::{fmt, time::Duration};

use tokn_core::{time::Moment, types::SmolStr};

const MAX_COOLDOWN_EXPONENT: u32 = 5;
const AFFINITY_GC_INTERVAL: usize = 1024;

/// Live session TTL and the additional time an expired entry is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AffinityPolicy {
  ttl: Duration,
  expired_retention: Duration,
}

impl AffinityPolicy {
  pub const fn new(ttl: Duration, expired_retention: Duration) -> Self {
    Self { ttl, expired_retention }
  }

  pub const fn ttl(self) -> Duration {
    self.ttl
  }

  pub const fn expired_retention(self) -> Duration {
    self.expired_retention
  }

  /// Retention is measured from the last successful use, including live TTL.
  pub const fn retained_ttl(self) -> Duration {
    self.ttl.saturating_add(self.expired_retention)
  }
}

/// Selection outcome for one request, independent of account runtime ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection<K, M> {
  Selected(K),
  CoolingDown { retry_at: M },
  NoEligible,
}

/// Invalid pool construction or a mutation for a key outside this pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolError<K> {
  DuplicateBinding(K),
  UnknownBinding(K),
}

impl<K: fmt::Debug> fmt::Display for PoolError<K> {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::DuplicateBinding(key) => write!(formatter, "pool contains duplicate binding {key:?}"),
      Self::UnknownBinding(key) => write!(formatter, "pool does not contain binding {key:?}"),
    }
  }
}

impl<K: fmt::Debug> core::error::Error for PoolError<K> {}

#[derive(Clone, Copy, Debug)]
struct Cooldown<M> {
  consecutive_failures: u32,
  retry_at: Option<M>,
}

impl<M> Default for Cooldown<M> {
  fn default() -> Self {
    Self {
      consecutive_failures: 0,
      retry_at: None,
    }
  }
}

#[derive(Clone, Debug)]
struct AffinityEntry<K, M> {
  key: K,
  stamped_at: M,
}

/// Mutable state belonging to one account pool.
///
/// Keys include the provider/account identity chosen by the caller. Their input
/// order determines round-robin order. Tiers never mix: any eligible healthy
/// active binding takes precedence over fallback bindings, regardless of rank.
/// Among healthy bindings in a tier, only the highest rank participates in
/// round-robin. A successfully committed healthy session binding stays sticky.
///
/// All timestamps must come from the same monotonic clock. `Duration` is the
/// default timestamp representation for runtimes that supply elapsed time.
#[derive(Clone, Debug)]
pub struct PoolState<K: Ord + Clone, M: Moment = Duration> {
  active: Vec<K>,
  fallback: Vec<K>,
  failure_cooldown: Duration,
  affinity_policy: Option<AffinityPolicy>,
  cooldowns: BTreeMap<K, Cooldown<M>>,
  affinity: BTreeMap<SmolStr, AffinityEntry<K, M>>,
  active_cursor: usize,
  fallback_cursor: usize,
  affinity_ops: usize,
}

impl<K: Ord + Clone, M: Moment> PoolState<K, M> {
  /// Construct a pool with distinct keys across both tiers.
  pub fn new(
    active: Vec<K>,
    fallback: Vec<K>,
    failure_cooldown: Duration,
    affinity: Option<AffinityPolicy>,
  ) -> Result<Self, PoolError<K>> {
    let mut cooldowns = BTreeMap::new();
    for key in active.iter().chain(&fallback) {
      if cooldowns.insert(key.clone(), Cooldown::default()).is_some() {
        return Err(PoolError::DuplicateBinding(key.clone()));
      }
    }
    Ok(Self {
      active,
      fallback,
      failure_cooldown,
      affinity_policy: affinity,
      cooldowns,
      affinity: BTreeMap::new(),
      active_cursor: 0,
      fallback_cursor: 0,
      affinity_ops: 0,
    })
  }

  pub fn binding_count(&self) -> usize {
    self.cooldowns.len()
  }

  /// Number of retained affinity entries, including expired tombstones.
  ///
  /// Entries older than the retention TTL are removed when looked up and by
  /// an opportunistic sweep every 1024 affinity lookup/record operations.
  pub fn affinity_count(&self) -> usize {
    self.affinity.len()
  }

  /// Select a binding without recording success or changing cooldown history.
  ///
  /// `eligible_ranks` contains only bindings allowed for this request. Keys
  /// outside this pool are ignored. Cooling eligible bindings contribute to
  /// the earliest retry deadline, even when their ranks differ. Successful
  /// selection advances only the chosen tier's round-robin cursor.
  pub fn select(&mut self, now: M, session_id: Option<&str>, eligible_ranks: &BTreeMap<K, i32>) -> Selection<K, M> {
    if let Some(session_id) = session_id.filter(|_| self.affinity_policy.is_some()) {
      if let Some(key) = self.affinity_binding(now, session_id) {
        if eligible_ranks.contains_key(&key) && !is_cooling(&self.cooldowns[&key], now) {
          return Selection::Selected(key);
        }
      }
    }

    let mut earliest_retry = None;
    if let Some(key) = select_tier(
      &self.active,
      &mut self.active_cursor,
      &self.cooldowns,
      now,
      eligible_ranks,
      &mut earliest_retry,
    ) {
      return Selection::Selected(key);
    }
    if let Some(key) = select_tier(
      &self.fallback,
      &mut self.fallback_cursor,
      &self.cooldowns,
      now,
      eligible_ranks,
      &mut earliest_retry,
    ) {
      return Selection::Selected(key);
    }
    match earliest_retry {
      Some(retry_at) => Selection::CoolingDown { retry_at },
      None => Selection::NoEligible,
    }
  }

  /// Commit success for an exact binding and optionally refresh its affinity.
  ///
  /// Success resets the binding's failure history. Invalid keys leave every
  /// part of the state untouched, including affinity retention counters.
  pub fn record_success(&mut self, now: M, session_id: Option<&str>, key: &K) -> Result<(), PoolError<K>> {
    let cooldown = self
      .cooldowns
      .get_mut(key)
      .ok_or_else(|| PoolError::UnknownBinding(key.clone()))?;
    *cooldown = Cooldown::default();
    if let Some(session_id) = session_id.filter(|_| self.affinity_policy.is_some()) {
      self.maybe_sweep_affinity(now);
      self.affinity.insert(
        SmolStr::new(session_id),
        AffinityEntry {
          key: key.clone(),
          stamped_at: now,
        },
      );
    }
    Ok(())
  }

  /// Record a failure and return this binding's new retry deadline.
  ///
  /// Consecutive failures multiply the configured cooldown by 1, 2, 4, 8, 16,
  /// then 32. The multiplier stays capped at 32. Both duration and timestamp
  /// addition saturate; elapsed cooldowns do not reset failure history.
  pub fn record_failure(&mut self, now: M, key: &K) -> Result<M, PoolError<K>> {
    let cooldown = self
      .cooldowns
      .get_mut(key)
      .ok_or_else(|| PoolError::UnknownBinding(key.clone()))?;
    cooldown.consecutive_failures = cooldown.consecutive_failures.saturating_add(1);
    let duration = cooldown_duration(self.failure_cooldown, cooldown.consecutive_failures);
    let retry_at = now.saturating_add(duration);
    cooldown.retry_at = Some(retry_at);
    Ok(retry_at)
  }

  fn affinity_binding(&mut self, now: M, session_id: &str) -> Option<K> {
    self.maybe_sweep_affinity(now);
    let entry = self.affinity.get(session_id)?;
    let policy = self.affinity_policy.expect("affinity lookup requires a policy");
    let age = now.elapsed_since(entry.stamped_at);
    if age < policy.ttl() {
      Some(entry.key.clone())
    } else {
      if age >= policy.retained_ttl() {
        self.affinity.remove(session_id);
      }
      None
    }
  }

  fn maybe_sweep_affinity(&mut self, now: M) {
    self.affinity_ops += 1;
    if self.affinity_ops < AFFINITY_GC_INTERVAL {
      return;
    }
    self.affinity_ops = 0;
    let retained_ttl = self
      .affinity_policy
      .expect("affinity operation requires a policy")
      .retained_ttl();
    self
      .affinity
      .retain(|_, entry| now.elapsed_since(entry.stamped_at) < retained_ttl);
  }
}

fn is_cooling<M: Moment>(cooldown: &Cooldown<M>, now: M) -> bool {
  cooldown.retry_at.is_some_and(|retry_at| retry_at > now)
}

fn select_tier<K: Ord + Clone, M: Moment>(
  accounts: &[K],
  cursor: &mut usize,
  cooldowns: &BTreeMap<K, Cooldown<M>>,
  now: M,
  eligible_ranks: &BTreeMap<K, i32>,
  earliest_retry: &mut Option<M>,
) -> Option<K> {
  let mut candidates = Vec::new();
  let mut best_rank = None;
  for key in accounts {
    let Some(&rank) = eligible_ranks.get(key) else {
      continue;
    };
    let cooldown = &cooldowns[key];
    if let Some(retry_at) = cooldown.retry_at.filter(|retry_at| *retry_at > now) {
      if earliest_retry.is_none_or(|current| retry_at < current) {
        *earliest_retry = Some(retry_at);
      }
      continue;
    }
    match best_rank {
      Some(current) if rank < current => {}
      Some(current) if rank == current => candidates.push(key),
      _ => {
        best_rank = Some(rank);
        candidates.clear();
        candidates.push(key);
      }
    }
  }
  if candidates.is_empty() {
    return None;
  }
  let index = *cursor % candidates.len();
  *cursor = cursor.wrapping_add(1);
  Some((*candidates[index]).clone())
}

fn cooldown_duration(base: Duration, consecutive_failures: u32) -> Duration {
  let exponent = consecutive_failures.saturating_sub(1).min(MAX_COOLDOWN_EXPONENT);
  base.saturating_mul(1_u32 << exponent)
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec;

  fn time(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
  }

  fn pool(affinity: Option<AffinityPolicy>) -> PoolState<&'static str> {
    PoolState::new(vec!["a", "b", "gap", "c"], vec!["fallback"], time(3), affinity).unwrap()
  }

  fn ranks<const N: usize>(values: [(&'static str, i32); N]) -> BTreeMap<&'static str, i32> {
    values.into_iter().collect()
  }

  #[test]
  fn filtered_round_robin_uses_input_order_and_highest_rank_ties() {
    let mut state = pool(None);
    let eligible = ranks([("a", -3), ("b", 7), ("c", 7), ("fallback", i32::MAX)]);
    let decisions: Vec<_> = (0..6).map(|_| state.select(time(0), None, &eligible)).collect();
    assert_eq!(decisions, ["b", "c", "b", "c", "b", "c"].map(Selection::Selected));
    let only_gaps = ranks([("a", 0), ("c", 0)]);
    let decisions: Vec<_> = (0..4).map(|_| state.select(time(0), None, &only_gaps)).collect();
    assert_eq!(decisions, ["a", "c", "a", "c"].map(Selection::Selected));
  }

  #[test]
  fn cooldown_and_tier_rules_recover_at_the_exact_deadline() {
    let mut state = pool(None);
    let eligible = ranks([("a", 1), ("fallback", 100)]);
    assert_eq!(state.select(time(10), None, &eligible), Selection::Selected("a"));
    assert_eq!(state.record_failure(time(10), &"a").unwrap(), time(13));
    assert_eq!(state.select(time(12), None, &eligible), Selection::Selected("fallback"));
    assert_eq!(state.record_failure(time(12), &"fallback").unwrap(), time(15));
    assert_eq!(
      state.select(time(12), None, &eligible),
      Selection::CoolingDown { retry_at: time(13) }
    );
    assert_eq!(state.select(time(13), None, &eligible), Selection::Selected("a"));
  }

  #[test]
  fn earliest_retry_is_global_to_eligible_bindings_in_both_tiers() {
    let mut state = pool(None);
    for _ in 0..2 {
      state.record_failure(time(0), &"a").unwrap();
    }
    for _ in 0..3 {
      state.record_failure(time(0), &"b").unwrap();
    }
    state.record_failure(time(0), &"fallback").unwrap();
    // The fallback deadline is earlier than either eligible active binding.
    assert_eq!(
      state.select(time(1), None, &ranks([("a", 0), ("b", 100), ("fallback", -1)])),
      Selection::CoolingDown { retry_at: time(3) }
    );
    // Removing that binding from request eligibility also removes its deadline.
    assert_eq!(
      state.select(time(1), None, &ranks([("a", 0), ("b", 100)])),
      Selection::CoolingDown { retry_at: time(6) }
    );
    assert_eq!(
      state.select(time(1), None, &ranks([("unknown", 0)])),
      Selection::NoEligible
    );
  }

  #[test]
  fn affinity_is_committed_only_by_success_and_overrides_rank_and_tier() {
    let mut state = pool(Some(AffinityPolicy::new(time(10), time(20))));
    let eligible = ranks([("a", 0), ("b", 0)]);
    assert_eq!(
      state.select(time(0), Some("session"), &eligible),
      Selection::Selected("a")
    );
    assert_eq!(
      state.select(time(0), Some("session"), &eligible),
      Selection::Selected("b")
    );
    state.record_success(time(0), Some("session"), &"b").unwrap();
    assert_eq!(
      state.select(time(1), Some("session"), &ranks([("a", 100), ("b", -100)])),
      Selection::Selected("b")
    );
    // A successfully used fallback can remain sticky even after active recovery.
    state.record_success(time(1), Some("session"), &"fallback").unwrap();
    assert_eq!(
      state.select(time(2), Some("session"), &ranks([("a", 100), ("fallback", -100)])),
      Selection::Selected("fallback")
    );
    state.record_failure(time(2), &"fallback").unwrap();
    assert_eq!(
      state.select(time(3), Some("session"), &ranks([("a", 100), ("fallback", -100)])),
      Selection::Selected("a")
    );
    assert_eq!(
      state.select(time(3), Some("session"), &ranks([("b", 0)])),
      Selection::Selected("b")
    );
  }

  #[test]
  fn exact_affinity_ttl_and_retention_boundaries_are_preserved() {
    let mut state = pool(Some(AffinityPolicy::new(time(10), time(20))));
    let eligible = ranks([("a", 100), ("b", 0)]);
    state.record_success(time(5), Some("session"), &"b").unwrap();
    assert_eq!(
      state.select(time(14), Some("session"), &eligible),
      Selection::Selected("b")
    );
    // TTL expires at equality, but the tombstone is retained for 20 more seconds.
    assert_eq!(
      state.select(time(15), Some("session"), &eligible),
      Selection::Selected("a")
    );
    assert_eq!(state.affinity_count(), 1);
    assert_eq!(
      state.select(time(34), Some("session"), &eligible),
      Selection::Selected("a")
    );
    assert_eq!(state.affinity_count(), 1);
    assert_eq!(
      state.select(time(35), Some("session"), &eligible),
      Selection::Selected("a")
    );
    assert_eq!(state.affinity_count(), 0);
  }

  #[test]
  fn successful_touch_refreshes_expired_affinity_and_clears_failure_history() {
    let mut state = pool(Some(AffinityPolicy::new(time(10), time(20))));
    let eligible = ranks([("a", 100), ("b", 0)]);
    state.record_success(time(0), Some("session"), &"b").unwrap();
    state.record_failure(time(1), &"b").unwrap();
    state.record_failure(time(2), &"b").unwrap();
    assert_eq!(state.record_failure(time(3), &"b").unwrap(), time(15));
    state.record_success(time(11), Some("session"), &"b").unwrap();
    assert_eq!(
      state.select(time(20), Some("session"), &eligible),
      Selection::Selected("b")
    );
    assert_eq!(state.record_failure(time(20), &"b").unwrap(), time(23));
  }

  #[test]
  fn opportunistic_sweep_reclaims_other_sessions_after_retention() {
    let mut state = pool(Some(AffinityPolicy::new(time(1), time(2))));
    state.record_success(time(0), Some("stale-one"), &"a").unwrap();
    state.record_success(time(0), Some("stale-two"), &"b").unwrap();
    let eligible = ranks([("a", 0)]);
    for _ in 0..AFFINITY_GC_INTERVAL {
      state.select(time(3), Some("unrelated-session"), &eligible);
    }
    assert_eq!(state.affinity_count(), 0);
  }

  #[test]
  fn zero_ttl_disables_stickiness_and_zero_retention_frees_on_lookup() {
    let mut state = pool(Some(AffinityPolicy::new(Duration::ZERO, Duration::ZERO)));
    state.record_success(time(1), Some("session"), &"b").unwrap();
    assert_eq!(
      state.select(time(1), Some("session"), &ranks([("a", 100), ("b", 0)])),
      Selection::Selected("a")
    );
    assert_eq!(state.affinity_count(), 0);
  }

  #[test]
  fn unknown_mutations_leave_counters_affinity_and_cooldowns_untouched() {
    let mut state = pool(Some(AffinityPolicy::new(time(10), time(20))));
    let retry_at = state.record_failure(time(0), &"a").unwrap();
    state.record_success(time(0), Some("session"), &"b").unwrap();
    let old = state.clone();
    assert_eq!(
      state.record_failure(time(1), &"missing"),
      Err(PoolError::UnknownBinding("missing"))
    );
    assert_eq!(
      state.record_success(time(1), Some("new-session"), &"missing"),
      Err(PoolError::UnknownBinding("missing"))
    );
    assert_eq!(state.binding_count(), old.binding_count());
    assert_eq!(state.affinity_count(), old.affinity_count());
    assert_eq!(state.affinity_ops, old.affinity_ops);
    assert_eq!(state.active_cursor, old.active_cursor);
    assert_eq!(state.fallback_cursor, old.fallback_cursor);
    assert_eq!(
      state.cooldowns[&"a"].consecutive_failures,
      old.cooldowns[&"a"].consecutive_failures
    );
    assert_eq!(state.cooldowns[&"a"].retry_at, Some(retry_at));
    assert_eq!(
      state.select(time(1), None, &ranks([("a", 0)])),
      Selection::CoolingDown { retry_at }
    );
    assert!(!state.affinity.contains_key("new-session"));
  }

  #[test]
  fn duplicate_keys_are_rejected_across_and_within_tiers() {
    for (active, fallback) in [
      (vec!["a", "a"], vec![]),
      (vec![], vec!["a", "a"]),
      (vec!["a"], vec!["a"]),
    ] {
      assert!(matches!(
        PoolState::<_, Duration>::new(active, fallback, time(1), None),
        Err(PoolError::DuplicateBinding("a"))
      ));
    }
    let mut empty = PoolState::<&str>::new(vec![], vec![], time(1), None).unwrap();
    assert_eq!(empty.binding_count(), 0);
    assert_eq!(empty.select(time(0), None, &BTreeMap::new()), Selection::NoEligible);
  }

  #[test]
  fn backoff_is_capped_and_duration_and_deadline_arithmetic_saturate() {
    let base = time(3);
    for (failures, expected) in [(1, 3), (2, 6), (5, 48), (6, 96), (7, 96), (u32::MAX, 96)] {
      assert_eq!(cooldown_duration(base, failures), time(expected));
    }
    let mut state = PoolState::new(vec!["a"], vec![], Duration::MAX, None).unwrap();
    assert_eq!(state.record_failure(time(1), &"a").unwrap(), Duration::MAX);
    assert_eq!(state.record_failure(time(2), &"a").unwrap(), Duration::MAX);
    assert_eq!(
      state.select(Duration::MAX, None, &ranks([("a", 0)])),
      Selection::Selected("a")
    );
    let policy = AffinityPolicy::new(Duration::MAX, time(1));
    assert_eq!(policy.retained_ttl(), Duration::MAX);
  }

  #[test]
  fn zero_cooldown_and_cursor_wrap_are_well_defined() {
    let mut state = PoolState::new(vec!["a", "b"], vec![], Duration::ZERO, None).unwrap();
    state.active_cursor = usize::MAX;
    let eligible = ranks([("a", 0), ("b", 0)]);
    assert_eq!(state.select(time(3), None, &eligible), Selection::Selected("b"));
    assert_eq!(state.active_cursor, 0);
    assert_eq!(state.select(time(3), None, &eligible), Selection::Selected("a"));
    assert_eq!(state.record_failure(time(3), &"a").unwrap(), time(3));
    assert_eq!(
      state.select(time(3), None, &ranks([("a", 0)])),
      Selection::Selected("a")
    );
  }

  #[test]
  fn different_pools_do_not_share_cooldown_cursor_or_affinity() {
    let policy = Some(AffinityPolicy::new(time(10), time(20)));
    let mut first = pool(policy);
    let mut second = pool(policy);
    let eligible = ranks([("a", 0), ("b", 0)]);
    first.select(time(0), None, &eligible);
    first.record_success(time(0), Some("session"), &"b").unwrap();
    first.record_failure(time(0), &"a").unwrap();
    assert_eq!(
      first.select(time(0), Some("session"), &eligible),
      Selection::Selected("b")
    );
    assert_eq!(
      second.select(time(0), Some("session"), &eligible),
      Selection::Selected("a")
    );
    assert_eq!(second.affinity_count(), 0);
  }
}
