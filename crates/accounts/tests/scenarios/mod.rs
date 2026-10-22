use alloc::{collections::BTreeMap, vec, vec::Vec};
use core::time::Duration;
use tokn_accounts::pool_state::{AffinityPolicy, PoolError, PoolState, Selection};
use tokn_core::types::{Bytes, SmolStr};

fn at(seconds: u64) -> Duration {
  Duration::from_secs(seconds)
}

fn selected(result: Selection<SmolStr, Duration>) -> SmolStr {
  match result {
    Selection::Selected(key) => key,
    other => panic!("expected selected account, got {other:?}"),
  }
}

/// Identical source is run with the native std types and the local Rc types.
/// Explicit monotonic time makes cooldown and affinity decisions deterministic.
pub fn pool_selection_transcript() {
  let a = SmolStr::new("provider-a/account-with-a-long-identity");
  let b = SmolStr::new("provider-b/account-with-a-long-identity");
  let c = SmolStr::new("provider-c/account-with-a-long-identity");
  let fallback = SmolStr::new("fallback/account-with-a-long-identity");
  let session = "a-long-session-identity-with-Unicode-🌍";
  let mut pool = PoolState::new(
    vec![a.clone(), b.clone(), c.clone()],
    vec![fallback.clone()],
    at(2),
    Some(AffinityPolicy::new(at(5), at(3))),
  )
  .unwrap();
  let mut ranks = BTreeMap::from([
    (a.clone(), 100),
    (b.clone(), 100),
    (c.clone(), 0),
    (fallback.clone(), 1000),
  ]);
  let mut transcript = Vec::new();
  for _ in 0..3 {
    transcript.push(selected(pool.select(at(0), None, &ranks)));
  }
  pool.record_success(at(0), Some(session), &b).unwrap();
  ranks.insert(a.clone(), 200);
  transcript.push(selected(pool.select(at(0), Some(session), &ranks)));
  assert_eq!(pool.record_failure(at(1), &b).unwrap(), at(3));
  transcript.push(selected(pool.select(at(1), Some(session), &ranks)));
  pool.record_failure(at(1), &a).unwrap();
  transcript.push(selected(pool.select(at(1), Some(session), &ranks)));
  pool.record_failure(at(1), &c).unwrap();
  transcript.push(selected(pool.select(at(1), Some(session), &ranks)));
  pool.record_failure(at(1), &fallback).unwrap();
  assert_eq!(
    pool.select(at(1), Some(session), &ranks),
    Selection::CoolingDown { retry_at: at(3) }
  );

  pool.record_success(at(1), Some(session), &a).unwrap();
  transcript.push(selected(pool.select(at(1), Some(session), &ranks)));
  ranks.insert(b.clone(), 300);
  // At the exact live TTL boundary affinity no longer overrides ranking.
  transcript.push(selected(pool.select(at(6), Some(session), &ranks)));
  assert_eq!(
    transcript,
    [
      a.clone(),
      b.clone(),
      a.clone(),
      b.clone(),
      a.clone(),
      c,
      fallback.clone(),
      a.clone(),
      b.clone()
    ]
  );
  assert_eq!(pool.affinity_count(), 1);
  pool.select(at(9), Some(session), &ranks);
  assert_eq!(pool.affinity_count(), 0);

  let unknown = SmolStr::new("unknown");
  assert_eq!(
    pool.record_failure(at(9), &unknown),
    Err(PoolError::UnknownBinding(unknown.clone()))
  );
  assert_eq!(
    pool.record_success(at(9), Some("unknown-session"), &unknown),
    Err(PoolError::UnknownBinding(unknown))
  );
  assert_eq!(pool.binding_count(), 4);
  assert_eq!(pool.affinity_count(), 0);
  assert_eq!(pool.select(at(9), None, &BTreeMap::new()), Selection::NoEligible);

  // A fresh pool over the same keys owns independent cursors and cooldowns.
  let mut independent = PoolState::new(vec![a.clone(), b.clone()], vec![fallback.clone()], at(2), None).unwrap();
  for _ in 0..2 {
    independent.record_failure(at(0), &a).unwrap();
  }
  for _ in 0..3 {
    independent.record_failure(at(0), &b).unwrap();
  }
  independent.record_failure(at(0), &fallback).unwrap();
  assert_eq!(
    independent.select(at(1), None, &ranks),
    Selection::CoolingDown { retry_at: at(2) }
  );
  assert_eq!(selected(pool.select(at(9), None, &ranks)), b);

  let mut backoff = PoolState::new(vec![a.clone()], vec![], at(2), None).unwrap();
  for delay in [2, 4, 8, 16, 32, 64, 64, 64] {
    assert_eq!(backoff.record_failure(at(10), &a).unwrap(), at(10 + delay));
  }
}

/// Check the concrete backend contract while code-generating clone, slice,
/// and destruction on the embedded target as well as the standard host.
pub fn storage_round_trip() {
  let long = SmolStr::new("a-shared-string-longer-than-inline-storage-🌍");
  let cloned = long.clone();
  assert_eq!(long.as_str().as_ptr(), cloned.as_str().as_ptr());
  drop(long);
  assert_eq!(cloned.as_str(), "a-shared-string-longer-than-inline-storage-🌍");

  let payload = vec![11, 22, 33, 44, 55];
  let pointer = payload.as_ptr();
  let bytes = Bytes::from(payload);
  assert_eq!(bytes.as_ptr(), pointer);
  let view = bytes.slice(1..4);
  let clone = view.clone();
  assert_eq!(view.as_ptr(), clone.as_ptr());
  assert_eq!(view.as_ptr(), bytes[1..].as_ptr());
  drop(bytes);
  drop(view);
  assert_eq!(&clone[..], [22, 33, 44]);
}
