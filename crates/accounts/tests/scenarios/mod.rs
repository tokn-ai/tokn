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

/// Compile and link actual v2 configuration, then run the same route decisions
/// with the host and alloc-only binding/storage backends.
pub fn configured_routing_transcript() {
  use tokn_accounts::link::portable::{
    link_pool_metadata, link_provider_metadata, BindingMetadata, DestinationMetadata,
  };
  use tokn_accounts::selection::{select_route, CandidateBinding, RouteSelection, SelectionRequest};
  use tokn_core::{account::AccountConfig, Endpoint};
  use tokn_policy::{DriverId, ProfileId, ProviderId};

  struct Candidate<'a> {
    binding: &'a BindingMetadata,
    destination: &'a DestinationMetadata,
  }
  impl CandidateBinding for Candidate<'_> {
    fn provider_id(&self) -> &ProviderId {
      self.binding.provider_id()
    }
    fn driver_id(&self) -> &DriverId {
      self.binding.driver_id()
    }
    fn supports(&self, model: &str, endpoint: Endpoint) -> bool {
      model == "discovered-model" && self.has_endpoint(model, endpoint)
    }
    fn has_endpoint(&self, model: &str, endpoint: Endpoint) -> bool {
      tokn_accounts::native::supports_model_endpoint(
        self.driver_id().as_str(),
        model,
        endpoint,
        self.destination.base_url().as_str(),
      )
    }
  }

  let raw = serde_json::from_str(
    r#"{
      "schema_version": 2,
      "profiles": {
        "default": {
          "route": "qualified",
          "account_pool": {
            "accounts": ["active", "fallback", "disabled"],
            "failure_cooldown_secs": 2
          }
        },
        "independent": {
          "route": "qualified",
          "account_pool": { "accounts": ["active", "fallback"], "failure_cooldown_secs": 2 }
        },
        "outside": {
          "route": "qualified",
          "account_pool": { "accounts": ["outside"] }
        }
      },
      "routes": {
        "qualified": {
          "kind": "managed",
          "providers": ["*"],
          "provider": { "kind": "any" },
          "model": { "kind": "qualified", "namespace": "provider" },
          "operation": "translate_compatible"
        }
      },
      "providers": {
        "primary": { "driver": "llama-cpp", "base_url": "https://primary.example/v1" },
        "outside": { "driver": "llama-cpp", "base_url": "https://outside.example/v1" }
      }
    }"#,
  )
  .unwrap();
  let plan = tokn_config::v2::compile_execution(&raw).unwrap();
  let accounts: Vec<AccountConfig> = serde_json::from_str(
    r#"[
      { "id": "disabled", "provider": "primary", "enabled": false },
      { "id": "fallback", "provider": "primary", "tier": "fallback" },
      { "id": "outside", "provider": "outside" },
      { "id": "active", "provider": "primary" }
    ]"#,
  )
  .unwrap();
  let graph = link_provider_metadata(&plan, &accounts, tokn_accounts::native::provider_default_base_url).unwrap();
  let pools = link_pool_metadata(&plan, &graph).unwrap();
  let candidates = graph
    .bindings()
    .iter()
    .map(|binding| Candidate {
      binding,
      destination: graph.destination(binding.provider_id()).unwrap(),
    })
    .collect::<Vec<_>>();
  let profile = |name| plan.profile(&ProfileId::new(name).unwrap()).unwrap();
  let main_pool = &pools[profile("default").account_pool().unwrap()];
  let independent_pool = &pools[profile("independent").account_pool().unwrap()];
  let outside_pool = &pools[profile("outside").account_pool().unwrap()];
  let route = plan.route(profile("default").route()).unwrap();
  let make_state = |metadata: &tokn_accounts::link::portable::PoolMetadata| {
    PoolState::new(
      metadata.active().to_vec(),
      metadata.fallback().to_vec(),
      metadata.failure_cooldown(),
      metadata
        .session_affinity()
        .map(|policy| AffinityPolicy::new(policy.ttl(), policy.expired_retention())),
    )
    .unwrap()
  };
  let mut state = make_state(main_pool);
  let mut independent = make_state(independent_pool);
  let mut outside = make_state(outside_pool);
  assert_eq!(main_pool.active().len(), 1);
  assert_eq!(main_pool.fallback().len(), 1);
  assert_eq!(graph.accounts().len(), 4);
  assert_eq!(graph.bindings().len(), 3);
  assert!(graph.account("disabled").unwrap().binding_index().is_none());

  let decide =
    |metadata: &tokn_accounts::link::portable::PoolMetadata, state: &mut PoolState<usize>, now, model: &str| {
      select_route(
        route,
        SelectionRequest {
          model,
          endpoint: Endpoint::Responses,
          allowed_providers: None,
        },
        metadata
          .active()
          .iter()
          .chain(metadata.fallback())
          .map(|&index| (index, &candidates[index])),
        |ranks| match state.select(now, None, ranks) {
          Selection::Selected(index) => Some(index),
          Selection::CoolingDown { .. } | Selection::NoEligible => None,
        },
      )
      .unwrap()
    };
  let record = |selection: RouteSelection<usize>| {
    let RouteSelection::Selected {
      binding,
      model,
      endpoint,
    } = selection
    else {
      panic!("expected a configured route selection");
    };
    (
      SmolStr::new(graph.bindings()[binding].key().account_id()),
      model,
      endpoint,
    )
  };
  let mut transcript = vec![record(decide(main_pool, &mut state, at(0), "primary/unlisted"))];
  state.record_failure(at(0), &main_pool.active()[0]).unwrap();
  transcript.push(record(decide(main_pool, &mut state, at(0), "primary/unlisted")));
  state.record_failure(at(0), &main_pool.fallback()[0]).unwrap();
  assert_eq!(
    decide(main_pool, &mut state, at(0), "primary/unlisted"),
    RouteSelection::NoAccount
  );
  transcript.push(record(decide(
    independent_pool,
    &mut independent,
    at(0),
    "primary/unlisted",
  )));
  assert_eq!(
    decide(main_pool, &mut state, at(0), "outside/unlisted"),
    RouteSelection::NoAccount
  );
  transcript.push(record(decide(outside_pool, &mut outside, at(0), "outside/unlisted")));
  transcript.push(record(decide(main_pool, &mut state, at(2), "primary/unlisted")));
  assert_eq!(
    transcript,
    ["active", "fallback", "active", "outside", "active"].map(|account| (
      SmolStr::new(account),
      SmolStr::new("unlisted"),
      Endpoint::ChatCompletions,
    ))
  );
}
