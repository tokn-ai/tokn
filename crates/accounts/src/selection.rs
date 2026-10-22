//! Shared v2 provider/model routing decisions used by gateway and injected execution.
//!
//! These rules are independent of account ownership, synchronization, transport,
//! and time. Pool mutation belongs to [`crate::pool_state::PoolState`].

use alloc::{
  collections::{BTreeMap, BTreeSet},
  string::{String, ToString},
  vec::Vec,
};
use core::fmt;
use tokn_core::{provider::Endpoint, types::SmolStr};
use tokn_policy::{
  DriverId, ManagedRoute, ModelSelector, OperationPolicy, ProviderId, ProviderSelector, QualificationNamespace,
  RelayDestination, RoutePlan,
};

const BUILTIN_OPERATION_ORDER: [Endpoint; 3] = [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages];

/// Capability view of one provider/account binding. Implementations may borrow
/// in-memory metadata or expose the standard provider registry.
pub trait CandidateBinding {
  fn provider_id(&self) -> &ProviderId;
  fn driver_id(&self) -> &DriverId;
  fn supports(&self, model: &str, operation: Endpoint) -> bool;
  fn has_endpoint(&self, model: &str, operation: Endpoint) -> bool;
}

/// Request inputs shared by managed and fixed-provider account routing.
#[derive(Clone, Copy, Debug)]
pub struct SelectionRequest<'a> {
  pub model: &'a str,
  pub endpoint: Endpoint,
  pub allowed_providers: Option<&'a BTreeSet<String>>,
}

/// A route decision, independent of how the selected binding is owned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteSelection<T> {
  Selected {
    binding: T,
    model: SmolStr,
    endpoint: Endpoint,
  },
  NoAccount,
  ProviderAccessDenied,
}

/// Select an account for a managed route or a fixed-provider relay.
///
/// `bindings` must contain only members of the selected pool. In particular,
/// bindings in other pools must not contribute to access-denial diagnostics.
/// Eligibility and ranking finish before each call to `acquire`, allowing the
/// platform to lock or borrow pool state only for the selection transaction.
/// The callback returns `None` for both cooldown and exhaustion; ownership,
/// synchronization, clocks, and cooldown deadlines remain with the platform.
///
/// Relay destination admission and client-owned credentials are handled by the
/// caller. An origin-based relay must be resolved before using account routing.
pub fn select_route<'a, K, B, I, T, F>(
  route: &RoutePlan,
  request: SelectionRequest<'_>,
  bindings: I,
  mut acquire: F,
) -> Result<RouteSelection<T>, SelectionError>
where
  K: Ord,
  B: CandidateBinding + ?Sized + 'a,
  I: Iterator<Item = (K, &'a B)> + Clone,
  F: FnMut(&BTreeMap<K, i32>) -> Option<T>,
{
  let managed = match route {
    RoutePlan::Managed(managed) => managed,
    RoutePlan::Relay(relay) => {
      let RelayDestination::FixedProvider(provider) = relay.destination() else {
        return Err(invalid_request("origin-based relay requires an admitted destination"));
      };
      if !provider_allowed(provider.as_str(), request.allowed_providers) {
        return Ok(RouteSelection::ProviderAccessDenied);
      }
      let ranks = bindings
        .filter(|(_, binding)| binding.provider_id() == provider && route.allows_provider(provider))
        .map(|(key, _)| (key, 0))
        .collect();
      return Ok(match acquire(&ranks) {
        Some(binding) => RouteSelection::Selected {
          binding,
          model: SmolStr::new(request.model),
          endpoint: request.endpoint,
        },
        None => RouteSelection::NoAccount,
      });
    }
  };
  let candidates = model_candidates(managed, request.model)?;
  let operations = operation_candidates(managed.operation(), request.endpoint);
  let mut matching_binding_exists = false;
  let mut allowed_matching_binding_exists = false;
  for candidate in candidates {
    for evidence in candidate.evidence.order().iter().copied() {
      for endpoint in operations.iter().copied() {
        let mut ranks = BTreeMap::new();
        for (key, binding) in bindings.clone() {
          if !route.allows_provider(binding.provider_id())
            || !managed_binding_matches(managed, &candidate, evidence, endpoint, binding)
          {
            continue;
          }
          matching_binding_exists = true;
          if provider_allowed(binding.provider_id().as_str(), request.allowed_providers) {
            allowed_matching_binding_exists = true;
            ranks.insert(key, model_provider_score(managed, &candidate, binding));
          }
        }
        if let Some(binding) = acquire(&ranks) {
          return Ok(RouteSelection::Selected {
            binding,
            model: candidate.model.clone(),
            endpoint,
          });
        }
      }
    }
  }
  Ok(if matching_binding_exists && !allowed_matching_binding_exists {
    RouteSelection::ProviderAccessDenied
  } else {
    RouteSelection::NoAccount
  })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectionError(pub SmolStr);

impl fmt::Display for SelectionError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    self.0.fmt(f)
  }
}
impl core::error::Error for SelectionError {}

fn invalid_request(message: impl AsRef<str>) -> SelectionError {
  SelectionError(SmolStr::new(message.as_ref()))
}

#[derive(Clone, Debug)]
pub struct ModelCandidate {
  pub model: SmolStr,
  pub constraint: ProviderConstraint,
  pub evidence: DiscoveryEvidence,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DiscoveryEvidence {
  /// Family expansion requires evidence so its configured fallback order remains meaningful.
  Required,
  /// Automatic concrete routing prefers evidence, then lets a compatible upstream decide.
  Preferred,
  /// Explicit destinations already identify where the request should go.
  Ignored,
}

impl DiscoveryEvidence {
  pub fn order(self) -> &'static [DiscoveryEvidence] {
    match self {
      Self::Required => &[Self::Required],
      Self::Preferred => &[Self::Required, Self::Ignored],
      Self::Ignored => &[Self::Ignored],
    }
  }
}

#[derive(Clone, Debug)]
pub enum ProviderConstraint {
  Any,
  Driver(DriverId),
  Provider(ProviderId),
}

impl ProviderConstraint {
  pub fn matches(&self, binding: &(impl CandidateBinding + ?Sized)) -> bool {
    match self {
      Self::Any => true,
      Self::Driver(driver) => binding.driver_id() == driver,
      Self::Provider(provider) => binding.provider_id() == provider,
    }
  }
}

pub fn model_candidates(route: &ManagedRoute, requested_model: &str) -> Result<Vec<ModelCandidate>, SelectionError> {
  let fixed_provider = matches!(route.target().provider(), ProviderSelector::Fixed(_));
  match route.target().model() {
    ModelSelector::Capability => Ok(vec![ModelCandidate {
      model: SmolStr::new(requested_model),
      constraint: ProviderConstraint::Any,
      evidence: if fixed_provider {
        DiscoveryEvidence::Ignored
      } else {
        DiscoveryEvidence::Preferred
      },
    }]),
    ModelSelector::Qualified { namespace } => {
      let (qualifier, model) = requested_model.split_once('/').ok_or_else(|| {
        invalid_request(format!(
          "{}-qualified model must use '<qualifier>/<model>'",
          qualification_name(*namespace)
        ))
      })?;
      if model.is_empty() || model.trim() != model {
        return Err(invalid_request("qualified model name is empty or non-canonical"));
      }
      let constraint = match namespace {
        QualificationNamespace::Driver => {
          ProviderConstraint::Driver(DriverId::new(qualifier).map_err(|error| invalid_request(error.to_string()))?)
        }
        QualificationNamespace::Provider => {
          ProviderConstraint::Provider(ProviderId::new(qualifier).map_err(|error| invalid_request(error.to_string()))?)
        }
      };
      Ok(vec![ModelCandidate {
        model: SmolStr::new(model),
        constraint,
        evidence: DiscoveryEvidence::Ignored,
      }])
    }
    ModelSelector::Family(families) => {
      let Some(family) = families.iter().find(|family| family.name() == requested_model) else {
        return Ok(vec![ModelCandidate {
          model: SmolStr::new(requested_model),
          constraint: ProviderConstraint::Any,
          evidence: if fixed_provider {
            DiscoveryEvidence::Ignored
          } else {
            DiscoveryEvidence::Preferred
          },
        }]);
      };
      Ok(
        family
          .members()
          .iter()
          .cloned()
          .map(|model| ModelCandidate {
            model,
            constraint: ProviderConstraint::Any,
            evidence: DiscoveryEvidence::Required,
          })
          .collect(),
      )
    }
  }
}

pub fn managed_binding_matches(
  route: &ManagedRoute,
  candidate: &ModelCandidate,
  evidence: DiscoveryEvidence,
  operation: Endpoint,
  binding: &(impl CandidateBinding + ?Sized),
) -> bool {
  let route_provider_matches = match route.target().provider() {
    ProviderSelector::Any => true,
    ProviderSelector::Fixed(provider) => binding.provider_id() == provider,
  };
  route_provider_matches
    && candidate.constraint.matches(binding)
    && match evidence {
      DiscoveryEvidence::Required => binding.supports(candidate.model.as_str(), operation),
      DiscoveryEvidence::Preferred => unreachable!("preferred discovery expands into concrete matching passes"),
      DiscoveryEvidence::Ignored => binding.has_endpoint(candidate.model.as_str(), operation),
    }
}

pub fn model_provider_score(
  route: &ManagedRoute,
  candidate: &ModelCandidate,
  binding: &(impl CandidateBinding + ?Sized),
) -> i32 {
  if matches!(route.target().model(), ModelSelector::Qualified { .. }) {
    return 0;
  }
  route.provider_score(candidate.model.as_str(), binding.provider_id())
}

pub fn operation_candidates(policy: OperationPolicy, requested: Endpoint) -> Vec<Endpoint> {
  if policy == OperationPolicy::Preserve {
    return vec![requested];
  }
  core::iter::once(requested)
    .chain(
      BUILTIN_OPERATION_ORDER
        .into_iter()
        .filter(|operation| *operation != requested),
    )
    .collect()
}

pub fn provider_allowed(provider_id: &str, allowed: Option<&BTreeSet<String>>) -> bool {
  allowed.is_none_or(|providers| providers.contains(provider_id))
}

pub fn qualification_name(namespace: QualificationNamespace) -> &'static str {
  match namespace {
    QualificationNamespace::Driver => "driver",
    QualificationNamespace::Provider => "provider",
  }
}

#[cfg(feature = "std")]
impl CandidateBinding for crate::link::ProviderBinding {
  fn provider_id(&self) -> &ProviderId {
    self.provider_id()
  }
  fn driver_id(&self) -> &DriverId {
    self.driver_id()
  }
  fn supports(&self, model: &str, operation: Endpoint) -> bool {
    self.driver().supports(model, operation)
  }
  fn has_endpoint(&self, model: &str, operation: Endpoint) -> bool {
    self.driver().has_endpoint(model, operation)
  }
}

#[cfg(feature = "std")]
impl<T: CandidateBinding + ?Sized> CandidateBinding for alloc::sync::Arc<T> {
  fn provider_id(&self) -> &ProviderId {
    (**self).provider_id()
  }
  fn driver_id(&self) -> &DriverId {
    (**self).driver_id()
  }
  fn supports(&self, model: &str, operation: Endpoint) -> bool {
    (**self).supports(model, operation)
  }
  fn has_endpoint(&self, model: &str, operation: Endpoint) -> bool {
    (**self).has_endpoint(model, operation)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::pool_state::{AffinityPolicy, PoolState, Selection};
  use core::time::Duration;
  use tokn_policy::{ManagedRetry, ManagedTarget, ModelFamily, RelayCredentials, RelayRetry, RelayRoute};

  struct Binding {
    provider: ProviderId,
    driver: DriverId,
    models: Vec<&'static str>,
    endpoints: Vec<Endpoint>,
  }

  impl CandidateBinding for Binding {
    fn provider_id(&self) -> &ProviderId {
      &self.provider
    }
    fn driver_id(&self) -> &DriverId {
      &self.driver
    }
    fn supports(&self, model: &str, operation: Endpoint) -> bool {
      self.models.contains(&model) && self.has_endpoint(model, operation)
    }
    fn has_endpoint(&self, _: &str, operation: Endpoint) -> bool {
      self.endpoints.contains(&operation)
    }
  }

  fn binding(provider: &str, models: &[&'static str], endpoints: &[Endpoint]) -> Binding {
    Binding {
      provider: ProviderId::new(provider).unwrap(),
      driver: DriverId::new("custom-driver").unwrap(),
      models: models.to_vec(),
      endpoints: endpoints.to_vec(),
    }
  }

  fn managed(model: ModelSelector) -> RoutePlan {
    RoutePlan::Managed(ManagedRoute::new(
      ManagedTarget::new(ProviderSelector::Any, model),
      OperationPolicy::TranslateCompatible,
      None,
      ManagedRetry::Never,
    ))
  }

  fn request(model: &str) -> SelectionRequest<'_> {
    SelectionRequest {
      model,
      endpoint: Endpoint::Responses,
      allowed_providers: None,
    }
  }

  fn select(
    route: &RoutePlan,
    request: SelectionRequest<'_>,
    bindings: &[Binding],
    state: &mut PoolState<usize>,
    session: Option<&str>,
  ) -> RouteSelection<usize> {
    select_route(route, request, bindings.iter().enumerate(), |ranks| {
      match state.select(Duration::ZERO, session, ranks) {
        Selection::Selected(key) => Some(key),
        Selection::CoolingDown { .. } | Selection::NoEligible => None,
      }
    })
    .unwrap()
  }

  fn pool(active: Vec<usize>, fallback: Vec<usize>) -> PoolState<usize> {
    PoolState::new(
      active,
      fallback,
      Duration::from_secs(60),
      Some(AffinityPolicy::new(Duration::from_secs(60), Duration::ZERO)),
    )
    .unwrap()
  }

  #[test]
  fn discovery_precedes_operation_fallback_and_unproven_providers() {
    let bindings = [
      binding("unproven", &[], &[Endpoint::Responses]),
      binding("discovered", &["model"], &[Endpoint::Messages]),
    ];
    let route = managed(ModelSelector::Capability);
    let mut state = pool(vec![0, 1], vec![]);
    assert_eq!(
      select(&route, request("model"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 1,
        model: "model".into(),
        endpoint: Endpoint::Messages,
      }
    );
    state.record_failure(Duration::ZERO, &1).unwrap();
    assert_eq!(
      select(&route, request("model"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 0,
        model: "model".into(),
        endpoint: Endpoint::Responses,
      }
    );
  }

  #[test]
  fn families_keep_member_order_and_require_discovery() {
    let bindings = [
      binding("first-provider", &["first"], &[Endpoint::Messages]),
      binding("second-provider", &["second"], &[Endpoint::Responses]),
      binding("unproven", &[], &[Endpoint::Responses]),
    ];
    let route = managed(ModelSelector::Family(
      vec![ModelFamily::new("family", ["first", "second"])].into_boxed_slice(),
    ));
    let mut state = pool(vec![0, 1, 2], vec![]);
    assert_eq!(
      select(&route, request("family"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 0,
        model: "first".into(),
        endpoint: Endpoint::Messages,
      }
    );
    state.record_failure(Duration::ZERO, &0).unwrap();
    assert_eq!(
      select(&route, request("family"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 1,
        model: "second".into(),
        endpoint: Endpoint::Responses,
      }
    );
    state.record_failure(Duration::ZERO, &1).unwrap();
    assert_eq!(
      select(&route, request("family"), &bindings, &mut state, None),
      RouteSelection::NoAccount
    );
  }

  #[test]
  fn provider_qualification_preserves_custom_binding_and_strips_qualifier() {
    let bindings = [
      binding("other", &[], &[Endpoint::Responses]),
      binding("custom", &[], &[Endpoint::Responses]),
    ];
    let route = managed(ModelSelector::Qualified {
      namespace: QualificationNamespace::Provider,
    });
    let mut state = pool(vec![0, 1], vec![]);
    assert_eq!(
      select(&route, request("custom/unlisted"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 1,
        model: "unlisted".into(),
        endpoint: Endpoint::Responses,
      }
    );
    let error: Result<RouteSelection<usize>, _> =
      select_route(&route, request("unqualified"), bindings.iter().enumerate(), |_| {
        panic!("invalid request must not acquire a binding")
      });
    assert!(error.is_err());
  }

  #[test]
  fn ranks_keep_active_tier_precedence_and_successful_session_affinity() {
    let bindings = [
      binding("neutral", &["model"], &[Endpoint::Responses]),
      binding("preferred", &["model"], &[Endpoint::Responses]),
      binding("fallback", &["model"], &[Endpoint::Responses]),
    ];
    let RoutePlan::Managed(route) = managed(ModelSelector::Capability) else {
      unreachable!();
    };
    let route = RoutePlan::Managed(route.with_model_scores(BTreeMap::from([(
      "model".into(),
      BTreeMap::from([
        (ProviderId::new("preferred").unwrap(), 10),
        (ProviderId::new("fallback").unwrap(), 100),
      ]),
    )])));
    let mut state = pool(vec![0, 1], vec![2]);
    let chosen = |binding| RouteSelection::Selected {
      binding,
      model: "model".into(),
      endpoint: Endpoint::Responses,
    };
    assert_eq!(select(&route, request("model"), &bindings, &mut state, None), chosen(1));
    state.record_success(Duration::ZERO, Some("session"), &0).unwrap();
    assert_eq!(
      select(&route, request("model"), &bindings, &mut state, Some("session")),
      chosen(0)
    );
    state.record_failure(Duration::ZERO, &0).unwrap();
    state.record_failure(Duration::ZERO, &1).unwrap();
    assert_eq!(select(&route, request("model"), &bindings, &mut state, None), chosen(2));
  }

  #[test]
  fn access_denial_does_not_replace_exhaustion_or_route_restrictions() {
    let bindings = [binding("custom", &["model"], &[Endpoint::Responses])];
    let route = managed(ModelSelector::Capability);
    let mut state = pool(vec![0], vec![]);
    state.record_failure(Duration::ZERO, &0).unwrap();
    let allowed = BTreeSet::from(["custom".to_string()]);
    let denied = BTreeSet::new();
    let allowed_request = SelectionRequest {
      allowed_providers: Some(&allowed),
      ..request("model")
    };
    assert_eq!(
      select(&route, allowed_request, &bindings, &mut state, None),
      RouteSelection::NoAccount
    );
    let denied_request = SelectionRequest {
      allowed_providers: Some(&denied),
      ..request("model")
    };
    assert_eq!(
      select(&route, denied_request, &bindings, &mut state, None),
      RouteSelection::ProviderAccessDenied
    );
    assert_eq!(
      select(&route, denied_request, &[], &mut state, None),
      RouteSelection::NoAccount
    );
    let excluded = route.with_providers(Some(BTreeSet::from([ProviderId::new("other").unwrap()])));
    assert_eq!(
      select(&excluded, denied_request, &bindings, &mut state, None),
      RouteSelection::NoAccount
    );
  }

  #[test]
  fn fixed_relay_preserves_opaque_model_and_operation_without_capability_checks() {
    let bindings = [binding("custom", &[], &[])];
    let route = RoutePlan::Relay(RelayRoute::new(
      RelayDestination::FixedProvider(ProviderId::new("custom").unwrap()),
      RelayCredentials::AccountPool,
      None,
      RelayRetry::Never,
    ));
    let mut state = pool(vec![0], vec![]);
    assert_eq!(
      select(&route, request("opaque-model"), &bindings, &mut state, None),
      RouteSelection::Selected {
        binding: 0,
        model: "opaque-model".into(),
        endpoint: Endpoint::Responses,
      }
    );
    let denied = BTreeSet::new();
    let result: RouteSelection<usize> = select_route(
      &route,
      SelectionRequest {
        allowed_providers: Some(&denied),
        ..request("opaque-model")
      },
      bindings.iter().enumerate(),
      |_| panic!("denied relay must not acquire a binding"),
    )
    .unwrap();
    assert_eq!(result, RouteSelection::ProviderAccessDenied);
  }
}
