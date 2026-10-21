//! Shared provider/model routing decisions used by all execution platforms.
//!
//! These rules are independent of account ownership, synchronization, transport,
//! and time. Pool mutation belongs to [`crate::pool_state::PoolState`].

use alloc::{
  collections::BTreeSet,
  string::{String, ToString},
  vec::Vec,
};
use core::fmt;
use tokn_core::{provider::Endpoint, types::SmolStr};
use tokn_policy::{
  DriverId, ManagedRoute, ModelSelector, OperationPolicy, ProviderId, ProviderSelector, QualificationNamespace,
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
