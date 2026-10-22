//! Retry policy shared by standard and injected request execution.
//!
//! Adapters classify their errors and supply the request method. This module
//! resolves route configuration and decides whether another send is allowed;
//! the shared driver owns replay ordering and the platform supplies the timer.

use core::{fmt, time::Duration};
use tokn_policy::{ExecutionPlan, ManagedRetry, RelayRetry, RetryPolicyId, RoutePlan};

/// Bounded replay of recoverable send failures.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetryPolicy {
  /// Additional attempts permitted after the first send.
  pub max_retries: u32,
  pub initial_backoff: Duration,
  replay_safety: ReplaySafety,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ReplaySafety {
  #[default]
  Buffered,
  SafeMethods,
}

/// Error facts needed for a retry decision, independent of the platform's
/// concrete error and stage types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryFailure {
  pub send_stage: bool,
  pub recoverable: bool,
  pub stop: bool,
}

/// A route references a retry policy absent from its execution plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MissingRetryPolicy {
  policy_id: RetryPolicyId,
}

impl MissingRetryPolicy {
  pub fn policy_id(&self) -> &RetryPolicyId {
    &self.policy_id
  }
}

impl fmt::Display for MissingRetryPolicy {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(formatter, "route references missing retry policy '{}'", self.policy_id)
  }
}

impl core::error::Error for MissingRetryPolicy {}

impl RetryPolicy {
  pub const fn new(max_retries: u32, initial_backoff: Duration) -> Self {
    Self {
      max_retries,
      initial_backoff,
      replay_safety: ReplaySafety::Buffered,
    }
  }

  /// Retry only HTTP methods defined as safe by RFC 9110. Requests without a
  /// transport method are not retried under this policy.
  pub const fn safe_methods(max_retries: u32, initial_backoff: Duration) -> Self {
    Self {
      max_retries,
      initial_backoff,
      replay_safety: ReplaySafety::SafeMethods,
    }
  }

  /// Resolve the retry choice and named policy for a v2 route.
  pub fn from_route(plan: &ExecutionPlan, route: &RoutePlan) -> Result<Self, MissingRetryPolicy> {
    let (policy_id, replay_safety) = match route {
      RoutePlan::Managed(route) => match route.retry() {
        ManagedRetry::Never => return Ok(Self::default()),
        ManagedRetry::Recoverable(id) => (id, ReplaySafety::Buffered),
      },
      RoutePlan::Relay(route) => match route.retry() {
        RelayRetry::Never => return Ok(Self::default()),
        RelayRetry::SafeMethods(id) => (id, ReplaySafety::SafeMethods),
        RelayRetry::Buffered(id) => (id, ReplaySafety::Buffered),
      },
    };
    let policy = plan.retry_policy(policy_id).ok_or_else(|| MissingRetryPolicy {
      policy_id: policy_id.clone(),
    })?;
    Ok(Self {
      max_retries: policy.max_retries(),
      initial_backoff: policy.initial_backoff(),
      replay_safety,
    })
  }

  /// Return the delay before replaying a failed send. `attempt` is zero-based,
  /// and `method` is the case-sensitive HTTP method used by the transport.
  /// A zero duration permits immediate replay without invoking a timer.
  pub fn next_delay(&self, attempt: u32, failure: RetryFailure, method: Option<&str>) -> Option<Duration> {
    let replayable = match self.replay_safety {
      ReplaySafety::Buffered => true,
      ReplaySafety::SafeMethods => matches!(method, Some("GET" | "HEAD" | "OPTIONS" | "TRACE")),
    };
    (attempt < self.max_retries && failure.send_stage && failure.recoverable && !failure.stop && replayable)
      .then(|| crate::driver::backoff(self.initial_backoff, attempt))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::{collections::BTreeMap, string::ToString};
  use tokn_policy::{
    ManagedRoute, ManagedTarget, ModelSelector, OperationPolicy, ProviderSelector, RelayCredentials, RelayDestination,
    RelayRoute, RetryPolicyPlan,
  };

  const RECOVERABLE_SEND: RetryFailure = RetryFailure {
    send_stage: true,
    recoverable: true,
    stop: false,
  };

  #[test]
  fn only_recoverable_send_failures_can_replay() {
    let policy = RetryPolicy::new(1, Duration::from_millis(20));
    for send_stage in [false, true] {
      for recoverable in [false, true] {
        for stop in [false, true] {
          let failure = RetryFailure {
            send_stage,
            recoverable,
            stop,
          };
          let expected = if failure == RECOVERABLE_SEND {
            Some(Duration::from_millis(20))
          } else {
            None
          };
          assert_eq!(policy.next_delay(0, failure, Some("POST")), expected, "{failure:?}");
        }
      }
    }
  }

  #[test]
  fn retry_budget_counts_additional_attempts_and_backs_off() {
    let policy = RetryPolicy::new(3, Duration::from_millis(25));
    let delays = [0, 1, 2, 3, u32::MAX].map(|attempt| policy.next_delay(attempt, RECOVERABLE_SEND, None));
    assert_eq!(
      delays,
      [
        Some(Duration::from_millis(25)),
        Some(Duration::from_millis(50)),
        Some(Duration::from_millis(100)),
        None,
        None,
      ]
    );
    assert_eq!(RetryPolicy::default().next_delay(0, RECOVERABLE_SEND, None), None);
  }

  #[test]
  fn safe_method_policy_requires_an_explicit_safe_http_method() {
    let policy = RetryPolicy::safe_methods(1, Duration::ZERO);
    for method in ["GET", "HEAD", "OPTIONS", "TRACE"] {
      assert_eq!(
        policy.next_delay(0, RECOVERABLE_SEND, Some(method)),
        Some(Duration::ZERO),
        "{method}"
      );
    }
    for method in [
      None,
      Some("POST"),
      Some("PUT"),
      Some("DELETE"),
      Some("PATCH"),
      Some("CONNECT"),
      Some("get"),
    ] {
      assert_eq!(policy.next_delay(0, RECOVERABLE_SEND, method), None, "{method:?}");
    }
    let buffered = RetryPolicy::new(1, Duration::ZERO);
    assert_eq!(
      buffered.next_delay(0, RECOVERABLE_SEND, Some("POST")),
      Some(Duration::ZERO)
    );
    assert_eq!(buffered.next_delay(0, RECOVERABLE_SEND, None), Some(Duration::ZERO));
  }

  #[test]
  fn delays_preserve_zero_and_saturate_without_overflow() {
    let immediate = RetryPolicy::new(u32::MAX, Duration::ZERO);
    assert_eq!(
      immediate.next_delay(u32::MAX - 1, RECOVERABLE_SEND, None),
      Some(Duration::ZERO)
    );

    let saturated = RetryPolicy::new(u32::MAX, Duration::MAX);
    assert_eq!(saturated.next_delay(10, RECOVERABLE_SEND, None), Some(Duration::MAX));
    let bounded_exponent = RetryPolicy::new(u32::MAX, Duration::from_nanos(1));
    assert_eq!(
      bounded_exponent.next_delay(u32::MAX - 1, RECOVERABLE_SEND, None),
      Some(Duration::from_nanos(1 << 31))
    );
  }

  fn managed(retry: ManagedRetry) -> RoutePlan {
    RoutePlan::Managed(ManagedRoute::new(
      ManagedTarget::new(ProviderSelector::Any, ModelSelector::Capability),
      OperationPolicy::Preserve,
      None,
      retry,
    ))
  }

  fn relay(retry: RelayRetry) -> RoutePlan {
    RoutePlan::Relay(RelayRoute::new(
      RelayDestination::Original,
      RelayCredentials::Client,
      None,
      retry,
    ))
  }

  #[test]
  fn route_resolution_preserves_replay_safety_and_named_limits() {
    let id = RetryPolicyId::new("failover").unwrap();
    let plan = ExecutionPlan::new(
      BTreeMap::new(),
      BTreeMap::new(),
      BTreeMap::from([(id.clone(), RetryPolicyPlan::new(2, Duration::from_millis(17)))]),
      BTreeMap::new(),
      BTreeMap::new(),
    );
    for (route, post_allowed) in [
      (managed(ManagedRetry::Recoverable(id.clone())), true),
      (relay(RelayRetry::Buffered(id.clone())), true),
      (relay(RelayRetry::SafeMethods(id)), false),
    ] {
      let policy = RetryPolicy::from_route(&plan, &route).unwrap();
      assert_eq!(policy.max_retries, 2);
      assert_eq!(policy.initial_backoff, Duration::from_millis(17));
      assert_eq!(
        policy.next_delay(1, RECOVERABLE_SEND, Some("GET")),
        Some(Duration::from_millis(34))
      );
      assert_eq!(policy.next_delay(2, RECOVERABLE_SEND, Some("GET")), None);
      assert_eq!(
        policy.next_delay(0, RECOVERABLE_SEND, Some("POST")).is_some(),
        post_allowed
      );
    }
  }

  #[test]
  fn never_routes_need_no_named_policy() {
    let plan = ExecutionPlan::default();
    for route in [managed(ManagedRetry::Never), relay(RelayRetry::Never)] {
      let policy = RetryPolicy::from_route(&plan, &route).unwrap();
      assert_eq!(policy.next_delay(0, RECOVERABLE_SEND, Some("GET")), None);
    }
  }

  #[test]
  fn every_named_retry_mode_rejects_missing_policy() {
    let id = RetryPolicyId::new("missing").unwrap();
    for route in [
      managed(ManagedRetry::Recoverable(id.clone())),
      relay(RelayRetry::SafeMethods(id.clone())),
      relay(RelayRetry::Buffered(id.clone())),
    ] {
      let error = RetryPolicy::from_route(&ExecutionPlan::default(), &route).unwrap_err();
      assert_eq!(error.policy_id(), &id);
      assert_eq!(error.to_string(), "route references missing retry policy 'missing'");
    }
  }
}
