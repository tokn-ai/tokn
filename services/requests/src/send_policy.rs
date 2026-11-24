//! Send-outcome decisions shared by standard and injected execution.
//!
//! Response handling and account health are separate decisions: a proxy can
//! forward an upstream error while marking its selected account unavailable.
//! Adapters classify transport errors and apply account feedback after the send,
//! before response decoding or streaming completes. Receiving response headers
//! can therefore establish session affinity even if later body decoding fails.
//! Reading bodies, updating pools, and deciding whether a failure can be retried
//! remain with the request adapters and retry policy.

use crate::driver::{classify_status, StatusDisposition};

/// Whether a received status proceeds to response handling or fails the send.
/// `StatusDisposition::Success` means the response proceeds, including upstream
/// error statuses explicitly forwarded by a proxy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponsePolicy {
  /// API execution classifies client errors as permanent and server errors as
  /// recoverable. This also applies to account-credential API relays.
  Api,
  /// Legacy proxy execution forwards client errors and fails on server errors.
  LegacyProxy,
  /// Transparent execution forwards every received response status.
  ForwardAll,
}

impl ResponsePolicy {
  pub const fn classify_status(self, status: u16) -> StatusDisposition {
    match self {
      Self::Api => classify_status(status),
      Self::LegacyProxy => match classify_status(status) {
        StatusDisposition::RecoverableFailure => StatusDisposition::RecoverableFailure,
        _ => StatusDisposition::Success,
      },
      Self::ForwardAll => StatusDisposition::Success,
    }
  }
}

/// The send result needed to update a selected account's health. Transport
/// adapters supply recoverability using their own concrete error types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
  Response { status: u16 },
  Failure { recoverable: bool },
}

/// Update to apply to a selected account after its send completes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountFeedback {
  None,
  Success,
  Failure,
}

/// Account-health rules are independent of whether a response is forwarded.
/// Client-credential relays have no selected account and apply no feedback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountFeedbackPolicy {
  /// Managed API execution and account-credential API relays.
  Api,
  /// Account-credential proxy relays forward statuses but cool unavailable
  /// accounts, including authorization, throttling, and transient failures.
  ForwardProxy,
}

impl AccountFeedbackPolicy {
  pub const fn feedback(self, outcome: SendOutcome) -> AccountFeedback {
    match outcome {
      SendOutcome::Failure { recoverable: true } => AccountFeedback::Failure,
      SendOutcome::Failure { recoverable: false } => AccountFeedback::None,
      SendOutcome::Response { status } => match self {
        Self::Api => match classify_status(status) {
          StatusDisposition::Success => AccountFeedback::Success,
          StatusDisposition::PermanentFailure => AccountFeedback::None,
          StatusDisposition::RecoverableFailure => AccountFeedback::Failure,
        },
        Self::ForwardProxy => match status {
          401 | 403 | 408 | 425 | 429 | 500..=599 => AccountFeedback::Failure,
          _ => AccountFeedback::Success,
        },
      },
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn response_policies_preserve_error_and_forwarding_boundaries() {
    use StatusDisposition::{PermanentFailure, RecoverableFailure, Success};
    for (status, api, legacy_proxy) in [
      (0, Success, Success),
      (100, Success, Success),
      (199, Success, Success),
      (200, Success, Success),
      (299, Success, Success),
      (300, Success, Success),
      (399, Success, Success),
      (400, PermanentFailure, Success),
      (401, PermanentFailure, Success),
      (429, PermanentFailure, Success),
      (499, PermanentFailure, Success),
      (500, RecoverableFailure, RecoverableFailure),
      (503, RecoverableFailure, RecoverableFailure),
      (599, RecoverableFailure, RecoverableFailure),
      (600, RecoverableFailure, RecoverableFailure),
      (u16::MAX, RecoverableFailure, RecoverableFailure),
    ] {
      assert_eq!(ResponsePolicy::Api.classify_status(status), api, "API {status}");
      assert_eq!(
        ResponsePolicy::LegacyProxy.classify_status(status),
        legacy_proxy,
        "legacy proxy {status}"
      );
      assert_eq!(
        ResponsePolicy::ForwardAll.classify_status(status),
        Success,
        "forwarded {status}"
      );
    }
  }

  #[test]
  fn api_feedback_leaves_permanent_failures_unchanged() {
    for (status, expected) in [
      (0, AccountFeedback::Success),
      (100, AccountFeedback::Success),
      (200, AccountFeedback::Success),
      (302, AccountFeedback::Success),
      (399, AccountFeedback::Success),
      (400, AccountFeedback::None),
      (401, AccountFeedback::None),
      (403, AccountFeedback::None),
      (408, AccountFeedback::None),
      (425, AccountFeedback::None),
      (429, AccountFeedback::None),
      (499, AccountFeedback::None),
      (500, AccountFeedback::Failure),
      (599, AccountFeedback::Failure),
      (600, AccountFeedback::Failure),
      (u16::MAX, AccountFeedback::Failure),
    ] {
      assert_eq!(
        AccountFeedbackPolicy::Api.feedback(SendOutcome::Response { status }),
        expected,
        "{status}"
      );
    }
  }

  #[test]
  fn forwarding_an_unavailable_account_response_still_records_failure() {
    for status in [401, 403, 408, 425, 429, 500, 503, 599] {
      assert_eq!(
        ResponsePolicy::ForwardAll.classify_status(status),
        StatusDisposition::Success
      );
      assert_eq!(
        AccountFeedbackPolicy::ForwardProxy.feedback(SendOutcome::Response { status }),
        AccountFeedback::Failure,
        "{status}"
      );
    }
    for status in [
      0,
      100,
      200,
      302,
      399,
      400,
      402,
      404,
      407,
      409,
      424,
      426,
      428,
      430,
      499,
      600,
      u16::MAX,
    ] {
      assert_eq!(
        AccountFeedbackPolicy::ForwardProxy.feedback(SendOutcome::Response { status }),
        AccountFeedback::Success,
        "{status}"
      );
    }
  }

  #[test]
  fn failure_feedback_uses_adapter_recoverability() {
    for policy in [AccountFeedbackPolicy::Api, AccountFeedbackPolicy::ForwardProxy] {
      assert_eq!(
        policy.feedback(SendOutcome::Failure { recoverable: true }),
        AccountFeedback::Failure
      );
      assert_eq!(
        policy.feedback(SendOutcome::Failure { recoverable: false }),
        AccountFeedback::None
      );
    }
  }
}
