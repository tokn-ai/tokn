//! Per-request template variables resolved from inbound headers.
//!
//! Shared verbatim between profile header rendering and provider header
//! patching, so both surfaces see the same view of the request. Fields are
//! [`Option<SmolStr>`] because every value is optional in principle (e.g.
//! some clients omit `x-session-id`).

use crate::types::SmolStr;
use serde::{Deserialize, Serialize};

/// Correlation metadata extracted from a single inbound HTTP request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemplateVars {
  /// Stable identifier for the user-facing chat session.
  pub session_id: Option<SmolStr>,
  /// Identifier for the individual upstream request attempt.
  pub request_id: Option<SmolStr>,
  /// Working directory of the originating tool, when known.
  pub project_cwd: Option<SmolStr>,
  /// Identifier for a single user interaction (may span several requests).
  pub interaction_id: Option<SmolStr>,
  /// Account or tenant identifier supplied by the originating tool.
  pub account_id: Option<SmolStr>,
}

impl TemplateVars {
  /// Construct an empty [`TemplateVars`].
  pub fn empty() -> Self {
    Self::default()
  }

  /// Whether all fields are unset.
  pub fn is_empty(&self) -> bool {
    self.session_id.is_none()
      && self.request_id.is_none()
      && self.project_cwd.is_none()
      && self.interaction_id.is_none()
      && self.account_id.is_none()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn empty_is_empty() {
    assert!(TemplateVars::empty().is_empty());
  }

  #[test]
  fn populated_is_not_empty() {
    let v = TemplateVars {
      session_id: Some(SmolStr::new("ses_42")),
      ..Default::default()
    };
    assert!(!v.is_empty());
  }

  #[test]
  fn round_trips_through_json() {
    let v = TemplateVars {
      session_id: Some("ses_42".into()),
      request_id: Some("req_99".into()),
      project_cwd: Some("/work".into()),
      interaction_id: None,
      account_id: Some("acct_1".into()),
    };
    let s = serde_json::to_string(&v).unwrap();
    let back: TemplateVars = serde_json::from_str(&s).unwrap();
    assert_eq!(v, back);
  }
}
