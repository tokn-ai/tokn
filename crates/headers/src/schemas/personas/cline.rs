//! Headers emitted by the Cline CLI client.
//!
//! NOTE: not yet verified against real-world inbound captures — no `cline`
//! traffic was observed in the mined request logs. Field set is a
//! best-effort outbound model and may need refinement once captures
//! become available.

use crate::error::Error;
use crate::keys;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::schema::{from_inbound_or, opt_from_inbound, optional, put, put_opt, required, HeaderSchema};
use crate::vars::TemplateVars;
use serde::{Deserialize, Serialize};
use tokn_core::types::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClineHeaders {
  #[serde(rename = "User-Agent")]
  pub user_agent: SmolStr,
  #[serde(rename = "X-Session-Id")]
  pub session_id: Option<SmolStr>,
}

impl HeaderSchema for ClineHeaders {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      user_agent: required(map, &keys::USER_AGENT)?,
      session_id: optional(map, &keys::X_SESSION_ID),
    })
  }
  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::USER_AGENT, &self.user_agent);
    put_opt(&mut m, &keys::X_SESSION_ID, &self.session_id);
    m
  }
  fn known_names() -> &'static [&'static HeaderName] {
    const NAMES: [&HeaderName; 2] = [&keys::USER_AGENT, &keys::X_SESSION_ID];
    &NAMES
  }
}

impl ClineHeaders {
  /// Build a [`ClineHeaders`] from inbound transport headers and
  /// correlation [`TemplateVars`].
  pub fn build(vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      user_agent: from_inbound_or(inbound, &keys::USER_AGENT, || "cline/3.0.0".into()),
      session_id: vars
        .session_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_SESSION_ID)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trip() {
    let h = ClineHeaders {
      user_agent: "cline/3.0.0".into(),
      session_id: Some("ses_cli".into()),
    };
    assert_eq!(ClineHeaders::parse(&h.dump()).unwrap(), h);
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = ClineHeaders::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(h.user_agent.as_str(), "cline/3.0.0");
    assert!(h.session_id.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::USER_AGENT, "cline/9.9");
    let h = ClineHeaders::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.user_agent.as_str(), "cline/9.9");
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      ..Default::default()
    };
    let h = ClineHeaders::build(&vars, &HeaderMap::new());
    assert_eq!(h.session_id.as_deref(), Some("ses_xyz"));
  }
}
