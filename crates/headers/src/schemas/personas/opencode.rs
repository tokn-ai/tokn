//! Headers emitted by the OpenCode CLI client.
//!
//! Field set derived from the inbound real-world matrix (see
//! `tests/fixtures/inbound_real_world.json`). Required fields are present in
//! ≥99% of captured requests; optional fields are observed but inconsistent.
//!
//! `Authorization` is modelled as required even though its value may be the
//! literal `"<redacted>"` in fixtures: the *header* is universally present,
//! and downstream layers replace its value before transmission.

use crate::error::Error;
use crate::keys;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::schema::{from_inbound_or, opt_from_inbound, optional, put, put_opt, required, HeaderSchema};
use crate::vars::TemplateVars;
use serde::{Deserialize, Serialize};
use tokn_core::types::SmolStr;

/// Inbound headers consistently emitted by the OpenCode CLI persona.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpencodeHeaders {
  // Transport (always present)
  #[serde(rename = "User-Agent")]
  pub user_agent: SmolStr,
  #[serde(rename = "Authorization")]
  pub authorization: SmolStr,
  /// Optional. NEVER stamped from a persona default: the persona-default
  /// host (e.g. `api.deepseek.com`) is wrong for any other upstream and
  /// caused real 403s when it leaked into Send. `build` only sets this
  /// from inbound traffic; outbound transport derives `Host` from the URL.
  #[serde(rename = "Host", skip_serializing_if = "Option::is_none")]
  pub host: Option<SmolStr>,
  #[serde(rename = "Accept")]
  pub accept: SmolStr,
  #[serde(rename = "Accept-Encoding")]
  pub accept_encoding: SmolStr,
  #[serde(rename = "Connection")]
  pub connection: SmolStr,
  #[serde(rename = "Content-Type")]
  pub content_type: SmolStr,

  // Body framing (present on every POST; absent on GET /models)
  #[serde(rename = "Content-Length", skip_serializing_if = "Option::is_none")]
  pub content_length: Option<SmolStr>,

  // Session correlation (X-Session-Affinity is the inbound name; X-Session-Id
  // is router-injected and never appears in opencode-emitted captures).
  #[serde(rename = "X-Session-Affinity", skip_serializing_if = "Option::is_none")]
  pub session_affinity: Option<SmolStr>,
  #[serde(rename = "X-Parent-Session-Id", skip_serializing_if = "Option::is_none")]
  pub parent_session_id: Option<SmolStr>,
}

impl HeaderSchema for OpencodeHeaders {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      user_agent: required(map, &keys::USER_AGENT)?,
      authorization: required(map, &keys::AUTHORIZATION)?,
      host: optional(map, &keys::HOST),
      accept: required(map, &keys::ACCEPT)?,
      accept_encoding: required(map, &keys::ACCEPT_ENCODING)?,
      connection: required(map, &keys::CONNECTION)?,
      content_type: required(map, &keys::CONTENT_TYPE)?,
      content_length: optional(map, &keys::CONTENT_LENGTH),
      session_affinity: optional(map, &keys::X_SESSION_AFFINITY),
      parent_session_id: optional(map, &keys::X_PARENT_SESSION_ID),
    })
  }
  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::USER_AGENT, &self.user_agent);
    put(&mut m, &keys::AUTHORIZATION, &self.authorization);
    put_opt(&mut m, &keys::HOST, &self.host);
    put(&mut m, &keys::ACCEPT, &self.accept);
    put(&mut m, &keys::ACCEPT_ENCODING, &self.accept_encoding);
    put(&mut m, &keys::CONNECTION, &self.connection);
    put(&mut m, &keys::CONTENT_TYPE, &self.content_type);
    put_opt(&mut m, &keys::CONTENT_LENGTH, &self.content_length);
    put_opt(&mut m, &keys::X_SESSION_AFFINITY, &self.session_affinity);
    put_opt(&mut m, &keys::X_PARENT_SESSION_ID, &self.parent_session_id);
    m
  }
  fn known_names() -> &'static [&'static HeaderName] {
    const NAMES: [&HeaderName; 10] = [
      &keys::USER_AGENT,
      &keys::AUTHORIZATION,
      &keys::HOST,
      &keys::ACCEPT,
      &keys::ACCEPT_ENCODING,
      &keys::CONNECTION,
      &keys::CONTENT_TYPE,
      &keys::CONTENT_LENGTH,
      &keys::X_SESSION_AFFINITY,
      &keys::X_PARENT_SESSION_ID,
    ];
    &NAMES
  }
}

impl OpencodeHeaders {
  /// Build an [`OpencodeHeaders`] from inbound transport headers and
  /// correlation [`TemplateVars`]. Inbound values win for transport fields;
  /// correlation fields prefer `vars`. Missing required fields fall back to
  /// persona-specific defaults derived from real captured traffic.
  pub fn build(vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      user_agent: from_inbound_or(inbound, &keys::USER_AGENT, || {
        "opencode/1.14.28 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13".into()
      }),
      authorization: from_inbound_or(inbound, &keys::AUTHORIZATION, || "<missing>".into()),
      host: None,
      accept: from_inbound_or(inbound, &keys::ACCEPT, || "*/*".into()),
      accept_encoding: from_inbound_or(inbound, &keys::ACCEPT_ENCODING, || "gzip, deflate, br, zstd".into()),
      connection: from_inbound_or(inbound, &keys::CONNECTION, || "keep-alive".into()),
      content_type: from_inbound_or(inbound, &keys::CONTENT_TYPE, || "application/json".into()),
      content_length: opt_from_inbound(inbound, &keys::CONTENT_LENGTH),
      session_affinity: vars
        .session_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_SESSION_AFFINITY)),
      parent_session_id: opt_from_inbound(inbound, &keys::X_PARENT_SESSION_ID),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn sample() -> OpencodeHeaders {
    OpencodeHeaders {
      user_agent: "opencode/1.14.28 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13".into(),
      authorization: "<redacted>".into(),
      host: Some("api.deepseek.com".into()),
      accept: "*/*".into(),
      accept_encoding: "gzip, deflate, br, zstd".into(),
      connection: "keep-alive".into(),
      content_type: "application/json".into(),
      content_length: Some("4429".into()),
      session_affinity: Some("ses_1dddd2016ffed1A1u3yj5LmNWC".into()),
      parent_session_id: None,
    }
  }

  #[test]
  fn opencode_round_trip() {
    let h = sample();
    let parsed = OpencodeHeaders::parse(&h.dump()).unwrap();
    assert_eq!(parsed, h);
  }

  #[test]
  fn opencode_optional_fields_omitted_when_none() {
    let mut h = sample();
    h.content_length = None;
    h.session_affinity = None;
    h.parent_session_id = None;
    let m = h.dump();
    // 7 required fields, 0 optional written.
    assert_eq!(m.len(), 7);
    assert!(!m.contains_key(&keys::CONTENT_LENGTH));
    assert!(!m.contains_key(&keys::X_SESSION_AFFINITY));
  }

  #[test]
  fn opencode_missing_required_returns_error() {
    let m = HeaderMap::new();
    assert!(matches!(OpencodeHeaders::parse(&m), Err(Error::MissingHeader { .. })));
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = OpencodeHeaders::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(
      h.user_agent.as_str(),
      "opencode/1.14.28 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13"
    );
    assert_eq!(h.authorization.as_str(), "<missing>");
    assert!(
      h.host.is_none(),
      "no inbound Host => no persona-default Host (would leak to wire)"
    );
    assert_eq!(h.accept.as_str(), "*/*");
    assert_eq!(h.accept_encoding.as_str(), "gzip, deflate, br, zstd");
    assert_eq!(h.connection.as_str(), "keep-alive");
    assert_eq!(h.content_type.as_str(), "application/json");
    assert!(h.content_length.is_none());
    assert!(h.session_affinity.is_none());
    assert!(h.parent_session_id.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::USER_AGENT, "custom-ua/9.9");
    inbound.insert(&keys::AUTHORIZATION, "Bearer secret");
    inbound.insert(&keys::CONTENT_LENGTH, "1234");
    inbound.insert(&keys::HOST, "api.deepseek.com");
    let h = OpencodeHeaders::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.user_agent.as_str(), "custom-ua/9.9");
    assert_eq!(h.authorization.as_str(), "Bearer secret");
    assert_eq!(h.content_length.as_deref(), Some("1234"));
    assert_eq!(h.host.as_deref(), None);
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      ..Default::default()
    };
    let h = OpencodeHeaders::build(&vars, &HeaderMap::new());
    assert_eq!(h.session_affinity.as_deref(), Some("ses_xyz"));
  }
}
