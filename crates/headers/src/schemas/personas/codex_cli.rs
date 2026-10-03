//! Headers emitted by the Codex CLI clients (`codex_exec`, `codex-tui`).
//!
//! Field set derived from the inbound real-world matrix. Codex sends several
//! transport-class headers using lowercase, no-prefix names (`originator`,
//! `version`, `session_id`, `thread_id`); these are kept verbatim rather
//! than canonicalised because that's what the upstream chatgpt.com endpoint
//! expects.
//!
//! The captured matrix contains multiple transport shapes:
//! `chatgpt.com` websocket upgrades, local/router SSE POSTs, and
//! browser-context account calls. A single typed schema therefore needs to
//! accept endpoint-specific optional headers rather than assuming one
//! normalized baseline.

use crate::error::Error;
use crate::keys;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::schema::{
  from_inbound_or, opt_from_inbound, optional, optional_joined, put, put_opt, required, HeaderSchema,
};
use crate::vars::TemplateVars;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexCliHeaders {
  // Always present
  #[serde(rename = "User-Agent")]
  pub user_agent: SmolStr,
  #[serde(rename = "Authorization")]
  pub authorization: SmolStr,
  /// Optional. NEVER stamped from a persona default: the persona-default
  /// host (e.g. `chatgpt.com`) is wrong for any other upstream and would
  /// cause edge/WAF rejections (TLS SNI vs HTTP Host mismatch) when it
  /// leaks into Send. `build` only sets this from inbound traffic;
  /// outbound transport derives `Host` from the URL.
  #[serde(rename = "Host", skip_serializing_if = "Option::is_none")]
  pub host: Option<SmolStr>,
  #[serde(rename = "Accept", skip_serializing_if = "Option::is_none")]
  pub accept: Option<SmolStr>,
  #[serde(rename = "Connection", skip_serializing_if = "Option::is_none")]
  pub connection: Option<SmolStr>,
  #[serde(rename = "Upgrade", skip_serializing_if = "Option::is_none")]
  pub upgrade: Option<SmolStr>,
  #[serde(rename = "originator", skip_serializing_if = "Option::is_none")]
  pub originator: Option<SmolStr>,
  #[serde(rename = "chatgpt-account-id", skip_serializing_if = "Option::is_none")]
  pub chatgpt_account_id: Option<SmolStr>,
  #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
  pub version: Option<SmolStr>,

  // Body framing — present on POSTs (responses, analytics-events), absent on
  // GETs (models, plugins/featured).
  #[serde(rename = "Content-Type", skip_serializing_if = "Option::is_none")]
  pub content_type: Option<SmolStr>,
  #[serde(rename = "Content-Length", skip_serializing_if = "Option::is_none")]
  pub content_length: Option<SmolStr>,

  // Responses-endpoint specific
  #[serde(rename = "session_id", skip_serializing_if = "Option::is_none")]
  pub session_id: Option<SmolStr>,
  #[serde(rename = "thread_id", skip_serializing_if = "Option::is_none")]
  pub thread_id: Option<SmolStr>,
  #[serde(rename = "x-client-request-id", skip_serializing_if = "Option::is_none")]
  pub client_request_id: Option<SmolStr>,
  #[serde(rename = "x-codex-window-id", skip_serializing_if = "Option::is_none")]
  pub codex_window_id: Option<SmolStr>,
  #[serde(rename = "x-codex-beta-features", skip_serializing_if = "Option::is_none")]
  pub codex_beta_features: Option<SmolStr>,
  #[serde(rename = "x-codex-turn-metadata", skip_serializing_if = "Option::is_none")]
  pub codex_turn_metadata: Option<SmolStr>,
  #[serde(rename = "x-codex-routing-hint", skip_serializing_if = "Option::is_none")]
  pub codex_routing_hint: Option<SmolStr>,
  /// Selects the wire format that carries tools in `additional_tools` input items.
  #[serde(
    rename = "x-openai-internal-codex-responses-lite",
    skip_serializing_if = "Option::is_none"
  )]
  pub codex_responses_lite: Option<SmolStr>,
  #[serde(rename = "OpenAI-Beta", skip_serializing_if = "Option::is_none")]
  pub openai_beta: Option<SmolStr>,
  #[serde(rename = "X-Request-Id", skip_serializing_if = "Option::is_none")]
  pub request_id: Option<SmolStr>,
  #[serde(rename = "Sec-WebSocket-Extensions", skip_serializing_if = "Option::is_none")]
  pub sec_websocket_extensions: Option<SmolStr>,
  #[serde(rename = "Sec-WebSocket-Key", skip_serializing_if = "Option::is_none")]
  pub sec_websocket_key: Option<SmolStr>,
  #[serde(rename = "Sec-WebSocket-Version", skip_serializing_if = "Option::is_none")]
  pub sec_websocket_version: Option<SmolStr>,

  // Browser-context state
  #[serde(rename = "Cookie", skip_serializing_if = "Option::is_none")]
  pub cookie: Option<SmolStr>,
}

impl HeaderSchema for CodexCliHeaders {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      user_agent: required(map, &keys::USER_AGENT)?,
      authorization: required(map, &keys::AUTHORIZATION)?,
      host: optional(map, &keys::HOST),
      accept: optional(map, &keys::ACCEPT),
      connection: optional(map, &keys::CONNECTION),
      upgrade: optional(map, &keys::UPGRADE),
      originator: optional(map, &keys::ORIGINATOR),
      chatgpt_account_id: optional(map, &keys::CHATGPT_ACCOUNT_ID),
      version: optional(map, &keys::VERSION),
      content_type: optional(map, &keys::CONTENT_TYPE),
      content_length: optional(map, &keys::CONTENT_LENGTH),
      session_id: optional(map, &keys::SESSION_ID_LOWER),
      thread_id: optional(map, &keys::THREAD_ID),
      client_request_id: optional(map, &keys::X_CLIENT_REQUEST_ID),
      codex_window_id: optional(map, &keys::X_CODEX_WINDOW_ID),
      codex_beta_features: optional(map, &keys::X_CODEX_BETA_FEATURES),
      codex_turn_metadata: optional(map, &keys::X_CODEX_TURN_METADATA),
      codex_routing_hint: optional_joined(map, &keys::X_CODEX_ROUTING_HINT, ";"),
      codex_responses_lite: optional(map, &keys::X_CODEX_RESPONSES_LITE),
      openai_beta: optional(map, &keys::OPENAI_BETA),
      request_id: optional(map, &keys::X_REQUEST_ID),
      sec_websocket_extensions: optional(map, &keys::SEC_WEBSOCKET_EXTENSIONS),
      sec_websocket_key: optional(map, &keys::SEC_WEBSOCKET_KEY),
      sec_websocket_version: optional(map, &keys::SEC_WEBSOCKET_VERSION),
      cookie: optional(map, &keys::COOKIE),
    })
  }
  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::USER_AGENT, &self.user_agent);
    put(&mut m, &keys::AUTHORIZATION, &self.authorization);
    put_opt(&mut m, &keys::HOST, &self.host);
    put_opt(&mut m, &keys::ACCEPT, &self.accept);
    put_opt(&mut m, &keys::CONNECTION, &self.connection);
    put_opt(&mut m, &keys::UPGRADE, &self.upgrade);
    put_opt(&mut m, &keys::ORIGINATOR, &self.originator);
    put_opt(&mut m, &keys::CHATGPT_ACCOUNT_ID, &self.chatgpt_account_id);
    put_opt(&mut m, &keys::VERSION, &self.version);
    put_opt(&mut m, &keys::CONTENT_TYPE, &self.content_type);
    put_opt(&mut m, &keys::CONTENT_LENGTH, &self.content_length);
    put_opt(&mut m, &keys::SESSION_ID_LOWER, &self.session_id);
    put_opt(&mut m, &keys::THREAD_ID, &self.thread_id);
    put_opt(&mut m, &keys::X_CLIENT_REQUEST_ID, &self.client_request_id);
    put_opt(&mut m, &keys::X_CODEX_WINDOW_ID, &self.codex_window_id);
    put_opt(&mut m, &keys::X_CODEX_BETA_FEATURES, &self.codex_beta_features);
    put_opt(&mut m, &keys::X_CODEX_TURN_METADATA, &self.codex_turn_metadata);
    put_opt(&mut m, &keys::X_CODEX_ROUTING_HINT, &self.codex_routing_hint);
    put_opt(&mut m, &keys::X_CODEX_RESPONSES_LITE, &self.codex_responses_lite);
    put_opt(&mut m, &keys::OPENAI_BETA, &self.openai_beta);
    put_opt(&mut m, &keys::X_REQUEST_ID, &self.request_id);
    put_opt(&mut m, &keys::SEC_WEBSOCKET_EXTENSIONS, &self.sec_websocket_extensions);
    put_opt(&mut m, &keys::SEC_WEBSOCKET_KEY, &self.sec_websocket_key);
    put_opt(&mut m, &keys::SEC_WEBSOCKET_VERSION, &self.sec_websocket_version);
    put_opt(&mut m, &keys::COOKIE, &self.cookie);
    m
  }
  fn known_names() -> &'static [&'static HeaderName] {
    static NAMES: [&HeaderName; 25] = [
      &keys::USER_AGENT,
      &keys::AUTHORIZATION,
      &keys::HOST,
      &keys::ACCEPT,
      &keys::CONNECTION,
      &keys::UPGRADE,
      &keys::ORIGINATOR,
      &keys::CHATGPT_ACCOUNT_ID,
      &keys::VERSION,
      &keys::CONTENT_TYPE,
      &keys::CONTENT_LENGTH,
      &keys::SESSION_ID_LOWER,
      &keys::THREAD_ID,
      &keys::X_CLIENT_REQUEST_ID,
      &keys::X_CODEX_WINDOW_ID,
      &keys::X_CODEX_BETA_FEATURES,
      &keys::X_CODEX_TURN_METADATA,
      &keys::X_CODEX_ROUTING_HINT,
      &keys::X_CODEX_RESPONSES_LITE,
      &keys::OPENAI_BETA,
      &keys::X_REQUEST_ID,
      &keys::SEC_WEBSOCKET_EXTENSIONS,
      &keys::SEC_WEBSOCKET_KEY,
      &keys::SEC_WEBSOCKET_VERSION,
      &keys::COOKIE,
    ];
    &NAMES
  }
}

impl CodexCliHeaders {
  /// Build a [`CodexCliHeaders`] from inbound transport headers and
  /// correlation [`TemplateVars`]. Inbound values win for transport fields;
  /// correlation fields prefer `vars`. Missing required fields fall back to
  /// persona-specific defaults derived from real captured traffic.
  pub fn build(vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      user_agent: from_inbound_or(inbound, &keys::USER_AGENT, || {
        "codex_exec/0.130.0 (Ubuntu 24.4.0; x86_64) unknown (codex_exec; 0.130.0)".into()
      }),
      authorization: from_inbound_or(inbound, &keys::AUTHORIZATION, || "<missing>".into()),
      host: None,
      accept: Some(from_inbound_or(inbound, &keys::ACCEPT, || "text/event-stream".into())),
      connection: opt_from_inbound(inbound, &keys::CONNECTION),
      upgrade: opt_from_inbound(inbound, &keys::UPGRADE),
      originator: Some(from_inbound_or(inbound, &keys::ORIGINATOR, || "codex_exec".into())),
      chatgpt_account_id: vars
        .account_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::CHATGPT_ACCOUNT_ID)),
      version: Some(from_inbound_or(inbound, &keys::VERSION, || "0.130.0".into())),
      content_type: opt_from_inbound(inbound, &keys::CONTENT_TYPE),
      content_length: opt_from_inbound(inbound, &keys::CONTENT_LENGTH),
      session_id: vars
        .session_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::SESSION_ID_LOWER)),
      thread_id: opt_from_inbound(inbound, &keys::THREAD_ID),
      client_request_id: opt_from_inbound(inbound, &keys::X_CLIENT_REQUEST_ID).or_else(|| vars.request_id.clone()),
      codex_window_id: opt_from_inbound(inbound, &keys::X_CODEX_WINDOW_ID),
      codex_beta_features: opt_from_inbound(inbound, &keys::X_CODEX_BETA_FEATURES),
      codex_turn_metadata: opt_from_inbound(inbound, &keys::X_CODEX_TURN_METADATA),
      codex_routing_hint: optional_joined(inbound, &keys::X_CODEX_ROUTING_HINT, ";"),
      codex_responses_lite: opt_from_inbound(inbound, &keys::X_CODEX_RESPONSES_LITE),
      openai_beta: opt_from_inbound(inbound, &keys::OPENAI_BETA),
      request_id: vars
        .request_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_REQUEST_ID)),
      sec_websocket_extensions: opt_from_inbound(inbound, &keys::SEC_WEBSOCKET_EXTENSIONS),
      sec_websocket_key: opt_from_inbound(inbound, &keys::SEC_WEBSOCKET_KEY),
      sec_websocket_version: opt_from_inbound(inbound, &keys::SEC_WEBSOCKET_VERSION),
      cookie: opt_from_inbound(inbound, &keys::COOKIE),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn responses_sample() -> CodexCliHeaders {
    CodexCliHeaders {
      user_agent: "codex_exec/0.130.0 (Ubuntu 24.4.0; x86_64) unknown (codex_exec; 0.130.0)".into(),
      authorization: "<redacted>".into(),
      host: Some("chatgpt.com".into()),
      accept: Some("text/event-stream".into()),
      connection: None,
      upgrade: None,
      originator: Some("codex_exec".into()),
      chatgpt_account_id: Some("<redacted>".into()),
      version: Some("0.130.0".into()),
      content_type: Some("application/json".into()),
      content_length: Some("45273".into()),
      session_id: Some("019e271b-4023-7081-be3e-7a69d97138a2".into()),
      thread_id: Some("019e271b-4023-7081-be3e-7a69d97138a2".into()),
      client_request_id: Some("019e271b-4023-7081-be3e-7a69d97138a2".into()),
      codex_window_id: Some("019e271b-4023-7081-be3e-7a69d97138a2:0".into()),
      codex_beta_features: Some("terminal_resize_reflow".into()),
      codex_turn_metadata: Some("{\"session_id\":\"019e271b\"}".into()),
      codex_routing_hint: Some("model=gpt-6.1-sol;tier=priority;shard=example".into()),
      codex_responses_lite: Some("true".into()),
      openai_beta: None,
      request_id: None,
      sec_websocket_extensions: None,
      sec_websocket_key: None,
      sec_websocket_version: None,
      cookie: None,
    }
  }

  #[test]
  fn responses_round_trip() {
    let h = responses_sample();
    assert_eq!(CodexCliHeaders::parse(&h.dump()).unwrap(), h);
  }

  #[test]
  fn missing_required_errors() {
    let m = HeaderMap::new();
    assert!(matches!(CodexCliHeaders::parse(&m), Err(Error::MissingHeader { .. })));
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = CodexCliHeaders::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(
      h.user_agent.as_str(),
      "codex_exec/0.130.0 (Ubuntu 24.4.0; x86_64) unknown (codex_exec; 0.130.0)"
    );
    assert_eq!(h.authorization.as_str(), "<missing>");
    assert!(
      h.host.is_none(),
      "no inbound Host => no persona-default Host (would leak to wire)"
    );
    assert_eq!(h.accept.as_deref(), Some("text/event-stream"));
    assert_eq!(h.originator.as_deref(), Some("codex_exec"));
    assert!(h.chatgpt_account_id.is_none());
    assert_eq!(h.version.as_deref(), Some("0.130.0"));
    assert!(h.content_type.is_none());
    assert!(h.session_id.is_none());
    assert!(h.thread_id.is_none());
    assert!(h.codex_routing_hint.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::USER_AGENT, "codex_exec/9.9.9");
    inbound.insert(&keys::AUTHORIZATION, "Bearer abc");
    inbound.insert(&keys::OPENAI_BETA, "responses=v1");
    inbound.insert(&keys::HOST, "chatgpt.com");
    inbound.insert(&keys::X_CLIENT_REQUEST_ID, "codex-req");
    inbound.insert(&keys::X_CODEX_ROUTING_HINT, "model=gpt-6.1-sol;shard=example");
    inbound.append(&keys::X_CODEX_ROUTING_HINT, "sticky=on");
    let h = CodexCliHeaders::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.user_agent.as_str(), "codex_exec/9.9.9");
    assert_eq!(h.authorization.as_str(), "Bearer abc");
    assert_eq!(h.openai_beta.as_deref(), Some("responses=v1"));
    assert_eq!(h.host.as_deref(), None);
    assert_eq!(h.client_request_id.as_deref(), Some("codex-req"));
    assert_eq!(
      h.codex_routing_hint.as_deref(),
      Some("model=gpt-6.1-sol;shard=example;sticky=on")
    );
    assert_eq!(
      CodexCliHeaders::parse(&inbound).unwrap().codex_routing_hint,
      h.codex_routing_hint
    );
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      request_id: Some("req_42".into()),
      account_id: Some("acct_z".into()),
      ..Default::default()
    };
    let h = CodexCliHeaders::build(&vars, &HeaderMap::new());
    assert_eq!(h.session_id.as_deref(), Some("ses_xyz"));
    assert_eq!(h.client_request_id.as_deref(), Some("req_42"));
    assert_eq!(h.request_id.as_deref(), Some("req_42"));
    assert_eq!(h.chatgpt_account_id.as_deref(), Some("acct_z"));
  }
}
