//! Headers emitted by the GitHub Copilot CLI client (`copilot/<ver>`).
//!
//! Field set verified against one captured request:
//!
//! ```text
//! POST https://api.business.githubcopilot.com/responses
//! user-agent: copilot/1.0.25 (client/sdk win32 v22.19.0) term/unknown
//! copilot-integration-id: copilot-developer-cli
//! ```
//!
//! The Copilot CLI is built on the OpenAI Stainless-generated JS SDK; the
//! `x-stainless-*` family is therefore present on every call and modelled as
//! required. `x-agent-task-id` is required as well — it correlates
//! multi-turn agent invocations and was present on the captured POST
//! /responses call. If future captures show it absent on simpler endpoints
//! (e.g. /models), demote to optional.

use crate::error::Error;
use crate::keys;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::schema::{from_inbound_or, opt_from_inbound, optional, put, put_opt, required, HeaderSchema};
use crate::vars::TemplateVars;
use serde::{Deserialize, Serialize};
use tokn_core::types::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopilotCliHeaders {
  // Transport
  #[serde(rename = "User-Agent")]
  pub user_agent: SmolStr,
  #[serde(rename = "Authorization")]
  pub authorization: SmolStr,
  #[serde(rename = "Content-Type")]
  pub content_type: SmolStr,
  #[serde(rename = "Accept")]
  pub accept: SmolStr,
  #[serde(rename = "Accept-Encoding")]
  pub accept_encoding: SmolStr,
  #[serde(rename = "Accept-Language")]
  pub accept_language: SmolStr,
  #[serde(rename = "Sec-Fetch-Mode")]
  pub sec_fetch_mode: SmolStr,

  // Copilot identity / contract
  #[serde(rename = "Copilot-Integration-Id")]
  pub copilot_integration_id: SmolStr,
  #[serde(rename = "OpenAI-Intent")]
  pub openai_intent: SmolStr,
  #[serde(rename = "X-Initiator")]
  pub initiator: SmolStr,
  #[serde(rename = "X-GitHub-Api-Version")]
  pub github_api_version: SmolStr,

  // Per-call correlation
  #[serde(rename = "X-Interaction-Id")]
  pub interaction_id: SmolStr,
  #[serde(rename = "X-Interaction-Type")]
  pub interaction_type: SmolStr,
  #[serde(rename = "X-Client-Session-Id")]
  pub client_session_id: SmolStr,
  #[serde(rename = "X-Agent-Task-Id")]
  pub agent_task_id: SmolStr,

  // Stainless SDK fingerprint — present on this SDK-flavoured capture but
  // demoted to optional: a future Copilot-CLI build using a non-Stainless
  // transport (or a hand-rolled HTTP path for `gh copilot suggest`/`explain`)
  // would omit them entirely.
  #[serde(rename = "X-Stainless-Retry-Count", skip_serializing_if = "Option::is_none")]
  pub stainless_retry_count: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Lang", skip_serializing_if = "Option::is_none")]
  pub stainless_lang: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Package-Version", skip_serializing_if = "Option::is_none")]
  pub stainless_package_version: Option<SmolStr>,
  #[serde(rename = "X-Stainless-OS", skip_serializing_if = "Option::is_none")]
  pub stainless_os: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Arch", skip_serializing_if = "Option::is_none")]
  pub stainless_arch: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Runtime", skip_serializing_if = "Option::is_none")]
  pub stainless_runtime: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Runtime-Version", skip_serializing_if = "Option::is_none")]
  pub stainless_runtime_version: Option<SmolStr>,

  // Body framing — present on POST, absent on GET.
  #[serde(rename = "Content-Length", skip_serializing_if = "Option::is_none")]
  pub content_length: Option<SmolStr>,

  // Plausibly per-endpoint optional.
  #[serde(rename = "Cookie", skip_serializing_if = "Option::is_none")]
  pub cookie: Option<SmolStr>,
  #[serde(rename = "X-Request-Id", skip_serializing_if = "Option::is_none")]
  pub request_id: Option<SmolStr>,
}

impl HeaderSchema for CopilotCliHeaders {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      user_agent: required(map, &keys::USER_AGENT)?,
      authorization: required(map, &keys::AUTHORIZATION)?,
      content_type: required(map, &keys::CONTENT_TYPE)?,
      accept: required(map, &keys::ACCEPT)?,
      accept_encoding: required(map, &keys::ACCEPT_ENCODING)?,
      accept_language: required(map, &keys::ACCEPT_LANGUAGE)?,
      sec_fetch_mode: required(map, &keys::SEC_FETCH_MODE)?,
      copilot_integration_id: required(map, &keys::COPILOT_INTEGRATION_ID)?,
      openai_intent: required(map, &keys::OPENAI_INTENT)?,
      initiator: required(map, &keys::X_INITIATOR)?,
      github_api_version: required(map, &keys::X_GITHUB_API_VERSION)?,
      interaction_id: required(map, &keys::X_INTERACTION_ID)?,
      interaction_type: required(map, &keys::X_INTERACTION_TYPE)?,
      client_session_id: required(map, &keys::X_CLIENT_SESSION_ID)?,
      agent_task_id: required(map, &keys::X_AGENT_TASK_ID)?,
      stainless_retry_count: optional(map, &keys::X_STAINLESS_RETRY_COUNT),
      stainless_lang: optional(map, &keys::X_STAINLESS_LANG),
      stainless_package_version: optional(map, &keys::X_STAINLESS_PACKAGE_VERSION),
      stainless_os: optional(map, &keys::X_STAINLESS_OS),
      stainless_arch: optional(map, &keys::X_STAINLESS_ARCH),
      stainless_runtime: optional(map, &keys::X_STAINLESS_RUNTIME),
      stainless_runtime_version: optional(map, &keys::X_STAINLESS_RUNTIME_VERSION),
      content_length: optional(map, &keys::CONTENT_LENGTH),
      cookie: optional(map, &keys::COOKIE),
      request_id: optional(map, &keys::X_REQUEST_ID),
    })
  }

  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::USER_AGENT, &self.user_agent);
    put(&mut m, &keys::AUTHORIZATION, &self.authorization);
    put(&mut m, &keys::CONTENT_TYPE, &self.content_type);
    put(&mut m, &keys::ACCEPT, &self.accept);
    put(&mut m, &keys::ACCEPT_ENCODING, &self.accept_encoding);
    put(&mut m, &keys::ACCEPT_LANGUAGE, &self.accept_language);
    put(&mut m, &keys::SEC_FETCH_MODE, &self.sec_fetch_mode);
    put(&mut m, &keys::COPILOT_INTEGRATION_ID, &self.copilot_integration_id);
    put(&mut m, &keys::OPENAI_INTENT, &self.openai_intent);
    put(&mut m, &keys::X_INITIATOR, &self.initiator);
    put(&mut m, &keys::X_GITHUB_API_VERSION, &self.github_api_version);
    put(&mut m, &keys::X_INTERACTION_ID, &self.interaction_id);
    put(&mut m, &keys::X_INTERACTION_TYPE, &self.interaction_type);
    put(&mut m, &keys::X_CLIENT_SESSION_ID, &self.client_session_id);
    put(&mut m, &keys::X_AGENT_TASK_ID, &self.agent_task_id);
    put_opt(&mut m, &keys::X_STAINLESS_RETRY_COUNT, &self.stainless_retry_count);
    put_opt(&mut m, &keys::X_STAINLESS_LANG, &self.stainless_lang);
    put_opt(
      &mut m,
      &keys::X_STAINLESS_PACKAGE_VERSION,
      &self.stainless_package_version,
    );
    put_opt(&mut m, &keys::X_STAINLESS_OS, &self.stainless_os);
    put_opt(&mut m, &keys::X_STAINLESS_ARCH, &self.stainless_arch);
    put_opt(&mut m, &keys::X_STAINLESS_RUNTIME, &self.stainless_runtime);
    put_opt(
      &mut m,
      &keys::X_STAINLESS_RUNTIME_VERSION,
      &self.stainless_runtime_version,
    );
    put_opt(&mut m, &keys::CONTENT_LENGTH, &self.content_length);
    put_opt(&mut m, &keys::COOKIE, &self.cookie);
    put_opt(&mut m, &keys::X_REQUEST_ID, &self.request_id);
    m
  }

  fn known_names() -> &'static [&'static HeaderName] {
    const NAMES: [&HeaderName; 25] = [
      &keys::USER_AGENT,
      &keys::AUTHORIZATION,
      &keys::CONTENT_TYPE,
      &keys::ACCEPT,
      &keys::ACCEPT_ENCODING,
      &keys::ACCEPT_LANGUAGE,
      &keys::SEC_FETCH_MODE,
      &keys::COPILOT_INTEGRATION_ID,
      &keys::OPENAI_INTENT,
      &keys::X_INITIATOR,
      &keys::X_GITHUB_API_VERSION,
      &keys::X_INTERACTION_ID,
      &keys::X_INTERACTION_TYPE,
      &keys::X_CLIENT_SESSION_ID,
      &keys::X_AGENT_TASK_ID,
      &keys::X_STAINLESS_RETRY_COUNT,
      &keys::X_STAINLESS_LANG,
      &keys::X_STAINLESS_PACKAGE_VERSION,
      &keys::X_STAINLESS_OS,
      &keys::X_STAINLESS_ARCH,
      &keys::X_STAINLESS_RUNTIME,
      &keys::X_STAINLESS_RUNTIME_VERSION,
      &keys::CONTENT_LENGTH,
      &keys::COOKIE,
      &keys::X_REQUEST_ID,
    ];
    &NAMES
  }
}

impl CopilotCliHeaders {
  /// Build a [`CopilotCliHeaders`] from inbound transport headers and
  /// correlation [`TemplateVars`]. Defaults derived from the captured POST
  /// `/responses` request documented at the module top.
  pub fn build(vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      user_agent: from_inbound_or(inbound, &keys::USER_AGENT, || {
        "copilot/1.0.25 (client/sdk win32 v22.19.0) term/unknown".into()
      }),
      authorization: from_inbound_or(inbound, &keys::AUTHORIZATION, || "<missing>".into()),
      content_type: from_inbound_or(inbound, &keys::CONTENT_TYPE, || "application/json".into()),
      accept: from_inbound_or(inbound, &keys::ACCEPT, || "application/json".into()),
      accept_encoding: from_inbound_or(inbound, &keys::ACCEPT_ENCODING, || "br, gzip, deflate".into()),
      accept_language: from_inbound_or(inbound, &keys::ACCEPT_LANGUAGE, || "*".into()),
      sec_fetch_mode: from_inbound_or(inbound, &keys::SEC_FETCH_MODE, || "cors".into()),
      copilot_integration_id: from_inbound_or(inbound, &keys::COPILOT_INTEGRATION_ID, || {
        "copilot-developer-cli".into()
      }),
      openai_intent: from_inbound_or(inbound, &keys::OPENAI_INTENT, || "conversation-agent".into()),
      initiator: from_inbound_or(inbound, &keys::X_INITIATOR, || "user".into()),
      github_api_version: from_inbound_or(inbound, &keys::X_GITHUB_API_VERSION, || "2026-01-09".into()),
      interaction_id: vars.interaction_id.clone().unwrap_or_else(|| {
        from_inbound_or(inbound, &keys::X_INTERACTION_ID, || {
          "00000000-0000-0000-0000-000000000000".into()
        })
      }),
      interaction_type: from_inbound_or(inbound, &keys::X_INTERACTION_TYPE, || "conversation-user".into()),
      client_session_id: vars.session_id.clone().unwrap_or_else(|| {
        from_inbound_or(inbound, &keys::X_CLIENT_SESSION_ID, || {
          "00000000-0000-0000-0000-000000000000".into()
        })
      }),
      agent_task_id: from_inbound_or(inbound, &keys::X_AGENT_TASK_ID, || {
        "00000000-0000-0000-0000-000000000000".into()
      }),
      stainless_retry_count: opt_from_inbound(inbound, &keys::X_STAINLESS_RETRY_COUNT),
      stainless_lang: opt_from_inbound(inbound, &keys::X_STAINLESS_LANG),
      stainless_package_version: opt_from_inbound(inbound, &keys::X_STAINLESS_PACKAGE_VERSION),
      stainless_os: opt_from_inbound(inbound, &keys::X_STAINLESS_OS),
      stainless_arch: opt_from_inbound(inbound, &keys::X_STAINLESS_ARCH),
      stainless_runtime: opt_from_inbound(inbound, &keys::X_STAINLESS_RUNTIME),
      stainless_runtime_version: opt_from_inbound(inbound, &keys::X_STAINLESS_RUNTIME_VERSION),
      content_length: opt_from_inbound(inbound, &keys::CONTENT_LENGTH),
      cookie: opt_from_inbound(inbound, &keys::COOKIE),
      request_id: vars
        .request_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_REQUEST_ID)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn captured() -> CopilotCliHeaders {
    CopilotCliHeaders {
      user_agent: "copilot/1.0.25 (client/sdk win32 v22.19.0) term/unknown".into(),
      authorization: "Bearer gho_xxxx".into(),
      content_type: "application/json".into(),
      accept: "application/json".into(),
      accept_encoding: "br, gzip, deflate".into(),
      accept_language: "*".into(),
      sec_fetch_mode: "cors".into(),
      copilot_integration_id: "copilot-developer-cli".into(),
      openai_intent: "conversation-agent".into(),
      initiator: "user".into(),
      github_api_version: "2026-01-09".into(),
      interaction_id: "6342b4bb-3441-4440-aae6-884912e51b08".into(),
      interaction_type: "conversation-user".into(),
      client_session_id: "07e363b0-ea39-48e6-a45e-fe5c6066e50d".into(),
      agent_task_id: "08a2565a-6ab6-4613-ae6d-3b0814061942".into(),
      stainless_retry_count: Some("0".into()),
      stainless_lang: Some("js".into()),
      stainless_package_version: Some("5.20.1".into()),
      stainless_os: Some("Windows".into()),
      stainless_arch: Some("x64".into()),
      stainless_runtime: Some("node".into()),
      stainless_runtime_version: Some("v22.19.0".into()),
      content_length: Some("58364".into()),
      cookie: None,
      request_id: None,
    }
  }

  #[test]
  fn round_trip_matches_capture() {
    let h = captured();
    assert_eq!(CopilotCliHeaders::parse(&h.dump()).unwrap(), h);
  }

  #[test]
  fn missing_required_errors() {
    let m = HeaderMap::new();
    assert!(matches!(CopilotCliHeaders::parse(&m), Err(Error::MissingHeader { .. })));
  }

  #[test]
  fn optional_fields_omitted_when_none() {
    let mut h = captured();
    h.content_length = None;
    h.cookie = None;
    h.request_id = None;
    h.stainless_retry_count = None;
    h.stainless_lang = None;
    h.stainless_package_version = None;
    h.stainless_os = None;
    h.stainless_arch = None;
    h.stainless_runtime = None;
    h.stainless_runtime_version = None;
    let m = h.dump();
    assert!(!m.contains_key(&keys::CONTENT_LENGTH));
    assert!(!m.contains_key(&keys::COOKIE));
    assert!(!m.contains_key(&keys::X_REQUEST_ID));
    assert!(!m.contains_key(&keys::X_STAINLESS_LANG));
    // 15 required fields written (22 - 7 stainless now optional).
    assert_eq!(m.len(), 15);
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = CopilotCliHeaders::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(
      h.user_agent.as_str(),
      "copilot/1.0.25 (client/sdk win32 v22.19.0) term/unknown"
    );
    assert_eq!(h.authorization.as_str(), "<missing>");
    assert_eq!(h.copilot_integration_id.as_str(), "copilot-developer-cli");
    assert_eq!(h.openai_intent.as_str(), "conversation-agent");
    assert_eq!(h.initiator.as_str(), "user");
    assert_eq!(h.github_api_version.as_str(), "2026-01-09");
    assert!(h.stainless_lang.is_none());
    assert!(h.cookie.is_none());
    assert!(h.request_id.is_none());
    assert!(h.content_length.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::USER_AGENT, "copilot/2.0");
    inbound.insert(&keys::AUTHORIZATION, "Bearer gho_abc");
    inbound.insert(&keys::X_STAINLESS_LANG, "js");
    let h = CopilotCliHeaders::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.user_agent.as_str(), "copilot/2.0");
    assert_eq!(h.authorization.as_str(), "Bearer gho_abc");
    assert_eq!(h.stainless_lang.as_deref(), Some("js"));
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      interaction_id: Some("int_42".into()),
      request_id: Some("req_99".into()),
      ..Default::default()
    };
    let h = CopilotCliHeaders::build(&vars, &HeaderMap::new());
    assert_eq!(h.client_session_id.as_str(), "ses_xyz");
    assert_eq!(h.interaction_id.as_str(), "int_42");
    assert_eq!(h.request_id.as_deref(), Some("req_99"));
  }
}
