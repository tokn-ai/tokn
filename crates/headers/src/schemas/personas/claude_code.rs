//! Headers emitted by the Claude Code CLI client.
//!
//! NOTE: not yet verified against real-world inbound captures — no
//! `claude-cli` traffic was observed in the mined request logs. Field set is
//! a best-effort outbound model and may need refinement once captures
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
pub struct ClaudeCodeHeaders {
  #[serde(rename = "User-Agent")]
  pub user_agent: SmolStr,
  #[serde(rename = "Accept", skip_serializing_if = "Option::is_none")]
  pub accept: Option<SmolStr>,
  #[serde(rename = "Anthropic-Version")]
  pub anthropic_version: Option<SmolStr>,
  #[serde(rename = "Anthropic-Beta")]
  pub anthropic_beta: Option<SmolStr>,
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
  #[serde(rename = "X-Stainless-Retry-Count", skip_serializing_if = "Option::is_none")]
  pub stainless_retry_count: Option<SmolStr>,
  #[serde(rename = "X-Stainless-Timeout", skip_serializing_if = "Option::is_none")]
  pub stainless_timeout: Option<SmolStr>,
  #[serde(rename = "x-stainless-helper-method", skip_serializing_if = "Option::is_none")]
  pub stainless_helper_method: Option<SmolStr>,
  #[serde(rename = "X-App", skip_serializing_if = "Option::is_none")]
  pub app: Option<SmolStr>,
  #[serde(
    rename = "Anthropic-Dangerous-Direct-Browser-Access",
    skip_serializing_if = "Option::is_none"
  )]
  pub direct_browser_access: Option<SmolStr>,
  #[serde(rename = "x-client-request-id", skip_serializing_if = "Option::is_none")]
  pub client_request_id: Option<SmolStr>,
  #[serde(rename = "X-Session-Id")]
  pub session_id: Option<SmolStr>,
  #[serde(rename = "X-Interaction-Id")]
  pub interaction_id: Option<SmolStr>,
}

impl HeaderSchema for ClaudeCodeHeaders {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      user_agent: required(map, &keys::USER_AGENT)?,
      accept: optional(map, &keys::ACCEPT),
      anthropic_version: optional(map, &keys::ANTHROPIC_VERSION),
      anthropic_beta: optional(map, &keys::ANTHROPIC_BETA),
      stainless_lang: optional(map, &keys::X_STAINLESS_LANG),
      stainless_package_version: optional(map, &keys::X_STAINLESS_PACKAGE_VERSION),
      stainless_os: optional(map, &keys::X_STAINLESS_OS),
      stainless_arch: optional(map, &keys::X_STAINLESS_ARCH),
      stainless_runtime: optional(map, &keys::X_STAINLESS_RUNTIME),
      stainless_runtime_version: optional(map, &keys::X_STAINLESS_RUNTIME_VERSION),
      stainless_retry_count: optional(map, &keys::X_STAINLESS_RETRY_COUNT),
      stainless_timeout: optional(map, &keys::X_STAINLESS_TIMEOUT),
      stainless_helper_method: optional(map, &keys::X_STAINLESS_HELPER_METHOD),
      app: optional(map, &keys::X_APP),
      direct_browser_access: optional(map, &keys::ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS),
      client_request_id: optional(map, &keys::X_CLIENT_REQUEST_ID),
      session_id: optional(map, &keys::X_SESSION_ID),
      interaction_id: optional(map, &keys::X_INTERACTION_ID),
    })
  }
  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::USER_AGENT, &self.user_agent);
    put_opt(&mut m, &keys::ACCEPT, &self.accept);
    put_opt(&mut m, &keys::ANTHROPIC_VERSION, &self.anthropic_version);
    put_opt(&mut m, &keys::ANTHROPIC_BETA, &self.anthropic_beta);
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
    put_opt(&mut m, &keys::X_STAINLESS_RETRY_COUNT, &self.stainless_retry_count);
    put_opt(&mut m, &keys::X_STAINLESS_TIMEOUT, &self.stainless_timeout);
    put_opt(&mut m, &keys::X_STAINLESS_HELPER_METHOD, &self.stainless_helper_method);
    put_opt(&mut m, &keys::X_APP, &self.app);
    put_opt(
      &mut m,
      &keys::ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS,
      &self.direct_browser_access,
    );
    put_opt(&mut m, &keys::X_CLIENT_REQUEST_ID, &self.client_request_id);
    put_opt(&mut m, &keys::X_SESSION_ID, &self.session_id);
    put_opt(&mut m, &keys::X_INTERACTION_ID, &self.interaction_id);
    m
  }
  fn known_names() -> &'static [&'static HeaderName] {
    const NAMES: [&HeaderName; 18] = [
      &keys::USER_AGENT,
      &keys::ACCEPT,
      &keys::ANTHROPIC_VERSION,
      &keys::ANTHROPIC_BETA,
      &keys::X_STAINLESS_LANG,
      &keys::X_STAINLESS_PACKAGE_VERSION,
      &keys::X_STAINLESS_OS,
      &keys::X_STAINLESS_ARCH,
      &keys::X_STAINLESS_RUNTIME,
      &keys::X_STAINLESS_RUNTIME_VERSION,
      &keys::X_STAINLESS_RETRY_COUNT,
      &keys::X_STAINLESS_TIMEOUT,
      &keys::X_STAINLESS_HELPER_METHOD,
      &keys::X_APP,
      &keys::ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS,
      &keys::X_CLIENT_REQUEST_ID,
      &keys::X_SESSION_ID,
      &keys::X_INTERACTION_ID,
    ];
    &NAMES
  }
}

impl ClaudeCodeHeaders {
  /// Build a [`ClaudeCodeHeaders`] from inbound transport headers and
  /// correlation [`TemplateVars`].
  pub fn build(vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      user_agent: from_inbound_or(inbound, &keys::USER_AGENT, || {
        "claude-cli/2.1.92 (external, cli)".into()
      }),
      accept: Some(from_inbound_or(inbound, &keys::ACCEPT, || "application/json".into())),
      anthropic_version: Some(from_inbound_or(inbound, &keys::ANTHROPIC_VERSION, || {
        "2023-06-01".into()
      })),
      anthropic_beta: Some(from_inbound_or(inbound, &keys::ANTHROPIC_BETA, || {
        "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14"
          .into()
      })),
      stainless_lang: Some(from_inbound_or(inbound, &keys::X_STAINLESS_LANG, || "js".into())),
      stainless_package_version: Some(from_inbound_or(inbound, &keys::X_STAINLESS_PACKAGE_VERSION, || {
        "0.70.0".into()
      })),
      stainless_os: Some(from_inbound_or(inbound, &keys::X_STAINLESS_OS, || "Linux".into())),
      stainless_arch: Some(from_inbound_or(inbound, &keys::X_STAINLESS_ARCH, || "arm64".into())),
      stainless_runtime: Some(from_inbound_or(inbound, &keys::X_STAINLESS_RUNTIME, || "node".into())),
      stainless_runtime_version: Some(from_inbound_or(inbound, &keys::X_STAINLESS_RUNTIME_VERSION, || {
        "v24.13.0".into()
      })),
      stainless_retry_count: Some(from_inbound_or(inbound, &keys::X_STAINLESS_RETRY_COUNT, || "0".into())),
      stainless_timeout: Some(from_inbound_or(inbound, &keys::X_STAINLESS_TIMEOUT, || "600".into())),
      stainless_helper_method: opt_from_inbound(inbound, &keys::X_STAINLESS_HELPER_METHOD),
      app: Some(from_inbound_or(inbound, &keys::X_APP, || "cli".into())),
      direct_browser_access: Some(from_inbound_or(
        inbound,
        &keys::ANTHROPIC_DANGEROUS_DIRECT_BROWSER_ACCESS,
        || "true".into(),
      )),
      client_request_id: vars
        .request_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_CLIENT_REQUEST_ID)),
      session_id: vars
        .session_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_SESSION_ID)),
      interaction_id: vars
        .interaction_id
        .clone()
        .or_else(|| opt_from_inbound(inbound, &keys::X_INTERACTION_ID)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trip() {
    let h = ClaudeCodeHeaders {
      user_agent: "claude-code/1.2.3".into(),
      accept: Some("application/json".into()),
      anthropic_version: Some("2023-06-01".into()),
      anthropic_beta: Some("messages-2023-12-15".into()),
      stainless_lang: Some("js".into()),
      stainless_package_version: Some("0.70.0".into()),
      stainless_os: Some("Linux".into()),
      stainless_arch: Some("arm64".into()),
      stainless_runtime: Some("node".into()),
      stainless_runtime_version: Some("v24.13.0".into()),
      stainless_retry_count: Some("0".into()),
      stainless_timeout: Some("600".into()),
      stainless_helper_method: Some("stream".into()),
      app: Some("cli".into()),
      direct_browser_access: Some("true".into()),
      client_request_id: Some("req-1".into()),
      session_id: Some("ses_cc".into()),
      interaction_id: Some("int_99".into()),
    };
    assert_eq!(ClaudeCodeHeaders::parse(&h.dump()).unwrap(), h);
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = ClaudeCodeHeaders::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(h.user_agent.as_str(), "claude-cli/2.1.92 (external, cli)");
    assert_eq!(h.accept.as_deref(), Some("application/json"));
    assert_eq!(h.anthropic_version.as_deref(), Some("2023-06-01"));
    assert_eq!(
      h.anthropic_beta.as_deref(),
      Some(
        "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14"
      )
    );
    assert_eq!(h.stainless_lang.as_deref(), Some("js"));
    assert_eq!(h.stainless_package_version.as_deref(), Some("0.70.0"));
    assert_eq!(h.stainless_os.as_deref(), Some("Linux"));
    assert_eq!(h.stainless_arch.as_deref(), Some("arm64"));
    assert_eq!(h.stainless_runtime.as_deref(), Some("node"));
    assert_eq!(h.stainless_runtime_version.as_deref(), Some("v24.13.0"));
    assert_eq!(h.stainless_retry_count.as_deref(), Some("0"));
    assert_eq!(h.stainless_timeout.as_deref(), Some("600"));
    assert_eq!(h.app.as_deref(), Some("cli"));
    assert_eq!(h.direct_browser_access.as_deref(), Some("true"));
    assert!(h.stainless_helper_method.is_none());
    assert!(h.client_request_id.is_none());
    assert!(h.session_id.is_none());
    assert!(h.interaction_id.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::USER_AGENT, "claude-cli/2.0");
    inbound.insert(&keys::ANTHROPIC_VERSION, "2023-06-01");
    let h = ClaudeCodeHeaders::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.user_agent.as_str(), "claude-cli/2.0");
    assert_eq!(h.anthropic_version.as_deref(), Some("2023-06-01"));
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      request_id: Some("req_xyz".into()),
      interaction_id: Some("int_42".into()),
      ..Default::default()
    };
    let h = ClaudeCodeHeaders::build(&vars, &HeaderMap::new());
    assert_eq!(h.client_request_id.as_deref(), Some("req_xyz"));
    assert_eq!(h.session_id.as_deref(), Some("ses_xyz"));
    assert_eq!(h.interaction_id.as_deref(), Some("int_42"));
  }
}
