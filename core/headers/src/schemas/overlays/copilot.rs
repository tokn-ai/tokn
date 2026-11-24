//! GitHub Copilot transport overlay.
//!
//! Headers required by the Copilot proxy regardless of which CLI persona
//! originated the request.
//!
//! SCOPE: this overlay models **outbound** headers the router injects when
//! forwarding to `api.githubcopilot.com`. The mined inbound matrix never
//! shows `Editor-Version`, `Editor-Plugin-Version`, `Copilot-Integration-Id`,
//! or `Copilot-Vision-Request` because those are added downstream of the
//! gateway. Inbound-only Copilot signals (e.g. `X-Initiator`,
//! `OpenAI-Intent`) are observed from CLI clients targeting the gateway.

use crate::error::Error;
use crate::keys;
use crate::map::HeaderMap;
use crate::name::HeaderName;
use crate::schema::{from_inbound_or, opt_from_inbound, optional, put, put_opt, required, HeaderSchema};
use crate::vars::TemplateVars;
use alloc::string::ToString;
use serde::{Deserialize, Serialize};
use tokn_core::types::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopilotOverlay {
  #[serde(rename = "Editor-Version")]
  pub editor_version: SmolStr,
  #[serde(rename = "Editor-Plugin-Version")]
  pub editor_plugin_version: SmolStr,
  #[serde(rename = "Copilot-Integration-Id")]
  pub integration_id: SmolStr,
  #[serde(rename = "Copilot-Vision-Request")]
  pub vision_request: Option<SmolStr>,
  #[serde(rename = "X-Initiator")]
  pub initiator: Option<SmolStr>,
}

impl HeaderSchema for CopilotOverlay {
  fn parse(map: &HeaderMap) -> Result<Self, Error> {
    Ok(Self {
      editor_version: required(map, &keys::EDITOR_VERSION)?,
      editor_plugin_version: required(map, &keys::EDITOR_PLUGIN_VERSION)?,
      integration_id: required(map, &keys::COPILOT_INTEGRATION_ID)?,
      vision_request: optional(map, &keys::COPILOT_VISION_REQUEST),
      initiator: optional(map, &keys::X_INITIATOR),
    })
  }
  fn dump(&self) -> HeaderMap {
    let mut m = HeaderMap::new();
    put(&mut m, &keys::EDITOR_VERSION, &self.editor_version);
    put(&mut m, &keys::EDITOR_PLUGIN_VERSION, &self.editor_plugin_version);
    put(&mut m, &keys::COPILOT_INTEGRATION_ID, &self.integration_id);
    put_opt(&mut m, &keys::COPILOT_VISION_REQUEST, &self.vision_request);
    put_opt(&mut m, &keys::X_INITIATOR, &self.initiator);
    m
  }
  fn known_names() -> &'static [&'static HeaderName] {
    const NAMES: [&HeaderName; 5] = [
      &keys::EDITOR_VERSION,
      &keys::EDITOR_PLUGIN_VERSION,
      &keys::COPILOT_INTEGRATION_ID,
      &keys::COPILOT_VISION_REQUEST,
      &keys::X_INITIATOR,
    ];
    &NAMES
  }
}

impl CopilotOverlay {
  /// Build a [`CopilotOverlay`] from inbound transport headers and
  /// correlation [`TemplateVars`]. Required overlay fields fall back to
  /// canonical Copilot gateway defaults when absent from the inbound map.
  pub fn build(_vars: &TemplateVars, inbound: &HeaderMap) -> Self {
    Self {
      editor_version: from_inbound_or(inbound, &keys::EDITOR_VERSION, || "vscode/1.95.0".into()),
      editor_plugin_version: from_inbound_or(inbound, &keys::EDITOR_PLUGIN_VERSION, || "copilot-chat/0.23.0".into()),
      integration_id: from_inbound_or(inbound, &keys::COPILOT_INTEGRATION_ID, || "vscode-chat".into()),
      vision_request: opt_from_inbound(inbound, &keys::COPILOT_VISION_REQUEST),
      initiator: opt_from_inbound(inbound, &keys::X_INITIATOR),
    }
  }

  /// Apply this overlay onto an outbound [`HeaderMap`]. Gateway-identifying
  /// headers (`Editor-Version`, `Editor-Plugin-Version`,
  /// `Copilot-Integration-Id`) override any existing values; optional fields
  /// (`Copilot-Vision-Request`, `X-Initiator`) are filled in only when the
  /// overlay has a value AND the header is not already present on the map.
  pub fn apply_to(&self, map: &mut HeaderMap, _vars: &TemplateVars) {
    map.insert(&keys::EDITOR_VERSION, self.editor_version.to_string());
    map.insert(&keys::EDITOR_PLUGIN_VERSION, self.editor_plugin_version.to_string());
    map.insert(&keys::COPILOT_INTEGRATION_ID, self.integration_id.to_string());
    if let Some(v) = &self.vision_request {
      if !map.contains_key(&keys::COPILOT_VISION_REQUEST) {
        map.insert(&keys::COPILOT_VISION_REQUEST, v.to_string());
      }
    }
    if let Some(v) = &self.initiator {
      if !map.contains_key(&keys::X_INITIATOR) {
        map.insert(&keys::X_INITIATOR, v.to_string());
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trip() {
    let h = CopilotOverlay {
      editor_version: "vscode/1.95.0".into(),
      editor_plugin_version: "copilot-chat/0.23.0".into(),
      integration_id: "vscode-chat".into(),
      vision_request: Some("true".into()),
      initiator: Some("agent".into()),
    };
    assert_eq!(CopilotOverlay::parse(&h.dump()).unwrap(), h);
  }

  #[test]
  fn missing_required_errors() {
    let m = HeaderMap::new();
    assert!(matches!(CopilotOverlay::parse(&m), Err(Error::MissingHeader { .. })));
  }

  #[test]
  fn build_with_empty_inbound_uses_defaults() {
    let h = CopilotOverlay::build(&TemplateVars::default(), &HeaderMap::new());
    assert_eq!(h.editor_version.as_str(), "vscode/1.95.0");
    assert_eq!(h.editor_plugin_version.as_str(), "copilot-chat/0.23.0");
    assert_eq!(h.integration_id.as_str(), "vscode-chat");
    assert!(h.vision_request.is_none());
    assert!(h.initiator.is_none());
  }

  #[test]
  fn build_passes_through_inbound() {
    let mut inbound = HeaderMap::new();
    inbound.insert(&keys::EDITOR_VERSION, "vscode/1.99.0");
    inbound.insert(&keys::COPILOT_VISION_REQUEST, "true");
    let h = CopilotOverlay::build(&TemplateVars::default(), &inbound);
    assert_eq!(h.editor_version.as_str(), "vscode/1.99.0");
    assert_eq!(h.vision_request.as_deref(), Some("true"));
  }

  #[test]
  fn build_uses_vars_for_correlation() {
    // CopilotOverlay holds no correlation fields itself; vars should not panic
    // and required fields should still come from defaults.
    let vars = TemplateVars {
      session_id: Some("ses_xyz".into()),
      ..Default::default()
    };
    let h = CopilotOverlay::build(&vars, &HeaderMap::new());
    assert_eq!(h.integration_id.as_str(), "vscode-chat");
  }

  #[test]
  fn apply_to_overrides_managed_fields_and_skips_optionals_when_none() {
    // Start from an outbound map dumped from a CopilotCli persona-ish request.
    let mut map = HeaderMap::new();
    map.insert(&keys::EDITOR_VERSION, "stale/0.0.0");
    map.insert(&keys::COPILOT_INTEGRATION_ID, "old-integration");
    map.insert(&keys::X_INITIATOR, "preexisting");

    let overlay = CopilotOverlay {
      editor_version: "vscode/1.95.0".into(),
      editor_plugin_version: "copilot-chat/0.23.0".into(),
      integration_id: "vscode-chat".into(),
      vision_request: None,
      initiator: Some("agent".into()),
    };
    overlay.apply_to(&mut map, &TemplateVars::default());

    // Managed fields overridden.
    assert_eq!(map.get(&keys::EDITOR_VERSION).unwrap().as_str(), "vscode/1.95.0");
    assert_eq!(map.get(&keys::COPILOT_INTEGRATION_ID).unwrap().as_str(), "vscode-chat");
    assert_eq!(
      map.get(&keys::EDITOR_PLUGIN_VERSION).unwrap().as_str(),
      "copilot-chat/0.23.0"
    );
    // Pre-existing X-Initiator preserved (we only fill if absent).
    assert_eq!(map.get(&keys::X_INITIATOR).unwrap().as_str(), "preexisting");
    // None-valued optional not inserted.
    assert!(!map.contains_key(&keys::COPILOT_VISION_REQUEST));
  }
}
