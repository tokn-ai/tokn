use crate::config::CopilotHeaders;
use crate::provider::Result;
use tokn_headers::keys::{
  ACCEPT, AUTHORIZATION, COPILOT_INTEGRATION_ID, EDITOR_PLUGIN_VERSION, EDITOR_VERSION, OPENAI_INTENT, USER_AGENT,
  X_INITIATOR,
};
use tokn_headers::{HeaderMap, HeaderValue};

/// Headers for the Copilot API token exchange (`api.github.com`).
pub fn token_exchange_headers(github_token: &str, h: &CopilotHeaders) -> Result<HeaderMap> {
  Ok(crate::native::token_exchange_headers(github_token, h))
}

/// Headers for upstream Copilot API requests (chat / models).
///
/// `initiator` must be "user" or "agent". It is sent as `X-Initiator` and is
/// what GitHub's billing pipeline uses to attribute premium-request charges to
/// a single user-initiated turn rather than to every tool-call follow-up.
pub fn copilot_request_headers(
  api_token: &str,
  h: &CopilotHeaders,
  streaming: bool,
  initiator: &str,
) -> Result<HeaderMap> {
  let mut m = HeaderMap::new();
  m.insert(&AUTHORIZATION, HeaderValue::from_string(format!("Bearer {api_token}")));
  m.insert(
    &ACCEPT,
    HeaderValue::from_static(if streaming {
      "text/event-stream"
    } else {
      "application/json"
    }),
  );
  m.insert(&USER_AGENT, HeaderValue::from_string(h.user_agent.clone()));
  m.insert(&EDITOR_VERSION, HeaderValue::from_string(h.editor_version.clone()));
  m.insert(
    &EDITOR_PLUGIN_VERSION,
    HeaderValue::from_string(h.editor_plugin_version.clone()),
  );
  m.insert(
    &COPILOT_INTEGRATION_ID,
    HeaderValue::from_string(h.copilot_integration_id.clone()),
  );
  m.insert(&OPENAI_INTENT, HeaderValue::from_string(h.openai_intent.clone()));
  m.insert(&X_INITIATOR, HeaderValue::from_string(initiator.to_string()));

  // Extra (free-form) headers — applied last, overriding earlier values.
  for (k, v) in &h.extra_headers {
    m.insert(k.as_str(), HeaderValue::from_string(v.clone()));
  }
  Ok(m)
}

#[cfg(test)]
mod tests {
  use super::*;

  fn defaults() -> CopilotHeaders {
    CopilotHeaders::default()
  }

  #[test]
  fn copilot_request_headers_includes_editor_metadata_and_intent() {
    let h = copilot_request_headers("api-tok", &defaults(), false, "user").unwrap();
    assert_eq!(h.get("authorization").unwrap().as_str(), "Bearer api-tok");
    assert_eq!(h.get("accept").unwrap().as_str(), "application/json");
    assert_eq!(h.get("user-agent").unwrap().as_str(), "GitHubCopilotChat/0.20.0");
    assert_eq!(h.get("editor-version").unwrap().as_str(), "vscode/1.95.0");
    assert_eq!(h.get("editor-plugin-version").unwrap().as_str(), "copilot-chat/0.20.0");
    assert_eq!(h.get("copilot-integration-id").unwrap().as_str(), "vscode-chat");
    assert_eq!(h.get("openai-intent").unwrap().as_str(), "conversation-panel");
    assert_eq!(h.get("x-initiator").unwrap().as_str(), "user");
    let names: Vec<_> = h.iter().map(|(n, _)| n.as_str().to_string()).collect();
    assert_eq!(names.len(), 8, "unexpected extra headers: {names:?}");
  }

  #[test]
  fn copilot_request_headers_streaming_toggles_accept() {
    let h = copilot_request_headers("api-tok", &defaults(), true, "user").unwrap();
    assert_eq!(h.get("accept").unwrap().as_str(), "text/event-stream");
  }

  #[test]
  fn copilot_request_headers_x_initiator_round_trips_user_and_agent() {
    let user = copilot_request_headers("t", &defaults(), false, "user").unwrap();
    let agent = copilot_request_headers("t", &defaults(), false, "agent").unwrap();
    assert_eq!(user.get("x-initiator").unwrap().as_str(), "user");
    assert_eq!(agent.get("x-initiator").unwrap().as_str(), "agent");
  }

  #[test]
  fn copilot_request_headers_extra_headers_override_defaults_last() {
    let mut h_cfg = defaults();
    // Same key as a default to prove last-wins; plus a brand-new key.
    h_cfg
      .extra_headers
      .insert("editor-version".into(), "neovim/0.10.0".into());
    h_cfg.extra_headers.insert("x-custom".into(), "yes".into());
    let h = copilot_request_headers("t", &h_cfg, false, "user").unwrap();
    assert_eq!(h.get("editor-version").unwrap().as_str(), "neovim/0.10.0");
    assert_eq!(h.get("x-custom").unwrap().as_str(), "yes");
  }

  #[test]
  fn token_exchange_headers_shape_is_stable() {
    let h = token_exchange_headers("gh-pat", &defaults()).unwrap();
    assert_eq!(h.get("authorization").unwrap().as_str(), "token gh-pat");
    assert_eq!(h.get("accept").unwrap().as_str(), "application/json");
    assert_eq!(h.get("user-agent").unwrap().as_str(), "GitHubCopilotChat/0.20.0");
    assert_eq!(h.get("editor-version").unwrap().as_str(), "vscode/1.95.0");
    assert_eq!(h.get("editor-plugin-version").unwrap().as_str(), "copilot-chat/0.20.0");
    let names: Vec<_> = h.iter().map(|(n, _)| n.as_str().to_string()).collect();
    assert_eq!(names.len(), 5, "unexpected extra headers: {names:?}");
  }
}
