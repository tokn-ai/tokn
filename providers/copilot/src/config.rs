use alloc::{collections::BTreeMap, string::String};
#[cfg(feature = "std")]
use reqwest::header::HeaderName;
use serde::{Deserialize, Serialize};
#[cfg(feature = "std")]
use serde_json::Value;
#[cfg(feature = "std")]
use snafu::ResultExt;
#[cfg(feature = "std")]
use tokn_core::provider::error;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InitiatorMode {
  #[default]
  Auto,
  AlwaysUser,
  AlwaysAgent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopilotHeaders {
  #[serde(default = "default_editor_version")]
  pub editor_version: String,
  #[serde(default = "default_editor_plugin_version")]
  pub editor_plugin_version: String,
  #[serde(default = "default_user_agent")]
  pub user_agent: String,
  #[serde(default = "default_integration_id")]
  pub copilot_integration_id: String,
  #[serde(default = "default_openai_intent")]
  pub openai_intent: String,
  #[serde(default)]
  pub initiator_mode: InitiatorMode,
  #[serde(default)]
  pub extra_headers: BTreeMap<String, String>,
}

impl Default for CopilotHeaders {
  fn default() -> Self {
    Self {
      editor_version: default_editor_version(),
      editor_plugin_version: default_editor_plugin_version(),
      user_agent: default_user_agent(),
      copilot_integration_id: default_integration_id(),
      openai_intent: default_openai_intent(),
      initiator_mode: InitiatorMode::default(),
      extra_headers: BTreeMap::new(),
    }
  }
}

fn default_editor_version() -> String {
  "vscode/1.95.0".into()
}

fn default_editor_plugin_version() -> String {
  "copilot-chat/0.20.0".into()
}

fn default_user_agent() -> String {
  "GitHubCopilotChat/0.20.0".into()
}

fn default_integration_id() -> String {
  "vscode-chat".into()
}

fn default_openai_intent() -> String {
  "conversation-panel".into()
}

impl CopilotHeaders {
  /// Decode provider policy once for both platform implementations.
  pub fn from_account(account: &tokn_core::account::AccountConfig) -> Result<Self, serde_json::Error> {
    let value = serde_json::to_value(&account.settings)?;
    let mut headers: Self = serde_json::from_value(value)?;
    for (name, value) in &account.headers {
      headers.extra_headers.insert(name.clone(), value.clone());
    }
    Ok(headers)
  }

  #[cfg(feature = "std")]
  pub fn from_value(value: &Value) -> tokn_core::provider::Result<Self> {
    if value.is_null() {
      return Ok(Self::default());
    }
    serde_json::from_value(value.clone()).map_err(|source| error::Error::Json {
      what: "copilot headers config",
      body: value.to_string(),
      source,
    })
  }

  pub fn merged(&self, override_: Option<&CopilotHeaders>) -> CopilotHeaders {
    match override_ {
      None => self.clone(),
      Some(o) => {
        let mut extra = self.extra_headers.clone();
        for (k, v) in &o.extra_headers {
          extra.insert(k.clone(), v.clone());
        }
        CopilotHeaders {
          editor_version: o.editor_version.clone(),
          editor_plugin_version: o.editor_plugin_version.clone(),
          user_agent: o.user_agent.clone(),
          copilot_integration_id: o.copilot_integration_id.clone(),
          openai_intent: o.openai_intent.clone(),
          initiator_mode: o.initiator_mode,
          extra_headers: extra,
        }
      }
    }
  }

  #[cfg(feature = "std")]
  pub fn validate(&self) -> tokn_core::provider::Result<()> {
    for name in self.extra_headers.keys() {
      if !is_token(name) {
        HeaderName::from_bytes(name.as_bytes()).context(error::HeaderNameSnafu { name: name.clone() })?;
      }
    }
    Ok(())
  }
}

#[cfg(feature = "std")]
fn is_token(s: &str) -> bool {
  !s.is_empty()
    && s.bytes().all(|b| {
      matches!(b,
            b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+'
            | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            | b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z')
    })
}
