//! Logical identifier for the *agent* whose behavior an upstream call should
//! impersonate. Carried end-to-end through the pipeline so downstream stages
//! (header shaping, provider selection) can branch on it without reparsing
//! inbound traffic.

use crate::types::SmolStr;
use alloc::string::String;
#[cfg(test)]
use alloc::string::ToString;
use core::convert::Infallible;
use core::fmt;
use core::str::FromStr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "SmolStr", into = "SmolStr")]
pub enum AgentId {
  #[default]
  Opencode,
  CodexCli,
  ClaudeCode,
  Cline,
  CopilotCli,
  Other(SmolStr),
}

impl AgentId {
  pub fn as_str(&self) -> &str {
    match self {
      Self::Opencode => "opencode",
      Self::CodexCli => "codex-cli",
      Self::ClaudeCode => "claude-code",
      Self::Cline => "cline",
      Self::CopilotCli => "copilot-cli",
      Self::Other(value) => value.as_str(),
    }
  }

  pub fn from_slug(slug: &str) -> Option<Self> {
    match slug {
      "opencode" => Some(Self::Opencode),
      "codex_exec" | "codex-tui" | "codex" | "codex-cli" => Some(Self::CodexCli),
      "claude-cli" | "claude-code" => Some(Self::ClaudeCode),
      "cline" => Some(Self::Cline),
      "copilot" | "copilot-cli" => Some(Self::CopilotCli),
      _ => None,
    }
  }

  pub fn provider_default(provider_id: &str) -> Option<Self> {
    match provider_id {
      "openai" | "deepseek" | "llama-cpp" | "zai" | "zai-coding-plan" | "zhipuai" | "zhipuai-coding-plan" => {
        Some(Self::Opencode)
      }
      "codex" => Some(Self::CodexCli),
      "copilot" | "github-copilot" => Some(Self::CopilotCli),
      _ => None,
    }
  }
}

impl From<&str> for AgentId {
  fn from(s: &str) -> Self {
    Self::from_slug(s).unwrap_or_else(|| Self::Other(SmolStr::new(s)))
  }
}

impl From<String> for AgentId {
  fn from(s: String) -> Self {
    Self::from(s.as_str())
  }
}

impl From<SmolStr> for AgentId {
  fn from(s: SmolStr) -> Self {
    Self::from(s.as_str())
  }
}

impl From<AgentId> for SmolStr {
  fn from(value: AgentId) -> Self {
    match value {
      AgentId::Other(value) => value,
      other => SmolStr::new(other.as_str()),
    }
  }
}

impl FromStr for AgentId {
  type Err = Infallible;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(Self::from(s))
  }
}

impl fmt::Display for AgentId {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn known_ids_round_trip() {
    for (slug, expected) in [
      ("opencode", AgentId::Opencode),
      ("codex-cli", AgentId::CodexCli),
      ("claude-code", AgentId::ClaudeCode),
      ("cline", AgentId::Cline),
      ("copilot-cli", AgentId::CopilotCli),
    ] {
      assert_eq!(AgentId::from_slug(slug), Some(expected.clone()));
      assert_eq!(expected.as_str(), slug);
      assert_eq!(expected.to_string(), slug);
    }
  }

  #[test]
  fn aliases_normalize() {
    assert_eq!(AgentId::from_slug("codex"), Some(AgentId::CodexCli));
    assert_eq!(AgentId::from_slug("codex_exec"), Some(AgentId::CodexCli));
    assert_eq!(AgentId::from_slug("claude-cli"), Some(AgentId::ClaudeCode));
    assert_eq!(AgentId::from_slug("copilot"), Some(AgentId::CopilotCli));
  }

  #[test]
  fn unknown_slug_falls_back_to_other() {
    let agent_id = AgentId::from("my-bespoke-tool");
    assert_eq!(agent_id, AgentId::Other(SmolStr::new("my-bespoke-tool")));
    assert_eq!(agent_id.as_str(), "my-bespoke-tool");
  }

  #[test]
  fn serde_round_trip_other() {
    let agent_id = AgentId::Other(SmolStr::new("custom-tool"));
    let encoded = serde_json::to_string(&agent_id).unwrap();
    assert_eq!(encoded, "\"custom-tool\"");
    let decoded: AgentId = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, agent_id);
  }

  #[test]
  fn provider_defaults_cover_known_providers() {
    assert_eq!(AgentId::provider_default("openai"), Some(AgentId::Opencode));
    assert_eq!(AgentId::provider_default("llama-cpp"), Some(AgentId::Opencode));
    assert_eq!(AgentId::provider_default("codex"), Some(AgentId::CodexCli));
    assert_eq!(AgentId::provider_default("github-copilot"), Some(AgentId::CopilotCli));
    assert_eq!(AgentId::provider_default("nonesuch"), None);
  }
}
