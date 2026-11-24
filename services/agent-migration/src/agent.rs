use serde::{Deserialize, Serialize};
use tokn_core::provider::{ID_CODEX, ID_OPENAI};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentKind {
  CodexCli,
  Opencode,
}

impl AgentKind {
  pub fn slug(self) -> &'static str {
    match self {
      Self::CodexCli => "codex-cli",
      Self::Opencode => "opencode",
    }
  }

  pub(crate) fn agent_id(self) -> &'static str {
    match self {
      Self::CodexCli => "codex-cli",
      Self::Opencode => "opencode",
    }
  }

  pub(crate) fn default_provider_id(self) -> &'static str {
    match self {
      Self::CodexCli => ID_CODEX,
      Self::Opencode => ID_OPENAI,
    }
  }
}
