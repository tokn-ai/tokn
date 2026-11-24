//! Agent-specific header builders used to synthesize outbound request headers.

use crate::map::HeaderMap;
use crate::schema::HeaderSchema;
use crate::schemas::{ClaudeCodeHeaders, ClineHeaders, CodexCliHeaders, CopilotCliHeaders, OpencodeHeaders};
use crate::vars::TemplateVars;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgentKind {
  Opencode,
  CodexCli,
  ClaudeCode,
  Cline,
  CopilotCli,
}

impl AgentKind {
  pub fn from_agent_id(agent_id: &str) -> Option<Self> {
    match agent_id {
      "opencode" => Some(Self::Opencode),
      "codex_exec" | "codex-tui" | "codex" | "codex-cli" => Some(Self::CodexCli),
      "claude-cli" | "claude-code" => Some(Self::ClaudeCode),
      "cline" => Some(Self::Cline),
      "copilot" | "copilot-cli" => Some(Self::CopilotCli),
      _ => None,
    }
  }

  pub fn build_outbound(self, vars: &TemplateVars, inbound: &HeaderMap) -> HeaderMap {
    match self {
      Self::Opencode => OpencodeHeaders::build(vars, inbound).dump(),
      Self::CodexCli => CodexCliHeaders::build(vars, inbound).dump(),
      Self::ClaudeCode => ClaudeCodeHeaders::build(vars, inbound).dump(),
      Self::Cline => ClineHeaders::build(vars, inbound).dump(),
      Self::CopilotCli => CopilotCliHeaders::build(vars, inbound).dump(),
    }
  }
}

pub fn build_agent_headers(agent_id: &str, vars: &TemplateVars, inbound: &HeaderMap) -> HeaderMap {
  AgentKind::from_agent_id(agent_id)
    .map(|kind| kind.build_outbound(vars, inbound))
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn known_agents_round_trip() {
    for (slug, expected) in [
      ("opencode", AgentKind::Opencode),
      ("codex-cli", AgentKind::CodexCli),
      ("claude-code", AgentKind::ClaudeCode),
      ("cline", AgentKind::Cline),
      ("copilot-cli", AgentKind::CopilotCli),
    ] {
      assert_eq!(AgentKind::from_agent_id(slug), Some(expected));
    }
  }

  #[test]
  fn aliases_normalize() {
    assert_eq!(AgentKind::from_agent_id("codex"), Some(AgentKind::CodexCli));
    assert_eq!(AgentKind::from_agent_id("codex_exec"), Some(AgentKind::CodexCli));
    assert_eq!(AgentKind::from_agent_id("claude-cli"), Some(AgentKind::ClaudeCode));
    assert_eq!(AgentKind::from_agent_id("copilot"), Some(AgentKind::CopilotCli));
  }

  #[test]
  fn unknown_agent_yields_empty_header_map() {
    let out = build_agent_headers("anything", &TemplateVars::default(), &HeaderMap::new());
    assert_eq!(out.len(), 0);
  }

  #[test]
  fn build_outbound_for_known_agent_returns_nonempty_map() {
    let vars = TemplateVars::default();
    let inbound = HeaderMap::new();
    for kind in [
      AgentKind::Opencode,
      AgentKind::CodexCli,
      AgentKind::ClaudeCode,
      AgentKind::Cline,
      AgentKind::CopilotCli,
    ] {
      let out = kind.build_outbound(&vars, &inbound);
      assert!(!out.is_empty(), "{kind:?} should build a non-empty HeaderMap");
    }
  }
}
