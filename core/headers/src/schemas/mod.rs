//! Module index for persona and overlay header schemas.
//!
//! Each submodule defines exactly one schema struct that implements
//! [`crate::HeaderSchema`].

pub mod personas {
  pub mod claude_code;
  pub mod cline;
  pub mod codex_cli;
  pub mod copilot_cli;
  pub mod opencode;
}

pub mod overlays {
  pub mod codex;
  pub mod copilot;
}

pub use overlays::codex::CodexOverlay;
pub use overlays::copilot::CopilotOverlay;
pub use personas::claude_code::ClaudeCodeHeaders;
pub use personas::cline::ClineHeaders;
pub use personas::codex_cli::CodexCliHeaders;
pub use personas::copilot_cli::CopilotCliHeaders;
pub use personas::opencode::OpencodeHeaders;
