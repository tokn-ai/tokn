use alloc::string::String;
use serde::{Deserialize, Serialize};

/// Normalised stop/finish reason categories.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
  Stop,
  Length,
  ToolCalls,
  ContentFilter,
  #[serde(untagged)]
  Other(String),
}
