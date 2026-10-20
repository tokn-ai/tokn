use alloc::string::String;
use serde::{Deserialize, Serialize};

/// Role of a message author across endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
  System,
  User,
  Assistant,
  Tool,
  Developer,
  #[serde(untagged)]
  Other(String),
}

impl Role {
  pub fn as_str(&self) -> &str {
    match self {
      Self::System => "system",
      Self::User => "user",
      Self::Assistant => "assistant",
      Self::Tool => "tool",
      Self::Developer => "developer",
      Self::Other(s) => s.as_str(),
    }
  }
}
