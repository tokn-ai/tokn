use alloc::string::{String, ToString};
use core::str::FromStr;
use serde::{Deserialize, Serialize};

/// Identifier for the LLM endpoint a payload belongs to.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
  ChatCompletions,
  Responses,
  Messages,
}

impl Endpoint {
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::ChatCompletions => "chat_completions",
      Self::Responses => "responses",
      Self::Messages => "messages",
    }
  }
}

impl FromStr for Endpoint {
  type Err = UnknownEndpoint;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "chat_completions" | "chat" | "chat-completions" => Ok(Self::ChatCompletions),
      "responses" => Ok(Self::Responses),
      "messages" => Ok(Self::Messages),
      other => Err(UnknownEndpoint(other.to_string())),
    }
  }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown endpoint: {0}")]
pub struct UnknownEndpoint(pub String);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_trip_strings() {
    for ep in [Endpoint::ChatCompletions, Endpoint::Responses, Endpoint::Messages] {
      assert_eq!(Endpoint::from_str(ep.as_str()).unwrap(), ep);
    }
  }
}
