use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

use crate::content::{ContentBlock, ContentBlockDelta};
use crate::response::MessagesResponse;
use crate::usage::MessagesUsage;

/// Streaming events emitted by the Messages API.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessagesEvent {
  MessageStart {
    message: MessagesResponse,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ContentBlockStart {
    index: u32,
    content_block: ContentBlock,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ContentBlockDelta {
    index: u32,
    delta: ContentBlockDelta,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ContentBlockStop {
    index: u32,
    #[serde(default, flatten)]
    extras: Extras,
  },
  MessageDelta {
    #[serde(default)]
    delta: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    usage: Option<MessagesUsage>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  MessageStop {
    #[serde(default, flatten)]
    extras: Extras,
  },
  Ping {
    #[serde(default, flatten)]
    extras: Extras,
  },
  Error {
    #[serde(default)]
    error: Value,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(untagged)]
  Other(Value),
}

impl MessagesEvent {
  pub fn kind(&self) -> &str {
    match self {
      Self::MessageStart { .. } => "message_start",
      Self::ContentBlockStart { .. } => "content_block_start",
      Self::ContentBlockDelta { .. } => "content_block_delta",
      Self::ContentBlockStop { .. } => "content_block_stop",
      Self::MessageDelta { .. } => "message_delta",
      Self::MessageStop { .. } => "message_stop",
      Self::Ping { .. } => "ping",
      Self::Error { .. } => "error",
      Self::Other(v) => v.get("type").and_then(Value::as_str).unwrap_or(""),
    }
  }
}
