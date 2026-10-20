use alloc::{boxed::Box, string::String, vec::Vec};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use tokn_endpoint_core::{Extras, FinishReason, Role};

use crate::usage::ChatUsage;

use crate::message::ChatToolCall;

/// One chunk emitted by a streaming chat completion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatChunk {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub object: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub created: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub model: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub choices: Vec<ChunkChoice>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub usage: Option<ChatUsage>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkChoice {
  #[serde(default)]
  pub index: u32,
  pub delta: ChatDelta,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub finish_reason: Option<FinishReason>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatDelta {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub role: Option<Role>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reasoning_content: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub tool_calls: Vec<ChatToolCall>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// Top-level wrapper for events seen on the SSE stream of a chat
/// completion. Either a JSON [`ChatChunk`] payload or the literal
/// `[DONE]` sentinel.
#[derive(Clone, Debug)]
pub enum ChatEvent {
  Chunk(Box<ChatChunk>),
  Done,
}

impl Serialize for ChatEvent {
  fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
    match self {
      Self::Chunk(c) => c.serialize(serializer),
      Self::Done => serializer.serialize_str("[DONE]"),
    }
  }
}

impl<'de> Deserialize<'de> for ChatEvent {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.as_str() == Some("[DONE]") {
      return Ok(Self::Done);
    }
    serde_json::from_value(value)
      .map(Box::new)
      .map(Self::Chunk)
      .map_err(serde::de::Error::custom)
  }
}
