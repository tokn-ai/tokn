use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::{Extras, Role};

use crate::content::ChatContent;

/// One entry in the `messages` array of a Chat Completions request, or
/// the `message` field of a non-streaming response choice.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatMessage {
  pub role: Role,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content: Option<ChatContent>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub name: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub tool_call_id: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub tool_calls: Vec<ChatToolCall>,
  /// Provider extension carrying chain-of-thought style reasoning.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub reasoning_content: Option<String>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// A `tool_calls[]` entry in Chat Completions.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatToolCall {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  /// Position within the streamed tool call list.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub index: Option<u32>,
  #[serde(rename = "type", default = "default_function_type")]
  pub kind: String,
  pub function: ChatToolFunction,
  #[serde(default, flatten)]
  pub extras: Extras,
}

fn default_function_type() -> String {
  "function".into()
}

/// `function` payload inside a tool call.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatToolFunction {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub name: Option<String>,
  /// Arguments are conventionally a JSON-encoded string in Chat
  /// Completions, but some providers emit a structured value during
  /// streaming. Keep it flexible.
  #[serde(default, skip_serializing_if = "Value::is_null")]
  pub arguments: Value,
  #[serde(default, flatten)]
  pub extras: Extras,
}
