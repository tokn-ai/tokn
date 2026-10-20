use alloc::string::String;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// One block in a Messages API content array.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
  Text {
    text: String,
    /// Anthropic prompt-caching directive (e.g. `{"type":"ephemeral"}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  Thinking {
    thinking: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  RedactedThinking {
    #[serde(default, flatten)]
    fields: Extras,
  },
  ToolUse {
    id: String,
    name: String,
    #[serde(default)]
    input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ToolResult {
    tool_use_id: String,
    #[serde(default)]
    content: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    is_error: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  Image {
    #[serde(default)]
    source: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  Document {
    #[serde(default)]
    source: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_control: Option<Value>,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(other)]
  Other,
}

/// Delta variants emitted inside `content_block_delta` events.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlockDelta {
  TextDelta {
    text: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  ThinkingDelta {
    thinking: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  SignatureDelta {
    signature: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  InputJsonDelta {
    partial_json: String,
    #[serde(default, flatten)]
    extras: Extras,
  },
  #[serde(other)]
  Other,
}
