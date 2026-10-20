use alloc::{borrow::ToOwned, string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::{Extras, Role};

use crate::content::{InputContentPart, OutputContentPart, ReasoningPart};

/// Discriminated union of items the Responses API accepts in `input[]`.
///
/// The Responses API distinguishes items by a `type` field, except for
/// plain user/assistant/system messages which carry only a `role`. This
/// enum dispatches on `type` first and falls back to a typed
/// [`InputMessage`] when only `role` is present.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum InputItem {
  Message(InputMessage),
  Reasoning(TaggedReasoning),
  FunctionCall(TaggedFunctionCall),
  FunctionCallOutput(TaggedFunctionCallOutput),
  Other(Value),
}

impl<'de> Deserialize<'de> for InputItem {
  fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let mut value = Value::deserialize(deserializer)?;
    let kind = value.get("type").and_then(Value::as_str).map(str::to_owned);
    match kind.as_deref() {
      Some("reasoning") => serde_json::from_value(value)
        .map(InputItem::Reasoning)
        .map_err(serde::de::Error::custom),
      Some("function_call") => serde_json::from_value(value)
        .map(InputItem::FunctionCall)
        .map_err(serde::de::Error::custom),
      Some("function_call_output") => serde_json::from_value(value)
        .map(InputItem::FunctionCallOutput)
        .map_err(serde::de::Error::custom),
      Some("message") | None => {
        // `InputMessage` doesn't model a `type` field, so strip the
        // discriminator before forwarding to avoid it landing in extras.
        if let Some(obj) = value.as_object_mut() {
          obj.remove("type");
        }
        serde_json::from_value(value)
          .map(InputItem::Message)
          .map_err(serde::de::Error::custom)
      }
      Some(_) => Ok(InputItem::Other(value)),
    }
  }
}

/// `{type:"message", role, content}` input item, also used when the
/// `type` field is omitted and only `role` is present.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputMessage {
  pub role: Role,
  #[serde(default)]
  pub content: InputMessageContent,
  #[serde(default, flatten)]
  pub extras: Extras,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputMessageContent {
  Text(String),
  Parts(Vec<InputContentPart>),
}

impl Default for InputMessageContent {
  fn default() -> Self {
    Self::Parts(Vec::new())
  }
}

/// Wrapper that re-asserts the `type:"reasoning"` tag during
/// serialization so round-trips preserve the discriminator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedReasoning {
  #[serde(rename = "type", default = "reasoning_type")]
  pub kind: String,
  #[serde(flatten)]
  pub item: ReasoningItem,
}

fn reasoning_type() -> String {
  "reasoning".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedFunctionCall {
  #[serde(rename = "type", default = "function_call_type")]
  pub kind: String,
  #[serde(flatten)]
  pub item: FunctionCallItem,
}

fn function_call_type() -> String {
  "function_call".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedFunctionCallOutput {
  #[serde(rename = "type", default = "function_call_output_type")]
  pub kind: String,
  #[serde(flatten)]
  pub item: FunctionCallOutputItem,
}

fn function_call_output_type() -> String {
  "function_call_output".into()
}

/// `{type:"reasoning", content?, summary?}` item.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReasoningItem {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub content: Vec<ReasoningPart>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub summary: Vec<ReasoningPart>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub encrypted_content: Option<String>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// `{type:"function_call", name, arguments, call_id}` item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionCallItem {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub call_id: Option<String>,
  pub name: String,
  /// Arguments are typically a JSON-encoded string; structured values
  /// are tolerated.
  #[serde(default)]
  pub arguments: Value,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub status: Option<String>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// `{type:"function_call_output", call_id, output}` item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionCallOutputItem {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub call_id: Option<String>,
  #[serde(default)]
  pub output: Value,
  #[serde(default, flatten)]
  pub extras: Extras,
}

/// Discriminated union of items appearing in a Responses `output[]`.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OutputItem {
  Message(TaggedOutputMessage),
  Reasoning(TaggedReasoning),
  FunctionCall(TaggedFunctionCall),
  Other(Value),
}

impl<'de> Deserialize<'de> for OutputItem {
  fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    let value = Value::deserialize(deserializer)?;
    let kind = value.get("type").and_then(Value::as_str);
    match kind {
      Some("message") => serde_json::from_value(value)
        .map(OutputItem::Message)
        .map_err(serde::de::Error::custom),
      Some("reasoning") => serde_json::from_value(value)
        .map(OutputItem::Reasoning)
        .map_err(serde::de::Error::custom),
      Some("function_call") => serde_json::from_value(value)
        .map(OutputItem::FunctionCall)
        .map_err(serde::de::Error::custom),
      _ => Ok(OutputItem::Other(value)),
    }
  }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaggedOutputMessage {
  #[serde(rename = "type", default = "message_type")]
  pub kind: String,
  #[serde(flatten)]
  pub message: OutputMessage,
}

fn message_type() -> String {
  "message".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputMessage {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub status: Option<String>,
  pub role: Role,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub content: Vec<OutputContentPart>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
