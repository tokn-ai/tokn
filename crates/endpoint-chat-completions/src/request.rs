use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub use tokn_endpoint_core::ToolChoice as ChatToolChoice;
use tokn_endpoint_core::{
  drain_into_extras, peel_lenient, take_optional, take_optional_default, take_required, Extras,
};

use crate::message::ChatMessage;
use crate::parameters::{ChatExtraParameters, ChatRequestParameters};

/// Request body for `POST /v1/chat/completions`.
///
/// Behavior knobs (temperature, top_p, max_*_tokens, tool_choice,
/// reasoning, thinking, etc.) live on the embedded
/// [`ChatRequestParameters`]; structured payloads (messages, tools,
/// stop) and streaming controls stay at the top level.
///
/// Deserialization is **lenient on parameter fields**: if a key
/// declared in [`ChatRequestParameters`] or [`ChatExtraParameters`]
/// has a value whose JSON shape doesn't match the typed schema (e.g.
/// `"temperature": "hot"`), the raw value is captured in
/// [`extras`](Self::extras) instead of failing the whole parse.
/// Required and structured top-level fields (`model`, `messages`,
/// `tools`, `stop`, `stream`) remain strict.
#[derive(Clone, Debug, Serialize)]
pub struct ChatRequest {
  pub model: String,
  pub messages: Vec<ChatMessage>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stream: Option<bool>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub tools: Vec<ChatToolDef>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stop: Option<Value>,
  #[serde(flatten)]
  pub params: ChatRequestParameters,
  #[serde(flatten)]
  pub extra_params: ChatExtraParameters,
  #[serde(flatten)]
  pub extras: Extras,
}

impl<'de> Deserialize<'de> for ChatRequest {
  fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
    let mut root = Map::<String, Value>::deserialize(d)?;

    let model: String = take_required(&mut root, "model")?;
    let messages: Vec<ChatMessage> = take_required(&mut root, "messages")?;
    let stream: Option<bool> = take_optional(&mut root, "stream")?;
    let tools: Vec<ChatToolDef> = take_optional_default(&mut root, "tools")?;
    let stop: Option<Value> = root.remove("stop");

    let mut extras: Extras = Map::new();
    let params: ChatRequestParameters = peel_lenient(&mut root, &mut extras);
    let extra_params: ChatExtraParameters = peel_lenient(&mut root, &mut extras);
    drain_into_extras(&mut root, &mut extras);

    Ok(ChatRequest {
      model,
      messages,
      stream,
      tools,
      stop,
      params,
      extra_params,
      extras,
    })
  }
}

/// A `tools[]` entry. Chat Completions only defines function tools.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatToolDef {
  #[serde(rename = "type", default = "default_function_type")]
  pub kind: String,
  pub function: Value,
  #[serde(default, flatten)]
  pub extras: Extras,
}

fn default_function_type() -> String {
  "function".into()
}
