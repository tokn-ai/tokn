use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub use tokn_endpoint_core::ToolChoice as ResponsesToolChoice;
use tokn_endpoint_core::{
  drain_into_extras, peel_lenient, take_optional, take_optional_default, take_required, Extras,
};

use crate::item::InputItem;
use crate::parameters::{ResponsesExtraParameters, ResponsesRequestParameters};

/// `input` field accepts either a plain string or a list of items.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponsesInput {
  Text(String),
  Items(Vec<InputItem>),
}

impl Default for ResponsesInput {
  fn default() -> Self {
    Self::Items(Vec::new())
  }
}

/// Request body for `POST /v1/responses`.
///
/// Behavior knobs (temperature, top_p, max_*_tokens, tool_choice,
/// reasoning, text, etc.) live on the embedded
/// [`ResponsesRequestParameters`]; this struct keeps content,
/// streaming controls and structured payloads at the top level.
///
/// Deserialization is **lenient on parameter fields**: see
/// [`ChatRequest`](crate::ResponsesRequest) for details; the same
/// semantics apply.
#[derive(Clone, Debug, Serialize)]
pub struct ResponsesRequest {
  pub model: String,
  pub input: ResponsesInput,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub instructions: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stream: Option<bool>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub tools: Vec<ResponsesToolDef>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stop: Option<Value>,
  /// Optional list of additional fields to include in the response
  /// (e.g. `"reasoning.encrypted_content"`, `"file_search_call.results"`).
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub include: Option<Vec<String>>,
  /// Free-form per-request metadata echoed back by some providers.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub client_metadata: Option<Value>,
  #[serde(flatten)]
  pub params: ResponsesRequestParameters,
  #[serde(flatten)]
  pub extra_params: ResponsesExtraParameters,
  #[serde(flatten)]
  pub extras: Extras,
}

impl<'de> Deserialize<'de> for ResponsesRequest {
  fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
    let mut root = Map::<String, Value>::deserialize(d)?;

    let model: String = take_required(&mut root, "model")?;
    let input: ResponsesInput = take_required(&mut root, "input")?;
    let instructions: Option<String> = take_optional(&mut root, "instructions")?;
    let stream: Option<bool> = take_optional(&mut root, "stream")?;
    let tools: Vec<ResponsesToolDef> = take_optional_default(&mut root, "tools")?;
    let stop: Option<Value> = root.remove("stop");
    let include: Option<Vec<String>> = take_optional(&mut root, "include")?;
    let client_metadata: Option<Value> = root.remove("client_metadata");

    let mut extras: Extras = Map::new();
    let params: ResponsesRequestParameters = peel_lenient(&mut root, &mut extras);
    let extra_params: ResponsesExtraParameters = peel_lenient(&mut root, &mut extras);
    drain_into_extras(&mut root, &mut extras);

    Ok(ResponsesRequest {
      model,
      input,
      instructions,
      stream,
      tools,
      stop,
      include,
      client_metadata,
      params,
      extra_params,
      extras,
    })
  }
}

/// `tools[]` entry. The Responses API permits multiple tool kinds
/// (function, web_search, file_search, custom, etc.). For function tools
/// the standard fields are typed directly; non-function tools leave
/// those fields as `None` and use `extras` for kind-specific data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesToolDef {
  #[serde(rename = "type")]
  pub kind: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub name: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub description: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub parameters: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub strict: Option<bool>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
