use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

use crate::item::OutputItem;
use crate::parameters::{ResponsesExtraParameters, ResponsesRequestParameters};
use crate::request::ResponsesToolDef;
use crate::usage::ResponsesUsage;

/// Non-streaming response body returned by the Responses API.
///
/// In addition to result fields (`output`, `usage`, `error`, …) the
/// Responses API echoes the effective request configuration back to the
/// caller. The same structured payloads (`tools`, `tool_choice`,
/// `reasoning`, `text`, `metadata`) and scalar parameters
/// ([`ResponsesRequestParameters`]) appear here as on the request, so
/// downstream code can recover the exact knobs that produced this
/// response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesResponse {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub object: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub created_at: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub completed_at: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub status: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub model: Option<String>,
  /// On the response, instructions can echo back as a string or as an
  /// array of input items, so it is kept as a free-form Value.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub instructions: Option<Value>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub output: Vec<OutputItem>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_text: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub tools: Vec<ResponsesToolDef>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub metadata: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub usage: Option<ResponsesUsage>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub error: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub incomplete_details: Option<Value>,
  #[serde(default, flatten)]
  pub params: ResponsesRequestParameters,
  #[serde(default, flatten)]
  pub extra_params: ResponsesExtraParameters,
  #[serde(default, flatten)]
  pub extras: Extras,
}
