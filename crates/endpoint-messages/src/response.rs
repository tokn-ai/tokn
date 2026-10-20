use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::{Extras, Role};

use crate::content::ContentBlock;
use crate::usage::MessagesUsage;

/// Non-streaming response body returned by the Messages API.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagesResponse {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
  pub kind: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub role: Option<Role>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub model: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub content: Vec<ContentBlock>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stop_reason: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stop_sequence: Option<Value>,
  /// Provider-specific structured stop information (e.g. Anthropic's
  /// `stop_details` block); kept as a Value because the schema varies.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub stop_details: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub usage: Option<MessagesUsage>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
