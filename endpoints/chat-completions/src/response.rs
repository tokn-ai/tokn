use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use tokn_endpoint_core::{Extras, FinishReason};

use crate::message::ChatMessage;
use crate::usage::ChatUsage;

/// Non-streaming response body returned by `chat.completions`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatResponse {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub id: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub object: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub created: Option<i64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub model: Option<String>,
  #[serde(default, skip_serializing_if = "Vec::is_empty")]
  pub choices: Vec<ChatChoice>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub usage: Option<ChatUsage>,
  #[serde(default, flatten)]
  pub extras: Extras,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChatChoice {
  #[serde(default)]
  pub index: u32,
  pub message: ChatMessage,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub finish_reason: Option<FinishReason>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
