use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// Token accounting fields returned by the OpenAI Responses API.
///
/// Provider extensions and rare token-class breakdowns are kept in `extras`
/// or in the free-form `*_tokens_details` Values.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResponsesUsage {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub input_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub total_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub input_tokens_details: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_tokens_details: Option<Value>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
