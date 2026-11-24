use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// Token accounting fields returned by the OpenAI Chat Completions API.
///
/// Mirrors OpenAI's wire shape (`prompt_tokens`/`completion_tokens`)
/// rather than the normalized Responses naming. Provider extensions are
/// captured in `extras`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatUsage {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub prompt_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub completion_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub total_tokens: Option<u64>,
  /// Free-form details object; OpenAI emits `cached_tokens`, `audio_tokens`,
  /// etc. Schema varies enough that we keep it as a Value.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub prompt_tokens_details: Option<Value>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub completion_tokens_details: Option<Value>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
