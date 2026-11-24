use serde::{Deserialize, Serialize};
use serde_json::Value;

use tokn_endpoint_core::Extras;

/// Token accounting fields returned by the Anthropic Messages API.
///
/// Includes Anthropic's prompt-caching counters; provider-specific
/// extensions land in `extras`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MessagesUsage {
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub input_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub output_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub cache_creation_input_tokens: Option<u64>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub cache_read_input_tokens: Option<u64>,
  /// Anthropic emits a nested `cache_creation` object alongside the scalar
  /// counters; schema is small but variable, so kept as a Value.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub cache_creation: Option<Value>,
  #[serde(default, flatten)]
  pub extras: Extras,
}
